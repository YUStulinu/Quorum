using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Quorum.Cli.Lab;

/// <summary>
/// A cluster of Quorum node processes on this machine. Nodes are real OS processes (this same executable run
/// with <c>node</c>), so killing one is a real crash: whatever it had not fsynced is gone. Faults other than
/// crashes are injected through each node's admin API.
/// </summary>
public sealed class LocalCluster : IAsyncDisposable
{
    public sealed class NodeProcess
    {
        public required ulong Id { get; init; }
        public required int GrpcPort { get; init; }
        public required int HttpPort { get; init; }
        public required string DataDir { get; init; }
        public required bool Join { get; init; }
        public Process? Process { get; set; }
        public bool Running => Process is { HasExited: false };
        public bool Removed { get; set; }
        public string GrpcAddress => $"http://127.0.0.1:{GrpcPort}";
        public string HttpAddress => $"http://127.0.0.1:{HttpPort}";
        public int Restarts { get; set; }
        public DateTime? StartedAt { get; set; }
    }

    public sealed record ClusterSettings
    {
        public string DataDir { get; init; } = "data";
        public int GrpcBase { get; init; } = 7100;
        public int HttpBase { get; init; } = 7200;
        public int TickMs { get; init; } = 20;
        public int ElectionTicks { get; init; } = 15;
        public int HeartbeatTicks { get; init; } = 3;
        public int SnapshotEvery { get; init; } = 20_000;
        public bool Quiet { get; init; }
    }

    private readonly ClusterSettings _s;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(3) };
    private readonly Dictionary<ulong, NodeProcess> _nodes = [];
    private readonly object _lock = new();
    private readonly JobObject? _job = OperatingSystem.IsWindows() ? new JobObject() : null;
    private List<(ulong Id, string Address)> _founders = [];

    public LocalCluster(ClusterSettings settings) => _s = settings;

    public ClusterSettings Settings => _s;

    public IReadOnlyList<NodeProcess> Nodes
    {
        get
        {
            lock (_lock) return _nodes.Values.OrderBy(n => n.Id).ToList();
        }
    }

    public NodeProcess this[ulong id]
    {
        get
        {
            lock (_lock) return _nodes[id];
        }
    }

    /// <summary>gRPC endpoints of every node that is a member (running or not).</summary>
    public string[] Endpoints => Nodes.Where(n => !n.Removed).Select(n => n.GrpcAddress).ToArray();

    public static void Wipe(string dir)
    {
        if (!Directory.Exists(dir)) return;
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
                return;
            }
            catch (IOException) when (attempt < 20)
            {
                Thread.Sleep(250); // a process we just killed may still hold a file
            }
            catch (UnauthorizedAccessException) when (attempt < 20)
            {
                Thread.Sleep(250);
            }
        }
    }

    private static void EnsurePortFree(int port)
    {
        try
        {
            var l = new TcpListener(IPAddress.Loopback, port);
            l.Start();
            l.Stop();
        }
        catch (SocketException)
        {
            throw new CliException($"port {port} is already in use (is another Quorum cluster running? stop it first)");
        }
    }

    /// <summary>Starts a new cluster of <paramref name="count"/> founding members, or restarts an existing one.</summary>
    public async Task StartAsync(int count, CancellationToken ct)
    {
        Directory.CreateDirectory(_s.DataDir);
        _founders = Enumerable.Range(1, count).Select(i => ((ulong)i, $"http://127.0.0.1:{_s.GrpcBase + i}")).ToList();
        for (int i = 1; i <= count; i++)
        {
            EnsurePortFree(_s.GrpcBase + i);
            EnsurePortFree(_s.HttpBase + i);
        }
        foreach (var (id, _) in _founders) AddProcess(id, join: false);
        foreach (var n in Nodes) Start(n);
        await WaitForLeaderAsync(TimeSpan.FromSeconds(20), ct);
    }

    private NodeProcess AddProcess(ulong id, bool join)
    {
        var n = new NodeProcess
        {
            Id = id,
            GrpcPort = _s.GrpcBase + (int)id,
            HttpPort = _s.HttpBase + (int)id,
            DataDir = Path.Combine(_s.DataDir, $"node-{id}"),
            Join = join,
        };
        lock (_lock) _nodes[id] = n;
        return n;
    }

    private static (string File, List<string> Prefix) SelfCommand()
    {
        string exe = Environment.ProcessPath ?? throw new InvalidOperationException("cannot find our own executable");
        if (Path.GetFileNameWithoutExtension(exe).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            return (exe, [typeof(LocalCluster).Assembly.Location]);
        return (exe, []);
    }

    public void Start(NodeProcess n)
    {
        if (n.Running || n.Removed) return;
        var (file, prefix) = SelfCommand();
        var psi = new ProcessStartInfo(file)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        foreach (var p in prefix) psi.ArgumentList.Add(p);
        psi.ArgumentList.Add("node");
        void Arg(string name, object value)
        {
            psi.ArgumentList.Add("--" + name);
            psi.ArgumentList.Add(value.ToString()!);
        }
        Arg("id", n.Id);
        Arg("data", n.DataDir);
        Arg("grpc", n.GrpcPort);
        Arg("http", n.HttpPort);
        Arg("tick", _s.TickMs);
        Arg("election", _s.ElectionTicks);
        Arg("heartbeat", _s.HeartbeatTicks);
        Arg("snapshot-every", _s.SnapshotEvery);
        Arg("snapshot-trail", Math.Max(100, _s.SnapshotEvery / 10));
        if (n.Join) psi.ArgumentList.Add("--join");
        else Arg("peers", string.Join(",", _founders.Select(f => $"{f.Id}={f.Address}")));

        Directory.CreateDirectory(_s.DataDir);
        string logPath = Path.Combine(_s.DataDir, $"node-{n.Id}.log");
        var log = new StreamWriter(new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true };
        var proc = new Process { StartInfo = psi, EnableRaisingEvents = true };
        void Write(string? line)
        {
            if (line is null) return;
            lock (log) log.WriteLine(line);
        }
        proc.OutputDataReceived += (_, e) => Write(e.Data);
        proc.ErrorDataReceived += (_, e) => Write(e.Data);
        proc.Exited += (_, _) =>
        {
            lock (log)
            {
                log.WriteLine($"--- process exited ({proc.ExitCode}) at {DateTime.Now:HH:mm:ss.fff} ---");
                log.Dispose();
            }
        };
        Write($"--- starting node {n.Id} at {DateTime.Now:HH:mm:ss.fff} ---");
        proc.Start();
        _job?.Add(proc);
        proc.BeginOutputReadLine();
        proc.BeginErrorReadLine();
        n.Process = proc;
        n.StartedAt = DateTime.UtcNow;
    }

    /// <summary>Kills a node's process outright (no clean shutdown: unsynced data is lost, like a power cut).</summary>
    public void Kill(ulong id)
    {
        var n = this[id];
        if (!n.Running) return;
        try
        {
            n.Process!.Kill(entireProcessTree: true);
            n.Process.WaitForExit(5000);
        }
        catch (InvalidOperationException) { }
    }

    public void Restart(ulong id)
    {
        var n = this[id];
        Kill(id);
        n.Restarts++;
        Start(n);
    }

    // ------------------------------------------------------------------
    // Admin API
    // ------------------------------------------------------------------

    public async Task<JsonElement?> StatusAsync(ulong id, CancellationToken ct = default)
    {
        var n = this[id];
        if (!n.Running) return null;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(1));
            return await _http.GetFromJsonAsync<JsonElement>($"{n.HttpAddress}/status", cts.Token);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
        {
            return null;
        }
    }

    public async Task<JsonElement?> GetJsonAsync(ulong id, string path, CancellationToken ct = default)
    {
        var n = this[id];
        if (!n.Running) return null;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(5));
            return await _http.GetFromJsonAsync<JsonElement>($"{n.HttpAddress}{path}", cts.Token);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or JsonException)
        {
            return null;
        }
    }

    public async Task<bool> PostAsync(ulong id, string path, object body, CancellationToken ct = default)
    {
        var n = this[id];
        if (!n.Running) return false;
        try
        {
            using var r = await _http.PostAsJsonAsync($"{n.HttpAddress}{path}", body, ct);
            return r.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return false;
        }
    }

    public async Task<(bool Ok, string Body)> PostWithBodyAsync(ulong id, string path, object body, CancellationToken ct = default)
    {
        var n = this[id];
        if (!n.Running) return (false, "node is not running");
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(15));
            using var r = await _http.PostAsJsonAsync($"{n.HttpAddress}{path}", body, cts.Token);
            return (r.IsSuccessStatusCode, await r.Content.ReadAsStringAsync(cts.Token));
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return (false, ex.Message);
        }
    }

    /// <summary>Splits the cluster: nodes can only talk to nodes in their own group.</summary>
    public async Task PartitionAsync(IReadOnlyList<IReadOnlyCollection<ulong>> groups, CancellationToken ct = default)
    {
        var all = Nodes.Select(n => n.Id).ToList();
        var tasks = new List<Task>();
        foreach (var id in all)
        {
            var mine = groups.FirstOrDefault(g => g.Contains(id)) ?? [id];
            var blocked = all.Where(o => !mine.Contains(o)).ToArray();
            tasks.Add(PostAsync(id, "/admin/faults", new { blocked, dropRate = 0.0, delayMs = 0, jitterMs = 0 }, ct));
        }
        await Task.WhenAll(tasks);
    }

    /// <summary>Gives each node its own set of blocked peers (for non-transitive "bridge" partitions).</summary>
    public async Task BlockAsync(IReadOnlyDictionary<ulong, ulong[]> blockedByNode, CancellationToken ct = default)
    {
        await Task.WhenAll(blockedByNode.Select(kv =>
            PostAsync(kv.Key, "/admin/faults", new { blocked = kv.Value, dropRate = 0.0, delayMs = 0, jitterMs = 0 }, ct)));
    }

    public Task HealAsync(CancellationToken ct = default) =>
        Task.WhenAll(Nodes.Where(n => n.Running).Select(n => PostAsync(n.Id, "/admin/heal", new { }, ct)));

    public Task<bool> SetClockRateAsync(ulong id, double rate, CancellationToken ct = default) =>
        PostAsync(id, "/admin/clock", new { rate }, ct);

    public Task<bool> PauseAsync(ulong id, int ms, CancellationToken ct = default) =>
        PostAsync(id, "/admin/pause", new { ms }, ct);

    public Task<bool> SetNetworkAsync(ulong id, double dropRate, int delayMs, int jitterMs, CancellationToken ct = default) =>
        PostAsync(id, "/admin/faults", new { blocked = Array.Empty<ulong>(), dropRate, delayMs, jitterMs }, ct);

    /// <summary>The node that leads the highest term, as reported by the nodes themselves.</summary>
    public async Task<ulong?> LeaderAsync(CancellationToken ct = default)
    {
        var statuses = await Task.WhenAll(Nodes.Where(n => n.Running).Select(async n => (n.Id, Status: await StatusAsync(n.Id, ct))));
        ulong? leader = null;
        ulong bestTerm = 0;
        foreach (var (id, s) in statuses)
        {
            if (s is not { } st || !st.TryGetProperty("role", out var role) || role.GetString() != "Leader") continue;
            ulong term = st.GetProperty("term").GetUInt64();
            if (term >= bestTerm)
            {
                bestTerm = term;
                leader = id;
            }
        }
        return leader;
    }

    public async Task<ulong> WaitForLeaderAsync(TimeSpan timeout, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            ct.ThrowIfCancellationRequested();
            if (await LeaderAsync(ct) is { } l) return l;
            foreach (var n in Nodes.Where(n => n.Process is { HasExited: true } && !n.Removed && n.Restarts == 0 && sw.Elapsed > TimeSpan.FromSeconds(3)))
                throw new CliException($"node {n.Id} exited with code {n.Process!.ExitCode}; see {Path.Combine(_s.DataDir, $"node-{n.Id}.log")}");
            await Task.Delay(100, ct);
        }
        throw new CliException($"no leader was elected within {timeout.TotalSeconds:0} s (see the node logs in {_s.DataDir})");
    }

    // ------------------------------------------------------------------
    // Membership
    // ------------------------------------------------------------------

    /// <summary>
    /// Adds a brand-new node: starts it empty, adds it as a learner, waits until it has caught up with the
    /// leader's log, then promotes it to voter (the safe way to grow a cluster, dissertation 4.2.1).
    /// </summary>
    public async Task<(bool Ok, string Message)> AddNodeAsync(CancellationToken ct = default)
    {
        ulong id;
        lock (_lock) id = _nodes.Count == 0 ? 1 : _nodes.Keys.Max() + 1;
        if (id > 9) return (false, "at most 9 nodes are supported by the local port layout");
        EnsurePortFree(_s.GrpcBase + (int)id);
        EnsurePortFree(_s.HttpBase + (int)id);
        var n = AddProcess(id, join: true);
        Start(n);
        await Task.Delay(300, ct);

        var leader = await LeaderAsync(ct);
        if (leader is null) return (false, "no leader to add the node through");
        var (ok, body) = await PostWithBodyAsync(leader.Value, "/admin/members/add", new { id, address = n.GrpcAddress, learner = true }, ct);
        if (!ok) return (false, $"adding learner {id} failed: {body}");

        // Wait for the learner to catch up.
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(30))
        {
            leader = await LeaderAsync(ct);
            if (leader is not null && await StatusAsync(leader.Value, ct) is { } st &&
                st.GetProperty("progress").TryGetProperty(id.ToString(), out var pr) &&
                pr.GetProperty("match").GetUInt64() + 50 >= st.GetProperty("lastIndex").GetUInt64())
            {
                break;
            }
            await Task.Delay(100, ct);
        }
        if (leader is null) return (false, "lost the leader while the learner caught up");
        (ok, body) = await PostWithBodyAsync(leader.Value, "/admin/members/add", new { id, address = n.GrpcAddress, learner = false }, ct);
        return ok ? (true, $"node {id} joined as a voter") : (false, $"promoting {id} failed: {body}");
    }

    public async Task<(bool Ok, string Message)> RemoveNodeAsync(ulong id, CancellationToken ct = default)
    {
        var leader = await LeaderAsync(ct);
        if (leader is null) return (false, "no leader");
        if (leader == id)
        {
            // Hand leadership over first, so the cluster does not wait out an election.
            var other = Nodes.FirstOrDefault(n => n.Id != id && n.Running && !n.Removed);
            if (other is not null)
            {
                await PostAsync(id, "/admin/transfer", new { target = other.Id }, ct);
                await Task.Delay(500, ct);
                leader = await LeaderAsync(ct) ?? leader;
            }
        }
        var (ok, body) = await PostWithBodyAsync(leader.Value, "/admin/members/remove", new { id }, ct);
        if (!ok) return (false, $"removing {id} failed: {body}");
        this[id].Removed = true;
        await Task.Delay(300, ct);
        Kill(id);
        return (true, $"node {id} removed");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var n in Nodes) Kill(n.Id);
        _http.Dispose();
        _job?.Dispose();
        await Task.CompletedTask;
    }

    /// <summary>A Windows job object that kills the node processes if this process dies.</summary>
    private sealed class JobObject : IDisposable
    {
        private nint _handle;

        public JobObject()
        {
            _handle = CreateJobObject(0, null);
            if (_handle == 0) return;
            var info = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION
            {
                BasicLimitInformation = new JOBOBJECT_BASIC_LIMIT_INFORMATION { LimitFlags = 0x2000 /* KILL_ON_JOB_CLOSE */ },
            };
            int size = Marshal.SizeOf<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>();
            var ptr = Marshal.AllocHGlobal(size);
            try
            {
                Marshal.StructureToPtr(info, ptr, false);
                SetInformationJobObject(_handle, 9 /* ExtendedLimitInformation */, ptr, (uint)size);
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }
        }

        public void Add(Process p)
        {
            if (_handle != 0) AssignProcessToJobObject(_handle, p.Handle);
        }

        public void Dispose()
        {
            if (_handle != 0) CloseHandle(_handle);
            _handle = 0;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
        {
            public long PerProcessUserTimeLimit;
            public long PerJobUserTimeLimit;
            public uint LimitFlags;
            public nuint MinimumWorkingSetSize;
            public nuint MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public nuint Affinity;
            public uint PriorityClass;
            public uint SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IO_COUNTERS
        {
            public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public nuint ProcessMemoryLimit;
            public nuint JobMemoryLimit;
            public nuint PeakProcessMemoryUsed;
            public nuint PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
        private static extern nint CreateJobObject(nint attributes, string? name);

        [DllImport("kernel32.dll")]
        private static extern bool SetInformationJobObject(nint job, int infoClass, nint info, uint length);

        [DllImport("kernel32.dll")]
        private static extern bool AssignProcessToJobObject(nint job, nint process);

        [DllImport("kernel32.dll")]
        private static extern bool CloseHandle(nint handle);
    }
}
