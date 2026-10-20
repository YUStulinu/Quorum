using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using Quorum.Client;
using Quorum.Core;
using Quorum.Node;
using Quorum.Protocol;

namespace Quorum.Integration.Tests;

/// <summary>
/// End-to-end tests: real nodes (gRPC, Tessera on disk) running inside the test process on free ports.
/// </summary>
public sealed class ClusterTests : IAsyncLifetime
{
    private sealed class RunningNode
    {
        public required NodeOptions Options;
        public required CancellationTokenSource Stop;
        public required Task Task;
    }

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quorum-it", Guid.NewGuid().ToString("N"));
    private readonly Dictionary<ulong, RunningNode> _nodes = [];
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(3) };
    private List<Member> _members = [];

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public Task InitializeAsync()
    {
        var ports = Enumerable.Range(1, 3).Select(i => ((ulong)i, FreePort(), FreePort())).ToList();
        _members = ports.Select(p => new Member(p.Item1, $"http://127.0.0.1:{p.Item2}", false)).ToList();
        foreach (var (id, grpc, http) in ports) StartNode(id, grpc, http, join: false);
        return Task.CompletedTask;
    }

    private void StartNode(ulong id, int grpc, int http, bool join)
    {
        var options = new NodeOptions
        {
            Id = id,
            DataDir = Path.Combine(_dir, $"node-{id}"),
            GrpcPort = grpc,
            HttpPort = http,
            InitialMembers = join ? [] : _members,
            Join = join,
            TickMs = 10,
            ElectionTicks = 15,
            HeartbeatTicks = 3,
            SnapshotEvery = 200,
            SnapshotTrail = 20,
        };
        var stop = new CancellationTokenSource();
        _nodes[id] = new RunningNode { Options = options, Stop = stop, Task = Task.Run(() => NodeServer.RunAsync(options, stop.Token)) };
    }

    private async Task StopNode(ulong id)
    {
        var n = _nodes[id];
        n.Stop.Cancel();
        try { await n.Task.WaitAsync(TimeSpan.FromSeconds(10)); }
        catch (OperationCanceledException) { }
    }

    public async Task DisposeAsync()
    {
        foreach (var id in _nodes.Keys.ToList())
        {
            if (!_nodes[id].Stop.IsCancellationRequested) await StopNode(id);
        }
        _http.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private string[] Endpoints => _nodes.Values.Select(n => n.Options.GrpcAddress).ToArray();

    private async Task<JsonElement?> Status(ulong id)
    {
        try { return await _http.GetFromJsonAsync<JsonElement>($"{_nodes[id].Options.HttpAddress}/status"); }
        catch (Exception) { return null; }
    }

    private async Task<ulong> WaitForLeader(IEnumerable<ulong> among, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            foreach (var id in among)
            {
                if (await Status(id) is { } s && s.TryGetProperty("role", out var role) && role.GetString() == "Leader") return id;
            }
            await Task.Delay(50);
        }
        throw new TimeoutException("no leader elected");
    }

    [Fact]
    public async Task WritesSurviveTheLossOfTheLeader()
    {
        ulong leader = await WaitForLeader(_nodes.Keys, TimeSpan.FromSeconds(15));
        await using var client = new QuorumClient(new QuorumClientOptions { Endpoints = Endpoints, OperationTimeout = TimeSpan.FromSeconds(15) });
        for (int i = 0; i < 50; i++) Assert.Equal(KvStatusCode.Ok, (await client.PutAsync($"k{i}", $"v{i}")).Status);
        Assert.Equal("v49", (await client.GetAsync("k49")).ValueString);

        await StopNode(leader);
        var survivors = _nodes.Keys.Where(k => k != leader).ToList();
        ulong newLeader = await WaitForLeader(survivors, TimeSpan.FromSeconds(15));
        Assert.NotEqual(leader, newLeader);

        Assert.Equal(KvStatusCode.Ok, (await client.PutAsync("after", "failover")).Status);
        Assert.Equal("v10", (await client.GetAsync("k10")).ValueString);
        Assert.Equal("failover", (await client.GetAsync("after", ReadConsistency.Lease)).ValueString);

        var cas = await client.CasAsync("after", "wrong", "x");
        Assert.Equal(KvStatusCode.CasFailed, cas.Status);
        Assert.Equal("failover", cas.ValueString);
        Assert.Equal(KvStatusCode.Ok, (await client.CasAsync("after", "failover", "x")).Status);
    }

    [Fact]
    public async Task ANewNodeCatchesUpThroughASnapshotAndJoins()
    {
        ulong leader = await WaitForLeader(_nodes.Keys, TimeSpan.FromSeconds(15));
        await using var client = new QuorumClient(new QuorumClientOptions { Endpoints = Endpoints, OperationTimeout = TimeSpan.FromSeconds(15) });
        // Enough writes to trigger snapshots and log compaction (every 200 entries in this test).
        var writes = Enumerable.Range(0, 8).Select(async c =>
        {
            await using var w = new QuorumClient(new QuorumClientOptions { Endpoints = Endpoints });
            for (int i = 0; i < 80; i++) await w.PutAsync($"c{c}-{i}", $"value-{i}");
        });
        await Task.WhenAll(writes);

        var status = (await Status(leader))!.Value;
        Assert.True(status.GetProperty("snapshotIndex").GetUInt64() > 0, "the leader should have taken a snapshot");
        Assert.True(status.GetProperty("firstIndex").GetUInt64() > 1, "the leader should have compacted its log");

        int grpc = FreePort(), http = FreePort();
        StartNode(4, grpc, http, join: true);
        var leaderHost = _nodes[leader].Options.HttpAddress;
        var add = await _http.PostAsJsonAsync($"{leaderHost}/admin/members/add", new { id = 4, address = $"http://127.0.0.1:{grpc}", learner = true });
        Assert.True(add.IsSuccessStatusCode, await add.Content.ReadAsStringAsync());

        // The learner gets the snapshot, then the rest of the log.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        JsonElement? s4 = null;
        while (DateTime.UtcNow < deadline)
        {
            s4 = await Status(4);
            var sl = await Status(leader);
            if (s4 is { } a && sl is { } b && a.GetProperty("applied").GetUInt64() >= b.GetProperty("commit").GetUInt64()) break;
            await Task.Delay(100);
        }
        Assert.NotNull(s4);
        Assert.True(s4!.Value.GetProperty("metrics").GetProperty("snapshotsReceived").GetInt64() >= 1, "node 4 should have installed a snapshot");

        var promote = await _http.PostAsJsonAsync($"{leaderHost}/admin/members/add", new { id = 4, address = $"http://127.0.0.1:{grpc}", learner = false });
        Assert.True(promote.IsSuccessStatusCode, await promote.Content.ReadAsStringAsync());

        // Every replica holds the same data.
        await Task.Delay(500);
        var digests = new List<string>();
        foreach (var id in _nodes.Keys)
        {
            var d = await _http.GetFromJsonAsync<JsonElement>($"{_nodes[id].Options.HttpAddress}/digest");
            digests.Add($"{d.GetProperty("applied").GetUInt64()}:{d.GetProperty("digest").GetString()}");
        }
        Assert.Single(digests.Distinct());
        var stale = await client.GetAsync("c3-79", ReadConsistency.Stale);
        Assert.Equal("value-79", stale.ValueString);
    }

    [Fact]
    public async Task ACrashedNodeRecoversItsStateFromDisk()
    {
        await WaitForLeader(_nodes.Keys, TimeSpan.FromSeconds(15));
        await using var client = new QuorumClient(new QuorumClientOptions { Endpoints = Endpoints, OperationTimeout = TimeSpan.FromSeconds(15) });
        for (int i = 0; i < 30; i++) await client.PutAsync($"d{i}", $"{i}");

        var follower = _nodes.Keys.First(id => Status(id).Result?.GetProperty("role").GetString() != "Leader");
        var o = _nodes[follower].Options;
        await StopNode(follower);
        await client.PutAsync("while-down", "yes");
        StartNode(follower, o.GrpcPort, o.HttpPort, join: false);

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            await using var local = new QuorumClient(new QuorumClientOptions { Endpoints = [o.GrpcAddress], OperationTimeout = TimeSpan.FromSeconds(2) });
            try
            {
                var r = await local.GetAsync("while-down", ReadConsistency.Stale);
                if (r.ValueString == "yes") return;
            }
            catch (QuorumException) { }
            await Task.Delay(100);
        }
        Assert.Fail("the restarted node did not catch up");
    }
}
