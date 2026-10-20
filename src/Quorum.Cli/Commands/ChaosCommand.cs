using System.Diagnostics;
using System.Text.Json;
using Quorum.Cli.Chaos;
using Quorum.Cli.Lab;
using Quorum.Client;
using Quorum.Linearizability;
using Quorum.Protocol;

namespace Quorum.Cli.Commands;

/// <summary>
/// <c>quorum chaos</c>: a Jepsen-style test. Starts a fresh cluster, runs concurrent clients while a nemesis
/// injects faults, heals everything, then checks (1) that the recorded history is linearizable and (2) that all
/// replicas converged to the same state.
/// </summary>
public static class ChaosCommand
{
    public sealed record Sample(double T, ulong Node, string Role, ulong Term, ulong Commit, ulong Applied, bool Up);

    public static async Task<int> RunAsync(CliArgs a, CancellationToken ct)
    {
        int nodes = a.GetInt("nodes", 5);
        int seconds = a.GetInt("time", 60);
        int clients = a.GetInt("clients", 10);
        int keys = a.GetInt("keys", 8);
        string nemesisSpec = a.Get("nemesis", "all");
        int interval = a.GetInt("interval", 5);
        var consistency = KvCliCommand.ParseConsistency(a.Get("read"));
        int rate = a.GetInt("rate", 0);
        int seed = a.GetInt("seed", Environment.TickCount & 0x7FFFFFFF);
        string outRoot = a.Get("out", "chaos-results");
        bool keepData = a.Has("keep-data");
        bool unpinned = a.Has("unpinned");
        a.RejectUnknown();

        var kinds = nemesisSpec == "all"
            ? Nemesis.AllKinds.ToList()
            : nemesisSpec == "none" ? [] : nemesisSpec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        foreach (var k in kinds.Where(k => !Nemesis.AllKinds.Contains(k)))
            throw new CliException($"unknown nemesis '{k}'. Choose from: {string.Join(", ", Nemesis.AllKinds)}, all, none");
        if (nodes is < 1 or > 7) throw new CliException("--nodes must be between 1 and 7");

        string runName = $"{DateTime.Now:yyyyMMdd-HHmmss}-{consistency.ToString().ToLowerInvariant()}-{nemesisSpec.Replace(',', '+')}";
        string outDir = Path.GetFullPath(Path.Combine(outRoot, runName));
        string dataDir = Path.Combine(outDir, "data");
        Directory.CreateDirectory(outDir);

        Console.WriteLine($"Quorum chaos test  seed={seed}");
        Console.WriteLine($"  {nodes} nodes, {clients} clients on {keys} keys, {seconds} s, reads: {consistency}, nemesis: {(kinds.Count == 0 ? "none" : string.Join(",", kinds))} every {interval} s");
        Console.WriteLine($"  results in {outDir}");
        Console.WriteLine();

        var rand = new Random(seed);
        var clock = Stopwatch.StartNew();
        double Now() => clock.Elapsed.TotalSeconds;
        var log = new List<string>();
        void Log(string s)
        {
            lock (log) log.Add(s);
            Console.WriteLine(s);
        }

        await using var cluster = new LocalCluster(new LocalCluster.ClusterSettings
        {
            DataDir = dataDir,
            // Own port range, so a chaos run can go on while `quorum cluster` is open.
            GrpcBase = 7300,
            HttpBase = 7400,
            // Snapshots often enough that a test exercises them.
            SnapshotEvery = 2_000,
        });
        Log($"[{Now(),6:0.0}s] starting {nodes} nodes");
        await cluster.StartAsync(nodes, ct);
        Log($"[{Now(),6:0.0}s] leader elected: node {await cluster.LeaderAsync(ct)}");

        var workload = new Workload(() => cluster.Endpoints, new WorkloadOptions
        {
            Clients = clients,
            Keys = keys,
            Rate = rate,
            Consistency = consistency,
            RecordHistory = true,
            PinClients = !unpinned,
        }, clock);
        var nemesis = new Nemesis(cluster, kinds, rand, Now, Log);
        var samples = new List<Sample>();

        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        runCts.CancelAfter(TimeSpan.FromSeconds(seconds));
        var workloadTask = workload.RunAsync(runCts.Token);
        var nemesisTask = kinds.Count > 0 ? nemesis.RunAsync(TimeSpan.FromSeconds(interval), runCts.Token) : Task.CompletedTask;
        var samplerTask = SampleAsync(cluster, samples, Now, runCts.Token);
        var progressTask = ProgressAsync(workload, Now, runCts.Token);

        await Task.WhenAll(Swallow(workloadTask), Swallow(nemesisTask), Swallow(samplerTask), Swallow(progressTask));
        ct.ThrowIfCancellationRequested();

        // Recovery: heal everything and give the cluster time to settle.
        Log($"[{Now(),6:0.0}s] workload done; healing the cluster");
        await nemesis.HealAsync(CancellationToken.None);
        foreach (var n in cluster.Nodes.Where(n => !n.Removed && !n.Running)) cluster.Start(n);
        ulong leader;
        try
        {
            leader = await cluster.WaitForLeaderAsync(TimeSpan.FromSeconds(30), ct);
        }
        catch (CliException ex)
        {
            Log($"LIVENESS FAILURE: {ex.Message}");
            return 1;
        }
        Log($"[{Now(),6:0.0}s] leader after healing: node {leader}");

        // Final reads: every key, linearizably, recorded in the same history.
        var finalOps = await FinalReadsAsync(cluster, keys, clock, workload.History.Count + 1_000_000, ct);
        var history = workload.History.Concat(finalOps).ToList();
        HistoryFile.Save(Path.Combine(outDir, "history.jsonl"), history);

        // Convergence: every member reaches the same applied index with the same data.
        var convergence = await CheckConvergenceAsync(cluster, ct);
        Log($"[{Now(),6:0.0}s] convergence: {convergence.Message}");

        Console.WriteLine();
        Console.WriteLine($"Checking {history.Count} operations for linearizability...");
        var sw = Stopwatch.StartNew();
        var result = Checker.Check(history, TimeSpan.FromMinutes(3));
        Console.WriteLine($"  checked in {sw.Elapsed.TotalSeconds:0.0} s");

        var electionIssues = CheckElections(samples);
        int ok = history.Count(o => o.Outcome == OpOutcome.Ok);
        int unknown = history.Count(o => o.Outcome == OpOutcome.Unknown);
        int failed = history.Count(o => o.Outcome == OpOutcome.Fail);
        var lat = workload.LatencySummary();

        Console.WriteLine();
        Console.WriteLine("Results");
        Console.WriteLine($"  operations       {history.Count} ({ok} ok, {failed} failed, {unknown} unknown outcome)");
        Console.WriteLine($"  throughput       {workload.TotalOk / Math.Max(1.0, seconds):0} ok ops/s");
        Console.WriteLine($"  latency          p50 {lat.P50:0.0} ms, p99 {lat.P99:0.0} ms, max {lat.Max:0} ms");
        Console.WriteLine($"  faults injected  {nemesis.Events.Count}");
        foreach (var k in result.Keys)
        {
            string verdict = k.Linearizable switch { true => "linearizable", false => "NOT LINEARIZABLE", null => "unknown (search timed out)" };
            Console.WriteLine($"    {k.Key,-6} {k.Operations,6} ops  {verdict}");
        }
        Console.WriteLine($"  linearizable     {Verdict(result.Linearizable)}");
        Console.WriteLine($"  convergence      {(convergence.Ok ? "PASS" : "FAIL")} ({convergence.Message})");
        Console.WriteLine($"  election safety  {(electionIssues.Count == 0 ? "PASS (at most one leader per term observed)" : "FAIL: " + string.Join("; ", electionIssues))}");

        string reportPath = Path.Combine(outDir, "report.html");
        ChaosReport.Write(reportPath, new ChaosReport.Input(
            runName, seed, nodes, clients, keys, seconds, consistency.ToString(), kinds, history, result,
            nemesis.Events, workload.RecentSeconds, samples, convergence.Ok, convergence.Message, electionIssues, log, lat));
        File.WriteAllText(Path.Combine(outDir, "summary.json"), JsonSerializer.Serialize(new
        {
            seed,
            nodes,
            clients,
            keys,
            seconds,
            consistency = consistency.ToString(),
            nemesis = kinds,
            operations = history.Count,
            ok,
            failed,
            unknown,
            linearizable = result.Linearizable,
            converged = convergence.Ok,
            electionSafety = electionIssues.Count == 0,
        }, new JsonSerializerOptions { WriteIndented = true }));
        Console.WriteLine();
        Console.WriteLine($"Report: {reportPath}");

        await cluster.DisposeAsync();
        if (!keepData) LocalCluster.Wipe(dataDir);

        bool pass = result.Linearizable != false && convergence.Ok && electionIssues.Count == 0;
        Console.WriteLine(pass ? "PASS" : "FAIL");
        return pass ? 0 : 1;
    }

    private static string Verdict(bool? v) => v switch
    {
        true => "PASS",
        false => "FAIL - a client saw a result no sequential execution could produce",
        null => "UNKNOWN - the search did not finish in time",
    };

    private static async Task Swallow(Task t)
    {
        try { await t; }
        catch (OperationCanceledException) { }
    }

    private static async Task ProgressAsync(Workload w, Func<double> now, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(5000, ct);
            var last = w.RecentSeconds.TakeLast(5).ToList();
            double ops = last.Count == 0 ? 0 : last.Average(s => s.Ok);
            Console.WriteLine($"[{now(),6:0.0}s] {ops,6:0} ops/s  total ok {w.TotalOk}, failed {w.TotalFailed}, unknown {w.TotalUnknown}");
        }
    }

    private static async Task SampleAsync(LocalCluster cluster, List<Sample> samples, Func<double> now, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var nodes = cluster.Nodes;
            var statuses = await Task.WhenAll(nodes.Select(async n => (n, s: await cluster.StatusAsync(n.Id, ct))));
            double t = now();
            lock (samples)
            {
                foreach (var (n, s) in statuses)
                {
                    if (s is { } st && st.TryGetProperty("role", out var role))
                        samples.Add(new Sample(t, n.Id, role.GetString()!, st.GetProperty("term").GetUInt64(),
                            st.GetProperty("commit").GetUInt64(), st.GetProperty("applied").GetUInt64(), true));
                    else
                        samples.Add(new Sample(t, n.Id, n.Removed ? "Removed" : "Down", 0, 0, 0, false));
                }
            }
            await Task.Delay(250, ct);
        }
    }

    /// <summary>Election safety as observed: no two nodes ever claimed leadership of the same term.</summary>
    private static List<string> CheckElections(List<Sample> samples)
    {
        var issues = new List<string>();
        lock (samples)
        {
            foreach (var g in samples.Where(s => s.Role == "Leader").GroupBy(s => s.Term))
            {
                var leaders = g.Select(s => s.Node).Distinct().ToList();
                if (leaders.Count > 1) issues.Add($"term {g.Key} had leaders {string.Join(" and ", leaders)}");
            }
        }
        return issues;
    }

    private static async Task<List<Operation>> FinalReadsAsync(LocalCluster cluster, int keys, Stopwatch clock, int firstId, CancellationToken ct)
    {
        var ops = new List<Operation>();
        await using var client = new QuorumClient(new QuorumClientOptions { Endpoints = cluster.Endpoints, OperationTimeout = TimeSpan.FromSeconds(20) });
        for (int k = 0; k < keys; k++)
        {
            string key = $"k{k}";
            long call = clock.ElapsedTicks;
            try
            {
                var r = await client.GetAsync(key, ReadConsistency.Linearizable, ct);
                ops.Add(new Operation
                {
                    Id = firstId + k,
                    Client = -1,
                    Key = key,
                    Kind = OpKind.Read,
                    Observed = r.Found ? r.ValueString : null,
                    Call = call,
                    Return = clock.ElapsedTicks,
                    Outcome = OpOutcome.Ok,
                    Node = r.Node,
                });
            }
            catch (QuorumException)
            {
            }
        }
        return ops;
    }

    private static async Task<(bool Ok, string Message)> CheckConvergenceAsync(LocalCluster cluster, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        string last = "";
        while (sw.Elapsed < TimeSpan.FromSeconds(30))
        {
            var members = cluster.Nodes.Where(n => !n.Removed).ToList();
            var digests = await Task.WhenAll(members.Select(async n => (n.Id, d: await cluster.GetJsonAsync(n.Id, "/digest", ct))));
            if (digests.All(d => d.d is not null))
            {
                var values = digests.Select(d => (d.Id, Applied: d.d!.Value.GetProperty("applied").GetUInt64(),
                    Digest: d.d.Value.GetProperty("digest").GetString()!, Keys: d.d.Value.GetProperty("keys").GetInt64())).ToList();
                last = string.Join(", ", values.Select(v => $"{v.Id}@{v.Applied}:{v.Digest[..8]}"));
                if (values.Select(v => v.Applied).Distinct().Count() == 1)
                {
                    bool same = values.Select(v => v.Digest).Distinct().Count() == 1;
                    return same
                        ? (true, $"{values.Count} replicas at index {values[0].Applied} with identical data ({values[0].Keys} keys, digest {values[0].Digest})")
                        : (false, $"replicas at the same index disagree: {last}");
                }
            }
            await Task.Delay(300, ct);
        }
        return (false, $"replicas did not reach the same index within 30 s: {last}");
    }
}
