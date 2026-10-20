using Quorum.Cli.Lab;
using Quorum.Protocol;

namespace Quorum.Cli.Commands;

/// <summary><c>quorum bench</c>: throughput and latency of writes and of each read mode on a fresh local cluster.</summary>
public static class BenchCommand
{
    public static async Task<int> RunAsync(CliArgs a, CancellationToken ct)
    {
        int nodes = a.GetInt("nodes", 3);
        int clients = a.GetInt("clients", 64);
        int seconds = a.GetInt("time", 10);
        int valueSize = a.GetInt("value-size", 100);
        int keys = a.GetInt("keys", 10_000);
        string data = a.Get("data", Path.Combine(Path.GetTempPath(), "quorum-bench"));
        a.RejectUnknown();

        LocalCluster.Wipe(data);
        await using var cluster = new LocalCluster(new LocalCluster.ClusterSettings { DataDir = data, SnapshotEvery = 50_000, GrpcBase = 7500, HttpBase = 7600 });
        Console.WriteLine($"Starting {nodes} nodes (fsync on every commit)...");
        await cluster.StartAsync(nodes, ct);

        var phases = new (string Name, double ReadRatio, ReadConsistency Mode)[]
        {
            ("writes", 0.0, ReadConsistency.Linearizable),
            ("reads: ReadIndex", 1.0, ReadConsistency.Linearizable),
            ("reads: lease", 1.0, ReadConsistency.Lease),
            ("reads: local (stale)", 1.0, ReadConsistency.Stale),
            ("mixed 90/10 ReadIndex", 0.9, ReadConsistency.Linearizable),
        };

        Console.WriteLine($"{clients} clients, {valueSize}-byte values, {keys:N0} keys, {seconds} s per phase");
        Console.WriteLine();
        Console.WriteLine($"  {"phase",-24} {"ops/s",10} {"p50 ms",8} {"p90 ms",8} {"p99 ms",8} {"max ms",8}");
        foreach (var (name, readRatio, mode) in phases)
        {
            // Warm up (connections, JIT) for a second, then measure.
            var w = new Workload(() => cluster.Endpoints, new WorkloadOptions
            {
                Clients = clients,
                Keys = keys,
                ReadRatio = readRatio,
                CasRatio = 0,
                Consistency = mode,
                ValueSize = valueSize,
                KeyPrefix = "bench:",
            });
            using var run = CancellationTokenSource.CreateLinkedTokenSource(ct);
            run.CancelAfter(TimeSpan.FromSeconds(seconds + 1));
            var task = w.RunAsync(run.Token);
            await Task.Delay(1000, ct);
            long okStart = w.TotalOk;
            var started = DateTime.UtcNow;
            await task;
            double elapsed = (DateTime.UtcNow - started).TotalSeconds;
            var lat = w.LatencySummary();
            Console.WriteLine($"  {name,-24} {(w.TotalOk - okStart) / elapsed,10:N0} {lat.P50,8:0.0} {lat.P90,8:0.0} {lat.P99,8:0.0} {lat.Max,8:0}");
            if (w.TotalFailed + w.TotalUnknown > 0)
                Console.WriteLine($"  {"",-24} ({w.TotalFailed} failed, {w.TotalUnknown} unknown)");
        }

        if (await cluster.LeaderAsync(ct) is { } leader && await cluster.StatusAsync(leader, ct) is { } st)
        {
            var m = st.GetProperty("metrics");
            long fsyncs = m.GetProperty("fsyncs").GetInt64();
            long applied = m.GetProperty("applied").GetInt64();
            Console.WriteLine();
            Console.WriteLine($"  leader (node {leader}): {fsyncs:N0} fsyncs averaging {m.GetProperty("fsyncAvgMs").GetDouble():0.00} ms, " +
                              $"{(fsyncs == 0 ? 0 : (double)applied / fsyncs):0.0} entries applied per fsync, " +
                              $"commit latency p50 {m.GetProperty("commitP50Ms").GetDouble():0.0} ms");
        }
        Console.WriteLine();
        Console.WriteLine("Writes are batched: requests that arrive while the leader fsyncs share the next fsync.");
        Console.WriteLine("Lease reads skip the heartbeat round of ReadIndex; local reads skip the leader entirely (and may be stale).");
        await cluster.DisposeAsync();
        LocalCluster.Wipe(data);
        return 0;
    }
}
