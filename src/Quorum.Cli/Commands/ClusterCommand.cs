using System.Diagnostics;
using Quorum.Cli.Lab;

namespace Quorum.Cli.Commands;

/// <summary><c>quorum cluster</c>: a local cluster plus the live dashboard, until Ctrl+C.</summary>
public static class ClusterCommand
{
    public static async Task<int> RunAsync(CliArgs a, CancellationToken ct)
    {
        int nodes = a.GetInt("nodes", 3);
        string data = a.Get("data", "data");
        int port = a.GetInt("dashboard", 7000);
        bool clean = a.Has("clean");
        bool noBrowser = a.Has("no-browser");
        a.RejectUnknown();
        if (nodes is < 1 or > 7) throw new CliException("--nodes must be between 1 and 7");

        if (clean) LocalCluster.Wipe(data);
        if (Directory.Exists(data) && Directory.GetDirectories(data, "node-*").Length > 0 && !clean)
        {
            int existing = Directory.GetDirectories(data, "node-*").Length;
            Console.WriteLine($"Reusing the cluster data in {Path.GetFullPath(data)} (use --clean to start fresh).");
            if (existing != nodes)
                Console.WriteLine($"  note: it has {existing} node directories; starting the first {nodes} as founders.");
        }

        await using var cluster = new LocalCluster(new LocalCluster.ClusterSettings { DataDir = data });
        Console.WriteLine($"Starting {nodes} nodes...");
        await cluster.StartAsync(nodes, ct);
        var leader = await cluster.LeaderAsync(ct);
        var dashboard = new Dashboard(cluster, port);
        dashboard.Note($"cluster of {nodes} started; node {leader} is the leader");

        Console.WriteLine();
        foreach (var n in cluster.Nodes)
            Console.WriteLine($"  node {n.Id}   gRPC {n.GrpcAddress}   status {n.HttpAddress}/status   log {Path.Combine(data, $"node-{n.Id}.log")}");
        Console.WriteLine();
        Console.WriteLine($"  Dashboard: {dashboard.Url}");
        Console.WriteLine("  Try:       dotnet run --project src/Quorum.Cli -- kv put greeting hello");
        Console.WriteLine($"  Stop:      Ctrl+C");
        Console.WriteLine();

        if (!noBrowser)
        {
            try { Process.Start(new ProcessStartInfo(dashboard.Url) { UseShellExecute = true }); }
            catch (Exception) { /* no browser available: the URL is printed above */ }
        }
        await dashboard.RunAsync(ct);
        Console.WriteLine("Stopping the nodes...");
        return 0;
    }
}
