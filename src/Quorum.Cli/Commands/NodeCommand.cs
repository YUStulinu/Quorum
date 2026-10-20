using Quorum.Core;
using Quorum.Node;

namespace Quorum.Cli.Commands;

public static class NodeCommand
{
    /// <summary>Parses <c>1=http://host:port,2=...</c>.</summary>
    public static List<Member> ParsePeers(string spec)
    {
        var members = new List<Member>();
        foreach (var part in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2 || !ulong.TryParse(kv[0], out ulong id) || id == 0)
                throw new CliException($"bad peer '{part}': expected id=address, e.g. 1=http://127.0.0.1:7101");
            members.Add(new Member(id, kv[1], false));
        }
        return members;
    }

    public static async Task<int> RunAsync(CliArgs a, CancellationToken ct)
    {
        ulong id = a.GetULong("id", 0);
        if (id == 0) throw new CliException("--id is required (a positive integer)");
        var options = new NodeOptions
        {
            Id = id,
            DataDir = a.Get("data", Path.Combine("data", $"node-{id}")),
            Host = a.Get("host", "127.0.0.1"),
            GrpcPort = a.GetInt("grpc", 7100 + (int)id),
            HttpPort = a.GetInt("http", 7200 + (int)id),
            InitialMembers = a.Get("peers") is { } peers ? ParsePeers(peers) : [],
            Join = a.Has("join"),
            TickMs = a.GetInt("tick", 20),
            ElectionTicks = a.GetInt("election", 15),
            HeartbeatTicks = a.GetInt("heartbeat", 3),
            SnapshotEvery = a.GetInt("snapshot-every", 20_000),
            SnapshotTrail = a.GetInt("snapshot-trail", 2_000),
            PreVote = !a.Has("no-prevote"),
            CheckQuorum = !a.Has("no-checkquorum"),
            Sync = a.Get("sync", "full") switch
            {
                "full" => Storage.TesseraSync.Full,
                "normal" => Storage.TesseraSync.Normal,
                "off" => Storage.TesseraSync.Off,
                var s => throw new CliException($"--sync must be full, normal or off, not '{s}'"),
            },
        };
        a.RejectUnknown();
        if (options.InitialMembers.Count > 0 && options.InitialMembers.All(m => m.Id != id) && !options.Join)
            throw new CliException($"--peers does not include this node ({id}); use --join for a node joining later");

        await NodeServer.RunAsync(options, ct);
        return 0;
    }
}
