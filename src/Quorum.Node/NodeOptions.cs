using Quorum.Core;
using Quorum.Storage;

namespace Quorum.Node;

/// <summary>Configuration of one Quorum node process.</summary>
public sealed class NodeOptions
{
    public required ulong Id { get; init; }
    public required string DataDir { get; init; }
    public string Host { get; init; } = "127.0.0.1";
    /// <summary>Port for node-to-node and client gRPC traffic (HTTP/2, cleartext).</summary>
    public int GrpcPort { get; init; } = 7101;
    /// <summary>Port for the JSON status/admin API (HTTP/1.1).</summary>
    public int HttpPort { get; init; } = 7201;

    /// <summary>The founding members, used only the first time a node starts with an empty data directory.
    /// Leave empty (with <see cref="Join"/>) for a node that will be added to an existing cluster.</summary>
    public IReadOnlyList<Member> InitialMembers { get; init; } = [];
    /// <summary>Start empty and wait to be added by the leader (as a learner, then a voter).</summary>
    public bool Join { get; init; }

    public int TickMs { get; init; } = 20;
    public int ElectionTicks { get; init; } = 15;
    public int HeartbeatTicks { get; init; } = 3;
    public bool PreVote { get; init; } = true;
    public bool CheckQuorum { get; init; } = true;

    /// <summary>Take a snapshot and compact the log every this many applied entries.</summary>
    public int SnapshotEvery { get; init; } = 20_000;
    /// <summary>Entries kept after compaction, so that slightly lagging followers need no snapshot.</summary>
    public int SnapshotTrail { get; init; } = 2_000;

    public int DefaultTimeoutMs { get; init; } = 3_000;
    public int MaxBatch { get; init; } = 4_096;
    public TesseraSync Sync { get; init; } = TesseraSync.Full;
    public int MaxSessions { get; init; } = 10_000;

    public string GrpcAddress => $"http://{Host}:{GrpcPort}";
    public string HttpAddress => $"http://{Host}:{HttpPort}";
}
