namespace Quorum.Core;

/// <summary>Tuning and identity of a Raft node. Times are in ticks; the host decides how long a tick is.</summary>
public sealed class RaftOptions
{
    public required ulong Id { get; init; }
    /// <summary>Base election timeout. The actual timeout is randomized in [ElectionTicks, 2*ElectionTicks).</summary>
    public int ElectionTicks { get; init; } = 10;
    public int HeartbeatTicks { get; init; } = 1;
    /// <summary>Pre-vote phase (dissertation 9.6): a node rejoining after a partition does not bump the term.</summary>
    public bool PreVote { get; init; } = true;
    /// <summary>A leader that cannot reach a quorum for one election timeout steps down.</summary>
    public bool CheckQuorum { get; init; } = true;
    public int MaxMessageBytes { get; init; } = 1 << 20;
    public int MaxInflight { get; init; } = 256;
    public int MaxApplyBytes { get; init; } = 8 << 20;
    /// <summary>Lease length is ElectionTicks minus this margin, to absorb tick jitter.</summary>
    public int LeaseMarginTicks { get; init; } = 2;
    /// <summary>
    /// A node that just started refuses votes for one election timeout, as if it had heard from a leader: it may
    /// have acknowledged a lease before it crashed. Needed for lease reads to survive restarts.
    /// </summary>
    public bool LeaseOnStartup { get; init; } = true;
    public int Seed { get; init; } = Environment.TickCount;
}

/// <summary>What a node finds in its storage when it starts.</summary>
public sealed class RaftInitialState
{
    public HardState HardState { get; init; } = HardState.Empty;
    /// <summary>The last compacted (or snapshot-restored) index and its term.</summary>
    public ulong LogOffset { get; init; }
    public ulong LogOffsetTerm { get; init; }
    /// <summary>Entries after <see cref="LogOffset"/>, contiguous.</summary>
    public IReadOnlyList<Entry> Entries { get; init; } = [];
    /// <summary>The latest snapshot; its configuration is the base that later configuration entries apply to.</summary>
    public SnapshotMeta? Snapshot { get; init; }
    /// <summary>The state machine's applied index.</summary>
    public ulong Applied { get; init; }
}

/// <summary>
/// Everything the host must do after the node changed state, in this order:
/// <list type="number">
/// <item>install <see cref="Snapshot"/> (if any);</item>
/// <item>persist <see cref="Entries"/> (replacing any stored entries from the first one's index on) and
/// <see cref="HardState"/>, durably if <see cref="MustSync"/>;</item>
/// <item>apply <see cref="CommittedEntries"/> to the state machine;</item>
/// <item>send <see cref="Messages"/>;</item>
/// <item>serve <see cref="ReadStates"/> once the applied index reaches theirs;</item>
/// <item>call <see cref="RaftNode.Advance"/>.</item>
/// </list>
/// </summary>
public sealed class Ready
{
    public SoftState? SoftState { get; init; }
    public HardState? HardState { get; init; }
    public Entry[] Entries { get; init; } = [];
    public SnapshotMeta? Snapshot { get; init; }
    public Entry[] CommittedEntries { get; init; } = [];
    public List<Message> Messages { get; init; } = [];
    public List<ReadState> ReadStates { get; init; } = [];
    /// <summary>Human-readable notable events (elections, step-downs, configuration changes).</summary>
    public List<string> Events { get; init; } = [];
    public bool MustSync { get; init; }
}

public enum ProposeError
{
    None,
    NotLeader,
    TransferInProgress,
    ConfChangeInProgress,
    InvalidConfChange,
}

public readonly record struct ProposeResult(ProposeError Error, ulong Index, ulong Term, string? Detail = null)
{
    public bool Ok => Error == ProposeError.None;
}

/// <summary>A point-in-time view of a node, for dashboards and tests.</summary>
public sealed record NodeStatus(
    ulong Id,
    Role Role,
    ulong Term,
    ulong Vote,
    ulong Leader,
    ulong Commit,
    ulong Applied,
    ulong LastIndex,
    ulong LastTerm,
    ulong FirstIndex,
    ConfState Conf,
    IReadOnlyDictionary<ulong, ProgressStatus> Progress,
    ulong LeadTransferee,
    bool LeaseValid);

public sealed record ProgressStatus(ulong Match, ulong Next, ProgressState State, int Inflight, bool RecentActive, bool IsLearner);
