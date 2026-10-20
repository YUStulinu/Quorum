using System.Collections.Immutable;

namespace Quorum.Core;

/// <summary>The kind of a log entry. An empty <see cref="Normal"/> entry is the no-op a new leader appends.</summary>
public enum EntryType : byte
{
    Normal = 0,
    ConfChange = 1,
}

/// <summary>One entry of the replicated log. Entries are immutable once created.</summary>
public sealed class Entry
{
    public required ulong Index { get; init; }
    public required ulong Term { get; init; }
    public EntryType Type { get; init; } = EntryType.Normal;
    public byte[] Data { get; init; } = [];

    /// <summary>Approximate wire size, used to cap message sizes.</summary>
    public int Size => Data.Length + 24;

    public override string ToString() => $"{Index}@{Term}{(Type == EntryType.ConfChange ? " conf" : "")}";
}

public enum MessageType : byte
{
    Vote,
    VoteResponse,
    PreVote,
    PreVoteResponse,
    Append,
    AppendResponse,
    Heartbeat,
    HeartbeatResponse,
    Snapshot,
    TimeoutNow,
}

/// <summary>
/// A message between two Raft nodes. The meaning of <see cref="Index"/> and <see cref="LogTerm"/>
/// depends on the type:
/// <list type="bullet">
/// <item>Vote / PreVote: the candidate's last log index and term.</item>
/// <item>Append: the index and term of the entry just before <see cref="Entries"/>.</item>
/// <item>AppendResponse: the follower's last matching index (accepted) or the rejected index
/// (rejected; then <see cref="RejectHint"/> and <see cref="LogTerm"/> help the leader skip back fast).</item>
/// </list>
/// </summary>
public sealed class Message
{
    public MessageType Type { get; set; }
    public ulong From { get; set; }
    public ulong To { get; set; }
    public ulong Term { get; set; }
    public ulong LogTerm { get; set; }
    public ulong Index { get; set; }
    public Entry[] Entries { get; set; } = [];
    public ulong Commit { get; set; }
    public bool Reject { get; set; }
    public ulong RejectHint { get; set; }
    /// <summary>Heartbeat round number, echoed back in the response (used for ReadIndex and leases).</summary>
    public ulong Context { get; set; }
    public SnapshotMeta? Snapshot { get; set; }
    /// <summary>Set on votes caused by a leadership transfer: receivers ignore their leader lease.</summary>
    public bool Force { get; set; }

    public override string ToString() =>
        $"{Type} {From}->{To} t{Term} i{Index} lt{LogTerm} c{Commit}" +
        (Entries.Length > 0 ? $" [{Entries[0].Index}..{Entries[^1].Index}]" : "") +
        (Reject ? $" reject hint={RejectHint}" : "");
}

public readonly record struct HardState(ulong Term, ulong Vote, ulong Commit)
{
    public static readonly HardState Empty = new(0, 0, 0);
}

public enum Role : byte
{
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

public readonly record struct SoftState(ulong Leader, Role Role);

/// <summary>A member of the cluster configuration.</summary>
public sealed record Member(ulong Id, string Address, bool IsLearner);

public enum ConfChangeType : byte
{
    /// <summary>Adds a voting member, or promotes an existing learner.</summary>
    AddVoter = 0,
    /// <summary>Adds a non-voting member that receives the log but does not count for quorums.</summary>
    AddLearner = 1,
    Remove = 2,
}

/// <summary>A single-server membership change (Raft dissertation, section 4.1).</summary>
public sealed record ConfChange(ConfChangeType Type, ulong NodeId, string Address);

/// <summary>An immutable cluster configuration.</summary>
public sealed class ConfState : IEquatable<ConfState>
{
    public static readonly ConfState Empty = new(ImmutableSortedDictionary<ulong, Member>.Empty);

    public ImmutableSortedDictionary<ulong, Member> Members { get; }
    public ImmutableSortedSet<ulong> Voters { get; }
    public ImmutableSortedSet<ulong> Learners { get; }

    public ConfState(ImmutableSortedDictionary<ulong, Member> members)
    {
        Members = members;
        Voters = members.Values.Where(m => !m.IsLearner).Select(m => m.Id).ToImmutableSortedSet();
        Learners = members.Values.Where(m => m.IsLearner).Select(m => m.Id).ToImmutableSortedSet();
    }

    public static ConfState FromMembers(IEnumerable<Member> members) =>
        new(members.ToImmutableSortedDictionary(m => m.Id, m => m));

    public int Quorum => Voters.Count / 2 + 1;

    public bool IsVoter(ulong id) => Voters.Contains(id);
    public bool Contains(ulong id) => Members.ContainsKey(id);

    /// <summary>Checks a change before it is proposed. Returns null when it is valid, otherwise the reason.</summary>
    public string? Validate(ConfChange cc)
    {
        if (cc.NodeId == 0) return "node id 0 is reserved";
        switch (cc.Type)
        {
            case ConfChangeType.AddVoter:
                if (IsVoter(cc.NodeId)) return $"node {cc.NodeId} is already a voter";
                return null;
            case ConfChangeType.AddLearner:
                if (Contains(cc.NodeId)) return $"node {cc.NodeId} is already a member";
                return null;
            case ConfChangeType.Remove:
                if (!Contains(cc.NodeId)) return $"node {cc.NodeId} is not a member";
                if (IsVoter(cc.NodeId) && Voters.Count == 1) return "cannot remove the last voter";
                return null;
            default:
                return "unknown change type";
        }
    }

    /// <summary>Applies a change. Invalid changes (which can only come from a buggy proposer) are ignored,
    /// so that every node computes the same configuration from the same log.</summary>
    public ConfState Apply(ConfChange cc)
    {
        if (Validate(cc) is not null) return this;
        return cc.Type switch
        {
            ConfChangeType.AddVoter => new ConfState(Members.SetItem(cc.NodeId,
                new Member(cc.NodeId, string.IsNullOrEmpty(cc.Address) && Members.TryGetValue(cc.NodeId, out var m) ? m.Address : cc.Address, false))),
            ConfChangeType.AddLearner => new ConfState(Members.SetItem(cc.NodeId, new Member(cc.NodeId, cc.Address, true))),
            ConfChangeType.Remove => new ConfState(Members.Remove(cc.NodeId)),
            _ => this,
        };
    }

    public bool Equals(ConfState? other) =>
        other is not null && Members.Count == other.Members.Count &&
        Members.All(kv => other.Members.TryGetValue(kv.Key, out var o) && o == kv.Value);

    public override bool Equals(object? obj) => Equals(obj as ConfState);
    public override int GetHashCode() => Members.Aggregate(17, (h, kv) => HashCode.Combine(h, kv.Value));

    public override string ToString() =>
        "{" + string.Join(", ", Members.Values.Select(m => m.IsLearner ? $"{m.Id}(learner)" : m.Id.ToString())) + "}";
}

/// <summary>Describes a snapshot of the state machine: everything up to and including <see cref="Index"/>.</summary>
public sealed record SnapshotMeta(ulong Index, ulong Term, ConfState Conf);

/// <summary>A linearizable read that may proceed once the node has applied up to <see cref="Index"/>.</summary>
public readonly record struct ReadState(ulong Context, ulong Index);

public enum ReadMode : byte
{
    /// <summary>Confirms leadership with a heartbeat round before every read batch (always safe).</summary>
    ReadIndex = 0,
    /// <summary>Serves reads while the leader lease holds. Fast, but only safe if clocks run at the same rate.</summary>
    Lease = 1,
}
