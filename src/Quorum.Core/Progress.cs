namespace Quorum.Core;

public enum ProgressState : byte
{
    /// <summary>The leader does not know where the follower's log ends: one append at a time until it matches.</summary>
    Probe,
    /// <summary>The follower is in sync: appends are pipelined, up to a window of in-flight messages.</summary>
    Replicate,
    /// <summary>The follower is installing a snapshot; nothing else is sent until it reports back.</summary>
    Snapshot,
}

/// <summary>The leader's view of one follower's log.</summary>
public sealed class Progress(ulong next, int maxInflight, bool isLearner)
{
    private readonly Queue<ulong> _inflight = new();

    public ulong Match { get; private set; }
    public ulong Next { get; private set; } = next;
    public ProgressState State { get; private set; } = ProgressState.Probe;
    /// <summary>In Probe state: an append was sent and no answer has come back yet.</summary>
    public bool ProbeSent { get; set; }
    public ulong PendingSnapshot { get; private set; }
    /// <summary>Heard from in the current election timeout (used by CheckQuorum and to avoid snapshots to dead nodes).</summary>
    public bool RecentActive { get; set; } = true;
    public bool IsLearner { get; set; } = isLearner;
    public int MaxInflight { get; } = maxInflight;

    public int Inflight => _inflight.Count;

    public bool IsPaused => State switch
    {
        ProgressState.Probe => ProbeSent,
        ProgressState.Replicate => _inflight.Count >= MaxInflight,
        _ => true,
    };

    /// <summary>Records an acknowledged index. Returns true if it moved Match forward.</summary>
    public bool MaybeUpdate(ulong index)
    {
        bool updated = false;
        if (Match < index)
        {
            Match = index;
            updated = true;
            ProbeSent = false;
        }
        if (Next < index + 1) Next = index + 1;
        return updated;
    }

    /// <summary>
    /// Handles a rejected append for <paramref name="rejected"/>. Returns false if the rejection is stale
    /// (an answer to an older message).
    /// </summary>
    public bool MaybeDecrTo(ulong rejected, ulong matchHint)
    {
        if (State == ProgressState.Replicate)
        {
            if (rejected <= Match) return false;
            Next = Match + 1;
            return true;
        }
        if (Next - 1 != rejected) return false;
        Next = Math.Max(Math.Min(rejected, matchHint + 1), Match + 1);
        if (Next < 1) Next = 1;
        ProbeSent = false;
        return true;
    }

    public void BecomeProbe()
    {
        Next = State == ProgressState.Snapshot
            ? Math.Max(Match + 1, PendingSnapshot + 1)
            : Match + 1;
        State = ProgressState.Probe;
        PendingSnapshot = 0;
        ProbeSent = false;
        _inflight.Clear();
    }

    public void BecomeReplicate()
    {
        State = ProgressState.Replicate;
        Next = Match + 1;
        PendingSnapshot = 0;
        ProbeSent = false;
        _inflight.Clear();
    }

    public void BecomeSnapshot(ulong index)
    {
        State = ProgressState.Snapshot;
        PendingSnapshot = index;
        ProbeSent = false;
        _inflight.Clear();
    }

    /// <summary>Called after an append carrying entries up to <paramref name="lastIndex"/> was sent.</summary>
    public void SentEntries(ulong lastIndex, bool hadEntries)
    {
        if (State == ProgressState.Replicate)
        {
            if (hadEntries)
            {
                Next = lastIndex + 1;
                _inflight.Enqueue(lastIndex);
            }
        }
        else if (State == ProgressState.Probe)
        {
            ProbeSent = true;
        }
    }

    /// <summary>Frees the in-flight slots of every message acknowledged up to <paramref name="index"/>.</summary>
    public void FreeInflightUpTo(ulong index)
    {
        while (_inflight.Count > 0 && _inflight.Peek() <= index) _inflight.Dequeue();
    }

    /// <summary>A heartbeat response frees one slot so that a stuck window still makes progress.</summary>
    public void FreeFirstInflight()
    {
        if (_inflight.Count > 0) _inflight.Dequeue();
    }

    public void ResetMatch(ulong match, ulong next)
    {
        Match = match;
        Next = next;
    }

    public override string ToString() => $"{State} match={Match} next={Next} inflight={Inflight}";
}
