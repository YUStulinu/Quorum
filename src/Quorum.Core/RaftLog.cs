namespace Quorum.Core;

/// <summary>Thrown when the Raft core detects a state that violates its invariants (a bug or corrupted storage).</summary>
public sealed class RaftInvariantException(string message) : Exception(message);

/// <summary>
/// The in-memory view of a node's log: every entry after the last compaction point.
/// <para>
/// Indices: entries <c>(Offset, LastIndex]</c> are held here; <see cref="Offset"/> and <see cref="OffsetTerm"/>
/// describe the last compacted entry (or a restored snapshot). <see cref="Stable"/> is the last index the host
/// has persisted; everything after it is still to be written. <see cref="Committed"/> and <see cref="Applied"/>
/// follow the usual Raft meaning, with Applied &lt;= Committed &lt;= LastIndex.
/// </para>
/// </summary>
public sealed class RaftLog
{
    private readonly List<Entry> _entries;

    public ulong Offset { get; private set; }
    public ulong OffsetTerm { get; private set; }
    public ulong Committed { get; private set; }
    public ulong Applied { get; private set; }
    public ulong Stable { get; private set; }

    public RaftLog(ulong offset, ulong offsetTerm, IReadOnlyList<Entry> entries, ulong committed, ulong applied)
    {
        Offset = offset;
        OffsetTerm = offsetTerm;
        _entries = new List<Entry>(entries);
        for (int i = 0; i < _entries.Count; i++)
        {
            if (_entries[i].Index != offset + 1 + (ulong)i)
                throw new RaftInvariantException($"log entries are not contiguous at position {i}");
        }
        Stable = LastIndex;
        Applied = Math.Max(applied, offset);
        Committed = Math.Max(Math.Max(committed, Applied), offset);
        if (Committed > LastIndex)
            throw new RaftInvariantException($"commit index {Committed} is beyond the last index {LastIndex}");
    }

    public ulong FirstIndex => Offset + 1;
    public ulong LastIndex => Offset + (ulong)_entries.Count;
    public ulong LastTerm => _entries.Count > 0 ? _entries[^1].Term : OffsetTerm;
    public int Count => _entries.Count;

    /// <summary>The term of entry <paramref name="index"/>. False if the entry was compacted away or does not exist yet.</summary>
    public bool TryTerm(ulong index, out ulong term)
    {
        if (index == Offset) { term = OffsetTerm; return true; }
        if (index < Offset || index > LastIndex) { term = 0; return false; }
        term = _entries[(int)(index - Offset - 1)].Term;
        return true;
    }

    /// <summary>The term of an entry, 0 if it is unknown (compacted or beyond the end).</summary>
    public ulong TermOrZero(ulong index) => TryTerm(index, out var t) ? t : 0;

    public bool MatchTerm(ulong index, ulong term) => TryTerm(index, out var t) && t == term;

    public Entry EntryAt(ulong index)
    {
        if (index <= Offset || index > LastIndex)
            throw new RaftInvariantException($"entry {index} is not in the log ({FirstIndex}..{LastIndex})");
        return _entries[(int)(index - Offset - 1)];
    }

    /// <summary>Election restriction (Raft paper 5.4.1): is a candidate's log at least as up to date as ours?</summary>
    public bool IsUpToDate(ulong lastIndex, ulong lastTerm) =>
        lastTerm > LastTerm || (lastTerm == LastTerm && lastIndex >= LastIndex);

    /// <summary>The index of the first entry that conflicts with ours (different term) or that we do not have; 0 if none.</summary>
    public ulong FindConflict(IReadOnlyList<Entry> entries)
    {
        foreach (var e in entries)
        {
            if (!MatchTerm(e.Index, e.Term))
            {
                // Entries at or below the offset are committed and therefore match by definition.
                if (e.Index <= Offset) continue;
                return e.Index;
            }
        }
        return 0;
    }

    /// <summary>
    /// For a rejected append: walks back from <paramref name="index"/> to the last entry whose term is at most
    /// <paramref name="term"/>. Lets the leader skip a whole term of conflicting entries in one round trip.
    /// </summary>
    public (ulong Index, ulong Term) FindConflictByTerm(ulong index, ulong term)
    {
        if (index > LastIndex) index = LastIndex;
        while (true)
        {
            if (!TryTerm(index, out var t)) return (index, 0);
            if (t <= term) return (index, t);
            if (index == 0) return (0, 0);
            index--;
        }
    }

    /// <summary>
    /// The follower side of AppendEntries. Returns false if our log does not contain the leader's previous entry;
    /// otherwise appends the new entries (truncating conflicts) and returns the index of the last new entry.
    /// </summary>
    public bool MaybeAppend(ulong prevIndex, ulong prevTerm, ulong leaderCommit, IReadOnlyList<Entry> entries,
        out ulong lastNewIndex, out bool truncated)
    {
        truncated = false;
        lastNewIndex = 0;
        // The caller answers appends below the commit index directly; since Offset <= Committed,
        // prevIndex is never inside the compacted prefix here.
        if (prevIndex < Offset)
            throw new RaftInvariantException($"append with prevIndex {prevIndex} below the compaction point {Offset}");
        if (!MatchTerm(prevIndex, prevTerm)) return false;

        lastNewIndex = prevIndex + (ulong)entries.Count;
        ulong conflict = FindConflict(entries);
        if (conflict != 0)
        {
            if (conflict <= Committed)
                throw new RaftInvariantException($"entry {conflict} conflicts with a committed entry (commit {Committed})");
            int start = (int)(conflict - entries[0].Index);
            truncated = conflict <= LastIndex;
            AppendFrom(entries, start);
        }
        CommitTo(Math.Min(leaderCommit, lastNewIndex));
        return true;
    }

    /// <summary>Appends entries, truncating our log at the first one's index if needed.</summary>
    public void Append(IReadOnlyList<Entry> entries) => AppendFrom(entries, 0);

    private void AppendFrom(IReadOnlyList<Entry> entries, int start)
    {
        if (start >= entries.Count) return;
        ulong first = entries[start].Index;
        if (first <= Offset)
            throw new RaftInvariantException($"cannot append at {first}: entries up to {Offset} are compacted");
        if (first > LastIndex + 1)
            throw new RaftInvariantException($"gap in the log: appending {first} after {LastIndex}");
        if (first <= LastIndex)
        {
            if (first <= Committed)
                throw new RaftInvariantException($"truncating committed entry {first} (commit {Committed})");
            _entries.RemoveRange((int)(first - Offset - 1), (int)(LastIndex - first + 1));
            if (Stable >= first) Stable = first - 1;
        }
        for (int i = start; i < entries.Count; i++) _entries.Add(entries[i]);
    }

    /// <summary>Entries in <c>[lo, hi)</c>, at most <paramref name="maxBytes"/> (but always at least one).</summary>
    public Entry[] Slice(ulong lo, ulong hi, int maxBytes)
    {
        if (lo <= Offset) throw new RaftInvariantException($"slice from {lo} but entries up to {Offset} are compacted");
        if (hi > LastIndex + 1) hi = LastIndex + 1;
        if (lo >= hi) return [];
        int from = (int)(lo - Offset - 1);
        int count = (int)(hi - lo);
        int size = 0, n = 0;
        while (n < count)
        {
            size += _entries[from + n].Size;
            if (n > 0 && size > maxBytes) break;
            n++;
        }
        return _entries.GetRange(from, n).ToArray();
    }

    /// <summary>Entries the host has not persisted yet.</summary>
    public Entry[] UnstableEntries() =>
        Stable >= LastIndex ? [] : _entries.GetRange((int)(Stable - Offset), (int)(LastIndex - Stable)).ToArray();

    public void StableTo(ulong index)
    {
        if (index > LastIndex) throw new RaftInvariantException($"stable index {index} beyond last {LastIndex}");
        if (index > Stable) Stable = index;
    }

    public bool MaybeCommit(ulong index, ulong term)
    {
        if (index > Committed && MatchTerm(index, term))
        {
            CommitTo(index);
            return true;
        }
        return false;
    }

    public void CommitTo(ulong index)
    {
        if (index <= Committed) return;
        if (index > LastIndex)
            throw new RaftInvariantException($"commit {index} is beyond the last index {LastIndex}");
        Committed = index;
    }

    public bool HasNextCommitted => Committed > Applied;

    /// <summary>Committed entries not yet handed to the state machine.</summary>
    public Entry[] NextCommittedEntries(int maxBytes)
    {
        if (Committed <= Applied) return [];
        return Slice(Applied + 1, Committed + 1, maxBytes);
    }

    public void AppliedTo(ulong index)
    {
        if (index == 0) return;
        if (index > Committed || index < Applied)
            throw new RaftInvariantException($"applied {index} out of range [{Applied}, {Committed}]");
        Applied = index;
    }

    /// <summary>Discards entries up to and including <paramref name="index"/> (log compaction).</summary>
    public void Compact(ulong index)
    {
        if (index <= Offset) return;
        if (index > Applied) throw new RaftInvariantException($"compacting {index} beyond applied {Applied}");
        if (index > Stable) throw new RaftInvariantException($"compacting {index} beyond stable {Stable}");
        ulong term = TermOrZero(index);
        _entries.RemoveRange(0, (int)(index - Offset));
        Offset = index;
        OffsetTerm = term;
    }

    /// <summary>Replaces the whole log with a snapshot point.</summary>
    public void Restore(SnapshotMeta snapshot)
    {
        _entries.Clear();
        Offset = snapshot.Index;
        OffsetTerm = snapshot.Term;
        Committed = snapshot.Index;
        Applied = snapshot.Index;
        Stable = snapshot.Index;
    }

    /// <summary>Iterates over entries in <c>(after, LastIndex]</c>.</summary>
    public IEnumerable<Entry> EntriesAfter(ulong after)
    {
        ulong from = Math.Max(after, Offset);
        for (ulong i = from + 1; i <= LastIndex; i++) yield return _entries[(int)(i - Offset - 1)];
    }
}
