using System.Diagnostics;

namespace Quorum.Linearizability;

/// <summary>
/// The verdict for one key. When it is not linearizable, <c>BestOrder</c> is the longest valid order (operation
/// ids) found for a prefix of the history, and <c>Stuck</c> the operations none of which could come next.
/// </summary>
public sealed record KeyResult(
    string Key,
    bool? Linearizable,
    int Operations,
    TimeSpan Elapsed,
    IReadOnlyList<int> BestOrder,
    IReadOnlyList<int> Stuck,
    long StatesExplored);

public sealed record CheckResult(bool? Linearizable, IReadOnlyList<KeyResult> Keys, IReadOnlyList<Operation> Operations)
{
    public IEnumerable<KeyResult> Failures => Keys.Where(k => k.Linearizable == false);
}

/// <summary>
/// Checks that a history of operations on a map of registers is linearizable: that there is a single total
/// order of the operations, consistent with their real-time order, in which every result is what a
/// sequential register would have returned.
/// <para>
/// This is the Wing–Gong algorithm with Lowe's memoization (the same approach as Knossos and Porcupine): a
/// depth-first search that repeatedly linearizes some operation whose call precedes every pending return,
/// caching (set of linearized operations, register state) pairs it has already explored. Keys are independent
/// (P-compositionality), so each key is checked separately and in parallel.
/// </para>
/// <para>
/// Operations whose outcome is unknown (timeouts) are treated as returning at the end of time: they may take
/// effect at any point after their call, or never (linearized last, where nobody observes them).
/// </para>
/// </summary>
public static class Checker
{
    public static CheckResult Check(IReadOnlyList<Operation> history, TimeSpan? timeoutPerKey = null)
    {
        var ops = history.Where(o => o.Outcome != OpOutcome.Fail)
            // A read with an unknown outcome tells us nothing.
            .Where(o => !(o.Kind == OpKind.Read && o.Outcome == OpOutcome.Unknown))
            .ToList();
        var byKey = ops.GroupBy(o => o.Key).OrderBy(g => g.Key, StringComparer.Ordinal).ToList();
        var results = new KeyResult[byKey.Count];
        Parallel.For(0, byKey.Count, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
            i => results[i] = CheckKey(byKey[i].Key, byKey[i].ToList(), timeoutPerKey ?? TimeSpan.FromMinutes(2)));

        bool? overall = results.Any(r => r.Linearizable == false) ? false
            : results.Any(r => r.Linearizable is null) ? null : true;
        return new CheckResult(overall, results, ops);
    }

    // ------------------------------------------------------------------
    // The register model
    // ------------------------------------------------------------------

    /// <summary>Applies <paramref name="op"/> to a register holding <paramref name="state"/> (null = absent).</summary>
    private static bool Step(string? state, Operation op, out string? next)
    {
        next = state;
        bool unknown = op.Outcome == OpOutcome.Unknown;
        switch (op.Kind)
        {
            case OpKind.Read:
                return state == op.Observed;
            case OpKind.Write:
                next = op.Value;
                return true;
            case OpKind.Delete:
                next = null;
                return true;
            case OpKind.Cas:
                bool holds = state == op.Expected;
                if (unknown)
                {
                    // We did not see the result: whatever the state, the operation is possible.
                    if (holds) next = op.Value;
                    return true;
                }
                if (op.CasSucceeded)
                {
                    if (!holds) return false;
                    next = op.Value;
                    return true;
                }
                return !holds;
            default:
                return false;
        }
    }

    // ------------------------------------------------------------------
    // Search
    // ------------------------------------------------------------------

    private sealed class Event
    {
        public required int Op;      // index into the key's operation list
        public required bool IsCall;
        public long Time;
        public Event? Prev;
        public Event? Next;
        public Event? Match;         // call <-> return
    }

    /// <summary>
    /// A (set of linearized operations, register state) pair. Operations are numbered in call order, so a set is
    /// all ones up to its first missing operation and all zeros after its last member: only the words in between
    /// (the "window", starting at word <c>Lo</c>) are stored and compared. For a long history that is a few words
    /// instead of thousands.
    /// </summary>
    private readonly struct CacheKey(ulong[] words, int offset, int lo, int length, string? state, int hash) : IEquatable<CacheKey>
    {
        private readonly ulong[] _words = words;
        private readonly int _offset = offset;
        public readonly int Lo = lo;
        public readonly int Length = length;
        public readonly string? State = state;
        private readonly int _hash = hash;

        public ReadOnlySpan<ulong> Window => _words.AsSpan(_offset, Length);

        public CacheKey Compact() => new(Window.ToArray(), 0, Lo, Length, State, _hash);

        public bool Equals(CacheKey other) =>
            _hash == other._hash && Lo == other.Lo && Length == other.Length && State == other.State &&
            Window.SequenceEqual(other.Window);

        public override bool Equals(object? obj) => obj is CacheKey k && Equals(k);
        public override int GetHashCode() => _hash;
    }

    /// <summary>
    /// Zobrist hashing: every operation gets a random 64-bit value, and the hash of a set of operations is the XOR
    /// of their values. Adding or removing an operation updates it in O(1), instead of rehashing the whole bitset.
    /// </summary>
    private static ulong[] ZobristTable(int n)
    {
        var rand = new Random(0x5EED);
        var table = new ulong[n];
        for (int i = 0; i < n; i++) table[i] = (ulong)rand.NextInt64() ^ ((ulong)rand.NextInt64() << 32);
        return table;
    }

    private static KeyResult CheckKey(string key, List<Operation> ops, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        int n = ops.Count;
        if (n == 0) return new KeyResult(key, true, 0, sw.Elapsed, [], [], 0);

        // Cheap necessary condition first: a read can only return a value that some write started before the
        // read ended. A read of a value nobody wrote in time is impossible, and no search is needed to say so.
        var firstWrite = new Dictionary<string, long>();
        foreach (var o in ops)
        {
            if (o.Kind is OpKind.Write or OpKind.Cas && o.Value is not null &&
                (!firstWrite.TryGetValue(o.Value, out long t) || o.Call < t))
                firstWrite[o.Value] = o.Call;
        }
        foreach (var o in ops)
        {
            if (o.Kind == OpKind.Read && o.Observed is not null &&
                (!firstWrite.TryGetValue(o.Observed, out long t) || t > o.Return))
                return new KeyResult(key, false, n, sw.Elapsed, [], [o.Id], 0);
        }

        // Number the operations in call order (the cache window relies on it).
        ops = ops.OrderBy(o => o.Call).ThenBy(o => o.Id).ToList();

        // Build the event list: calls and returns sorted by time. At equal times calls come first, which treats
        // the operations as overlapping (the permissive reading of a tie).
        var events = new List<Event>(2 * n);
        for (int i = 0; i < n; i++)
        {
            var call = new Event { Op = i, IsCall = true, Time = ops[i].Call };
            long ret = ops[i].Outcome == OpOutcome.Unknown ? long.MaxValue : Math.Max(ops[i].Return, ops[i].Call);
            var r = new Event { Op = i, IsCall = false, Time = ret };
            call.Match = r;
            r.Match = call;
            events.Add(call);
            events.Add(r);
        }
        events.Sort((a, b) =>
        {
            int c = a.Time.CompareTo(b.Time);
            if (c != 0) return c;
            if (a.IsCall != b.IsCall) return a.IsCall ? -1 : 1;
            return a.Op.CompareTo(b.Op);
        });
        var head = new Event { Op = -1, IsCall = false };
        Event prev = head;
        foreach (var e in events)
        {
            prev.Next = e;
            e.Prev = prev;
            prev = e;
        }

        int words = (n + 63) / 64;
        var linearized = new ulong[words];
        var zobrist = ZobristTable(n);
        ulong setHash = 0;
        int first = 0;   // the first operation not linearized
        int last = -1;   // the last operation linearized
        string? state = null;
        var cache = new HashSet<CacheKey>();
        var stack = new Stack<(Event Call, string? State, int First, int Last)>();

        bool IsSet(int i) => (linearized[i >> 6] & (1UL << (i & 63))) != 0;
        var order = new List<int>();
        List<int> bestOrder = [];
        List<int> stuck = [];
        long explored = 0;

        List<int> Ids(List<int> local) => local.Select(i => ops[i].Id).ToList();

        Event? entry = head.Next;
        while (head.Next is not null)
        {
            if ((explored & 0xFFF) == 0 && sw.Elapsed > timeout)
                return new KeyResult(key, null, n, sw.Elapsed, Ids(bestOrder), [], explored);

            if (entry!.IsCall)
            {
                var op = ops[entry.Op];
                explored++;
                if (Step(state, op, out var next))
                {
                    linearized[entry.Op >> 6] |= 1UL << (entry.Op & 63);
                    setHash ^= zobrist[entry.Op];
                    int newFirst = first;
                    while (newFirst < n && IsSet(newFirst)) newFirst++;
                    int newLast = Math.Max(last, entry.Op);
                    int lo = newFirst >> 6;
                    int length = Math.Max(0, (newLast >> 6) - lo + 1);
                    int hash = HashCode.Combine(setHash, next);
                    // Look up with the live bitset; copy the window only when the state is new.
                    var probe = new CacheKey(linearized, lo, lo, length, next, hash);
                    if (!cache.Contains(probe))
                    {
                        cache.Add(probe.Compact());
                        stack.Push((entry, state, first, last));
                        state = next;
                        first = newFirst;
                        last = newLast;
                        order.Add(entry.Op);
                        Lift(entry);
                        entry = head.Next;
                        continue;
                    }
                    linearized[entry.Op >> 6] &= ~(1UL << (entry.Op & 63));
                    setHash ^= zobrist[entry.Op];
                }
                entry = entry.Next;
            }
            else
            {
                // A return whose operation is not linearized yet: everything before it is exhausted. Backtrack.
                if (order.Count > bestOrder.Count || (stuck.Count == 0 && order.Count >= bestOrder.Count))
                {
                    bestOrder = [.. order];
                    stuck = PendingCalls(head, entry);
                }
                if (stack.Count == 0)
                    return new KeyResult(key, false, n, sw.Elapsed, Ids(bestOrder), Ids(stuck), explored);
                var (call, s, f, l) = stack.Pop();
                state = s;
                first = f;
                last = l;
                order.RemoveAt(order.Count - 1);
                linearized[call.Op >> 6] &= ~(1UL << (call.Op & 63));
                setHash ^= zobrist[call.Op];
                Unlift(call);
                entry = call.Next;
            }
        }
        return new KeyResult(key, true, n, sw.Elapsed, [], [], explored);
    }

    /// <summary>The operations whose calls come before <paramref name="stop"/> (the candidates that all failed).</summary>
    private static List<int> PendingCalls(Event head, Event stop)
    {
        var list = new List<int>();
        for (var e = head.Next; e is not null && e != stop; e = e.Next)
        {
            if (e.IsCall) list.Add(e.Op);
        }
        list.Add(stop.Op);
        return list.Distinct().ToList();
    }

    private static void Lift(Event call)
    {
        call.Prev!.Next = call.Next;
        if (call.Next is not null) call.Next.Prev = call.Prev;
        var ret = call.Match!;
        ret.Prev!.Next = ret.Next;
        if (ret.Next is not null) ret.Next.Prev = ret.Prev;
    }

    private static void Unlift(Event call)
    {
        var ret = call.Match!;
        ret.Prev!.Next = ret;
        if (ret.Next is not null) ret.Next.Prev = ret;
        call.Prev!.Next = call;
        if (call.Next is not null) call.Next.Prev = call;
    }

    /// <summary>The operations on one key.</summary>
    public static IReadOnlyList<Operation> OperationsOf(CheckResult result, KeyResult key) =>
        result.Operations.Where(o => o.Key == key.Key).ToList();
}
