using Quorum.Linearizability;

namespace Quorum.Linearizability.Tests;

public sealed class CheckerTests
{
    private int _id;

    private Operation W(string v, long call, long ret, string key = "x", OpOutcome outcome = OpOutcome.Ok) =>
        new() { Id = ++_id, Client = _id, Key = key, Kind = OpKind.Write, Value = v, Call = call, Return = ret, Outcome = outcome };

    private Operation R(string? seen, long call, long ret, string key = "x") =>
        new() { Id = ++_id, Client = _id, Key = key, Kind = OpKind.Read, Observed = seen, Call = call, Return = ret, Outcome = OpOutcome.Ok };

    private Operation Cas(string? expected, string? value, bool ok, long call, long ret, OpOutcome outcome = OpOutcome.Ok) =>
        new() { Id = ++_id, Client = _id, Key = "x", Kind = OpKind.Cas, Expected = expected, Value = value, CasSucceeded = ok, Call = call, Return = ret, Outcome = outcome };

    [Fact]
    public void SequentialHistoryIsLinearizable()
    {
        var r = Checker.Check([R(null, 0, 1), W("a", 2, 3), R("a", 4, 5), W("b", 6, 7), R("b", 8, 9)]);
        Assert.True(r.Linearizable);
    }

    [Fact]
    public void StaleReadAfterACompletedWriteIsNotLinearizable()
    {
        var r = Checker.Check([W("a", 0, 10), R(null, 20, 30)]);
        Assert.False(r.Linearizable);
        var k = Assert.Single(r.Failures);
        Assert.Equal("x", k.Key);
        Assert.Contains(2, k.Stuck);
    }

    [Fact]
    public void ConcurrentReadMayObserveEitherValue()
    {
        Assert.True(Checker.Check([W("a", 0, 10), R(null, 5, 6)]).Linearizable);
        _id = 0;
        Assert.True(Checker.Check([W("a", 0, 10), R("a", 5, 6)]).Linearizable);
    }

    [Fact]
    public void ReadsMustNotGoBackInTime()
    {
        // Two concurrent writes; a read sees the second, a later read sees the first again.
        var r = Checker.Check([W("a", 0, 10), W("b", 0, 10), R("b", 20, 21), R("a", 30, 31)]);
        Assert.False(r.Linearizable);
    }

    [Fact]
    public void UnknownWriteMayTakeEffectLateOrNever()
    {
        Assert.True(Checker.Check([W("a", 0, 1, outcome: OpOutcome.Unknown), R(null, 10, 11), R("a", 20, 21)]).Linearizable);
        _id = 0;
        Assert.True(Checker.Check([W("a", 0, 1, outcome: OpOutcome.Unknown), R(null, 10, 11), R(null, 20, 21)]).Linearizable);
        _id = 0;
        // Once observed, it cannot be undone.
        Assert.False(Checker.Check([W("a", 0, 1, outcome: OpOutcome.Unknown), R("a", 10, 11), R(null, 20, 21)]).Linearizable);
    }

    [Fact]
    public void CompareAndSetSemantics()
    {
        Assert.True(Checker.Check([Cas(null, "a", true, 0, 1), Cas(null, "b", false, 2, 3), Cas("a", "b", true, 4, 5), R("b", 6, 7)]).Linearizable);
        _id = 0;
        // Two CAS from absent cannot both succeed.
        Assert.False(Checker.Check([Cas(null, "a", true, 0, 10), Cas(null, "b", true, 0, 10)]).Linearizable);
        _id = 0;
        // A CAS with an unknown outcome may explain a later read.
        Assert.True(Checker.Check([W("a", 0, 1), Cas("a", "c", false, 2, 3, OpOutcome.Unknown), R("c", 10, 11)]).Linearizable);
    }

    [Fact]
    public void FailedOperationsAreIgnored()
    {
        var failed = W("ghost", 0, 1) with { Outcome = OpOutcome.Fail };
        Assert.True(Checker.Check([failed, R(null, 5, 6)]).Linearizable);
    }

    [Fact]
    public void KeysAreCheckedIndependently()
    {
        var r = Checker.Check([W("a", 0, 1, key: "k1"), R("a", 2, 3, key: "k1"), W("a", 0, 1, key: "k2"), R(null, 2, 3, key: "k2")]);
        Assert.False(r.Linearizable);
        Assert.Equal("k2", Assert.Single(r.Failures).Key);
        Assert.Equal(true, r.Keys.Single(k => k.Key == "k1").Linearizable);
    }

    [Fact]
    public void HistoryFilesRoundTrip()
    {
        var ops = new List<Operation> { W("a", 0, 1), R("a", 2, 3), Cas("a", null, true, 4, 5) };
        string path = Path.GetTempFileName();
        try
        {
            HistoryFile.Save(path, ops);
            Assert.Equal(ops, HistoryFile.Load(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>A large concurrent history produced by a real (atomic) register must check quickly.</summary>
    [Fact]
    public void LargeConcurrentLinearizableHistoryChecksQuickly()
    {
        var ops = GenerateAtomicHistory(new Random(42), steps: 4000, clients: 8);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = Checker.Check(ops);
        Assert.True(result.Linearizable);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(20), $"took {sw.Elapsed}");

        // Corrupt one read in the middle: now it must be rejected.
        int victim = ops.FindIndex(o => o.Kind == OpKind.Read && o.Observed is not null && o.Id > ops.Count / 2);
        var original = ops[victim];
        ops[victim] = original with { Observed = "never-written" };
        Assert.False(Checker.Check(ops).Linearizable);

        // A stale read is harder: the value did exist, long ago. Only the search can reject it.
        string oldValue = ops.First(o => o.Kind == OpKind.Write).Value!;
        ops[victim] = original with { Observed = oldValue };
        sw.Restart();
        var stale = Checker.Check(ops);
        Assert.False(stale.Linearizable);
        Assert.Contains(original.Id, stale.Failures.Single().Stuck);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(20), $"took {sw.Elapsed}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void RandomAtomicHistoriesAreLinearizable(int seed)
    {
        var ops = GenerateAtomicHistory(new Random(seed), steps: 600, clients: 5);
        Assert.True(Checker.Check(ops).Linearizable);
    }

    /// <summary>
    /// Simulates clients against a truly atomic register: each operation takes effect at a random instant
    /// inside its interval, so the resulting history is linearizable by construction.
    /// </summary>
    private static List<Operation> GenerateAtomicHistory(Random rand, int steps, int clients)
    {
        var ops = new List<Operation>();
        string? register = null;
        long time = 0;
        int id = 0;
        var inflight = new List<(Operation Op, long EffectAt)>();
        for (int step = 0; step < steps; step++)
        {
            time += rand.Next(1, 5);
            while (inflight.Count < clients)
            {
                int kind = rand.Next(10);
                long call = time;
                long effect = call + rand.Next(1, 40);
                Operation op = kind switch
                {
                    < 5 => new Operation { Id = ++id, Client = inflight.Count, Key = "x", Kind = OpKind.Read, Call = call, Return = 0, Outcome = OpOutcome.Ok },
                    < 8 => new Operation { Id = ++id, Client = inflight.Count, Key = "x", Kind = OpKind.Write, Value = $"v{id}", Call = call, Return = 0, Outcome = OpOutcome.Ok },
                    _ => new Operation { Id = ++id, Client = inflight.Count, Key = "x", Kind = OpKind.Cas, Expected = register, Value = $"v{id}", Call = call, Return = 0, Outcome = OpOutcome.Ok },
                };
                inflight.Add((op, effect));
            }
            foreach (var item in inflight.Where(f => f.EffectAt <= time).OrderBy(f => f.EffectAt).ToList())
            {
                inflight.Remove(item);
                var op = item.Op;
                long ret = time + rand.Next(0, 20);
                switch (op.Kind)
                {
                    case OpKind.Read:
                        ops.Add(op with { Observed = register, Return = ret });
                        break;
                    case OpKind.Write:
                        register = op.Value;
                        ops.Add(op with { Return = ret });
                        break;
                    default:
                        bool ok = register == op.Expected;
                        if (ok) register = op.Value;
                        ops.Add(op with { CasSucceeded = ok, Return = ret });
                        break;
                }
            }
        }
        return ops;
    }
}
