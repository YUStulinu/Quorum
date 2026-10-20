using Quorum.Core;

namespace Quorum.Core.Tests;

/// <summary>Scenarios taken from the Raft paper and dissertation, reproduced message by message.</summary>
public sealed class PaperScenarioTests(Xunit.Abstractions.ITestOutputHelper output)
{
    private void Dump(TestNet net, string step)
    {
        foreach (var (id, n) in net.Nodes)
            output.WriteLine($"{step} S{id}: {n.Raft.Role} t{n.Raft.Term} last={n.Raft.Log.LastIndex}@{n.Raft.Log.LastTerm} commit={n.Raft.Log.Committed} t7={n.Raft.Log.TermOrZero(7)}");
    }

    private static void AssertAppliedLogsAgree(TestNet net)
    {
        var seen = new Dictionary<ulong, (ulong Term, ulong Node)>();
        foreach (var (id, n) in net.Nodes)
        {
            foreach (var e in n.Applied)
            {
                if (seen.TryGetValue(e.Index, out var other))
                    Assert.True(other.Term == e.Term,
                        $"index {e.Index}: node {other.Node} applied term {other.Term}, node {id} applied term {e.Term}");
                else
                    seen[e.Index] = (e.Term, id);
            }
        }
    }

    /// <summary>
    /// Figure 8 of the Raft paper: a leader must not commit an entry from an earlier term just because it is
    /// stored on a majority, since a later leader could still overwrite it.
    /// </summary>
    [Fact]
    public void Figure8_OldTermEntriesAreNotCommittedByCounting()
    {
        // One entry per append, no check-quorum (the figure predates it), so every step can be staged.
        var net = new TestNet(5, checkQuorum: false, maxMessageBytes: 1);

        // (a) S1 leads term 2 and replicates entry 7 only to S2.
        net.Elect(1);
        net.TickAll(2);
        net.Partition([1], [3, 4, 5]);
        net.Put(1, "x", "from-term-2");
        net.Deliver();
        Assert.Equal(7UL, net[2].Log.LastIndex);

        Dump(net, "a");
        // (b) S1 "crashes"; S5 wins term 3 with S3 and S4, writes its no-op at index 7, then crashes too.
        net.HealAll();
        net.Isolate(1);
        net.Isolate(2);
        for (int i = 0; i < 100 && net[5].Role != Role.Leader; i++)
        {
            net.Tick(5);
            for (int r = 0; r < 4 && net[5].Role != Role.Leader; r++) net.Deliver(maxRounds: 1);
        }
        Assert.Equal(Role.Leader, net[5].Role);
        net.Isolate(5);
        net.DropQueued();
        Assert.Equal(6UL, net[3].Log.LastIndex);

        Dump(net, "b");
        // (c) S1 comes back and wins term 4 with S2 and S3; it gets entry 7 (term 2) onto S3. Stop there.
        net.HealAll();
        net.Isolate(5);
        net.Cut(1, 4);
        net.Isolate(4);
        net.Cut(1, 4);
        for (int i = 0; i < 200 && !(net[3].Log.LastIndex == 7 && net[3].Log.TermOrZero(7) == net[1].Log.TermOrZero(7)); i++)
        {
            net.Tick(1);
            net.Deliver(maxRounds: 1);
        }
        // Let S3's acknowledgement reach S1 (a buggy leader would now commit entry 7), then crash S1
        // before its no-op reaches S3.
        net.Deliver(maxRounds: 1);
        net.Isolate(1);
        net.DropQueued();

        Dump(net, "c");
        // (d) S5 comes back and wins term 5 with S3 and S4 (its last term, 3, beats their 2) and overwrites entry 7.
        net.HealAll();
        net.Isolate(1);
        for (int i = 0; i < 200; i++)
        {
            net.Tick(5);
            net.Deliver();
            if (net[5].Role == Role.Leader && net[5].Term > 4) break;
        }
        net.TickAll(3);
        Dump(net, "d");
        AssertAppliedLogsAgree(net);
    }

    /// <summary>A configuration entry that is truncated must stop counting (dissertation 4.1).</summary>
    [Fact]
    public void TruncatedConfigurationChangeIsRolledBack()
    {
        var net = new TestNet(5);
        net.Elect(1);
        net.TickAll(2);
        net.Partition([1], [3, 4, 5]);
        Assert.True(net[1].ProposeConfChange(new ConfChange(ConfChangeType.AddLearner, 9, "n9")).Ok);
        net.Flush();
        net.Deliver();
        Assert.Contains(9UL, net[2].Conf.Learners);

        net.HealAll();
        net.Isolate(1);
        ulong leader = net.ElectAmong([3, 4, 5], net[1].Term);
        net.Put(leader, "a", "1");
        net.Deliver();
        net.TickAll(3);
        Assert.DoesNotContain(9UL, net[2].Conf.Members.Keys);
        Assert.Equal(net[leader].Conf, net[2].Conf);
    }
}
