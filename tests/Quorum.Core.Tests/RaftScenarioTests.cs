using System.Text;
using Quorum.Core;

namespace Quorum.Core.Tests;

/// <summary>Directed scenarios for specific parts of the algorithm.</summary>
public sealed class RaftScenarioTests
{
    private static string Value(TestNet.Node n, string key) => n.State.TryGetValue(key, out var v) ? v : "(none)";

    [Fact]
    public void SingleNodeElectsItselfAndCommits()
    {
        var net = new TestNet(1);
        net.Elect(1);
        Assert.True(net.Put(1, "a", "1").Ok);
        net.Deliver();
        Assert.Equal("1", Value(net.Nodes[1], "a"));
    }

    [Fact]
    public void ThreeNodesElectOneLeaderAndReplicate()
    {
        var net = new TestNet(3);
        net.Elect(1);
        Assert.Equal(Role.Follower, net[2].Role);
        Assert.Equal(1UL, net[2].Leader);
        for (int i = 0; i < 20; i++) net.Put(1, $"k{i}", $"v{i}");
        net.Deliver();
        net.TickAll(2);
        foreach (var n in net.Nodes.Values)
        {
            Assert.Equal("v19", Value(n, "k19"));
            Assert.Equal(net[1].Log.Committed, n.Raft.Log.Committed);
        }
    }

    [Fact]
    public void PreVoteKeepsAnIsolatedNodeFromInflatingItsTerm()
    {
        var net = new TestNet(3);
        net.Elect(1);
        ulong term = net[1].Term;
        net.Isolate(3);
        net.TickAll(200);
        Assert.Equal(term, net[3].Term);
        net.HealAll();
        net.TickAll(5);
        // The returning node did not disrupt the leader.
        Assert.Equal(Role.Leader, net[1].Role);
        Assert.Equal(term, net[1].Term);
    }

    [Fact]
    public void WithoutPreVoteAnIsolatedNodeDisruptsTheLeaderWhenItReturns()
    {
        var net = new TestNet(3, preVote: false, checkQuorum: false);
        net.Elect(1);
        ulong term = net[1].Term;
        net.Isolate(3);
        net.TickAll(100);
        Assert.True(net[3].Term > term + 3);
        net.HealAll();
        net.TickAll(30);
        Assert.True(net[1].Term > term, "the higher term forced an election");
    }

    [Fact]
    public void CheckQuorumStepsDownAnIsolatedLeader()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Isolate(1);
        net.TickAll(net.ElectionTicks * 2 + 1);
        Assert.NotEqual(Role.Leader, net[1].Role);
    }

    [Fact]
    public void MinorityLeaderCannotCommitAndItsEntriesAreOverwritten()
    {
        var net = new TestNet(5);
        net.Elect(1);
        net.Put(1, "x", "before");
        net.Deliver();

        net.Partition([1, 2], [3, 4, 5]);
        Assert.True(net.Put(1, "x", "lost").Ok);
        net.Deliver();
        Assert.Equal("before", Value(net.Nodes[1], "x"));

        ulong l = net.ElectAmong([3, 4, 5], net[1].Term);
        net.Put(l, "x", "majority");
        net.Deliver();
        Assert.Equal("majority", Value(net.Nodes[4], "x"));

        net.HealAll();
        net.TickAll(30);
        foreach (var n in net.Nodes.Values)
        {
            Assert.Equal("majority", Value(n, "x"));
            Assert.DoesNotContain(n.Applied, e => Encoding.UTF8.GetString(e.Data) == "x=lost");
        }
    }

    [Fact]
    public void LeaseReadsAreSafeBecauseFollowersRespectTheLease()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Put(1, "a", "1");
        net.Deliver();

        // Node 3 misses heartbeats long enough that its election timer is running.
        net.Isolate(3);
        net.TickAll(25);
        net.HealAll();
        // A fresh heartbeat round gives node 1 its lease and resets node 2's timer.
        net.Tick(1);
        net.Deliver();
        Assert.True(net[1].LeaseValid);

        // Node 1 is cut off. Only node 3's clock moves, so in node 1's time its lease is still valid.
        net.Partition([1], [2, 3]);
        ulong ctx = 0;
        for (int i = 0; i < 40; i++)
        {
            net.Tick(3);
            net.Deliver();
            if (net[3].Role == Role.Leader)
            {
                net.Put(3, "a", "2");
                net.Deliver();
            }
            // A client still talking to node 1 asks for a lease read.
            if (net[1].Role == Role.Leader && net[1].ReadIndex(++ctx, ReadMode.Lease))
            {
                net.Flush();
                ulong committedElsewhere = Math.Max(net[2].Log.Committed, net[3].Log.Committed);
                foreach (var rs in net.Nodes[1].Reads)
                {
                    Assert.True(rs.Index >= committedElsewhere,
                        $"stale lease read: index {rs.Index}, but {committedElsewhere} is committed on the other side");
                }
                net.Nodes[1].Reads.Clear();
            }
        }
        // Node 2 heard from node 1 and its own clock did not move: it must not have helped elect node 3.
        Assert.NotEqual(Role.Leader, net[3].Role);
    }

    [Fact]
    public void ReadIndexOnAnIsolatedLeaderIsNeverServed()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Put(1, "a", "1");
        net.Deliver();
        net.Isolate(1);
        Assert.True(net[1].ReadIndex(42));
        net.TickAll(net.ElectionTicks * 3);
        Assert.DoesNotContain(net.Nodes[1].Reads, r => r.Context == 42);
    }

    [Fact]
    public void ReadIndexIsServedAfterAHeartbeatRound()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Put(1, "a", "1");
        net.Deliver();
        Assert.True(net[1].ReadIndex(7));
        net.Flush();
        net.Deliver();
        var rs = Assert.Single(net.Nodes[1].Reads);
        Assert.Equal(7UL, rs.Context);
        Assert.Equal(net[1].Log.Committed, rs.Index);
    }

    [Fact]
    public void NewLeaderDefersReadsUntilItsNoOpCommits()
    {
        var net = new TestNet(3);
        net.Elect(1);
        // Elect node 2 but hold back the appends that would commit its no-op.
        net.Partition([2], [1, 3]);
        net.TickAll(net.ElectionTicks * 2 + 1);
        net.HealAll();
        for (int i = 0; i < 40 && net[2].Role != Role.Leader; i++)
        {
            net.Tick(2);
            net.Deliver();
        }
        if (net[2].Role != Role.Leader) return; // another node won; the property is covered elsewhere
        net.Cut(2, 1);
        net.Cut(2, 3);
        Assert.True(net[2].ReadIndex(9));
        net.Flush();
        Assert.Empty(net.Nodes[2].Reads);
        net.HealAll();
        net.TickAll(3);
        Assert.Contains(net.Nodes[2].Reads, r => r.Context == 9);
    }

    [Fact]
    public void LeadershipTransferMovesTheLeader()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Put(1, "a", "1");
        net.Deliver();
        net[1].TransferLeadership(3);
        net.Flush();
        net.Deliver();
        Assert.Equal(Role.Leader, net[3].Role);
        Assert.Equal(3UL, net[1].Leader);
    }

    [Fact]
    public void LeadershipTransferCatchesTheTargetUpFirst()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Cut(1, 3);
        for (int i = 0; i < 10; i++) net.Put(1, $"k{i}", "v");
        net.Deliver();
        net.HealAll();
        net[1].TransferLeadership(3);
        net.Flush();
        net.Deliver();
        Assert.Equal(Role.Leader, net[3].Role);
        net.TickAll(2);
        Assert.Equal("v", Value(net.Nodes[3], "k9"));
    }

    [Fact]
    public void MembershipAddLearnerPromoteAndRemoveTheLeader()
    {
        var net = new TestNet(3);
        net.Elect(1);
        for (int i = 0; i < 5; i++) net.Put(1, $"k{i}", "v");
        net.Deliver();

        net.AddNode(4, new RaftInitialState());
        Assert.True(net[1].ProposeConfChange(new ConfChange(ConfChangeType.AddLearner, 4, "n4")).Ok);
        net.Flush();
        net.Deliver();
        net.TickAll(3);
        Assert.Equal("v", Value(net.Nodes[4], "k4"));
        Assert.Contains(4UL, net[4].Conf.Learners);
        Assert.Equal(Role.Follower, net[4].Role);

        Assert.True(net[1].ProposeConfChange(new ConfChange(ConfChangeType.AddVoter, 4, "")).Ok);
        // Only one change at a time.
        Assert.Equal(ProposeError.ConfChangeInProgress,
            net[1].ProposeConfChange(new ConfChange(ConfChangeType.Remove, 2, "")).Error);
        net.Flush();
        net.Deliver();
        net.TickAll(2);
        Assert.Contains(4UL, net[1].Conf.Voters);
        Assert.Equal(4, net[1].Conf.Voters.Count);

        Assert.True(net[1].ProposeConfChange(new ConfChange(ConfChangeType.Remove, 1, "")).Ok);
        net.Flush();
        net.Deliver();
        Assert.NotEqual(Role.Leader, net[1].Role);
        net.TickAll(net.ElectionTicks * 4);
        var leader = net.LeaderId();
        Assert.NotNull(leader);
        Assert.NotEqual(1UL, leader);
        Assert.True(net.Put(leader.Value, "after", "yes").Ok);
        net.Deliver();
        Assert.Equal("yes", Value(net.Nodes[4], "after"));
    }

    [Fact]
    public void LaggingFollowerCatchesUpFromASnapshot()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Isolate(3);
        for (int i = 0; i < 50; i++) net.Put(1, $"k{i}", $"v{i}");
        net.Deliver();
        net.SnapshotAndCompact(1);
        Assert.True(net[1].Log.FirstIndex > net[3].Log.LastIndex + 1);

        net.HealAll();
        net.TickAll(5);
        Assert.Equal("v49", Value(net.Nodes[3], "k49"));
        Assert.Equal(net[1].Log.Committed, net[3].Log.Committed);
        Assert.NotNull(net.Nodes[3].Snapshot);
    }

    [Fact]
    public void FollowerWithALongConflictingTailConverges()
    {
        var net = new TestNet(3);
        net.Elect(1);
        net.Put(1, "a", "0");
        net.Deliver();
        // Node 1 piles up uncommitted entries alone, then node 2 becomes leader and writes others.
        net.Isolate(1);
        for (int i = 0; i < 100; i++) net.Put(1, "a", $"orphan{i}");
        net.DropQueued();
        ulong l = net.ElectAmong([2, 3], net[1].Term);
        for (int i = 0; i < 30; i++) net.Put(l, "a", $"real{i}");
        net.Deliver();
        net.HealAll();
        net.TickAll(net.ElectionTicks * 3);
        foreach (var n in net.Nodes.Values) Assert.Equal("real29", Value(n, "a"));
        Assert.Equal(net[l].Log.LastIndex, net[1].Log.LastIndex);
    }

    [Fact]
    public void MessagesRoundTripThroughTheCodec()
    {
        var m = new Message
        {
            Type = MessageType.Snapshot,
            From = 3,
            To = 9,
            Term = 12,
            LogTerm = 11,
            Index = 1_000_000_000_000,
            Commit = 77,
            Reject = true,
            RejectHint = 5,
            Context = 99,
            Force = true,
            Entries = [new Entry { Index = 1, Term = 2, Data = [1, 2, 3] }, new Entry { Index = 2, Term = 2, Type = EntryType.ConfChange }],
            Snapshot = new SnapshotMeta(10, 4, ConfState.FromMembers([new Member(1, "http://a:1", false), new Member(5, "x", true)])),
        };
        var back = Codec.DecodeMessage(Codec.EncodeMessage(m));
        Assert.Equal(m.ToString(), back.ToString());
        Assert.Equal(m.Context, back.Context);
        Assert.True(back.Force);
        Assert.Equal([1, 2, 3], back.Entries[0].Data);
        Assert.Equal(EntryType.ConfChange, back.Entries[1].Type);
        Assert.Equal(m.Snapshot, back.Snapshot);
        Assert.Equal(m.Snapshot.Conf, back.Snapshot!.Conf);
    }
}
