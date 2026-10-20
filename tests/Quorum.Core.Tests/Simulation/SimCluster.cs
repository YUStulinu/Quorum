using System.Text;
using Quorum.Core;

namespace Quorum.Core.Tests.Simulation;

/// <summary>Raised when the simulated cluster breaks a Raft safety property.</summary>
public sealed class SafetyViolation(string message) : Exception(message);

/// <summary>What one simulated node keeps on "disk": it survives crashes, the in-memory RaftNode does not.</summary>
public sealed class SimStorage
{
    public HardState Hard = HardState.Empty;
    public ulong Offset;
    public ulong OffsetTerm;
    public List<Entry> Entries = [];
    public SnapshotMeta? Snapshot;
    public Dictionary<string, string> SnapshotData = [];
    public ulong Applied;
    public ConfState AppliedConf = ConfState.Empty;
    public Dictionary<string, string> State = [];

    public ulong LastIndex => Offset + (ulong)Entries.Count;

    public RaftInitialState InitialState() => new()
    {
        HardState = Hard,
        LogOffset = Offset,
        LogOffsetTerm = OffsetTerm,
        Entries = Entries.ToList(),
        Snapshot = Snapshot,
        Applied = Applied,
    };

    public void Append(Entry[] entries)
    {
        if (entries.Length == 0) return;
        ulong first = entries[0].Index;
        if (first <= Offset) throw new SafetyViolation($"storage append at {first} below offset {Offset}");
        if (first <= LastIndex) Entries.RemoveRange((int)(first - Offset - 1), (int)(LastIndex - first + 1));
        if (first != LastIndex + 1) throw new SafetyViolation($"storage gap: {first} after {LastIndex}");
        Entries.AddRange(entries);
    }

    public void Compact(ulong index)
    {
        if (index <= Offset) return;
        var e = Entries[(int)(index - Offset - 1)];
        Entries.RemoveRange(0, (int)(index - Offset));
        Offset = index;
        OffsetTerm = e.Term;
    }
}

public sealed class SimNode
{
    public required ulong Id { get; init; }
    public required SimStorage Storage { get; init; }
    public RaftNode? Raft { get; set; }
    public bool Alive => Raft is not null;
    /// <summary>Linearizable reads waiting for their ReadState or for the applied index to catch up.</summary>
    public Dictionary<ulong, PendingSimRead> Reads { get; } = [];
    public List<ReadState> ReadyReads { get; } = [];
}

public sealed record PendingSimRead(ulong Context, string Key, ulong MinIndex, long IssuedAt);

public sealed class SimOptions
{
    public int Seed { get; init; } = 1;
    public int InitialNodes { get; init; } = 3;
    public int MaxNodes { get; init; } = 12;
    public double DropRate { get; init; } = 0.02;
    public int MaxDelayTicks { get; init; } = 2;
    public int SnapshotEvery { get; init; } = 25;
    public int SnapshotTrail { get; init; } = 5;
    public ReadMode ReadMode { get; init; } = ReadMode.ReadIndex;
    public bool Trace { get; init; }
}

/// <summary>
/// A whole Raft cluster in one thread, driven by a seeded random generator: same seed, same run.
/// <para>
/// The network delays, reorders and drops messages, and can be partitioned. Nodes can crash at any point
/// (losing everything not persisted) and restart from their storage. Snapshots, log compaction and membership
/// changes happen along the way. After every step the simulator checks the safety properties of Raft:
/// election safety, leader completeness, state machine safety, and linearizability of reads.
/// </para>
/// </summary>
public sealed class SimCluster
{
    private readonly record struct InFlight(long DeliverAt, long Seq, Message Message, Dictionary<string, string>? SnapshotData);

    private readonly SimOptions _opt;
    private readonly Random _rand;
    private readonly PriorityQueue<InFlight, (long, long)> _network = new();
    private long _seq;
    private ulong _readCtx;
    private int _valueSeq;
    private ulong _nextId;

    public Dictionary<ulong, SimNode> Nodes { get; } = [];
    public long Now { get; private set; }
    public HashSet<ulong>? PartitionA { get; private set; }

    // Safety bookkeeping.
    private readonly Dictionary<ulong, ulong> _leaderOfTerm = [];
    private readonly Dictionary<ulong, (ulong Term, string Data)> _applied = [];
    public ulong MaxCommitted { get; private set; }
    public int ReadsServed { get; private set; }
    public int Proposed { get; private set; }
    public int Elections => _leaderOfTerm.Count;
    public List<string> Log { get; } = [];

    public SimCluster(SimOptions options)
    {
        _opt = options;
        _rand = new Random(options.Seed);
        _nextId = (ulong)options.InitialNodes + 1;
        var initial = Enumerable.Range(1, options.InitialNodes).Select(i => (ulong)i).ToList();
        foreach (var id in initial)
        {
            var storage = new SimStorage();
            // Bootstrap: every initial node starts with the same configuration entries at term 1.
            ulong idx = 0;
            foreach (var peer in initial)
            {
                storage.Entries.Add(new Entry
                {
                    Index = ++idx,
                    Term = 1,
                    Type = EntryType.ConfChange,
                    Data = Codec.EncodeConfChange(new ConfChange(ConfChangeType.AddVoter, peer, $"sim://{peer}")),
                });
            }
            storage.Hard = new HardState(1, 0, idx);
            Nodes[id] = new SimNode { Id = id, Storage = storage };
            Start(Nodes[id]);
        }
    }

    private void Note(string s)
    {
        if (_opt.Trace) Console.WriteLine($"[{Now}] {s}");
        Log.Add($"[{Now}] {s}");
        if (Log.Count > 400) Log.RemoveRange(0, 100);
    }

    public void Start(SimNode n)
    {
        var storage = n.Storage;
        n.Raft = new RaftNode(new RaftOptions
        {
            Id = n.Id,
            ElectionTicks = 10,
            HeartbeatTicks = 2,
            MaxMessageBytes = 400,
            MaxInflight = 8,
            Seed = _rand.Next(),
        }, storage.InitialState(), () => storage.Snapshot);
        n.Reads.Clear();
        n.ReadyReads.Clear();
        Note($"node {n.Id} started (term {storage.Hard.Term}, last {storage.LastIndex}, applied {storage.Applied})");
    }

    public void Crash(SimNode n)
    {
        if (!n.Alive) return;
        n.Raft = null;
        n.Reads.Clear();
        n.ReadyReads.Clear();
        Note($"node {n.Id} crashed");
    }

    public void Partition(HashSet<ulong> sideA)
    {
        PartitionA = sideA;
        Note($"partition {{{string.Join(",", sideA)}}} | rest");
    }

    public void Heal()
    {
        PartitionA = null;
        Note("network healed");
    }

    private bool Connected(ulong a, ulong b) =>
        PartitionA is null || PartitionA.Contains(a) == PartitionA.Contains(b);

    public SimNode? Leader()
    {
        SimNode? best = null;
        foreach (var n in Nodes.Values)
        {
            if (n.Raft is { Role: Role.Leader } r && (best is null || r.Term > best.Raft!.Term)) best = n;
        }
        return best;
    }

    // ------------------------------------------------------------------
    // Driving
    // ------------------------------------------------------------------

    /// <summary>Runs one tick: every live node ticks, due messages are delivered, Ready states processed.</summary>
    public void Step()
    {
        Now++;
        foreach (var n in Nodes.Values)
        {
            n.Raft?.Tick();
            Process(n);
        }
        while (_network.Count > 0 && _network.Peek().DeliverAt <= Now)
        {
            var f = _network.Dequeue();
            if (!Nodes.TryGetValue(f.Message.To, out var to) || !to.Alive || !Connected(f.Message.From, f.Message.To))
            {
                if (f.Message.Type == MessageType.Snapshot) ReportSnapshotFailure(f.Message);
                continue;
            }
            if (f.Message.Type == MessageType.Snapshot)
            {
                _pendingSnapshotData[(to.Id, f.Message.Snapshot!.Index)] = f.SnapshotData!;
                // The sender learns the transfer finished.
                if (Nodes.TryGetValue(f.Message.From, out var from) && from.Alive)
                    from.Raft!.ReportSnapshot(to.Id, true);
            }
            to.Raft!.Step(f.Message);
            Process(to);
        }
        CheckReads();
        if (_deferredFailures.Count > 0)
        {
            var failed = _deferredFailures.ToArray();
            _deferredFailures.Clear();
            foreach (var m in failed) ReportSnapshotFailure(m);
        }
    }

    private readonly List<Message> _deferredFailures = [];

    private readonly Dictionary<(ulong, ulong), Dictionary<string, string>> _pendingSnapshotData = [];

    private void Process(SimNode n)
    {
        var raft = n.Raft;
        if (raft is null) return;
        while (raft.HasReady)
        {
            var rd = raft.GetReady();
            var st = n.Storage;

            if (rd.SoftState is { Role: Role.Leader })
            {
                if (_leaderOfTerm.TryGetValue(raft.Term, out var other) && other != n.Id)
                    throw new SafetyViolation($"election safety: {other} and {n.Id} are both leaders of term {raft.Term}");
                _leaderOfTerm[raft.Term] = n.Id;
                CheckLeaderCompleteness(n);
                Note($"node {n.Id} is leader of term {raft.Term}");
            }

            if (rd.Snapshot is { } snap)
            {
                if (!_pendingSnapshotData.Remove((n.Id, snap.Index), out var data))
                    throw new SafetyViolation($"node {n.Id} restored snapshot {snap.Index} without its data");
                st.Entries.Clear();
                st.Offset = snap.Index;
                st.OffsetTerm = snap.Term;
                st.Snapshot = snap;
                st.SnapshotData = new Dictionary<string, string>(data);
                st.State = new Dictionary<string, string>(data);
                st.Applied = snap.Index;
                st.AppliedConf = snap.Conf;
                Note($"node {n.Id} installed snapshot {snap.Index}");
            }
            st.Append(rd.Entries);
            if (rd.HardState is { } hs) st.Hard = hs;

            foreach (var e in rd.CommittedEntries) Apply(n, e);
            MaxCommitted = Math.Max(MaxCommitted, raft.Log.Committed);

            foreach (var m in rd.Messages) SendToNetwork(n, m);
            n.ReadyReads.AddRange(rd.ReadStates);

            raft.Advance(rd);
            MaybeSnapshot(n);
        }
    }

    private void Apply(SimNode n, Entry e)
    {
        var st = n.Storage;
        if (e.Index != st.Applied + 1)
            throw new SafetyViolation($"node {n.Id} applied {e.Index} after {st.Applied}");
        string data = e.Type == EntryType.ConfChange ? "conf:" + Convert.ToHexString(e.Data) : Encoding.UTF8.GetString(e.Data);
        if (_applied.TryGetValue(e.Index, out var prev))
        {
            if (prev.Term != e.Term || prev.Data != data)
                throw new SafetyViolation(
                    $"state machine safety: index {e.Index} applied as '{prev.Data}'@{prev.Term} elsewhere and '{data}'@{e.Term} on node {n.Id}");
        }
        else
        {
            _applied[e.Index] = (e.Term, data);
        }

        if (e.Type == EntryType.ConfChange)
        {
            st.AppliedConf = st.AppliedConf.Apply(Codec.DecodeConfChange(e.Data));
        }
        else if (e.Data.Length > 0)
        {
            var parts = data.Split('=', 2);
            st.State[parts[0]] = parts[1];
        }
        st.Applied = e.Index;
    }

    private void MaybeSnapshot(SimNode n)
    {
        var st = n.Storage;
        ulong snapIndex = st.Snapshot?.Index ?? 0;
        if (st.Applied < snapIndex + (ulong)_opt.SnapshotEvery) return;
        var raft = n.Raft!;
        var meta = new SnapshotMeta(st.Applied, raft.Log.TermOrZero(st.Applied), st.AppliedConf);
        st.Snapshot = meta;
        st.SnapshotData = new Dictionary<string, string>(st.State);
        ulong compact = st.Applied > (ulong)_opt.SnapshotTrail ? st.Applied - (ulong)_opt.SnapshotTrail : 0;
        compact = Math.Max(compact, st.Offset);
        raft.Compact(compact, meta);
        st.Compact(compact);
    }

    /// <summary>A real transport reports a failed snapshot stream to the sender; so does the simulator.</summary>
    private void ReportSnapshotFailure(Message m)
    {
        if (Nodes.TryGetValue(m.From, out var from) && from.Alive)
        {
            from.Raft!.ReportSnapshot(m.To, false);
            Process(from);
        }
    }

    private void SendToNetwork(SimNode from, Message m)
    {
        if (_rand.NextDouble() < _opt.DropRate)
        {
            if (m.Type == MessageType.Snapshot) _deferredFailures.Add(m);
            return;
        }
        Dictionary<string, string>? data = null;
        if (m.Type == MessageType.Snapshot)
        {
            if (from.Storage.Snapshot?.Index != m.Snapshot!.Index)
                throw new SafetyViolation("snapshot message does not match the sender's snapshot");
            data = new Dictionary<string, string>(from.Storage.SnapshotData);
        }
        long at = Now + _rand.Next(0, _opt.MaxDelayTicks + 1);
        long seq = ++_seq;
        _network.Enqueue(new InFlight(at, seq, m, data), (at, seq));
    }

    private void CheckLeaderCompleteness(SimNode n)
    {
        var log = n.Raft!.Log;
        foreach (var (index, (term, _)) in _applied)
        {
            if (index <= log.Offset) continue;
            if (index > log.LastIndex || log.TermOrZero(index) != term)
                throw new SafetyViolation(
                    $"leader completeness: new leader {n.Id} (term {n.Raft.Term}) lacks committed entry {index}@{term}");
        }
    }

    // ------------------------------------------------------------------
    // Client operations
    // ------------------------------------------------------------------

    public bool Propose(string key)
    {
        var leader = SomeLeader();
        if (leader is null) return false;
        string value = $"v{++_valueSeq}";
        var r = leader.Raft!.Propose(Encoding.UTF8.GetBytes($"{key}={value}"));
        if (r.Ok) Proposed++;
        Process(leader);
        return r.Ok;
    }

    /// <summary>Any node that believes it is the leader, as a client that cached an old leader would reach.</summary>
    private SimNode? SomeLeader()
    {
        var leaders = Nodes.Values.Where(n => n.Raft is { Role: Role.Leader }).ToList();
        return leaders.Count == 0 ? null : leaders[_rand.Next(leaders.Count)];
    }

    public bool Read(string key)
    {
        var leader = SomeLeader();
        if (leader is null) return false;
        ulong ctx = ++_readCtx;
        // Anything committed anywhere before the read was issued must be visible to it.
        if (!leader.Raft!.ReadIndex(ctx, _opt.ReadMode)) return false;
        leader.Reads[ctx] = new PendingSimRead(ctx, key, CommittedAnywhere(), Now);
        Process(leader);
        return true;
    }

    /// <summary>The highest index any node knows to be committed (crashed nodes included).</summary>
    private ulong CommittedAnywhere()
    {
        ulong max = 0;
        foreach (var n in Nodes.Values)
            max = Math.Max(max, n.Raft?.Log.Committed ?? n.Storage.Hard.Commit);
        return max;
    }

    private void CheckReads()
    {
        foreach (var n in Nodes.Values)
        {
            if (!n.Alive || n.ReadyReads.Count == 0) continue;
            for (int i = n.ReadyReads.Count - 1; i >= 0; i--)
            {
                var rs = n.ReadyReads[i];
                if (n.Storage.Applied < rs.Index) continue;
                n.ReadyReads.RemoveAt(i);
                if (!n.Reads.Remove(rs.Context, out var read)) continue;
                if (rs.Index < read.MinIndex)
                    throw new SafetyViolation(
                        $"stale read on node {n.Id}: read index {rs.Index} but {read.MinIndex} was committed before the read began");
                ReadsServed++;
            }
        }
    }

    // ------------------------------------------------------------------
    // Membership
    // ------------------------------------------------------------------

    public void ProposeConf(ConfChange cc)
    {
        var leader = Leader();
        if (leader is null) return;
        var r = leader.Raft!.ProposeConfChange(cc);
        if (r.Ok) Note($"proposed {cc.Type} {cc.NodeId} at {r.Index} via {leader.Id}");
        Process(leader);
    }

    public SimNode AddEmptyNode(ulong id)
    {
        var n = new SimNode { Id = id, Storage = new SimStorage() };
        Nodes[id] = n;
        Start(n);
        return n;
    }

    // ------------------------------------------------------------------
    // Random schedule
    // ------------------------------------------------------------------

    /// <summary>One random step of a chaotic schedule.</summary>
    public void ChaosStep()
    {
        double p = _rand.NextDouble();
        if (p < 0.30) Propose($"k{_rand.Next(4)}");
        else if (p < 0.45) Read($"k{_rand.Next(4)}");

        int roll = _rand.Next(1000);
        if (roll < 4)
        {
            var alive = Nodes.Values.Where(n => n.Alive).ToList();
            if (alive.Count > 0) Crash(alive[_rand.Next(alive.Count)]);
        }
        else if (roll < 14)
        {
            var dead = Nodes.Values.Where(n => !n.Alive).ToList();
            if (dead.Count > 0) Start(dead[_rand.Next(dead.Count)]);
        }
        else if (roll < 17 && PartitionA is null)
        {
            var ids = Nodes.Keys.OrderBy(_ => _rand.Next()).ToList();
            Partition(ids.Take(_rand.Next(1, ids.Count)).ToHashSet());
        }
        else if (roll < 27 && PartitionA is not null)
        {
            Heal();
        }
        else if (roll < 29)
        {
            MembershipStep();
        }
        else if (roll < 31)
        {
            var leader = Leader();
            var voters = leader?.Raft!.Conf.Voters.Where(v => v != leader.Id).ToList();
            if (voters is { Count: > 0 }) leader!.Raft!.TransferLeadership(voters[_rand.Next(voters.Count)]);
        }
        Step();
    }

    private void MembershipStep()
    {
        var leader = Leader();
        if (leader is null) return;
        var conf = leader.Raft!.Conf;
        // Promote a learner that has caught up.
        foreach (var learner in conf.Learners)
        {
            if (leader.Raft.Status().Progress.TryGetValue(learner, out var pr) && pr.Match + 5 >= leader.Raft.Log.LastIndex)
            {
                ProposeConf(new ConfChange(ConfChangeType.AddVoter, learner, ""));
                return;
            }
        }
        bool add = conf.Voters.Count < 3 || (conf.Voters.Count < 5 && _rand.Next(2) == 0);
        if (add)
        {
            // Ids are never reused: a node that lost its disk must not come back under its old identity,
            // since it would forget its vote.
            if (_nextId > (ulong)_opt.MaxNodes) return;
            var node = AddEmptyNode(_nextId++);
            ProposeConf(new ConfChange(ConfChangeType.AddLearner, node.Id, $"sim://{node.Id}"));
        }
        else
        {
            var victims = conf.Members.Keys.ToList();
            ProposeConf(new ConfChange(ConfChangeType.Remove, victims[_rand.Next(victims.Count)], ""));
        }
    }

    /// <summary>Heals everything and checks that the cluster recovers: a leader, and all members converge.</summary>
    public void Stabilize(int maxTicks = 3000)
    {
        Heal();
        foreach (var n in Nodes.Values.Where(n => !n.Alive)) Start(n);
        for (int i = 0; i < maxTicks; i++)
        {
            Step();
            if (i % 50 == 0) Propose("final");
            if (i > 200 && Converged()) return;
        }
        throw new SafetyViolation("liveness: the cluster did not converge after healing\n" + string.Join("\n", Log.TakeLast(40)) +
                                  "\n" + string.Join("\n", Nodes.Values.Select(n => n.Raft!.Status().ToString())));
    }

    private bool Converged()
    {
        var leader = Leader();
        if (leader is null) return false;
        var conf = leader.Raft!.Conf;
        ulong applied = leader.Storage.Applied;
        if (leader.Raft.Log.Committed != leader.Raft.Log.LastIndex) return false;
        foreach (var id in conf.Members.Keys)
        {
            if (!Nodes.TryGetValue(id, out var n) || !n.Alive || n.Storage.Applied != applied) return false;
            if (!n.Storage.State.OrderBy(kv => kv.Key).SequenceEqual(leader.Storage.State.OrderBy(kv => kv.Key)))
                throw new SafetyViolation($"nodes {leader.Id} and {id} applied the same index {applied} but differ in state");
        }
        return true;
    }
}
