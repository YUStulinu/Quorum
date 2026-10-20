using System.Text;
using Quorum.Core;

namespace Quorum.Core.Tests;

/// <summary>
/// A tiny hand-driven network for directed scenarios: messages are delivered only when the test says so,
/// and links can be cut per pair.
/// </summary>
public sealed class TestNet
{
    public sealed class Node
    {
        public required RaftNode Raft;
        public List<Entry> Applied { get; } = [];
        public List<ReadState> Reads { get; } = [];
        public Dictionary<string, string> State { get; } = [];
        public ulong AppliedIndex;
        public SnapshotMeta? Snapshot;
        public Dictionary<string, string> SnapshotData = [];
        public ConfState AppliedConf = ConfState.Empty;
    }

    private readonly List<Message> _queue = [];
    private readonly HashSet<(ulong, ulong)> _cut = [];
    private readonly Dictionary<(ulong, ulong), Dictionary<string, string>> _snapshotData = [];

    public Dictionary<ulong, Node> Nodes { get; } = [];
    public int ElectionTicks { get; }
    private readonly int _maxMessageBytes = 1 << 20;

    public TestNet(int nodes, int electionTicks = 10, bool preVote = true, bool checkQuorum = true, int seed = 1, int maxMessageBytes = 1 << 20)
    {
        ElectionTicks = electionTicks;
        _maxMessageBytes = maxMessageBytes;
        var ids = Enumerable.Range(1, nodes).Select(i => (ulong)i).ToList();
        foreach (var id in ids)
        {
            var entries = ids.Select((peer, i) => new Entry
            {
                Index = (ulong)i + 1,
                Term = 1,
                Type = EntryType.ConfChange,
                Data = Codec.EncodeConfChange(new ConfChange(ConfChangeType.AddVoter, peer, $"n{peer}")),
            }).ToList();
            AddNode(id, new RaftInitialState { HardState = new HardState(1, 0, (ulong)ids.Count), Entries = entries },
                electionTicks, preVote, checkQuorum, seed + (int)id);
        }
        Flush();
    }

    public Node AddNode(ulong id, RaftInitialState state, int electionTicks = 10, bool preVote = true, bool checkQuorum = true, int seed = 7)
    {
        Node? node = null;
        var raft = new RaftNode(new RaftOptions
        {
            Id = id,
            ElectionTicks = electionTicks,
            HeartbeatTicks = 1,
            PreVote = preVote,
            CheckQuorum = checkQuorum,
            LeaseOnStartup = false,
            MaxMessageBytes = _maxMessageBytes,
            Seed = seed,
        }, state, () => node!.Snapshot);
        node = new Node { Raft = raft };
        Nodes[id] = node;
        return node;
    }

    public RaftNode this[ulong id] => Nodes[id].Raft;

    public void Cut(ulong a, ulong b)
    {
        _cut.Add((a, b));
        _cut.Add((b, a));
    }

    /// <summary>Cuts every link between the two groups.</summary>
    public void Partition(ulong[] a, ulong[] b)
    {
        foreach (var x in a)
            foreach (var y in b) Cut(x, y);
    }

    public void Isolate(ulong id)
    {
        foreach (var other in Nodes.Keys)
        {
            if (other != id) Cut(id, other);
        }
    }

    public void HealAll() => _cut.Clear();

    /// <summary>Processes every node's Ready and queues the outgoing messages (without delivering them).</summary>
    public void Flush()
    {
        foreach (var (id, n) in Nodes) Process(n);
    }

    private void Process(Node n)
    {
        while (n.Raft.HasReady)
        {
            var rd = n.Raft.GetReady();
            if (rd.Snapshot is { } s)
            {
                var data = _snapshotData[(n.Raft.Id, s.Index)];
                n.State.Clear();
                foreach (var kv in data) n.State[kv.Key] = kv.Value;
                n.AppliedIndex = s.Index;
                n.AppliedConf = s.Conf;
                n.Snapshot = s;
                n.SnapshotData = new(data);
            }
            foreach (var e in rd.CommittedEntries)
            {
                n.Applied.Add(e);
                n.AppliedIndex = e.Index;
                if (e.Type == EntryType.ConfChange)
                {
                    n.AppliedConf = n.AppliedConf.Apply(Codec.DecodeConfChange(e.Data));
                }
                else if (e.Data.Length > 0)
                {
                    var kv = Encoding.UTF8.GetString(e.Data).Split('=', 2);
                    n.State[kv[0]] = kv[1];
                }
            }
            foreach (var m in rd.Messages)
            {
                if (m.Type == MessageType.Snapshot) _snapshotData[(m.To, m.Snapshot!.Index)] = new(n.SnapshotData);
                _queue.Add(m);
            }
            n.Reads.AddRange(rd.ReadStates);
            n.Raft.Advance(rd);
        }
    }

    /// <summary>Delivers queued messages (and the ones they cause) until the network is quiet.</summary>
    public void Deliver(int maxRounds = 1000)
    {
        for (int round = 0; round < maxRounds && _queue.Count > 0; round++)
        {
            var batch = _queue.ToArray();
            _queue.Clear();
            foreach (var m in batch)
            {
                if (_cut.Contains((m.From, m.To)) || !Nodes.TryGetValue(m.To, out var to))
                {
                    if (m.Type == MessageType.Snapshot && Nodes.TryGetValue(m.From, out var f)) f.Raft.ReportSnapshot(m.To, false);
                    continue;
                }
                if (m.Type == MessageType.Snapshot) Nodes[m.From].Raft.ReportSnapshot(m.To, true);
                to.Raft.Step(m);
            }
            Flush();
        }
    }

    /// <summary>Drops every queued message.</summary>
    public void DropQueued() => _queue.Clear();

    public void Tick(ulong id, int times = 1)
    {
        for (int i = 0; i < times; i++)
        {
            Nodes[id].Raft.Tick();
            Process(Nodes[id]);
        }
    }

    /// <summary>Ticks all nodes together, delivering messages after every tick.</summary>
    public void TickAll(int times = 1)
    {
        for (int i = 0; i < times; i++)
        {
            foreach (var n in Nodes.Values)
            {
                n.Raft.Tick();
                Process(n);
            }
            Deliver();
        }
    }

    /// <summary>Makes <paramref name="id"/> campaign right away (by ticking only it past its timeout).</summary>
    public void Elect(ulong id)
    {
        for (int i = 0; i < ElectionTicks * 4 && this[id].Role != Role.Leader; i++)
        {
            Tick(id);
            Deliver();
        }
        if (this[id].Role != Role.Leader) throw new InvalidOperationException($"node {id} failed to become leader");
    }

    /// <summary>Ticks everyone until one of <paramref name="candidates"/> leads a term newer than <paramref name="afterTerm"/>.</summary>
    public ulong ElectAmong(ulong[] candidates, ulong afterTerm)
    {
        for (int i = 0; i < ElectionTicks * 20; i++)
        {
            TickAll();
            foreach (var c in candidates)
            {
                if (this[c].Role == Role.Leader && this[c].Term > afterTerm) return c;
            }
        }
        throw new InvalidOperationException("no leader elected among " + string.Join(",", candidates));
    }

    public ProposeResult Put(ulong via, string key, string value)
    {
        var r = this[via].Propose(Encoding.UTF8.GetBytes($"{key}={value}"));
        Flush();
        return r;
    }

    public ulong? LeaderId() => Nodes.Values.Where(n => n.Raft.Role == Role.Leader).Select(n => (ulong?)n.Raft.Id).FirstOrDefault();

    /// <summary>Takes a snapshot at the applied index and compacts the log, as the host does.</summary>
    public void SnapshotAndCompact(ulong id, ulong trail = 0)
    {
        var n = Nodes[id];
        var meta = new SnapshotMeta(n.AppliedIndex, n.Raft.Log.TermOrZero(n.AppliedIndex), n.AppliedConf);
        n.Snapshot = meta;
        n.SnapshotData = new(n.State);
        n.Raft.Compact(Math.Max(n.Raft.Log.Offset, n.AppliedIndex - Math.Min(trail, n.AppliedIndex)), meta);
    }
}
