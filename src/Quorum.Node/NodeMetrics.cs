using System.Collections.Concurrent;

namespace Quorum.Node;

/// <summary>A timestamped line in the node's event log (elections, faults, membership changes...).</summary>
public sealed record NodeEvent(long Seq, DateTimeOffset Time, string Text);

/// <summary>A bounded, thread-safe log of recent events, readable incrementally by sequence number.</summary>
public sealed class EventLog(int capacity = 500)
{
    private readonly Queue<NodeEvent> _events = new();
    private long _seq;

    public void Add(string text)
    {
        lock (_events)
        {
            _events.Enqueue(new NodeEvent(++_seq, DateTimeOffset.UtcNow, text));
            while (_events.Count > capacity) _events.Dequeue();
        }
    }

    public IReadOnlyList<NodeEvent> After(long seq)
    {
        lock (_events) return _events.Where(e => e.Seq > seq).ToList();
    }
}

/// <summary>Counters and a latency reservoir, updated by the loop and transport threads.</summary>
public sealed class NodeMetrics
{
    private long _proposals, _applied, _reads, _readsServed, _fsyncs, _readyLoops, _snapshotsSent, _snapshotsReceived;
    private double _fsyncMsTotal;
    private readonly double[] _commitLatency = new double[2048];
    private long _latencyCount;

    public ConcurrentDictionary<ulong, PeerCounters> Peers { get; } = new();

    public sealed class PeerCounters
    {
        public long Sent;
        public long Received;
        public long BytesSent;
        public long Dropped;
        public long Errors;
        public volatile bool Connected;
    }

    public PeerCounters Peer(ulong id) => Peers.GetOrAdd(id, _ => new PeerCounters());

    public void Proposed() => Interlocked.Increment(ref _proposals);
    public void Applied(int n) => Interlocked.Add(ref _applied, n);
    public void ReadRequested() => Interlocked.Increment(ref _reads);
    public void ReadServed() => Interlocked.Increment(ref _readsServed);
    public void ReadyLoop() => Interlocked.Increment(ref _readyLoops);
    public void SnapshotSent() => Interlocked.Increment(ref _snapshotsSent);
    public void SnapshotReceived() => Interlocked.Increment(ref _snapshotsReceived);

    public void Fsync(double ms)
    {
        Interlocked.Increment(ref _fsyncs);
        lock (_commitLatency) _fsyncMsTotal += ms;
    }

    public void CommitLatency(double ms)
    {
        lock (_commitLatency)
        {
            _commitLatency[_latencyCount % _commitLatency.Length] = ms;
            _latencyCount++;
        }
    }

    public object Snapshot()
    {
        double[] lat;
        double fsyncTotal;
        lock (_commitLatency)
        {
            int n = (int)Math.Min(_latencyCount, _commitLatency.Length);
            lat = _commitLatency.AsSpan(0, n).ToArray();
            fsyncTotal = _fsyncMsTotal;
        }
        Array.Sort(lat);
        double P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)(q * lat.Length))];
        long fsyncs = Interlocked.Read(ref _fsyncs);
        return new
        {
            proposals = Interlocked.Read(ref _proposals),
            applied = Interlocked.Read(ref _applied),
            reads = Interlocked.Read(ref _reads),
            readsServed = Interlocked.Read(ref _readsServed),
            readyLoops = Interlocked.Read(ref _readyLoops),
            fsyncs,
            fsyncAvgMs = fsyncs == 0 ? 0 : Math.Round(fsyncTotal / fsyncs, 3),
            snapshotsSent = Interlocked.Read(ref _snapshotsSent),
            snapshotsReceived = Interlocked.Read(ref _snapshotsReceived),
            commitP50Ms = Math.Round(P(0.50), 2),
            commitP99Ms = Math.Round(P(0.99), 2),
            peers = Peers.ToDictionary(kv => kv.Key.ToString(), kv => new
            {
                sent = Interlocked.Read(ref kv.Value.Sent),
                received = Interlocked.Read(ref kv.Value.Received),
                bytesSent = Interlocked.Read(ref kv.Value.BytesSent),
                dropped = Interlocked.Read(ref kv.Value.Dropped),
                errors = Interlocked.Read(ref kv.Value.Errors),
                connected = kv.Value.Connected,
            }),
        };
    }
}
