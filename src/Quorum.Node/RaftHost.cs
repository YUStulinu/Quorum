using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Quorum.Core;
using Quorum.Protocol;
using Quorum.Storage;

namespace Quorum.Node;

/// <summary>
/// Runs one Raft node: a single loop thread owns the <see cref="RaftNode"/>, the <see cref="NodeStore"/> and the
/// state machine, so none of them needs locks. Everything else (gRPC handlers, the transport, the admin API)
/// talks to the loop through its inbox.
/// <para>
/// Each loop iteration drains the inbox, ticks the Raft clock if due, then for every Ready: installs a snapshot,
/// persists entries and hard state, applies committed entries — all in one Tessera transaction, one fsync —
/// then sends messages and answers the clients whose requests completed. Requests that arrive while the disk is
/// busy pile up in the inbox and go into the next batch, so throughput grows with load.
/// </para>
/// </summary>
public sealed class RaftHost : IDisposable
{
    private interface IInput;
    private sealed record PeerInput(Message Message) : IInput;
    private sealed record SnapshotInput(Message Message, string Path) : IInput;
    private sealed record ProposeInput(KvCommand Command, TaskCompletionSource<KvResponse> Reply, long Deadline, long Started) : IInput;
    private sealed record ReadInput(KvRequest Request, TaskCompletionSource<KvResponse> Reply, long Deadline) : IInput;
    private sealed record ConfInput(ConfChange Change, TaskCompletionSource<string?> Reply, long Deadline) : IInput;
    private sealed record TransferInput(ulong Target) : IInput;
    private sealed record UnreachableInput(ulong Peer) : IInput;
    private sealed record SnapshotStatusInput(ulong Peer, bool Ok) : IInput;
    private sealed record QueryInput(Func<object> Query, TaskCompletionSource<object> Reply) : IInput;
    private sealed record PauseInput(int Ms) : IInput;
    private sealed record WakeInput : IInput;

    private sealed record PendingProposal(ulong Term, TaskCompletionSource<KvResponse> Reply, long Deadline, long Started);
    private sealed record PendingConf(ulong Term, TaskCompletionSource<string?> Reply, long Deadline);
    private sealed class PendingRead(KvRequest request, TaskCompletionSource<KvResponse> reply, long deadline)
    {
        public KvRequest Request { get; } = request;
        public TaskCompletionSource<KvResponse> Reply { get; } = reply;
        public long Deadline { get; } = deadline;
        public ulong? Index { get; set; }
    }

    private readonly NodeOptions _opt;
    private readonly ILogger _log;
    private readonly BlockingCollection<IInput> _inbox = new(new ConcurrentQueue<IInput>());
    private readonly Thread _thread;
    private readonly CancellationTokenSource _stop = new();
    private readonly Stopwatch _wall = Stopwatch.StartNew();

    private readonly NodeStore _store;
    private RaftNode _raft = null!;
    private PeerTransport? _transport;

    private readonly Dictionary<ulong, PendingProposal> _proposals = [];
    private readonly Dictionary<ulong, PendingConf> _confs = [];
    private readonly Dictionary<ulong, PendingRead> _reads = [];
    private readonly List<PendingRead> _readyReads = [];
    private readonly Dictionary<(ulong Index, ulong Term), string> _incomingSnapshots = [];
    private ulong _readSeq;
    private double _nextTick;
    private long _lastSweep;
    private volatile object? _statusCache;
    private volatile bool _removed;

    public ulong Id => _opt.Id;
    public NodeOptions Options => _opt;
    public SkewClock Clock { get; } = new();
    public FaultInjector Faults { get; } = new();
    public EventLog Events { get; } = new();
    public NodeMetrics Metrics { get; } = new();
    public NodeStore Store => _store;

    /// <summary>The latest status, refreshed by the loop several times a second (safe to read from any thread).</summary>
    public object? CachedStatus => _statusCache;

    public RaftHost(NodeOptions options, ILogger logger)
    {
        _opt = options;
        _log = logger;
        _store = NodeStore.Open(options.DataDir, options.Sync, options.MaxSessions);
        if (_store.IsEmpty)
        {
            if (options.InitialMembers.Count > 0 && !options.Join)
            {
                _store.Bootstrap(options.InitialMembers);
                Events.Add($"bootstrapped a new cluster: {string.Join(", ", options.InitialMembers.Select(m => m.Id))}");
            }
            else
            {
                Events.Add("started empty, waiting to be added to a cluster");
            }
        }
        _thread = new Thread(Loop) { Name = $"raft-{options.Id}", IsBackground = true };
    }

    public void Start(PeerTransport transport)
    {
        _transport = transport;
        var init = _store.LoadInitialState();
        _raft = new RaftNode(new RaftOptions
        {
            Id = _opt.Id,
            ElectionTicks = _opt.ElectionTicks,
            HeartbeatTicks = _opt.HeartbeatTicks,
            PreVote = _opt.PreVote,
            CheckQuorum = _opt.CheckQuorum,
            Seed = unchecked((int)(_opt.Id * 7919) ^ Environment.TickCount),
        }, init, () => _store.Snapshot);
        Events.Add($"started at term {init.HardState.Term}: log {init.LogOffset + 1}..{init.LogOffset + (ulong)init.Entries.Count}, applied {init.Applied}");
        _transport.UpdatePeers(_raft.Conf);
        _nextTick = Clock.NowMs + _opt.TickMs;
        _thread.Start();
    }

    // ------------------------------------------------------------------
    // Thread-safe entry points
    // ------------------------------------------------------------------

    private long NowMs => _wall.ElapsedMilliseconds;

    private void Post(IInput input)
    {
        try { _inbox.Add(input); }
        catch (InvalidOperationException) { /* shutting down */ }
    }

    public void Deliver(Message m) => Post(new PeerInput(m));
    public void DeliverSnapshot(Message m, string path) => Post(new SnapshotInput(m, path));
    public void ReportUnreachable(ulong peer) => Post(new UnreachableInput(peer));
    public void ReportSnapshot(ulong peer, bool ok) => Post(new SnapshotStatusInput(peer, ok));
    public void TransferLeadership(ulong target) => Post(new TransferInput(target));
    public void Pause(int ms) => Post(new PauseInput(ms));

    public Task<KvResponse> ExecuteAsync(KvRequest request)
    {
        var tcs = new TaskCompletionSource<KvResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        long timeout = request.TimeoutMs > 0 ? request.TimeoutMs : _opt.DefaultTimeoutMs;
        long deadline = NowMs + timeout;
        switch (request.Op)
        {
            case KvOperation.Get:
            case KvOperation.Scan:
                Metrics.ReadRequested();
                Post(new ReadInput(request, tcs, deadline));
                break;
            case KvOperation.Put:
            case KvOperation.Delete:
            case KvOperation.Cas:
                if (request.Key.Length == 0 || request.Key.Length > KvStateMachine.MaxKeyBytes)
                {
                    tcs.SetResult(new KvResponse { Status = KvStatusCode.Error, Error = $"key must be 1..{KvStateMachine.MaxKeyBytes} bytes" });
                    break;
                }
                var cmd = new KvCommand(
                    request.Op switch { KvOperation.Put => KvOp.Put, KvOperation.Delete => KvOp.Delete, _ => KvOp.Cas },
                    request.ClientId, request.Seq, request.Key.ToByteArray(),
                    request.Op == KvOperation.Put || (request.Op == KvOperation.Cas && request.HasValue) ? request.Value.ToByteArray() : null,
                    request.Op == KvOperation.Cas && !request.ExpectAbsent ? request.Expected.ToByteArray() : null,
                    request.Op == KvOperation.Cas && request.ExpectAbsent);
                Metrics.Proposed();
                Post(new ProposeInput(cmd, tcs, deadline, NowMs));
                break;
            default:
                tcs.SetResult(new KvResponse { Status = KvStatusCode.Error, Error = "unknown operation" });
                break;
        }
        return tcs.Task;
    }

    /// <summary>Proposes a membership change. Returns null on success, otherwise the reason it failed.</summary>
    public Task<string?> ChangeMembershipAsync(ConfChange change, int timeoutMs = 10_000)
    {
        var tcs = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Post(new ConfInput(change, tcs, NowMs + timeoutMs));
        return tcs.Task;
    }

    /// <summary>Runs <paramref name="query"/> on the loop thread (for consistent views of the node's state).</summary>
    public async Task<T> QueryAsync<T>(Func<RaftHost, T> query) where T : notnull
    {
        var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        Post(new QueryInput(() => query(this), tcs));
        return (T)await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    // ------------------------------------------------------------------
    // The loop
    // ------------------------------------------------------------------

    private void Loop()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                int wait = Math.Clamp(Clock.RealMsUntil(_nextTick), 0, 100);
                if (_inbox.TryTake(out var input, wait, _stop.Token))
                {
                    Handle(input);
                    for (int i = 0; i < _opt.MaxBatch && _inbox.TryTake(out var more); i++) Handle(more);
                }

                double now = Clock.NowMs;
                int ticks = 0;
                while (now >= _nextTick && ticks < 10)
                {
                    _raft.Tick();
                    _nextTick += _opt.TickMs;
                    ticks++;
                }
                if (now >= _nextTick) _nextTick = now + _opt.TickMs; // we fell far behind (e.g. a pause): do not storm

                ProcessReady();
                ServeReads();
                long wall = NowMs;
                if (wall - _lastSweep >= 100)
                {
                    _lastSweep = wall;
                    SweepDeadlines(wall);
                    _statusCache = BuildStatus();
                }
                MaybeSnapshot();
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            // The state machine and the log must never diverge from what was persisted; on an unexpected error
            // the safe thing is to stop and let a restart recover from disk.
            _log.LogCritical(ex, "raft loop failed");
            Events.Add($"FATAL: {ex.Message}");
            Environment.FailFast($"quorum node {Id}: raft loop failed: {ex}");
        }
    }

    private void Handle(IInput input)
    {
        switch (input)
        {
            case PeerInput p:
                if (!Faults.IsBlocked(p.Message.From)) _raft.Step(p.Message);
                break;

            case SnapshotInput s:
                _incomingSnapshots[(s.Message.Snapshot!.Index, s.Message.Snapshot.Term)] = s.Path;
                Metrics.SnapshotReceived();
                _raft.Step(s.Message);
                break;

            case ProposeInput p:
                HandlePropose(p);
                break;

            case ReadInput r:
                HandleRead(r);
                break;

            case ConfInput c:
            {
                var result = _raft.ProposeConfChange(c.Change);
                if (result.Ok) _confs[result.Index] = new PendingConf(result.Term, c.Reply, c.Deadline);
                else c.Reply.TrySetResult(result.Detail ?? result.Error.ToString());
                break;
            }

            case TransferInput t:
                _raft.TransferLeadership(t.Target);
                break;

            case UnreachableInput u:
                _raft.ReportUnreachable(u.Peer);
                break;

            case SnapshotStatusInput s:
                _raft.ReportSnapshot(s.Peer, s.Ok);
                break;

            case QueryInput q:
                try { q.Reply.TrySetResult(q.Query()); }
                catch (Exception ex) { q.Reply.TrySetException(ex); }
                break;

            case PauseInput p:
                // Simulates a stop-the-world pause (GC, VM migration...): the node does nothing, then resumes.
                Events.Add($"paused for {p.Ms} ms");
                Thread.Sleep(p.Ms);
                break;
        }
    }

    private KvResponse NotLeader()
    {
        var r = new KvResponse { Status = KvStatusCode.NotLeader, NodeId = Id, Term = _raft.Term, LeaderId = _raft.Leader };
        if (_raft.Leader != 0 && _raft.Conf.Members.TryGetValue(_raft.Leader, out var m)) r.LeaderAddress = m.Address;
        return r;
    }

    private void HandlePropose(ProposeInput p)
    {
        if (_raft.Role != Role.Leader)
        {
            p.Reply.TrySetResult(NotLeader());
            return;
        }
        var result = _raft.Propose(p.Command.Encode());
        if (!result.Ok)
        {
            p.Reply.TrySetResult(NotLeader());
            return;
        }
        _proposals[result.Index] = new PendingProposal(result.Term, p.Reply, p.Deadline, p.Started);
    }

    private void HandleRead(ReadInput r)
    {
        var pending = new PendingRead(r.Request, r.Reply, r.Deadline);
        if (r.Request.Consistency == ReadConsistency.Stale)
        {
            pending.Index = 0;
            _readyReads.Add(pending);
            return;
        }
        ulong ctx = ++_readSeq;
        var mode = r.Request.Consistency == ReadConsistency.Lease ? ReadMode.Lease : ReadMode.ReadIndex;
        if (!_raft.ReadIndex(ctx, mode))
        {
            r.Reply.TrySetResult(NotLeader());
            return;
        }
        _reads[ctx] = pending;
    }

    private void ProcessReady()
    {
        while (_raft.HasReady)
        {
            var rd = _raft.GetReady();
            Metrics.ReadyLoop();
            var completions = new List<(TaskCompletionSource<KvResponse> Reply, KvResponse Response, long Started)>();
            var confCompletions = new List<(TaskCompletionSource<string?> Reply, string? Error)>();

            // A leader may send its appends while it writes them to its own disk (dissertation 10.2.1): it only
            // counts itself towards a commit when the acknowledgements arrive, which is after this fsync. Not if the
            // term or vote changed, though: those must be durable before anyone hears of them.
            bool termOrVoteChanged = rd.HardState is { } nh &&
                                     (nh.Term != _store.HardState.Term || nh.Vote != _store.HardState.Vote);
            bool sendEarly = _raft.Role == Role.Leader && rd.Snapshot is null && !termOrVoteChanged;
            if (sendEarly && rd.Messages.Count > 0) _transport!.Send(rd.Messages);

            // Committed entries that were already on disk before this Ready (on a quorum, so they survive any
            // crash) can be answered before this fsync; the state machine would recompute the same results.
            bool answerEarly = rd.Entries.Length == 0 || rd.CommittedEntries.Length == 0 ||
                               rd.CommittedEntries[^1].Index < rd.Entries[0].Index;

            bool write = rd.Snapshot is not null || rd.Entries.Length > 0 || rd.HardState is not null || rd.CommittedEntries.Length > 0;
            if (write)
            {
                _store.Begin();
                if (rd.Snapshot is { } snap) InstallSnapshot(snap);
                _store.AppendEntries(rd.Entries);
                if (rd.HardState is { } hs) _store.SetHardState(hs);
                foreach (var e in rd.CommittedEntries) Apply(e, completions, confCompletions);
                if (rd.CommittedEntries.Length > 0) _store.SetApplied(rd.CommittedEntries[^1].Index);
                if (answerEarly) Complete(completions, confCompletions);
                var sw = Stopwatch.StartNew();
                _store.Commit();
                Metrics.Fsync(sw.Elapsed.TotalMilliseconds);
                Metrics.Applied(rd.CommittedEntries.Length);
            }

            if (!sendEarly && rd.Messages.Count > 0) _transport!.Send(rd.Messages);
            Complete(completions, confCompletions);

            foreach (var rs in rd.ReadStates)
            {
                if (_reads.Remove(rs.Context, out var read))
                {
                    read.Index = rs.Index;
                    _readyReads.Add(read);
                }
            }

            foreach (var ev in rd.Events) Events.Add(ev);
            if (rd.SoftState is { } soft) OnSoftState(soft);

            _raft.Advance(rd);

            // Configuration changes take effect when appended, so the transport must learn new addresses now.
            if (!ReferenceEquals(_raft.Conf, _knownConf))
            {
                _knownConf = _raft.Conf;
                _transport!.UpdatePeers(_knownConf);
            }
        }
    }

    private ConfState? _knownConf;

    private void Complete(List<(TaskCompletionSource<KvResponse> Reply, KvResponse Response, long Started)> completions,
        List<(TaskCompletionSource<string?> Reply, string? Error)> confCompletions)
    {
        long now = NowMs;
        foreach (var (reply, response, started) in completions)
        {
            if (started > 0) Metrics.CommitLatency(now - started);
            reply.TrySetResult(response);
        }
        foreach (var (reply, error) in confCompletions) reply.TrySetResult(error);
        completions.Clear();
        confCompletions.Clear();
    }

    private void OnSoftState(SoftState soft)
    {
        if (soft.Role != Role.Leader)
        {
            // Reads in flight can no longer be confirmed by this node.
            foreach (var read in _reads.Values) read.Reply.TrySetResult(NotLeader());
            _reads.Clear();
            // Proposals may still commit under the new leader, or may be lost: their outcome is unknown.
            foreach (var p in _proposals.Values)
                p.Reply.TrySetResult(new KvResponse { Status = KvStatusCode.Timeout, NodeId = Id, Error = "leadership changed; outcome unknown" });
            _proposals.Clear();
            foreach (var c in _confs.Values) c.Reply.TrySetResult("leadership changed before the change was applied");
            _confs.Clear();
        }
        _log.LogInformation("node {Id}: {Role} at term {Term}, leader {Leader}", Id, soft.Role, _raft.Term, soft.Leader);
    }

    private void InstallSnapshot(SnapshotMeta snap)
    {
        if (!_incomingSnapshots.Remove((snap.Index, snap.Term), out var path))
            throw new InvalidOperationException($"no snapshot file received for {snap.Index}@{snap.Term}");
        _store.RestoreSnapshot(path, snap);
        try { File.Delete(path); } catch (IOException) { }
        foreach (var stale in _incomingSnapshots.Where(kv => kv.Key.Index <= snap.Index).ToList())
        {
            _incomingSnapshots.Remove(stale.Key);
            try { File.Delete(stale.Value); } catch (IOException) { }
        }
        // Writes waiting at indices the snapshot covers can no longer be matched to an entry.
        foreach (var idx in _proposals.Keys.Where(i => i <= snap.Index).ToList())
        {
            _proposals.Remove(idx, out var p);
            p!.Reply.TrySetResult(new KvResponse { Status = KvStatusCode.Timeout, NodeId = Id, Error = "outcome unknown" });
        }
        _transport!.UpdatePeers(snap.Conf);
        Events.Add($"restored snapshot {snap.Index}@{snap.Term}: {snap.Conf}");
    }

    private void Apply(Entry e, List<(TaskCompletionSource<KvResponse>, KvResponse, long)> completions,
        List<(TaskCompletionSource<string?>, string?)> confCompletions)
    {
        if (e.Type == EntryType.ConfChange)
        {
            var cc = Codec.DecodeConfChange(e.Data);
            var conf = _store.AppliedConf.Apply(cc);
            _store.SetAppliedConf(conf);
            if (cc.Type == ConfChangeType.Remove && cc.NodeId == Id)
            {
                _removed = true;
                Events.Add("this node was removed from the cluster");
            }
            if (_confs.Remove(e.Index, out var pc))
                confCompletions.Add((pc.Reply, pc.Term == e.Term ? null : "the change was overwritten by a new leader"));
            return;
        }

        KvResponse? response = null;
        if (e.Data.Length > 0)
        {
            var cmd = KvCommand.Decode(e.Data);
            var result = _store.Kv.Apply(e.Index, cmd);
            response = new KvResponse
            {
                Status = result.Status switch
                {
                    KvStatus.Ok => KvStatusCode.Ok,
                    KvStatus.CasFailed => KvStatusCode.CasFailed,
                    _ => KvStatusCode.StaleRequest,
                },
                Found = result.Existed,
                Index = result.Index,
                NodeId = Id,
                Term = e.Term,
            };
            if (result.Current is not null) response.Value = ByteString.CopyFrom(result.Current);
        }

        if (_proposals.Remove(e.Index, out var p))
        {
            completions.Add(p.Term == e.Term && response is not null
                ? (p.Reply, response, p.Started)
                : (p.Reply, NotLeader(), 0));
        }
    }

    private void ServeReads()
    {
        if (_readyReads.Count == 0) return;
        ulong applied = _store.Applied;
        // Serve what can be served and compact the rest in place (one pass, no repeated RemoveAt).
        int kept = 0;
        for (int i = 0; i < _readyReads.Count; i++)
        {
            var read = _readyReads[i];
            if (read.Index > applied)
            {
                _readyReads[kept++] = read;
                continue;
            }
            read.Reply.TrySetResult(ExecuteRead(read.Request, applied));
            Metrics.ReadServed();
        }
        _readyReads.RemoveRange(kept, _readyReads.Count - kept);
    }

    private KvResponse ExecuteRead(KvRequest req, ulong applied)
    {
        var resp = new KvResponse { NodeId = Id, Term = _raft.Term, Index = applied, Status = KvStatusCode.Ok };
        if (req.Op == KvOperation.Scan)
        {
            int limit = req.Limit == 0 ? 100 : (int)Math.Min(req.Limit, 10_000);
            foreach (var kv in _store.Kv.Scan(req.Key.Span, limit))
                resp.Pairs.Add(new KvPair { Key = ByteString.CopyFrom(kv.Key), Value = ByteString.CopyFrom(kv.Value) });
            return resp;
        }
        if (req.Key.Length == 0 || req.Key.Length > KvStateMachine.MaxKeyBytes)
            return new KvResponse { Status = KvStatusCode.Error, Error = "invalid key", NodeId = Id };
        var value = _store.Kv.Get(req.Key.Span);
        if (value is null)
        {
            resp.Status = KvStatusCode.NotFound;
        }
        else
        {
            resp.Found = true;
            resp.Value = ByteString.CopyFrom(value);
        }
        return resp;
    }

    private void SweepDeadlines(long now)
    {
        static KvResponse Timeout(ulong id) => new() { Status = KvStatusCode.Timeout, NodeId = id, Error = "timed out" };
        foreach (var (idx, p) in _proposals.Where(kv => kv.Value.Deadline <= now).ToList())
        {
            _proposals.Remove(idx);
            p.Reply.TrySetResult(Timeout(Id));
        }
        foreach (var (ctx, r) in _reads.Where(kv => kv.Value.Deadline <= now).ToList())
        {
            _reads.Remove(ctx);
            r.Reply.TrySetResult(Timeout(Id));
        }
        for (int i = _readyReads.Count - 1; i >= 0; i--)
        {
            if (_readyReads[i].Deadline > now) continue;
            _readyReads[i].Reply.TrySetResult(Timeout(Id));
            _readyReads.RemoveAt(i);
        }
        foreach (var (idx, c) in _confs.Where(kv => kv.Value.Deadline <= now).ToList())
        {
            _confs.Remove(idx);
            c.Reply.TrySetResult("timed out waiting for the change to be applied");
        }
    }

    private void MaybeSnapshot()
    {
        ulong last = _store.Snapshot?.Index ?? 0;
        if (_store.Applied < last + (ulong)_opt.SnapshotEvery) return;
        var sw = Stopwatch.StartNew();
        ulong applied = _store.Applied;
        var meta = _store.CreateSnapshot(_raft.Log.TermOrZero(applied));
        ulong compact = applied > (ulong)_opt.SnapshotTrail ? applied - (ulong)_opt.SnapshotTrail : 0;
        compact = Math.Max(compact, _raft.Log.Offset);
        ulong compactTerm = _raft.Log.TermOrZero(compact);
        _raft.Compact(compact, meta);
        _store.Begin();
        _store.CompactLog(compact, compactTerm);
        _store.Commit();
        Events.Add($"snapshot at {meta.Index} ({sw.ElapsedMilliseconds} ms); log compacted up to {compact}");
    }

    // ------------------------------------------------------------------
    // Status
    // ------------------------------------------------------------------

    private object BuildStatus()
    {
        var s = _raft.Status();
        var stats = _store.Database.Stats();
        return new
        {
            id = s.Id,
            role = s.Role.ToString(),
            term = s.Term,
            vote = s.Vote,
            leader = s.Leader,
            commit = s.Commit,
            applied = s.Applied,
            lastIndex = s.LastIndex,
            lastTerm = s.LastTerm,
            firstIndex = s.FirstIndex,
            snapshotIndex = _store.Snapshot?.Index ?? 0,
            leaseValid = s.LeaseValid,
            leadTransferee = s.LeadTransferee,
            removed = _removed,
            members = s.Conf.Members.Values.Select(m => new { id = m.Id, address = m.Address, learner = m.IsLearner }),
            progress = s.Progress.ToDictionary(kv => kv.Key.ToString(), kv => new
            {
                match = kv.Value.Match,
                next = kv.Value.Next,
                state = kv.Value.State.ToString(),
                inflight = kv.Value.Inflight,
                active = kv.Value.RecentActive,
                learner = kv.Value.IsLearner,
            }),
            pending = new { proposals = _proposals.Count, reads = _reads.Count + _readyReads.Count, inbox = _inbox.Count },
            clockRate = Clock.Rate,
            faults = Faults.Settings,
            storage = new
            {
                keys = stats.Keys,
                pages = stats.Pages,
                treeHeight = stats.TreeHeight,
                walFrames = stats.WalFrames,
                commits = stats.Commits,
                sessions = _store.Kv.SessionCount,
            },
            metrics = Metrics.Snapshot(),
            uptimeMs = NowMs,
        };
    }

    /// <summary>The node's status: the cached copy, or a fresh one computed on the loop thread right after start.</summary>
    public async Task<object> StatusAsync() => _statusCache ?? await QueryAsync(h => h.BuildStatus());

    /// <summary>Computes a digest of the key-value data together with the applied index it corresponds to.</summary>
    public Task<object> DigestAsync() => QueryAsync<object>(h => new { applied = h._store.Applied, digest = h._store.Kv.Digest(), keys = h._store.Kv.CountKeys() });

    public void Dispose()
    {
        _stop.Cancel();
        _inbox.CompleteAdding();
        if (_thread.IsAlive) _thread.Join(TimeSpan.FromSeconds(5));
        foreach (var p in _proposals.Values) p.Reply.TrySetResult(new KvResponse { Status = KvStatusCode.Timeout, Error = "node stopped" });
        foreach (var r in _reads.Values) r.Reply.TrySetResult(new KvResponse { Status = KvStatusCode.Timeout, Error = "node stopped" });
        _store.Dispose();
    }
}
