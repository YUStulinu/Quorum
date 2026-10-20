namespace Quorum.Core;

/// <summary>
/// The Raft consensus algorithm as a deterministic state machine.
/// <para>
/// It does no I/O and reads no clock. The host feeds it ticks (<see cref="Tick"/>), messages from peers
/// (<see cref="Step"/>) and requests (<see cref="Propose"/>, <see cref="ReadIndex"/>, ...), then collects a
/// <see cref="Ready"/> describing what to persist, apply and send, and acknowledges it with
/// <see cref="Advance"/>. Because the same inputs always produce the same outputs, a whole cluster can be
/// simulated in one thread with a seeded network, which is how the tests explore thousands of failure schedules.
/// </para>
/// <para>
/// Implemented: leader election with randomized timeouts, pre-vote and check-quorum; log replication with
/// pipelining, flow control and fast backtracking; commitment restricted to the current term; snapshots and log
/// compaction; single-server membership changes with learners; leadership transfer; linearizable reads by
/// ReadIndex (heartbeat round) or by leader lease.
/// </para>
/// </summary>
public sealed class RaftNode
{
    private enum CampaignType { PreElection, Election, Transfer }
    private enum VoteResult { Pending, Won, Lost }

    private sealed class Round(ulong startTick)
    {
        public ulong StartTick { get; } = startTick;
        public HashSet<ulong> Acks { get; } = [];
        public bool Done { get; set; }
    }

    private readonly record struct PendingRead(ulong Context, ulong Index, ulong Round);

    private readonly RaftOptions _opt;
    private readonly Random _rand;
    private readonly Func<SnapshotMeta?> _snapshotProvider;

    private readonly Dictionary<ulong, Progress> _prs = [];
    private readonly Dictionary<ulong, bool> _votes = [];
    private readonly List<Message> _msgs = [];
    private readonly List<ReadState> _readStates = [];
    private readonly List<string> _events = [];

    // ReadIndex / lease bookkeeping (leader only).
    private readonly SortedDictionary<ulong, Round> _rounds = [];
    private readonly List<PendingRead> _pendingReads = [];
    private readonly List<(ulong Context, ReadMode Mode)> _readsWaitingForCommit = [];
    private ulong _roundSeq;
    private ulong _leaseStartTick;
    private bool _hasLease;
    private bool _heartbeatRequested;
    private bool _broadcastRequested;

    private ConfState _baseConf;
    private ulong _baseConfIndex;
    private ulong _lastConfIndex;

    private int _electionElapsed;
    private int _heartbeatElapsed;
    private int _randomizedElectionTimeout;
    private ulong _tick;
    private SnapshotMeta? _pendingSnapshot;

    private HardState _prevHard;
    private SoftState _prevSoft;

    public ulong Id => _opt.Id;
    public ulong Term { get; private set; }
    public ulong Vote { get; private set; }
    public ulong Leader { get; private set; }
    public Role Role { get; private set; } = Role.Follower;
    public RaftLog Log { get; }
    public ConfState Conf { get; private set; }
    public ulong LeadTransferee { get; private set; }
    public ulong PendingConfIndex { get; private set; }
    public ulong Ticks => _tick;

    /// <summary>Optional trace sink for debugging (very chatty).</summary>
    public Action<string>? Trace { get; set; }

    public RaftNode(RaftOptions options, RaftInitialState initial, Func<SnapshotMeta?>? snapshotProvider = null)
    {
        if (options.Id == 0) throw new ArgumentException("node id 0 is reserved", nameof(options));
        if (options.HeartbeatTicks >= options.ElectionTicks)
            throw new ArgumentException("heartbeat ticks must be smaller than election ticks", nameof(options));
        _opt = options;
        _rand = new Random(options.Seed);
        _snapshotProvider = snapshotProvider ?? (() => null);

        Log = new RaftLog(initial.LogOffset, initial.LogOffsetTerm, initial.Entries, initial.HardState.Commit, initial.Applied);
        Term = initial.HardState.Term;
        Vote = initial.HardState.Vote;

        _baseConf = initial.Snapshot?.Conf ?? ConfState.Empty;
        _baseConfIndex = initial.Snapshot?.Index ?? 0;
        Conf = _baseConf;
        RecomputeConf();

        _prevHard = new HardState(Term, Vote, Log.Committed);
        _prevSoft = new SoftState(Leader, Role);
        BecomeFollower(Term, 0);
    }

    // ------------------------------------------------------------------
    // Inputs
    // ------------------------------------------------------------------

    /// <summary>Advances logical time by one tick.</summary>
    public void Tick()
    {
        _tick++;
        if (Role == Role.Leader) TickHeartbeat();
        else TickElection();
    }

    private bool Promotable => Conf.IsVoter(Id);

    private void TickElection()
    {
        _electionElapsed++;
        if (Promotable && _electionElapsed >= _randomizedElectionTimeout)
        {
            _electionElapsed = 0;
            Campaign(_opt.PreVote ? CampaignType.PreElection : CampaignType.Election);
        }
    }

    private void TickHeartbeat()
    {
        _heartbeatElapsed++;
        _electionElapsed++;

        if (_electionElapsed >= _opt.ElectionTicks)
        {
            _electionElapsed = 0;
            if (_opt.CheckQuorum)
            {
                if (!QuorumActive())
                {
                    AddEvent($"stepped down at term {Term}: no quorum heard from in an election timeout");
                    BecomeFollower(Term, 0);
                    return;
                }
                foreach (var (id, pr) in _prs)
                {
                    if (id != Id) pr.RecentActive = false;
                }
            }
            if (Role == Role.Leader && LeadTransferee != 0)
            {
                AddEvent($"leadership transfer to {LeadTransferee} timed out");
                LeadTransferee = 0;
            }
        }

        if (Role != Role.Leader) return;
        if (_heartbeatElapsed >= _opt.HeartbeatTicks)
        {
            _heartbeatElapsed = 0;
            BroadcastHeartbeat();
        }
        PruneRounds();
    }

    /// <summary>Handles a message from a peer.</summary>
    public void Step(Message m)
    {
        Trace?.Invoke($"[{Id} t{Term} {Role}] recv {m}");

        if (m.Term > Term)
        {
            if (m.Type is MessageType.Vote or MessageType.PreVote)
            {
                // Leader lease on the follower side: if we heard from a leader less than an election timeout ago,
                // refuse to help depose it. This keeps a partitioned node that comes back from disrupting the
                // cluster, and is what makes lease reads safe (as long as clocks run at the same rate).
                bool inLease = _opt.CheckQuorum && _electionElapsed < _opt.ElectionTicks &&
                               (Leader != 0 || (_opt.LeaseOnStartup && _tick < (ulong)_opt.ElectionTicks));
                if (!m.Force && inLease)
                {
                    Trace?.Invoke($"[{Id}] ignoring {m.Type} from {m.From}: in leader lease");
                    return;
                }
            }

            if (m.Type == MessageType.PreVote)
            {
                // A pre-vote never changes our term.
            }
            else if (m.Type == MessageType.PreVoteResponse && !m.Reject)
            {
                // A granted pre-vote carries the term we would campaign with; nothing to update yet.
            }
            else
            {
                bool fromLeader = m.Type is MessageType.Append or MessageType.Heartbeat or MessageType.Snapshot;
                BecomeFollower(m.Term, fromLeader ? m.From : 0);
            }
        }
        else if (m.Term < Term)
        {
            if ((_opt.CheckQuorum || _opt.PreVote) && m.Type is MessageType.Heartbeat or MessageType.Append)
            {
                // A stale leader (for example one that was partitioned). Tell it our term so it steps down;
                // otherwise, with pre-vote, it could keep a quorum of followers from ever electing anyone.
                Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term });
            }
            else if (m.Type == MessageType.PreVote)
            {
                Send(new Message { Type = MessageType.PreVoteResponse, To = m.From, Term = Term, Reject = true });
            }
            return;
        }

        switch (m.Type)
        {
            case MessageType.Vote:
            case MessageType.PreVote:
                HandleVoteRequest(m);
                return;
        }

        switch (Role)
        {
            case Role.Leader: StepLeader(m); break;
            case Role.Candidate:
            case Role.PreCandidate: StepCandidate(m); break;
            default: StepFollower(m); break;
        }
    }

    private void HandleVoteRequest(Message m)
    {
        bool canVote = Vote == m.From ||
                       (Vote == 0 && Leader == 0) ||
                       (m.Type == MessageType.PreVote && m.Term > Term);
        var respType = m.Type == MessageType.Vote ? MessageType.VoteResponse : MessageType.PreVoteResponse;
        if (canVote && Log.IsUpToDate(m.Index, m.LogTerm))
        {
            // The response carries the candidate's term, so that a pre-vote grant is recognised
            // by a pre-candidate that has not bumped its own term yet.
            Send(new Message { Type = respType, To = m.From, Term = m.Term });
            if (m.Type == MessageType.Vote)
            {
                _electionElapsed = 0;
                Vote = m.From;
            }
        }
        else
        {
            Send(new Message { Type = respType, To = m.From, Term = Term, Reject = true });
        }
    }

    private void StepFollower(Message m)
    {
        switch (m.Type)
        {
            case MessageType.Append:
                _electionElapsed = 0;
                Leader = m.From;
                HandleAppend(m);
                break;
            case MessageType.Heartbeat:
                _electionElapsed = 0;
                Leader = m.From;
                HandleHeartbeat(m);
                break;
            case MessageType.Snapshot:
                _electionElapsed = 0;
                Leader = m.From;
                HandleSnapshot(m);
                break;
            case MessageType.TimeoutNow:
                if (Promotable)
                {
                    AddEvent($"leader {m.From} asked us to take over (leadership transfer)");
                    Campaign(CampaignType.Transfer);
                }
                break;
        }
    }

    private void StepCandidate(Message m)
    {
        var myResp = Role == Role.PreCandidate ? MessageType.PreVoteResponse : MessageType.VoteResponse;
        switch (m.Type)
        {
            case MessageType.Append:
                BecomeFollower(m.Term, m.From);
                HandleAppend(m);
                break;
            case MessageType.Heartbeat:
                BecomeFollower(m.Term, m.From);
                HandleHeartbeat(m);
                break;
            case MessageType.Snapshot:
                BecomeFollower(m.Term, m.From);
                HandleSnapshot(m);
                break;
            case MessageType.VoteResponse:
            case MessageType.PreVoteResponse:
                if (m.Type != myResp) return;
                switch (Poll(m.From, !m.Reject))
                {
                    case VoteResult.Won:
                        if (Role == Role.PreCandidate) Campaign(CampaignType.Election);
                        else
                        {
                            BecomeLeader();
                            BroadcastAppend();
                        }
                        break;
                    case VoteResult.Lost:
                        BecomeFollower(Term, 0);
                        break;
                }
                break;
        }
    }

    private void StepLeader(Message m)
    {
        if (!_prs.TryGetValue(m.From, out var pr)) return;
        switch (m.Type)
        {
            case MessageType.AppendResponse:
                pr.RecentActive = true;
                if (m.Reject)
                {
                    ulong nextProbe = m.RejectHint;
                    if (m.LogTerm > 0) nextProbe = Log.FindConflictByTerm(m.RejectHint, m.LogTerm).Index;
                    if (pr.MaybeDecrTo(m.Index, nextProbe))
                    {
                        if (pr.State == ProgressState.Replicate) pr.BecomeProbe();
                        SendAppend(m.From, pr);
                    }
                }
                else
                {
                    bool wasPaused = pr.IsPaused;
                    if (pr.MaybeUpdate(m.Index))
                    {
                        switch (pr.State)
                        {
                            case ProgressState.Probe:
                                pr.BecomeReplicate();
                                break;
                            case ProgressState.Snapshot when pr.Match >= pr.PendingSnapshot:
                                pr.BecomeProbe();
                                pr.BecomeReplicate();
                                break;
                            case ProgressState.Replicate:
                                pr.FreeInflightUpTo(m.Index);
                                break;
                        }
                        if (MaybeCommit())
                        {
                            BroadcastAppend();
                        }
                        else if (wasPaused)
                        {
                            SendAppend(m.From, pr);
                        }
                        // Keep the pipeline full while there is more to send.
                        while (pr.State == ProgressState.Replicate && pr.Next <= Log.LastIndex && !pr.IsPaused)
                        {
                            if (!SendAppend(m.From, pr, sendIfEmpty: false)) break;
                        }
                        if (m.From == LeadTransferee && pr.Match == Log.LastIndex)
                        {
                            Send(new Message { Type = MessageType.TimeoutNow, To = m.From, Term = Term });
                        }
                    }
                }
                break;

            case MessageType.HeartbeatResponse:
                pr.RecentActive = true;
                pr.ProbeSent = false;
                if (pr.State == ProgressState.Replicate && pr.IsPaused) pr.FreeFirstInflight();
                if (pr.Match < Log.LastIndex) SendAppend(m.From, pr);
                if (m.Context != 0) AckRound(m.From, m.Context);
                break;
        }
    }

    private void HandleAppend(Message m)
    {
        if (m.Index < Log.Committed)
        {
            Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term, Index = Log.Committed });
            return;
        }
        if (Log.MaybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries, out ulong lastNew, out bool truncated))
        {
            if (truncated) RecomputeConf();
            else ApplyConfEntries(m.Entries);
            Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term, Index = lastNew });
        }
        else
        {
            // Help the leader find the match point quickly: report the last index in (or before) the
            // conflicting term, instead of just "not index-1".
            var (hintIndex, hintTerm) = Log.FindConflictByTerm(Math.Min(m.Index, Log.LastIndex), m.LogTerm);
            Send(new Message
            {
                Type = MessageType.AppendResponse,
                To = m.From,
                Term = Term,
                Index = m.Index,
                Reject = true,
                RejectHint = hintIndex,
                LogTerm = hintTerm,
            });
        }
    }

    private void HandleHeartbeat(Message m)
    {
        // The leader sends min(follower match, commit), so this never goes past our log.
        Log.CommitTo(Math.Min(m.Commit, Log.LastIndex));
        Send(new Message { Type = MessageType.HeartbeatResponse, To = m.From, Term = Term, Context = m.Context });
    }

    private void HandleSnapshot(Message m)
    {
        var s = m.Snapshot!;
        if (s.Index <= Log.Committed)
        {
            Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term, Index = Log.Committed });
            return;
        }
        if (Log.MatchTerm(s.Index, s.Term))
        {
            // We already have the entries the snapshot covers: just move the commit index forward.
            Log.CommitTo(s.Index);
            Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term, Index = Log.LastIndex });
            return;
        }
        AddEvent($"installing snapshot at index {s.Index} (term {s.Term}) from {m.From}");
        Log.Restore(s);
        _pendingSnapshot = s;
        _baseConf = s.Conf;
        _baseConfIndex = s.Index;
        _lastConfIndex = 0;
        Conf = s.Conf;
        Send(new Message { Type = MessageType.AppendResponse, To = m.From, Term = Term, Index = Log.LastIndex });
    }

    /// <summary>
    /// Appends a command to the log. Only the leader accepts proposals; the result says where the entry went,
    /// so the host can match it against the applied entry later (if the term differs, it was overwritten).
    /// </summary>
    public ProposeResult Propose(byte[] data)
    {
        if (Role != Role.Leader) return new ProposeResult(ProposeError.NotLeader, 0, 0);
        if (LeadTransferee != 0) return new ProposeResult(ProposeError.TransferInProgress, 0, 0);
        return AppendLocal(EntryType.Normal, data);
    }

    /// <summary>Proposes a membership change. Only one may be in flight at a time.</summary>
    public ProposeResult ProposeConfChange(ConfChange cc)
    {
        if (Role != Role.Leader) return new ProposeResult(ProposeError.NotLeader, 0, 0);
        if (LeadTransferee != 0) return new ProposeResult(ProposeError.TransferInProgress, 0, 0);
        if (PendingConfIndex > Log.Applied)
            return new ProposeResult(ProposeError.ConfChangeInProgress, 0, 0,
                $"a configuration change at index {PendingConfIndex} is not applied yet");
        if (Conf.Validate(cc) is { } reason)
            return new ProposeResult(ProposeError.InvalidConfChange, 0, 0, reason);
        var result = AppendLocal(EntryType.ConfChange, Codec.EncodeConfChange(cc));
        PendingConfIndex = result.Index;
        return result;
    }

    private ProposeResult AppendLocal(EntryType type, byte[] data)
    {
        var e = new Entry { Index = Log.LastIndex + 1, Term = Term, Type = type, Data = data };
        Log.Append([e]);
        if (type == EntryType.ConfChange) ApplyConfEntries([e]);
        _prs[Id].MaybeUpdate(Log.LastIndex);
        MaybeCommit();
        _broadcastRequested = true;
        return new ProposeResult(ProposeError.None, e.Index, e.Term);
    }

    /// <summary>
    /// Requests a linearizable read. Returns false if this node is not the leader. Otherwise a
    /// <see cref="ReadState"/> with the same context appears in a later <see cref="Ready"/>: once the node has
    /// applied up to its index, reading the local state machine is linearizable.
    /// </summary>
    public bool ReadIndex(ulong context, ReadMode mode = ReadMode.ReadIndex)
    {
        if (Role != Role.Leader) return false;
        if (!CommittedInCurrentTerm())
        {
            // The commit index of a new leader is only known to be up to date once its no-op commits.
            _readsWaitingForCommit.Add((context, mode));
            return true;
        }
        RegisterRead(context, mode);
        return true;
    }

    private void RegisterRead(ulong context, ReadMode mode)
    {
        if (Conf.Voters.Count == 1 && Conf.IsVoter(Id))
        {
            _readStates.Add(new ReadState(context, Log.Committed));
            return;
        }
        if (mode == ReadMode.Lease && LeaseValid)
        {
            _readStates.Add(new ReadState(context, Log.Committed));
            return;
        }
        // Wait for the next heartbeat round to be acknowledged by a quorum. All reads registered before a
        // round is broadcast share it, so a burst of reads costs one round trip.
        _pendingReads.Add(new PendingRead(context, Log.Committed, _roundSeq + 1));
        _heartbeatRequested = true;
    }

    /// <summary>The leader lease: a quorum acknowledged a heartbeat sent less than an election timeout ago.</summary>
    public bool LeaseValid =>
        Role == Role.Leader && _hasLease &&
        _tick < _leaseStartTick + (ulong)Math.Max(1, _opt.ElectionTicks - _opt.LeaseMarginTicks);

    /// <summary>Starts handing leadership to <paramref name="target"/> (dissertation 3.10).</summary>
    public void TransferLeadership(ulong target)
    {
        if (Role != Role.Leader || target == Id || !Conf.IsVoter(target) || LeadTransferee == target) return;
        if (!_prs.TryGetValue(target, out var pr)) return;
        LeadTransferee = target;
        _electionElapsed = 0;
        AddEvent($"transferring leadership to {target}");
        if (pr.Match == Log.LastIndex)
            Send(new Message { Type = MessageType.TimeoutNow, To = target, Term = Term });
        else
            SendAppend(target, pr);
    }

    /// <summary>The host failed to deliver a message to <paramref name="to"/>.</summary>
    public void ReportUnreachable(ulong to)
    {
        if (Role == Role.Leader && _prs.TryGetValue(to, out var pr) && pr.State == ProgressState.Replicate)
            pr.BecomeProbe();
    }

    /// <summary>The host finished (or failed) streaming a snapshot to <paramref name="to"/>.</summary>
    public void ReportSnapshot(ulong to, bool success)
    {
        if (Role != Role.Leader || !_prs.TryGetValue(to, out var pr) || pr.State != ProgressState.Snapshot) return;
        if (!success)
        {
            pr.BecomeProbe();
            pr.ProbeSent = false;
            return;
        }
        // Wait for the follower's append response before sending more.
        pr.BecomeProbe();
        pr.ProbeSent = true;
    }

    /// <summary>
    /// Log compaction: the host has a durable snapshot <paramref name="snapshot"/> and no longer needs entries up
    /// to <paramref name="compactIndex"/> (which may be below the snapshot index, to keep a tail of recent
    /// entries for followers that are only slightly behind).
    /// </summary>
    public void Compact(ulong compactIndex, SnapshotMeta snapshot)
    {
        if (compactIndex > snapshot.Index) throw new ArgumentException("cannot compact past the snapshot");
        if (snapshot.Index > Log.Applied) throw new ArgumentException("snapshot is ahead of the applied index");
        if (snapshot.Index > _baseConfIndex)
        {
            _baseConf = snapshot.Conf;
            _baseConfIndex = snapshot.Index;
        }
        Log.Compact(compactIndex);
    }

    // ------------------------------------------------------------------
    // Ready / Advance
    // ------------------------------------------------------------------

    public bool HasReady =>
        _msgs.Count > 0 || _readStates.Count > 0 || _pendingSnapshot is not null || _heartbeatRequested ||
        _broadcastRequested || Log.Stable < Log.LastIndex || Log.HasNextCommitted ||
        CurrentHard != _prevHard || CurrentSoft != _prevSoft || _events.Count > 0;

    private HardState CurrentHard => new(Term, Vote, Log.Committed);
    private SoftState CurrentSoft => new(Leader, Role);

    public Ready GetReady()
    {
        if (_broadcastRequested)
        {
            _broadcastRequested = false;
            if (Role == Role.Leader) BroadcastAppend();
        }
        if (_heartbeatRequested)
        {
            _heartbeatRequested = false;
            if (Role == Role.Leader) BroadcastHeartbeat();
        }

        var hard = CurrentHard;
        var soft = CurrentSoft;
        var entries = Log.UnstableEntries();
        var rd = new Ready
        {
            SoftState = soft != _prevSoft ? soft : null,
            HardState = hard != _prevHard ? hard : null,
            Entries = entries,
            Snapshot = _pendingSnapshot,
            CommittedEntries = _pendingSnapshot is null ? Log.NextCommittedEntries(_opt.MaxApplyBytes) : [],
            Messages = [.. _msgs],
            ReadStates = [.. _readStates],
            Events = [.. _events],
            MustSync = entries.Length > 0 || hard.Term != _prevHard.Term || hard.Vote != _prevHard.Vote,
        };
        _msgs.Clear();
        _readStates.Clear();
        _events.Clear();
        _prevHard = hard;
        _prevSoft = soft;
        return rd;
    }

    /// <summary>Acknowledges that everything in <paramref name="rd"/> has been persisted and applied.</summary>
    public void Advance(Ready rd)
    {
        if (rd.Snapshot is not null)
        {
            if (_pendingSnapshot == rd.Snapshot) _pendingSnapshot = null;
        }
        if (rd.Entries.Length > 0) Log.StableTo(rd.Entries[^1].Index);
        if (rd.CommittedEntries.Length > 0) Log.AppliedTo(rd.CommittedEntries[^1].Index);

        // A leader removed from the configuration keeps managing the cluster until the change commits,
        // then steps down (dissertation 4.2.2).
        if (Role == Role.Leader && !Conf.IsVoter(Id) && Log.Committed >= _lastConfIndex && _lastConfIndex > 0)
        {
            AddEvent($"stepping down: removed from the configuration at index {_lastConfIndex}");
            BecomeFollower(Term, 0);
        }
    }

    // ------------------------------------------------------------------
    // Role transitions
    // ------------------------------------------------------------------

    private void Reset(ulong term)
    {
        if (Term != term)
        {
            Term = term;
            Vote = 0;
        }
        Leader = 0;
        _electionElapsed = 0;
        _heartbeatElapsed = 0;
        _randomizedElectionTimeout = _opt.ElectionTicks + _rand.Next(_opt.ElectionTicks);
        LeadTransferee = 0;
        _votes.Clear();
        PendingConfIndex = 0;
        _rounds.Clear();
        _pendingReads.Clear();
        _readsWaitingForCommit.Clear();
        _hasLease = false;
        _heartbeatRequested = false;
        _broadcastRequested = false;
        ResetProgress();
    }

    private void ResetProgress()
    {
        _prs.Clear();
        foreach (var m in Conf.Members.Values)
        {
            var pr = new Progress(Log.LastIndex + 1, _opt.MaxInflight, m.IsLearner);
            if (m.Id == Id) pr.ResetMatch(Log.LastIndex, Log.LastIndex + 1);
            _prs[m.Id] = pr;
        }
        EnsureSelfProgress();
    }

    /// <summary>The node always tracks itself, even while it is being removed from the configuration.</summary>
    private void EnsureSelfProgress()
    {
        if (_prs.ContainsKey(Id)) return;
        var self = new Progress(Log.LastIndex + 1, _opt.MaxInflight, isLearner: true);
        self.ResetMatch(Log.LastIndex, Log.LastIndex + 1);
        _prs[Id] = self;
    }

    private void BecomeFollower(ulong term, ulong leader)
    {
        var old = Role;
        Reset(term);
        Leader = leader;
        Role = Role.Follower;
        if (old == Role.Leader) AddEvent($"became follower at term {term}");
    }

    private void BecomePreCandidate()
    {
        // A pre-candidate changes neither its term nor its vote.
        _votes.Clear();
        Leader = 0;
        Role = Role.PreCandidate;
        _randomizedElectionTimeout = _opt.ElectionTicks + _rand.Next(_opt.ElectionTicks);
    }

    private void BecomeCandidate()
    {
        Reset(Term + 1);
        Vote = Id;
        Role = Role.Candidate;
        AddEvent($"started an election for term {Term}");
    }

    private void BecomeLeader()
    {
        Reset(Term);
        Leader = Id;
        Role = Role.Leader;
        _prs[Id].BecomeReplicate();
        // No configuration change until everything up to this term's no-op is applied.
        PendingConfIndex = Log.LastIndex;
        AppendLocal(EntryType.Normal, []);
        AddEvent($"became leader at term {Term}");
    }

    private void Campaign(CampaignType type)
    {
        if (!Promotable) return;
        ulong term;
        MessageType voteType;
        if (type == CampaignType.PreElection)
        {
            BecomePreCandidate();
            voteType = MessageType.PreVote;
            term = Term + 1;
        }
        else
        {
            BecomeCandidate();
            voteType = MessageType.Vote;
            term = Term;
        }

        if (Poll(Id, true) == VoteResult.Won)
        {
            // Single-voter cluster.
            if (type == CampaignType.PreElection) Campaign(CampaignType.Election);
            else BecomeLeader();
            return;
        }
        foreach (var id in Conf.Voters)
        {
            if (id == Id) continue;
            Send(new Message
            {
                Type = voteType,
                To = id,
                Term = term,
                Index = Log.LastIndex,
                LogTerm = Log.LastTerm,
                Force = type == CampaignType.Transfer,
            });
        }
    }

    private VoteResult Poll(ulong from, bool granted)
    {
        if (!Conf.IsVoter(from)) return VoteResult.Pending;
        _votes.TryAdd(from, granted);
        int yes = 0, no = 0;
        foreach (var (id, g) in _votes)
        {
            if (!Conf.IsVoter(id)) continue;
            if (g) yes++;
            else no++;
        }
        int q = Conf.Quorum;
        if (yes >= q) return VoteResult.Won;
        if (no > Conf.Voters.Count - q) return VoteResult.Lost;
        return VoteResult.Pending;
    }

    // ------------------------------------------------------------------
    // Replication (leader)
    // ------------------------------------------------------------------

    private void Send(Message m)
    {
        m.From = Id;
        Trace?.Invoke($"[{Id} t{Term} {Role}] send {m}");
        _msgs.Add(m);
    }

    private void BroadcastAppend()
    {
        foreach (var (id, pr) in _prs)
        {
            if (id != Id) SendAppend(id, pr);
        }
    }

    private bool SendAppend(ulong to, Progress pr, bool sendIfEmpty = true)
    {
        if (pr.IsPaused) return false;
        ulong prevIndex = pr.Next - 1;
        if (!Log.TryTerm(prevIndex, out ulong prevTerm) || (pr.Next <= Log.LastIndex && pr.Next < Log.FirstIndex))
            return SendSnapshot(to, pr);

        var entries = pr.Next <= Log.LastIndex ? Log.Slice(pr.Next, Log.LastIndex + 1, _opt.MaxMessageBytes) : [];
        if (entries.Length == 0 && !sendIfEmpty) return false;
        Send(new Message
        {
            Type = MessageType.Append,
            To = to,
            Term = Term,
            Index = prevIndex,
            LogTerm = prevTerm,
            Entries = entries,
            Commit = Log.Committed,
        });
        pr.SentEntries(entries.Length > 0 ? entries[^1].Index : prevIndex, entries.Length > 0);
        return true;
    }

    private bool SendSnapshot(ulong to, Progress pr)
    {
        if (!pr.RecentActive) return false;
        var snap = _snapshotProvider();
        // The snapshot must cover everything we have compacted, otherwise the follower would have a gap.
        if (snap is null || snap.Index < Log.Offset) return false;
        pr.BecomeSnapshot(snap.Index);
        Send(new Message { Type = MessageType.Snapshot, To = to, Term = Term, Snapshot = snap });
        AddEvent($"sending snapshot at index {snap.Index} to {to}");
        return true;
    }

    private bool MaybeCommit()
    {
        var matches = new List<ulong>(Conf.Voters.Count);
        foreach (var id in Conf.Voters)
        {
            if (_prs.TryGetValue(id, out var pr)) matches.Add(pr.Match);
        }
        if (matches.Count < Conf.Quorum) return false;
        matches.Sort();
        matches.Reverse();
        ulong quorumIndex = matches[Conf.Quorum - 1];
        // Raft paper 5.4.2: only entries of the current term are committed by counting replicas.
        if (!Log.MaybeCommit(quorumIndex, Term)) return false;
        if (_readsWaitingForCommit.Count > 0 && CommittedInCurrentTerm())
        {
            var waiting = _readsWaitingForCommit.ToArray();
            _readsWaitingForCommit.Clear();
            foreach (var (ctx, mode) in waiting) RegisterRead(ctx, mode);
        }
        return true;
    }

    private bool CommittedInCurrentTerm() => Log.TermOrZero(Log.Committed) == Term;

    private bool QuorumActive()
    {
        int active = 0;
        foreach (var id in Conf.Voters)
        {
            if (id == Id || (_prs.TryGetValue(id, out var pr) && pr.RecentActive)) active++;
        }
        return active >= Conf.Quorum;
    }

    private void BroadcastHeartbeat()
    {
        ulong round = ++_roundSeq;
        var r = new Round(_tick);
        r.Acks.Add(Id);
        _rounds[round] = r;
        foreach (var (id, pr) in _prs)
        {
            if (id == Id) continue;
            Send(new Message
            {
                Type = MessageType.Heartbeat,
                To = id,
                Term = Term,
                Commit = Math.Min(pr.Match, Log.Committed),
                Context = round,
            });
        }
        CheckRound(round, r);
    }

    private void AckRound(ulong from, ulong round)
    {
        if (!_rounds.TryGetValue(round, out var r) || r.Done) return;
        r.Acks.Add(from);
        CheckRound(round, r);
    }

    private void CheckRound(ulong round, Round r)
    {
        int votes = 0;
        foreach (var id in r.Acks)
        {
            if (Conf.IsVoter(id)) votes++;
        }
        if (votes < Conf.Quorum) return;
        r.Done = true;

        if (!_hasLease || r.StartTick > _leaseStartTick)
        {
            _leaseStartTick = r.StartTick;
            _hasLease = true;
        }

        if (_pendingReads.Count > 0)
        {
            int n = 0;
            while (n < _pendingReads.Count && _pendingReads[n].Round <= round)
            {
                _readStates.Add(new ReadState(_pendingReads[n].Context, _pendingReads[n].Index));
                n++;
            }
            _pendingReads.RemoveRange(0, n);
        }

        // Earlier rounds are implied by this one.
        var older = _rounds.Keys.TakeWhile(k => k <= round).ToList();
        foreach (var k in older) _rounds.Remove(k);
    }

    private void PruneRounds()
    {
        // Rounds that never reached a quorum are useless once they are older than an election timeout.
        while (_rounds.Count > 0)
        {
            var first = _rounds.First();
            if (_tick - first.Value.StartTick <= (ulong)(_opt.ElectionTicks * 2)) break;
            _rounds.Remove(first.Key);
        }
    }

    // ------------------------------------------------------------------
    // Configuration
    // ------------------------------------------------------------------

    /// <summary>Configuration changes take effect as soon as they are in the log, committed or not.</summary>
    private void ApplyConfEntries(IReadOnlyList<Entry> entries)
    {
        bool changed = false;
        foreach (var e in entries)
        {
            if (e.Type != EntryType.ConfChange || e.Index <= _baseConfIndex) continue;
            var cc = Codec.DecodeConfChange(e.Data);
            Conf = Conf.Apply(cc);
            _lastConfIndex = e.Index;
            changed = true;
            AddEvent($"configuration {cc.Type} {cc.NodeId} at index {e.Index}: now {Conf}");
        }
        if (changed) SyncProgressWithConf();
    }

    /// <summary>After a truncation, a removed configuration entry must be undone (dissertation 4.1).</summary>
    private void RecomputeConf()
    {
        var conf = _baseConf;
        ulong last = 0;
        foreach (var e in Log.EntriesAfter(_baseConfIndex))
        {
            if (e.Type != EntryType.ConfChange) continue;
            conf = conf.Apply(Codec.DecodeConfChange(e.Data));
            last = e.Index;
        }
        bool changed = !conf.Equals(Conf);
        Conf = conf;
        _lastConfIndex = last;
        if (changed) SyncProgressWithConf();
    }

    private void SyncProgressWithConf()
    {
        foreach (var id in _prs.Keys.ToList())
        {
            if (id != Id && !Conf.Contains(id)) _prs.Remove(id);
        }
        foreach (var m in Conf.Members.Values)
        {
            if (_prs.TryGetValue(m.Id, out var pr))
            {
                pr.IsLearner = m.IsLearner;
                continue;
            }
            var np = new Progress(Log.LastIndex + 1, _opt.MaxInflight, m.IsLearner);
            if (m.Id == Id) np.ResetMatch(Log.LastIndex, Log.LastIndex + 1);
            _prs[m.Id] = np;
        }
        if (Role == Role.Leader)
        {
            // A smaller quorum may commit entries right away.
            if (MaybeCommit()) _broadcastRequested = true;
        }
    }

    private void AddEvent(string text)
    {
        _events.Add(text);
        Trace?.Invoke($"[{Id}] {text}");
    }

    // ------------------------------------------------------------------
    // Introspection
    // ------------------------------------------------------------------

    public HardState HardState => CurrentHard;

    public NodeStatus Status()
    {
        var progress = new Dictionary<ulong, ProgressStatus>();
        if (Role == Role.Leader)
        {
            foreach (var (id, pr) in _prs)
                progress[id] = new ProgressStatus(pr.Match, pr.Next, pr.State, pr.Inflight, pr.RecentActive, pr.IsLearner);
        }
        return new NodeStatus(Id, Role, Term, Vote, Leader, Log.Committed, Log.Applied, Log.LastIndex, Log.LastTerm,
            Log.FirstIndex, Conf, progress, LeadTransferee, LeaseValid);
    }
}
