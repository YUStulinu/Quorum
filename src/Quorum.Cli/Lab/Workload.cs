using System.Collections.Concurrent;
using System.Diagnostics;
using Quorum.Client;
using Quorum.Linearizability;
using Quorum.Protocol;

namespace Quorum.Cli.Lab;

public sealed record WorkloadOptions
{
    public int Clients { get; init; } = 8;
    public int Keys { get; init; } = 8;
    public double ReadRatio { get; init; } = 0.5;
    public double CasRatio { get; init; } = 0.2;
    /// <summary>Target operations per second over all clients; 0 = as fast as possible.</summary>
    public int Rate { get; init; }
    public ReadConsistency Consistency { get; init; } = ReadConsistency.Linearizable;
    public bool RecordHistory { get; init; }
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(1);
    public int ValueSize { get; init; }
    public string KeyPrefix { get; init; } = "k";
    /// <summary>Each client starts every operation at its own node (client i at node i mod n).</summary>
    public bool PinClients { get; init; }
}

/// <summary>One second of workload statistics.</summary>
public sealed record WorkloadSecond(long Second, int Ok, int Failed, int Unknown, double P50Ms, double P99Ms, double MaxMs);

/// <summary>
/// Concurrent clients doing random reads, writes and compare-and-sets on a small set of keys — small, so that
/// operations collide and the linearizability check has something to bite on. Every operation is recorded
/// with its call and return times on one shared clock.
/// </summary>
public sealed class Workload
{
    private readonly Func<string[]> _endpoints;
    private readonly Stopwatch _clock;
    private readonly ConcurrentQueue<Operation> _history = new();
    private readonly object _statsLock = new();
    private readonly List<double> _currentLatencies = [];
    private readonly Queue<WorkloadSecond> _seconds = new();
    private long _currentSecond = -1;
    private int _curOk, _curFailed, _curUnknown;
    private int _opId;
    private long _totalOk, _totalFailed, _totalUnknown;
    private readonly List<double> _allLatencies = [];

    public WorkloadOptions Options { get; }
    public bool Running { get; private set; }

    public Workload(Func<string[]> endpoints, WorkloadOptions options, Stopwatch? clock = null)
    {
        _endpoints = endpoints;
        Options = options;
        _clock = clock ?? Stopwatch.StartNew();
    }

    public IReadOnlyList<Operation> History => _history.OrderBy(o => o.Id).ToList();
    public long TotalOk => Interlocked.Read(ref _totalOk);
    public long TotalFailed => Interlocked.Read(ref _totalFailed);
    public long TotalUnknown => Interlocked.Read(ref _totalUnknown);

    public IReadOnlyList<WorkloadSecond> RecentSeconds
    {
        get
        {
            lock (_statsLock)
            {
                RollSecond(_clock.ElapsedMilliseconds / 1000);
                return _seconds.ToList();
            }
        }
    }

    public (double P50, double P90, double P99, double Max) LatencySummary()
    {
        lock (_statsLock)
        {
            if (_allLatencies.Count == 0) return (0, 0, 0, 0);
            var l = _allLatencies.ToArray();
            Array.Sort(l);
            double P(double q) => l[Math.Min(l.Length - 1, (int)(q * l.Length))];
            return (P(0.5), P(0.9), P(0.99), l[^1]);
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        Running = true;
        try
        {
            await Task.WhenAll(Enumerable.Range(0, Options.Clients).Select(i => Task.Run(() => ClientLoop(i, ct), ct)));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        finally
        {
            Running = false;
        }
    }

    private async Task ClientLoop(int index, CancellationToken ct)
    {
        var rand = new Random(HashCode.Combine(index, Environment.TickCount));
        var endpoints = _endpoints();
        await using var client = new QuorumClient(new QuorumClientOptions
        {
            Endpoints = endpoints,
            OperationTimeout = Options.OperationTimeout,
            AttemptTimeout = Options.AttemptTimeout,
            HomeEndpoint = Options.PinClients ? endpoints[index % endpoints.Length] : null,
        });
        var lastSeen = new Dictionary<string, string?>();
        int n = 0;
        double interval = Options.Rate > 0 ? 1000.0 * Options.Clients / Options.Rate : 0;
        var paceClock = Stopwatch.StartNew();
        string padding = Options.ValueSize > 0 ? new string('x', Options.ValueSize) : "";

        while (!ct.IsCancellationRequested)
        {
            if (interval > 0)
            {
                double due = n * interval;
                double wait = due - paceClock.Elapsed.TotalMilliseconds;
                if (wait > 1) await Task.Delay(TimeSpan.FromMilliseconds(wait), ct);
            }
            n++;
            string key = $"{Options.KeyPrefix}{rand.Next(Options.Keys)}";
            double roll = rand.NextDouble();
            string value = $"{index}-{n}{padding}";
            int id = Interlocked.Increment(ref _opId);
            long call = _clock.ElapsedTicks;
            var op = new Operation
            {
                Id = id,
                Client = index,
                Key = key,
                Kind = roll < Options.ReadRatio ? OpKind.Read : roll < Options.ReadRatio + Options.CasRatio ? OpKind.Cas : OpKind.Write,
                Call = call,
                Return = call,
                Outcome = OpOutcome.Unknown,
            };
            try
            {
                switch (op.Kind)
                {
                    case OpKind.Read:
                    {
                        var r = await client.GetAsync(key, Options.Consistency, ct);
                        string? seen = r.Found ? r.ValueString : null;
                        lastSeen[key] = seen;
                        op = op with { Observed = seen, Outcome = OpOutcome.Ok, Node = r.Node };
                        break;
                    }
                    case OpKind.Write:
                    {
                        op = op with { Value = value };
                        var r = await client.PutAsync(key, value, ct);
                        lastSeen[key] = value;
                        op = op with { Outcome = r.Status == KvStatusCode.StaleRequest ? OpOutcome.Fail : OpOutcome.Ok, Node = r.Node };
                        break;
                    }
                    default:
                    {
                        lastSeen.TryGetValue(key, out var expected);
                        op = op with { Expected = expected, Value = value };
                        var r = await client.CasAsync(key, expected, value, ct);
                        bool ok = r.Status == KvStatusCode.Ok;
                        lastSeen[key] = ok ? value : (r.Found ? r.ValueString : null);
                        op = op with
                        {
                            CasSucceeded = ok,
                            Outcome = r.Status == KvStatusCode.StaleRequest ? OpOutcome.Fail : OpOutcome.Ok,
                            Node = r.Node,
                        };
                        break;
                    }
                }
            }
            catch (QuorumTimeoutException ex)
            {
                op = op with { Outcome = op.Kind == OpKind.Read ? OpOutcome.Fail : ex.MayHaveSucceeded ? OpOutcome.Unknown : OpOutcome.Fail };
            }
            catch (QuorumException)
            {
                op = op with { Outcome = op.Kind == OpKind.Read ? OpOutcome.Fail : OpOutcome.Unknown };
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Cut off by the end of the run: a write in flight may or may not have happened.
                if (op.Kind != OpKind.Read && Options.RecordHistory) _history.Enqueue(op with { Outcome = OpOutcome.Unknown });
                return;
            }
            long ret = _clock.ElapsedTicks;
            op = op with { Return = ret };
            if (Options.RecordHistory) _history.Enqueue(op);
            Record(op.Outcome, (ret - call) * 1000.0 / Stopwatch.Frequency);
        }
    }

    private void Record(OpOutcome outcome, double ms)
    {
        switch (outcome)
        {
            case OpOutcome.Ok: Interlocked.Increment(ref _totalOk); break;
            case OpOutcome.Fail: Interlocked.Increment(ref _totalFailed); break;
            default: Interlocked.Increment(ref _totalUnknown); break;
        }
        lock (_statsLock)
        {
            RollSecond(_clock.ElapsedMilliseconds / 1000);
            switch (outcome)
            {
                case OpOutcome.Ok:
                    _curOk++;
                    _currentLatencies.Add(ms);
                    if (_allLatencies.Count < 2_000_000) _allLatencies.Add(ms);
                    break;
                case OpOutcome.Fail: _curFailed++; break;
                default: _curUnknown++; break;
            }
        }
    }

    private void RollSecond(long second)
    {
        if (_currentSecond == second) return;
        if (_currentSecond >= 0)
        {
            var l = _currentLatencies.ToArray();
            Array.Sort(l);
            double P(double q) => l.Length == 0 ? 0 : l[Math.Min(l.Length - 1, (int)(q * l.Length))];
            _seconds.Enqueue(new WorkloadSecond(_currentSecond, _curOk, _curFailed, _curUnknown,
                Math.Round(P(0.5), 2), Math.Round(P(0.99), 2), l.Length == 0 ? 0 : Math.Round(l[^1], 2)));
            // Seconds with no operation at all still appear, so charts show outages as gaps.
            for (long s = _currentSecond + 1; s < second && s < _currentSecond + 120; s++)
                _seconds.Enqueue(new WorkloadSecond(s, 0, 0, 0, 0, 0, 0));
            while (_seconds.Count > 300) _seconds.Dequeue();
        }
        _currentSecond = second;
        _curOk = _curFailed = _curUnknown = 0;
        _currentLatencies.Clear();
    }
}
