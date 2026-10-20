using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Quorum.Protocol;

namespace Quorum.Client;

public sealed class QuorumClientOptions
{
    /// <summary>gRPC addresses of some (ideally all) nodes, e.g. http://127.0.0.1:7101.</summary>
    public required IReadOnlyList<string> Endpoints { get; init; }
    /// <summary>How long one attempt against one node may take.</summary>
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(2);
    /// <summary>How long an operation may take in total, retries included.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(10);
    /// <summary>The session id for exactly-once writes; random by default.</summary>
    public ulong? ClientId { get; init; }
    /// <summary>
    /// If set, every operation starts at this node (following redirects only within the operation), like a client
    /// behind a load balancer that always lands on the same node. Used by the chaos tester so that a node that
    /// wrongly believes it is still the leader keeps receiving requests.
    /// </summary>
    public string? HomeEndpoint { get; init; }
}

/// <summary>The outcome of one operation.</summary>
public sealed record KvResult(KvStatusCode Status, byte[]? Value, bool Found, ulong Index, ulong Node, IReadOnlyList<KeyValuePair<byte[], byte[]>> Pairs)
{
    public string? ValueString => Value is null ? null : Encoding.UTF8.GetString(Value);
    public bool Succeeded => Status is KvStatusCode.Ok or KvStatusCode.NotFound;
}

public class QuorumException(string message) : Exception(message);

/// <summary>
/// The operation did not complete in time. For a write, <see cref="MayHaveSucceeded"/> tells whether it might
/// still have been applied (a node accepted it and then lost contact); in that case its outcome is unknown.
/// </summary>
public sealed class QuorumTimeoutException(string message, bool mayHaveSucceeded) : QuorumException(message)
{
    public bool MayHaveSucceeded { get; } = mayHaveSucceeded;
}

/// <summary>
/// A client for a Quorum cluster. It finds the leader (following NOT_LEADER redirects), retries on failures
/// and timeouts, and gives every write a (client id, sequence number) so that retries are applied at most once.
/// Writes from one client are therefore sent one at a time; use several clients for parallel writes.
/// </summary>
public sealed class QuorumClient : IAsyncDisposable
{
    private readonly QuorumClientOptions _opt;
    // Channels are shared by every client in the process: a GrpcChannel is thread-safe and multiplexes calls
    // over a few HTTP/2 connections, so a thousand clients do not need a thousand connections.
    private static readonly ConcurrentDictionary<string, (GrpcChannel Channel, Kv.KvClient Client)> Channels = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private volatile string? _leader;
    private int _rotation;
    private ulong _seq;

    public ulong ClientId { get; }
    public string? CurrentLeader => _leader;

    public QuorumClient(QuorumClientOptions options)
    {
        if (options.Endpoints.Count == 0) throw new ArgumentException("at least one endpoint is required");
        _opt = options;
        ClientId = options.ClientId ?? (BitConverter.ToUInt64(RandomNumberGenerator.GetBytes(8)) | 1);
        _rotation = Random.Shared.Next(options.Endpoints.Count);
    }

    public QuorumClient(params string[] endpoints) : this(new QuorumClientOptions { Endpoints = endpoints }) { }

    private static Kv.KvClient ClientFor(string address) => Channels.GetOrAdd(address, a =>
    {
        var ch = GrpcChannel.ForAddress(a, new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                ConnectTimeout = TimeSpan.FromMilliseconds(500),
                EnableMultipleHttp2Connections = true,
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            },
        });
        return (ch, new Kv.KvClient(ch));
    }).Client;

    private static ByteString B(string s) => ByteString.CopyFromUtf8(s);

    public Task<KvResult> PutAsync(string key, string value, CancellationToken ct = default) =>
        PutAsync(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), ct);

    public Task<KvResult> PutAsync(byte[] key, byte[] value, CancellationToken ct = default) =>
        WriteAsync(new KvRequest { Op = KvOperation.Put, Key = ByteString.CopyFrom(key), Value = ByteString.CopyFrom(value), HasValue = true }, ct);

    public Task<KvResult> DeleteAsync(string key, CancellationToken ct = default) =>
        WriteAsync(new KvRequest { Op = KvOperation.Delete, Key = B(key) }, ct);

    /// <summary>
    /// Compare-and-set: sets <paramref name="key"/> to <paramref name="newValue"/> (or deletes it, if null) only if
    /// its current value is <paramref name="expected"/> (or it does not exist, if <paramref name="expected"/> is null).
    /// The status is CAS_FAILED, with the current value, if the condition did not hold.
    /// </summary>
    public Task<KvResult> CasAsync(string key, string? expected, string? newValue, CancellationToken ct = default)
    {
        var req = new KvRequest { Op = KvOperation.Cas, Key = B(key), ExpectAbsent = expected is null };
        if (expected is not null) req.Expected = B(expected);
        if (newValue is not null)
        {
            req.Value = B(newValue);
            req.HasValue = true;
        }
        return WriteAsync(req, ct);
    }

    public Task<KvResult> GetAsync(string key, ReadConsistency consistency = ReadConsistency.Linearizable, CancellationToken ct = default) =>
        SendAsync(new KvRequest { Op = KvOperation.Get, Key = B(key), Consistency = consistency }, isWrite: false, ct);

    public Task<KvResult> ScanAsync(string prefix, int limit = 100, ReadConsistency consistency = ReadConsistency.Linearizable, CancellationToken ct = default) =>
        SendAsync(new KvRequest { Op = KvOperation.Scan, Key = B(prefix), Limit = (uint)limit, Consistency = consistency }, isWrite: false, ct);

    private async Task<KvResult> WriteAsync(KvRequest request, CancellationToken ct)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            request.ClientId = ClientId;
            request.Seq = ++_seq;
            return await SendAsync(request, isWrite: true, ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task<KvResult> SendAsync(KvRequest request, bool isWrite, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        bool uncertain = false;
        string lastError = "no attempt made";
        int attempt = 0;
        request.TimeoutMs = (uint)_opt.AttemptTimeout.TotalMilliseconds;
        if (_opt.HomeEndpoint is not null) _leader = _opt.HomeEndpoint;

        while (sw.Elapsed < _opt.OperationTimeout)
        {
            ct.ThrowIfCancellationRequested();
            string address = _leader ?? _opt.Endpoints[(int)((uint)Interlocked.Increment(ref _rotation) % (uint)_opt.Endpoints.Count)];
            attempt++;
            KvResponse response;
            try
            {
                var remaining = _opt.OperationTimeout - sw.Elapsed;
                var budget = remaining < _opt.AttemptTimeout ? remaining : _opt.AttemptTimeout;
                if (budget <= TimeSpan.Zero) break;
                request.TimeoutMs = (uint)Math.Max(1, budget.TotalMilliseconds);
                // The call deadline is a little longer than the node's own timeout, so that normally the node
                // answers TIMEOUT itself rather than the call being cut off.
                response = await ClientFor(address).ExecuteAsync(request,
                    deadline: DateTime.UtcNow + budget + TimeSpan.FromMilliseconds(250), cancellationToken: ct);
            }
            catch (RpcException) when (ct.IsCancellationRequested)
            {
                throw new OperationCanceledException(ct);
            }
            catch (RpcException ex)
            {
                // The connection may have failed after the node received the request, so a failed write
                // attempt may still be applied: from now on its outcome is unknown unless a retry answers.
                if (isWrite) uncertain = true;
                lastError = $"{address}: {ex.StatusCode}";
                _leader = null;
                await Backoff(attempt, ct);
                continue;
            }

            switch (response.Status)
            {
                case KvStatusCode.Ok:
                case KvStatusCode.NotFound:
                case KvStatusCode.CasFailed:
                case KvStatusCode.StaleRequest:
                    _leader = address;
                    return new KvResult(response.Status, response.Value.Length > 0 || response.Found ? response.Value.ToByteArray() : null,
                        response.Found, response.Index, response.NodeId,
                        response.Pairs.Select(p => new KeyValuePair<byte[], byte[]>(p.Key.ToByteArray(), p.Value.ToByteArray())).ToList());

                case KvStatusCode.NotLeader:
                    lastError = $"{address}: not the leader";
                    if (!string.IsNullOrEmpty(response.LeaderAddress) && response.LeaderAddress != address)
                    {
                        _leader = response.LeaderAddress;
                        continue; // follow the redirect at once
                    }
                    _leader = null;
                    await Backoff(attempt, ct);
                    continue;

                case KvStatusCode.Timeout:
                    if (isWrite) uncertain = true;
                    lastError = $"{address}: timed out ({response.Error})";
                    _leader = null;
                    await Backoff(attempt, ct);
                    continue;

                default:
                    throw new QuorumException($"{address}: {response.Status}: {response.Error}");
            }
        }
        throw new QuorumTimeoutException(
            $"operation did not complete within {_opt.OperationTimeout.TotalSeconds:0.#} s after {attempt} attempts (last: {lastError})",
            isWrite && uncertain);
    }

    private static Task Backoff(int attempt, CancellationToken ct) =>
        Task.Delay(Math.Min(10 * (1 << Math.Min(attempt, 5)), 250) + Random.Shared.Next(10), ct);

    public ValueTask DisposeAsync()
    {
        _writeLock.Dispose();
        return ValueTask.CompletedTask;
    }
}
