using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Quorum.Core;
using Quorum.Protocol;
using Quorum.Storage;

namespace Quorum.Node;

/// <summary>
/// Delivers Raft messages to peers over gRPC. Each peer has an outgoing queue and one long-lived client stream
/// on which queued messages go out in batches; snapshots are streamed separately from the snapshot file.
/// Raft tolerates loss, duplication and reordering, so on any error the transport simply reconnects with
/// backoff and tells the Raft node the peer was unreachable (which drops it back to probing).
/// </summary>
public sealed class PeerTransport : IAsyncDisposable
{
    private readonly RaftHost _host;
    private readonly ILogger _log;
    private readonly ConcurrentDictionary<ulong, PeerClient> _peers = new();
    private readonly CancellationTokenSource _stop = new();

    public PeerTransport(RaftHost host, ILogger log)
    {
        _host = host;
        _log = log;
    }

    /// <summary>Starts clients for new members and repoints clients whose address changed.</summary>
    public void UpdatePeers(ConfState conf)
    {
        foreach (var m in conf.Members.Values)
        {
            if (m.Id == _host.Id || string.IsNullOrEmpty(m.Address)) continue;
            Learn(m.Id, m.Address);
        }
    }

    /// <summary>Registers a peer address (from the configuration, or from a message a peer sent us).</summary>
    public void Learn(ulong id, string address)
    {
        if (id == _host.Id || string.IsNullOrEmpty(address)) return;
        if (_peers.TryGetValue(id, out var existing))
        {
            if (existing.Address == address) return;
            if (_peers.TryRemove(id, out var old)) _ = old.DisposeAsync().AsTask();
        }
        var client = new PeerClient(id, address, _host, _log, _stop.Token);
        if (!_peers.TryAdd(id, client)) _ = client.DisposeAsync().AsTask();
    }

    public void Send(List<Message> messages)
    {
        foreach (var m in messages)
        {
            if (_peers.TryGetValue(m.To, out var peer)) peer.Enqueue(m);
            else _host.Metrics.Peer(m.To).Dropped++;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        foreach (var p in _peers.Values) await p.DisposeAsync();
        _peers.Clear();
    }

    private sealed class PeerClient : IAsyncDisposable
    {
        private const int MaxBatchMessages = 512;
        private const int MaxBatchBytes = 4 << 20;
        private const int SnapshotChunkBytes = 1 << 20;

        private readonly ulong _id;
        private readonly RaftHost _host;
        private readonly ILogger _log;
        private readonly Channel<Message> _queue = Channel.CreateBounded<Message>(new BoundedChannelOptions(16_384)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });
        private readonly GrpcChannel _channel;
        private readonly RaftTransport.RaftTransportClient _client;
        private readonly CancellationTokenSource _cts;
        private readonly Task _loop;
        private readonly NodeMetrics.PeerCounters _counters;
        private int _snapshotting;

        public string Address { get; }

        public PeerClient(ulong id, string address, RaftHost host, ILogger log, CancellationToken stop)
        {
            _id = id;
            Address = address;
            _host = host;
            _log = log;
            _counters = host.Metrics.Peer(id);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(stop);
            _channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
            {
                HttpHandler = new SocketsHttpHandler
                {
                    ConnectTimeout = TimeSpan.FromMilliseconds(500),
                    KeepAlivePingDelay = TimeSpan.FromSeconds(5),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(5),
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                    EnableMultipleHttp2Connections = true,
                },
                MaxSendMessageSize = 64 << 20,
                MaxReceiveMessageSize = 64 << 20,
            });
            _client = new RaftTransport.RaftTransportClient(_channel);
            _loop = Task.Run(RunAsync);
        }

        public void Enqueue(Message m)
        {
            if (m.Type == MessageType.Snapshot)
            {
                _ = Task.Run(() => SendSnapshotAsync(m));
                return;
            }
            if (!_queue.Writer.TryWrite(m)) _counters.Dropped++;
        }

        private async Task RunAsync()
        {
            var ct = _cts.Token;
            int backoff = 50;
            var reader = _queue.Reader;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var call = _client.Stream(cancellationToken: ct);
                    while (await reader.WaitToReadAsync(ct))
                    {
                        var batch = new RaftBatch { From = _host.Id, FromAddress = _host.Options.GrpcAddress };
                        int bytes = 0;
                        while (batch.Messages.Count < MaxBatchMessages && bytes < MaxBatchBytes && reader.TryRead(out var m))
                        {
                            var encoded = Codec.EncodeMessage(m);
                            batch.Messages.Add(UnsafeByteOperations.UnsafeWrap(encoded)); // the array is ours alone: no copy
                            bytes += encoded.Length;
                        }
                        if (batch.Messages.Count == 0) continue;

                        var faults = _host.Faults;
                        if (faults.IsBlocked(_id) || faults.ShouldDrop())
                        {
                            Interlocked.Add(ref _counters.Dropped, batch.Messages.Count);
                            continue;
                        }
                        int delay = faults.NextDelayMs();
                        if (delay > 0) await Task.Delay(delay, ct);

                        await call.RequestStream.WriteAsync(batch, ct);
                        Interlocked.Add(ref _counters.Sent, batch.Messages.Count);
                        Interlocked.Add(ref _counters.BytesSent, bytes);
                        _counters.Connected = true;
                        backoff = 50;
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (_counters.Connected) _log.LogDebug("peer {Peer} at {Address}: {Error}", _id, Address, ex.Message);
                    _counters.Connected = false;
                    Interlocked.Increment(ref _counters.Errors);
                    _host.ReportUnreachable(_id);
                    try { await Task.Delay(backoff, ct); }
                    catch (OperationCanceledException) { break; }
                    backoff = Math.Min(backoff * 2, 1000);
                }
            }
        }

        private async Task SendSnapshotAsync(Message m)
        {
            if (Interlocked.Exchange(ref _snapshotting, 1) == 1)
            {
                _host.ReportSnapshot(_id, false);
                return;
            }
            bool ok = false;
            try
            {
                if (_host.Faults.IsBlocked(_id)) return;
                var meta = m.Snapshot!;
                string path = _host.Store.SnapshotPath(meta.Index);
                await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete,
                    1 << 16, useAsync: true);
                using var call = _client.InstallSnapshot(deadline: DateTime.UtcNow.AddMinutes(5), cancellationToken: _cts.Token);
                var buffer = new byte[SnapshotChunkBytes];
                bool first = true;
                while (true)
                {
                    int n = await file.ReadAsync(buffer, _cts.Token);
                    bool last = file.Position >= file.Length;
                    var chunk = new SnapshotChunk { Data = ByteString.CopyFrom(buffer, 0, n), Last = last };
                    if (first)
                    {
                        chunk.Message = ByteString.CopyFrom(Codec.EncodeMessage(m));
                        chunk.From = _host.Id;
                        chunk.FromAddress = _host.Options.GrpcAddress;
                        first = false;
                    }
                    if (_host.Faults.IsBlocked(_id)) return;
                    await call.RequestStream.WriteAsync(chunk, _cts.Token);
                    if (last || n == 0) break;
                }
                await call.RequestStream.CompleteAsync();
                var ack = await call.ResponseAsync;
                ok = ack.Ok;
                if (!ok) _log.LogWarning("peer {Peer} rejected snapshot {Index}: {Error}", _id, meta.Index, ack.Error);
                else _host.Metrics.SnapshotSent();
            }
            catch (Exception ex) when (ex is RpcException or IOException or OperationCanceledException)
            {
                _log.LogDebug("snapshot to {Peer} failed: {Error}", _id, ex.Message);
            }
            finally
            {
                Volatile.Write(ref _snapshotting, 0);
                _host.ReportSnapshot(_id, ok);
            }
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            _queue.Writer.TryComplete();
            try { await _loop.WaitAsync(TimeSpan.FromSeconds(2)); }
            catch (Exception) { /* shutting down */ }
            _channel.Dispose();
            _cts.Dispose();
        }
    }
}

/// <summary>Server side of the node-to-node transport.</summary>
public sealed class RaftTransportService(RaftHost host, PeerTransport transport) : RaftTransport.RaftTransportBase
{
    public override async Task<StreamAck> Stream(IAsyncStreamReader<RaftBatch> requestStream, ServerCallContext context)
    {
        await foreach (var batch in requestStream.ReadAllAsync(context.CancellationToken))
        {
            if (!string.IsNullOrEmpty(batch.FromAddress)) transport.Learn(batch.From, batch.FromAddress);
            if (host.Faults.IsBlocked(batch.From)) continue;
            Interlocked.Add(ref host.Metrics.Peer(batch.From).Received, batch.Messages.Count);
            foreach (var bytes in batch.Messages) host.Deliver(Codec.DecodeMessage(bytes.Memory));
        }
        return new StreamAck();
    }

    public override async Task<SnapshotAck> InstallSnapshot(IAsyncStreamReader<SnapshotChunk> requestStream, ServerCallContext context)
    {
        string tmp = Path.Combine(host.Store.SnapshotDirectory, $"incoming-{Guid.NewGuid():N}.tmp");
        Message? message = null;
        try
        {
            await using (var file = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16, useAsync: true))
            {
                await foreach (var chunk in requestStream.ReadAllAsync(context.CancellationToken))
                {
                    if (message is null)
                    {
                        message = Codec.DecodeMessage(chunk.Message.Memory);
                        if (!string.IsNullOrEmpty(chunk.FromAddress)) transport.Learn(chunk.From, chunk.FromAddress);
                        if (host.Faults.IsBlocked(chunk.From)) throw new IOException("partitioned");
                    }
                    await file.WriteAsync(chunk.Data.Memory, context.CancellationToken);
                }
            }
            if (message?.Snapshot is null) throw new InvalidDataException("no snapshot message");
            var meta = NodeStore.VerifySnapshotFile(tmp);
            if (meta.Index != message.Snapshot.Index || meta.Term != message.Snapshot.Term)
                throw new InvalidDataException("snapshot file does not match its message");
            host.DeliverSnapshot(message, tmp);
            return new SnapshotAck { Ok = true };
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or FormatException or OperationCanceledException)
        {
            try { File.Delete(tmp); } catch (IOException) { }
            return new SnapshotAck { Ok = false, Error = ex.Message };
        }
    }
}

/// <summary>The client API.</summary>
public sealed class KvService(RaftHost host) : Kv.KvBase
{
    public override Task<KvResponse> Execute(KvRequest request, ServerCallContext context) => host.ExecuteAsync(request);
}
