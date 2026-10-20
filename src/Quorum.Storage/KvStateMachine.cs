using System.Buffers.Binary;
using System.Security.Cryptography;
using Quorum.Core;

namespace Quorum.Storage;

/// <summary>
/// The replicated key-value map, kept in Tessera under the <c>k</c> prefix, plus the client session table
/// under <c>s</c>. Every method runs inside the caller's Tessera transaction, on the node's single loop thread.
/// </summary>
public sealed class KvStateMachine
{
    public const byte KvPrefix = (byte)'k';
    public const byte SessionPrefix = (byte)'s';
    public const int MaxKeyBytes = 999; // Tessera allows 1000 including our one-byte prefix

    private readonly Tessera _db;
    private readonly int _maxSessions;

    // In-memory index of the session table, rebuilt from storage on start and after a snapshot restore.
    private readonly Dictionary<ulong, (ulong Seq, ulong Index)> _sessions = [];
    private readonly SortedSet<(ulong Index, ulong Client)> _byAge = [];

    public KvStateMachine(Tessera db, int maxSessions = 10_000)
    {
        _db = db;
        _maxSessions = maxSessions;
        ReloadSessions();
    }

    public int SessionCount => _sessions.Count;

    public static byte[] DataKey(ReadOnlySpan<byte> key)
    {
        if (key.Length > MaxKeyBytes) throw new ArgumentException($"keys are limited to {MaxKeyBytes} bytes");
        var k = new byte[(byte)(key.Length + 1)];
        k[0] = KvPrefix;
        key.CopyTo(k.AsSpan(1));
        return k;
    }

    private static byte[] SessionKey(ulong client)
    {
        var k = new byte[9];
        k[0] = SessionPrefix;
        BinaryPrimitives.WriteUInt64BigEndian(k.AsSpan(1), client);
        return k;
    }

    public void ReloadSessions()
    {
        _sessions.Clear();
        _byAge.Clear();
        foreach (var kv in _db.Scan([SessionPrefix], [(byte)(SessionPrefix + 1)]))
        {
            ulong client = BinaryPrimitives.ReadUInt64BigEndian(kv.Key.AsSpan(1));
            var r = new BinReader(kv.Value);
            ulong seq = r.VarU();
            ulong index = r.VarU();
            _sessions[client] = (seq, index);
            _byAge.Add((index, client));
        }
    }

    /// <summary>Applies a committed command. Deterministic: every replica computes the same result.</summary>
    public KvResult Apply(ulong index, KvCommand cmd)
    {
        if (cmd.ClientId != 0 && _sessions.TryGetValue(cmd.ClientId, out var session))
        {
            if (cmd.Seq == session.Seq)
            {
                // A retry of the last request: answer from the session, do not apply again.
                var stored = _db.Get(SessionKey(cmd.ClientId));
                if (stored is not null)
                {
                    var r = new BinReader(stored);
                    r.VarU();
                    r.VarU();
                    return KvResult.Decode(r.Bytes());
                }
            }
            else if (cmd.Seq < session.Seq)
            {
                return new KvResult(KvStatus.Stale, false, null, index);
            }
        }

        var key = DataKey(cmd.Key);
        KvResult result;
        switch (cmd.Op)
        {
            case KvOp.Put:
            {
                bool existed = _db.Get(key) is not null;
                _db.Put(key, cmd.Value ?? []);
                result = new KvResult(KvStatus.Ok, existed, null, index);
                break;
            }
            case KvOp.Delete:
            {
                bool existed = _db.Delete(key);
                result = new KvResult(KvStatus.Ok, existed, null, index);
                break;
            }
            case KvOp.Cas:
            {
                var current = _db.Get(key);
                bool matches = cmd.ExpectAbsent
                    ? current is null
                    : current is not null && cmd.Expected is not null && current.AsSpan().SequenceEqual(cmd.Expected);
                if (matches)
                {
                    if (cmd.Value is null) _db.Delete(key);
                    else _db.Put(key, cmd.Value);
                    result = new KvResult(KvStatus.Ok, current is not null, null, index);
                }
                else
                {
                    result = new KvResult(KvStatus.CasFailed, current is not null, current, index);
                }
                break;
            }
            default:
                throw new FormatException($"unknown operation {cmd.Op}");
        }

        if (cmd.ClientId != 0) SaveSession(cmd.ClientId, cmd.Seq, index, result);
        return result;
    }

    private void SaveSession(ulong client, ulong seq, ulong index, KvResult result)
    {
        if (_sessions.TryGetValue(client, out var old)) _byAge.Remove((old.Index, client));
        _sessions[client] = (seq, index);
        _byAge.Add((index, client));
        _db.Put(SessionKey(client), new BinWriter(64).VarU(seq).VarU(index).Bytes(result.Encode()).ToArray());

        // Evict the least recently used sessions. Deterministic, because every replica sees the same indices.
        while (_sessions.Count > _maxSessions)
        {
            var oldest = _byAge.Min;
            _byAge.Remove(oldest);
            _sessions.Remove(oldest.Client);
            _db.Delete(SessionKey(oldest.Client));
        }
    }

    public byte[]? Get(ReadOnlySpan<byte> key) => _db.Get(DataKey(key));

    /// <summary>Keys starting with <paramref name="prefix"/>, in order, at most <paramref name="limit"/>.</summary>
    public List<KeyValuePair<byte[], byte[]>> Scan(ReadOnlySpan<byte> prefix, int limit)
    {
        var lo = DataKey(prefix);
        var hi = PrefixEnd(lo);
        var result = new List<KeyValuePair<byte[], byte[]>>();
        foreach (var kv in _db.Scan(lo, hi))
        {
            if (result.Count >= limit) break;
            result.Add(new KeyValuePair<byte[], byte[]>(kv.Key[1..], kv.Value));
        }
        return result;
    }

    /// <summary>The smallest key greater than every key with this prefix (null if there is none).</summary>
    public static byte[]? PrefixEnd(byte[] prefix)
    {
        var end = (byte[])prefix.Clone();
        for (int i = end.Length - 1; i >= 0; i--)
        {
            if (end[i] != 0xFF)
            {
                end[i]++;
                return end[..(i + 1)];
            }
        }
        return null;
    }

    public long CountKeys()
    {
        long n = 0;
        foreach (var _ in _db.ScanKeys([KvPrefix], [(byte)(KvPrefix + 1)])) n++;
        return n;
    }

    /// <summary>A hash of the whole key-value map: replicas at the same applied index must agree on it.</summary>
    public string Digest()
    {
        using var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> len = stackalloc byte[4];
        foreach (var kv in _db.Scan([KvPrefix], [(byte)(KvPrefix + 1)]))
        {
            BinaryPrimitives.WriteInt32LittleEndian(len, kv.Key.Length);
            h.AppendData(len);
            h.AppendData(kv.Key);
            BinaryPrimitives.WriteInt32LittleEndian(len, kv.Value.Length);
            h.AppendData(len);
            h.AppendData(kv.Value);
        }
        return Convert.ToHexString(h.GetHashAndReset())[..16];
    }
}
