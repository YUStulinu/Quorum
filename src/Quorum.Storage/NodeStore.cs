using System.Buffers.Binary;
using System.Security.Cryptography;
using Quorum.Core;

namespace Quorum.Storage;

/// <summary>
/// Everything one node keeps on disk, in a single Tessera database:
/// <list type="bullet">
/// <item><c>m/...</c> metadata: hard state, log offset, applied index and configuration, latest snapshot;</item>
/// <item><c>l</c> + big-endian index: the Raft log;</item>
/// <item><c>k</c> + key: the key-value data; <c>s</c> + client id: client sessions.</item>
/// </list>
/// Because the log, the hard state and the state machine live in one database, a Ready is persisted and applied
/// in one transaction with one fsync, and a crash can never leave the applied index out of step with the data.
/// Snapshots are files in <c>snapshots/</c>, written by <see cref="CreateSnapshot"/> and sent to lagging followers.
/// </summary>
public sealed class NodeStore : IDisposable
{
    private const byte LogPrefix = (byte)'l';
    private static readonly byte[] HardKey = "m/hard"u8.ToArray();
    private static readonly byte[] OffsetKey = "m/offset"u8.ToArray();
    private static readonly byte[] AppliedKey = "m/applied"u8.ToArray();
    private static readonly byte[] AppliedConfKey = "m/aconf"u8.ToArray();
    private static readonly byte[] SnapshotKey = "m/snap"u8.ToArray();
    private static readonly byte[] SnapshotMagic = "QSNAP01\n"u8.ToArray();

    private readonly Tessera _db;
    private ulong _lastIndex;

    public string Directory { get; }
    public string SnapshotDirectory { get; }
    public KvStateMachine Kv { get; }

    public HardState HardState { get; private set; }
    public ulong Offset { get; private set; }
    public ulong OffsetTerm { get; private set; }
    public ulong Applied { get; private set; }
    public ConfState AppliedConf { get; private set; } = ConfState.Empty;
    public SnapshotMeta? Snapshot { get; private set; }
    public ulong LastIndex => _lastIndex;

    private NodeStore(string dir, Tessera db, int maxSessions)
    {
        Directory = dir;
        SnapshotDirectory = Path.Combine(dir, "snapshots");
        System.IO.Directory.CreateDirectory(SnapshotDirectory);
        _db = db;
        Kv = new KvStateMachine(db, maxSessions);
        LoadMeta();
    }

    public static NodeStore Open(string dir, TesseraSync sync = TesseraSync.Full, int maxSessions = 10_000)
    {
        System.IO.Directory.CreateDirectory(dir);
        var db = Tessera.Open(Path.Combine(dir, "node.tdb"), sync);
        return new NodeStore(dir, db, maxSessions);
    }

    public Tessera Database => _db;

    /// <summary>True for a node that has never been part of a cluster.</summary>
    public bool IsEmpty => HardState == HardState.Empty && _lastIndex == 0 && Snapshot is null;

    private void LoadMeta()
    {
        HardState = _db.Get(HardKey) is { } h ? Codec.DecodeHardState(h) : HardState.Empty;
        if (_db.Get(OffsetKey) is { } o)
        {
            var r = new BinReader(o);
            Offset = r.VarU();
            OffsetTerm = r.VarU();
        }
        Applied = _db.Get(AppliedKey) is { } a ? new BinReader(a).VarU() : 0;
        AppliedConf = _db.Get(AppliedConfKey) is { } c ? Codec.ReadConf(new BinReader(c)) : ConfState.Empty;
        Snapshot = _db.Get(SnapshotKey) is { } s ? Codec.DecodeSnapshotMeta(s) : null;
        _lastIndex = Offset;
        foreach (var key in _db.ScanKeys([LogPrefix], [(byte)(LogPrefix + 1)]))
            _lastIndex = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(1));
    }

    private static byte[] LogKey(ulong index)
    {
        var k = new byte[9];
        k[0] = LogPrefix;
        BinaryPrimitives.WriteUInt64BigEndian(k.AsSpan(1), index);
        return k;
    }

    /// <summary>
    /// Writes the initial configuration of a new cluster: one AddVoter entry per member at term 1, committed.
    /// Every founding member writes the same entries, so their logs start identical.
    /// </summary>
    public void Bootstrap(IReadOnlyList<Member> members)
    {
        if (!IsEmpty) throw new InvalidOperationException("the node already has state; refusing to bootstrap");
        Begin();
        ulong index = 0;
        var entries = new List<Entry>();
        foreach (var m in members.OrderBy(m => m.Id))
        {
            entries.Add(new Entry
            {
                Index = ++index,
                Term = 1,
                Type = EntryType.ConfChange,
                Data = Codec.EncodeConfChange(new ConfChange(m.IsLearner ? ConfChangeType.AddLearner : ConfChangeType.AddVoter, m.Id, m.Address)),
            });
        }
        AppendEntries(entries);
        SetHardState(new HardState(1, 0, index));
        Commit();
    }

    public RaftInitialState LoadInitialState()
    {
        var entries = new List<Entry>();
        foreach (var kv in _db.Scan(LogKey(Offset + 1), [(byte)(LogPrefix + 1)]))
            entries.Add(Codec.DecodeEntry(kv.Value));
        return new RaftInitialState
        {
            HardState = HardState,
            LogOffset = Offset,
            LogOffsetTerm = OffsetTerm,
            Entries = entries,
            Snapshot = Snapshot,
            Applied = Applied,
        };
    }

    // ------------------------------------------------------------------
    // Transactions
    // ------------------------------------------------------------------

    public void Begin()
    {
        if (!_db.InTransaction) _db.Begin();
    }

    public void Commit()
    {
        if (_db.InTransaction) _db.Commit();
    }

    public void Rollback()
    {
        if (_db.InTransaction) _db.Rollback();
        LoadMeta();
        Kv.ReloadSessions();
    }

    // ------------------------------------------------------------------
    // Raft state
    // ------------------------------------------------------------------

    /// <summary>Stores entries, replacing any stored entries from the first one's index on.</summary>
    public void AppendEntries(IReadOnlyList<Entry> entries)
    {
        if (entries.Count == 0) return;
        ulong first = entries[0].Index;
        if (first <= Offset) throw new InvalidOperationException($"append at {first} is below the log offset {Offset}");
        for (ulong i = first; i <= _lastIndex; i++) _db.Delete(LogKey(i));
        foreach (var e in entries) _db.Put(LogKey(e.Index), Codec.EncodeEntry(e));
        _lastIndex = entries[^1].Index;
    }

    public void SetHardState(HardState hs)
    {
        _db.Put(HardKey, Codec.EncodeHardState(hs));
        HardState = hs;
    }

    public void SetApplied(ulong index)
    {
        _db.Put(AppliedKey, new BinWriter(12).VarU(index).ToArray());
        Applied = index;
    }

    public void SetAppliedConf(ConfState conf)
    {
        var w = new BinWriter();
        Codec.WriteConf(w, conf);
        _db.Put(AppliedConfKey, w.ToArray());
        AppliedConf = conf;
    }

    /// <summary>Deletes log entries up to and including <paramref name="index"/>.</summary>
    public void CompactLog(ulong index, ulong term)
    {
        if (index <= Offset) return;
        for (ulong i = Offset + 1; i <= index && i <= _lastIndex; i++) _db.Delete(LogKey(i));
        Offset = index;
        OffsetTerm = term;
        _db.Put(OffsetKey, new BinWriter(24).VarU(index).VarU(term).ToArray());
        if (_lastIndex < index) _lastIndex = index;
    }

    // ------------------------------------------------------------------
    // Snapshots
    // ------------------------------------------------------------------

    public string SnapshotPath(ulong index) => Path.Combine(SnapshotDirectory, $"snap-{index:D20}.qsnap");

    /// <summary>
    /// Writes the state machine as of the applied index to a snapshot file and records it as the latest snapshot.
    /// The caller must run this on the loop thread, between transactions, so the view is consistent.
    /// </summary>
    public SnapshotMeta CreateSnapshot(ulong term)
    {
        var meta = new SnapshotMeta(Applied, term, AppliedConf);
        string path = SnapshotPath(meta.Index);
        string tmp = path + ".tmp";
        using (var file = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 1 << 16))
        using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            var w = new BinWriter(1 << 16);
            void Flush()
            {
                hash.AppendData(w.Span);
                file.Write(w.Span);
                w.Clear();
            }

            w.Raw(SnapshotMagic);
            var metaBytes = Codec.EncodeSnapshotMeta(meta);
            w.Bytes(metaBytes);
            foreach (var kv in _db.Scan([KvStateMachine.KvPrefix], [(byte)(KvStateMachine.KvPrefix + 1)]))
            {
                w.U8(1).Bytes(kv.Key).Bytes(kv.Value);
                if (w.Length > 1 << 16) Flush();
            }
            foreach (var kv in _db.Scan([KvStateMachine.SessionPrefix], [(byte)(KvStateMachine.SessionPrefix + 1)]))
            {
                w.U8(1).Bytes(kv.Key).Bytes(kv.Value);
                if (w.Length > 1 << 16) Flush();
            }
            w.U8(0);
            Flush();
            file.Write(hash.GetHashAndReset());
            file.Flush(flushToDisk: true);
        }
        File.Move(tmp, path, overwrite: true);

        Begin();
        _db.Put(SnapshotKey, Codec.EncodeSnapshotMeta(meta));
        Commit();
        Snapshot = meta;
        DeleteOldSnapshots(keep: 2);
        return meta;
    }

    private void DeleteOldSnapshots(int keep)
    {
        var files = System.IO.Directory.GetFiles(SnapshotDirectory, "snap-*.qsnap").OrderDescending().Skip(keep);
        foreach (var f in files)
        {
            try { File.Delete(f); }
            catch (IOException) { /* still being streamed to a follower; removed next time */ }
        }
        foreach (var f in System.IO.Directory.GetFiles(SnapshotDirectory, "*.tmp"))
        {
            try
            {
                if (File.GetLastWriteTimeUtc(f) < DateTime.UtcNow.AddMinutes(-10)) File.Delete(f);
            }
            catch (IOException) { }
        }
    }

    /// <summary>Checks a snapshot file's checksum and returns its metadata.</summary>
    public static SnapshotMeta VerifySnapshotFile(string path)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete);
        if (file.Length < SnapshotMagic.Length + 32) throw new InvalidDataException("snapshot file too short");
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long bodyLength = file.Length - 32;
        var buf = new byte[1 << 16];
        long remaining = bodyLength;
        var head = new byte[Math.Min(4096, bodyLength)];
        file.ReadExactly(head);
        hash.AppendData(head);
        remaining -= head.Length;
        while (remaining > 0)
        {
            int n = file.Read(buf, 0, (int)Math.Min(buf.Length, remaining));
            if (n <= 0) throw new InvalidDataException("snapshot file truncated");
            hash.AppendData(buf, 0, n);
            remaining -= n;
        }
        var expected = new byte[32];
        file.ReadExactly(expected);
        if (!hash.GetHashAndReset().AsSpan().SequenceEqual(expected))
            throw new InvalidDataException("snapshot checksum mismatch");
        if (!head.AsSpan(0, SnapshotMagic.Length).SequenceEqual(SnapshotMagic))
            throw new InvalidDataException("not a Quorum snapshot");
        var r = new BinReader(head.AsMemory(SnapshotMagic.Length));
        return Codec.DecodeSnapshotMeta(r.Bytes());
    }

    /// <summary>
    /// Replaces the state machine with the contents of a snapshot file and resets the log to the snapshot point.
    /// Runs inside the caller's transaction.
    /// </summary>
    public void RestoreSnapshot(string path, SnapshotMeta expected)
    {
        var meta = VerifySnapshotFile(path);
        if (meta.Index != expected.Index || meta.Term != expected.Term)
            throw new InvalidDataException($"snapshot file is {meta.Index}@{meta.Term}, expected {expected.Index}@{expected.Term}");

        foreach (var key in _db.ScanKeys([KvStateMachine.KvPrefix], [(byte)(KvStateMachine.KvPrefix + 1)]).ToList()) _db.Delete(key);
        foreach (var key in _db.ScanKeys([KvStateMachine.SessionPrefix], [(byte)(KvStateMachine.SessionPrefix + 1)]).ToList()) _db.Delete(key);
        foreach (var key in _db.ScanKeys([LogPrefix], [(byte)(LogPrefix + 1)]).ToList()) _db.Delete(key);

        byte[] all;
        using (var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 1 << 16))
        {
            all = new byte[file.Length];
            file.ReadExactly(all);
        }
        var reader = new BinReader(all.AsMemory(SnapshotMagic.Length, all.Length - SnapshotMagic.Length - 32));
        reader.Bytes(); // metadata
        while (reader.U8() == 1)
        {
            var key = reader.BytesSpan();
            var value = reader.BytesSpan();
            _db.Put(key, value);
        }

        Offset = meta.Index;
        OffsetTerm = meta.Term;
        _db.Put(OffsetKey, new BinWriter(24).VarU(meta.Index).VarU(meta.Term).ToArray());
        _lastIndex = meta.Index;
        SetApplied(meta.Index);
        SetAppliedConf(meta.Conf);
        _db.Put(SnapshotKey, Codec.EncodeSnapshotMeta(meta));
        Snapshot = meta;
        Kv.ReloadSessions();

        string own = SnapshotPath(meta.Index);
        if (!string.Equals(Path.GetFullPath(path), Path.GetFullPath(own), StringComparison.OrdinalIgnoreCase))
            File.Copy(path, own, overwrite: true);
    }

    public void Dispose() => _db.Dispose();
}
