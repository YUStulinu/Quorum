using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Quorum.Core;

/// <summary>A small append-only binary writer with LEB128 varints.</summary>
public sealed class BinWriter(int capacity = 256)
{
    private byte[] _buf = new byte[Math.Max(16, capacity)];
    private int _len;

    public int Length => _len;

    private void Ensure(int extra)
    {
        if (_len + extra <= _buf.Length) return;
        int size = Math.Max(_buf.Length * 2, _len + extra);
        Array.Resize(ref _buf, size);
    }

    public BinWriter U8(byte v)
    {
        Ensure(1);
        _buf[_len++] = v;
        return this;
    }

    public BinWriter Bool(bool v) => U8(v ? (byte)1 : (byte)0);

    public BinWriter VarU(ulong v)
    {
        Ensure(10);
        while (v >= 0x80)
        {
            _buf[_len++] = (byte)(v | 0x80);
            v >>= 7;
        }
        _buf[_len++] = (byte)v;
        return this;
    }

    public BinWriter U64(ulong v)
    {
        Ensure(8);
        BinaryPrimitives.WriteUInt64LittleEndian(_buf.AsSpan(_len), v);
        _len += 8;
        return this;
    }

    public BinWriter U32(uint v)
    {
        Ensure(4);
        BinaryPrimitives.WriteUInt32LittleEndian(_buf.AsSpan(_len), v);
        _len += 4;
        return this;
    }

    public BinWriter Bytes(ReadOnlySpan<byte> v)
    {
        VarU((ulong)v.Length);
        Raw(v);
        return this;
    }

    public BinWriter Raw(ReadOnlySpan<byte> v)
    {
        Ensure(v.Length);
        v.CopyTo(_buf.AsSpan(_len));
        _len += v.Length;
        return this;
    }

    public BinWriter Str(string v)
    {
        int n = Encoding.UTF8.GetByteCount(v);
        VarU((ulong)n);
        Ensure(n);
        Encoding.UTF8.GetBytes(v, _buf.AsSpan(_len));
        _len += n;
        return this;
    }

    public ReadOnlySpan<byte> Span => _buf.AsSpan(0, _len);
    public byte[] ToArray() => _buf.AsSpan(0, _len).ToArray();
    public void Clear() => _len = 0;
}

/// <summary>Reads what <see cref="BinWriter"/> wrote. Throws <see cref="FormatException"/> on truncated input.</summary>
public sealed class BinReader(ReadOnlyMemory<byte> data)
{
    private readonly ReadOnlyMemory<byte> _data = data;
    private int _pos;

    public BinReader(byte[] data) : this(new ReadOnlyMemory<byte>(data)) { }

    public bool End => _pos >= _data.Length;
    public int Position => _pos;

    private ReadOnlySpan<byte> Take(int n)
    {
        if (n < 0 || _pos + n > _data.Length) throw new FormatException("unexpected end of data");
        var s = _data.Span.Slice(_pos, n);
        _pos += n;
        return s;
    }

    public byte U8() => Take(1)[0];
    public bool Bool() => U8() != 0;

    public ulong VarU()
    {
        ulong result = 0;
        int shift = 0;
        while (true)
        {
            byte b = U8();
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 63) throw new FormatException("varint too long");
        }
    }

    public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));
    public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));

    public byte[] Bytes()
    {
        ulong n = VarU();
        if (n > int.MaxValue) throw new FormatException("length too large");
        return Take((int)n).ToArray();
    }

    public ReadOnlySpan<byte> BytesSpan()
    {
        ulong n = VarU();
        if (n > int.MaxValue) throw new FormatException("length too large");
        return Take((int)n);
    }

    public string Str() => Encoding.UTF8.GetString(BytesSpan());
}

/// <summary>Binary encodings of the Raft types, shared by the transport and the storage.</summary>
public static class Codec
{
    private const byte MessageVersion = 1;

    public static void WriteEntry(BinWriter w, Entry e)
    {
        w.VarU(e.Index).VarU(e.Term).U8((byte)e.Type).Bytes(e.Data);
    }

    public static Entry ReadEntry(BinReader r)
    {
        ulong index = r.VarU();
        ulong term = r.VarU();
        var type = (EntryType)r.U8();
        var data = r.Bytes();
        return new Entry { Index = index, Term = term, Type = type, Data = data };
    }

    public static byte[] EncodeEntry(Entry e)
    {
        var w = new BinWriter(e.Data.Length + 24);
        WriteEntry(w, e);
        return w.ToArray();
    }

    public static Entry DecodeEntry(ReadOnlyMemory<byte> data) => ReadEntry(new BinReader(data));

    public static void WriteConf(BinWriter w, ConfState c)
    {
        w.VarU((ulong)c.Members.Count);
        foreach (var m in c.Members.Values) w.VarU(m.Id).Str(m.Address).Bool(m.IsLearner);
    }

    public static ConfState ReadConf(BinReader r)
    {
        int n = (int)r.VarU();
        var members = new List<Member>(n);
        for (int i = 0; i < n; i++) members.Add(new Member(r.VarU(), r.Str(), r.Bool()));
        return ConfState.FromMembers(members);
    }

    public static void WriteSnapshotMeta(BinWriter w, SnapshotMeta s)
    {
        w.VarU(s.Index).VarU(s.Term);
        WriteConf(w, s.Conf);
    }

    public static SnapshotMeta ReadSnapshotMeta(BinReader r) => new(r.VarU(), r.VarU(), ReadConf(r));

    public static byte[] EncodeSnapshotMeta(SnapshotMeta s)
    {
        var w = new BinWriter();
        WriteSnapshotMeta(w, s);
        return w.ToArray();
    }

    public static SnapshotMeta DecodeSnapshotMeta(ReadOnlyMemory<byte> data) => ReadSnapshotMeta(new BinReader(data));

    public static byte[] EncodeConfChange(ConfChange cc) =>
        new BinWriter(32).U8((byte)cc.Type).VarU(cc.NodeId).Str(cc.Address).ToArray();

    public static ConfChange DecodeConfChange(ReadOnlyMemory<byte> data)
    {
        var r = new BinReader(data);
        return new ConfChange((ConfChangeType)r.U8(), r.VarU(), r.Str());
    }

    public static byte[] EncodeHardState(HardState h) =>
        new BinWriter(32).VarU(h.Term).VarU(h.Vote).VarU(h.Commit).ToArray();

    public static HardState DecodeHardState(ReadOnlyMemory<byte> data)
    {
        var r = new BinReader(data);
        return new HardState(r.VarU(), r.VarU(), r.VarU());
    }

    public static void WriteMessage(BinWriter w, Message m)
    {
        w.U8(MessageVersion).U8((byte)m.Type).VarU(m.From).VarU(m.To).VarU(m.Term)
            .VarU(m.LogTerm).VarU(m.Index).VarU(m.Commit).Bool(m.Reject).VarU(m.RejectHint)
            .VarU(m.Context).Bool(m.Force);
        w.VarU((ulong)m.Entries.Length);
        foreach (var e in m.Entries) WriteEntry(w, e);
        w.Bool(m.Snapshot is not null);
        if (m.Snapshot is not null) WriteSnapshotMeta(w, m.Snapshot);
    }

    public static byte[] EncodeMessage(Message m)
    {
        int size = 64;
        foreach (var e in m.Entries) size += e.Size;
        var w = new BinWriter(size);
        WriteMessage(w, m);
        return w.ToArray();
    }

    public static Message DecodeMessage(ReadOnlyMemory<byte> data)
    {
        var r = new BinReader(data);
        byte version = r.U8();
        if (version != MessageVersion) throw new FormatException($"unsupported message version {version}");
        var m = new Message
        {
            Type = (MessageType)r.U8(),
            From = r.VarU(),
            To = r.VarU(),
            Term = r.VarU(),
            LogTerm = r.VarU(),
            Index = r.VarU(),
            Commit = r.VarU(),
            Reject = r.Bool(),
            RejectHint = r.VarU(),
            Context = r.VarU(),
            Force = r.Bool(),
        };
        int n = (int)r.VarU();
        if (n > 0)
        {
            var entries = new Entry[n];
            for (int i = 0; i < n; i++) entries[i] = ReadEntry(r);
            m.Entries = entries;
        }
        if (r.Bool()) m.Snapshot = ReadSnapshotMeta(r);
        return m;
    }
}
