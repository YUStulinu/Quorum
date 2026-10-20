using Quorum.Core;

namespace Quorum.Storage;

public enum KvOp : byte
{
    Put = 1,
    Delete = 2,
    /// <summary>Compare-and-set: writes <see cref="KvCommand.Value"/> (or deletes, if null) only if the current
    /// value equals <see cref="KvCommand.Expected"/> (or the key is absent, if <see cref="KvCommand.ExpectAbsent"/>).</summary>
    Cas = 3,
}

/// <summary>
/// A write, as stored in the Raft log. <see cref="ClientId"/> and <see cref="Seq"/> identify it for
/// exactly-once execution: a client that retries after a timeout sends the same pair, and the state machine
/// answers the retry from its session table instead of applying the write twice (dissertation 6.3).
/// ClientId 0 means "no session" (at-least-once).
/// </summary>
public sealed record KvCommand(
    KvOp Op,
    ulong ClientId,
    ulong Seq,
    byte[] Key,
    byte[]? Value = null,
    byte[]? Expected = null,
    bool ExpectAbsent = false)
{
    public byte[] Encode()
    {
        var w = new BinWriter(Key.Length + (Value?.Length ?? 0) + (Expected?.Length ?? 0) + 32);
        w.U8(1).U8((byte)Op).VarU(ClientId).VarU(Seq).Bytes(Key);
        w.Bool(Value is not null);
        if (Value is not null) w.Bytes(Value);
        w.Bool(Expected is not null);
        if (Expected is not null) w.Bytes(Expected);
        w.Bool(ExpectAbsent);
        return w.ToArray();
    }

    public static KvCommand Decode(ReadOnlyMemory<byte> data)
    {
        var r = new BinReader(data);
        byte version = r.U8();
        if (version != 1) throw new FormatException($"unknown command version {version}");
        var op = (KvOp)r.U8();
        ulong client = r.VarU();
        ulong seq = r.VarU();
        var key = r.Bytes();
        byte[]? value = r.Bool() ? r.Bytes() : null;
        byte[]? expected = r.Bool() ? r.Bytes() : null;
        bool absent = r.Bool();
        return new KvCommand(op, client, seq, key, value, expected, absent);
    }
}

public enum KvStatus : byte
{
    Ok = 0,
    /// <summary>A compare-and-set whose condition did not hold. <see cref="KvResult.Current"/> has the value found.</summary>
    CasFailed = 1,
    /// <summary>A request older than the client's latest one: it was superseded and had no effect.</summary>
    Stale = 2,
}

/// <summary>The outcome of applying a <see cref="KvCommand"/>.</summary>
public sealed record KvResult(KvStatus Status, bool Existed, byte[]? Current, ulong Index)
{
    public byte[] Encode()
    {
        var w = new BinWriter(32 + (Current?.Length ?? 0));
        w.U8((byte)Status).Bool(Existed).Bool(Current is not null);
        if (Current is not null) w.Bytes(Current);
        w.VarU(Index);
        return w.ToArray();
    }

    public static KvResult Decode(ReadOnlyMemory<byte> data)
    {
        var r = new BinReader(data);
        var status = (KvStatus)r.U8();
        bool existed = r.Bool();
        byte[]? current = r.Bool() ? r.Bytes() : null;
        return new KvResult(status, existed, current, r.VarU());
    }
}
