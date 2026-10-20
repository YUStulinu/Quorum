using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Quorum.Storage;

/// <summary>An error reported by the Tessera storage engine.</summary>
public sealed class TesseraException(int code, string operation)
    : Exception($"tessera: {operation} failed: {Tessera.ErrorString(code)} ({code})")
{
    public int Code { get; } = code;
}

public enum TesseraSync
{
    /// <summary>fsync on every commit.</summary>
    Full = 0,
    /// <summary>fsync only at checkpoints.</summary>
    Normal = 1,
    /// <summary>never fsync (tests only).</summary>
    Off = 2,
}

public readonly record struct TesseraStats(
    ulong Keys, ulong TxnId, uint PageSize, uint Pages, uint FreePages, uint TreeHeight, uint WalFrames,
    ulong CacheHits, ulong CacheMisses, ulong Checkpoints, ulong Commits);

/// <summary>
/// A handle to a Tessera database (the C11 B+tree + WAL engine in native/tessera).
/// Like the C API, a handle must be used by one thread at a time.
/// </summary>
public sealed unsafe class Tessera : IDisposable
{
    private const string Lib = "tessera";

    private nint _db;

    static Tessera()
    {
        NativeLibrary.SetDllImportResolver(typeof(Tessera).Assembly, Resolve);
    }

    private static nint Resolve(string name, Assembly assembly, DllImportSearchPath? path)
    {
        if (name != Lib) return 0;
        string file = OperatingSystem.IsWindows() ? "tessera.dll"
            : OperatingSystem.IsMacOS() ? "libtessera.dylib" : "libtessera.so";
        foreach (var dir in new[] { AppContext.BaseDirectory, Path.GetDirectoryName(assembly.Location) ?? "" })
        {
            string candidate = Path.Combine(dir, file);
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var h)) return h;
        }
        // Fall back to the repository's native/bin folder (useful when running from an IDE).
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
        {
            string candidate = Path.Combine(d.FullName, "native", "bin", file);
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var h)) return h;
        }
        return 0;
    }

    private Tessera(nint db) => _db = db;

    public static Tessera Open(string path, TesseraSync sync = TesseraSync.Full, int cachePages = 4096,
        uint checkpointPages = 2000)
    {
        Native.ts_options opt;
        Native.ts_options_default(&opt);
        opt.sync = (int)sync;
        opt.cache_pages = (nuint)cachePages;
        opt.checkpoint_pages = checkpointPages;
        opt.create = 1;
        nint db;
        int rc;
        fixed (byte* p = Utf8(path)) rc = Native.ts_open(p, &opt, &db);
        Check(rc, $"open '{path}'");
        return new Tessera(db);
    }

    public static string ErrorString(int code) =>
        Marshal.PtrToStringUTF8(Native.ts_errstr(code)) ?? $"error {code}";

    private static byte[] Utf8(string s)
    {
        var bytes = new byte[System.Text.Encoding.UTF8.GetByteCount(s) + 1];
        System.Text.Encoding.UTF8.GetBytes(s, bytes);
        return bytes;
    }

    private static void Check(int rc, string op)
    {
        if (rc < 0) throw new TesseraException(rc, op);
    }

    private nint Db
    {
        get
        {
            ObjectDisposedException.ThrowIf(_db == 0, this);
            return _db;
        }
    }

    public void Begin() => Check(Native.ts_begin(Db), "begin");
    public void Commit() => Check(Native.ts_commit(Db), "commit");
    public void Rollback() => Check(Native.ts_rollback(Db), "rollback");
    public bool InTransaction => Native.ts_in_transaction(Db) != 0;

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        fixed (byte* k = key)
        fixed (byte* v = value)
            Check(Native.ts_put(Db, k, (nuint)key.Length, v, (nuint)value.Length), "put");
    }

    /// <summary>Returns the value, or null if the key does not exist.</summary>
    public byte[]? Get(ReadOnlySpan<byte> key)
    {
        void* val;
        nuint len;
        int rc;
        fixed (byte* k = key) rc = Native.ts_get(Db, k, (nuint)key.Length, &val, &len);
        if (rc == 1) return null;
        Check(rc, "get");
        try
        {
            return val == null ? [] : new ReadOnlySpan<byte>(val, checked((int)len)).ToArray();
        }
        finally
        {
            Native.ts_free(val);
        }
    }

    /// <summary>Returns false if the key did not exist.</summary>
    public bool Delete(ReadOnlySpan<byte> key)
    {
        int rc;
        fixed (byte* k = key) rc = Native.ts_delete(Db, k, (nuint)key.Length);
        if (rc == 1) return false;
        Check(rc, "delete");
        return true;
    }

    /// <summary>Iterates over the keys in <c>[lo, hi)</c> in order. Null bounds are open.</summary>
    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[]? lo, byte[]? hi)
    {
        nint it = OpenIterator(lo, hi);
        try
        {
            while (true)
            {
                var kv = Next(it);
                if (kv is null) yield break;
                yield return kv.Value;
            }
        }
        finally
        {
            Native.ts_iter_close(it);
        }
    }

    /// <summary>Iterates over the keys only.</summary>
    public IEnumerable<byte[]> ScanKeys(byte[]? lo, byte[]? hi)
    {
        nint it = OpenIterator(lo, hi);
        try
        {
            while (true)
            {
                var key = NextKey(it);
                if (key is null) yield break;
                yield return key;
            }
        }
        finally
        {
            Native.ts_iter_close(it);
        }
    }

    private nint OpenIterator(byte[]? lo, byte[]? hi)
    {
        nint it;
        int rc;
        fixed (byte* l = lo)
        fixed (byte* h = hi)
        {
            rc = Native.ts_iter_open(Db, lo is null ? null : l, (nuint)(lo?.Length ?? 0),
                hi is null ? null : h, (nuint)(hi?.Length ?? 0), &it);
        }
        Check(rc, "iterator open");
        return it;
    }

    private static byte[]? NextKey(nint it)
    {
        int rc = Native.ts_iter_next(it);
        if (rc == 2) return null;
        Check(rc, "iterate");
        nuint kl;
        byte* k = (byte*)Native.ts_iter_key(it, &kl);
        return new ReadOnlySpan<byte>(k, (int)kl).ToArray();
    }

    private static KeyValuePair<byte[], byte[]>? Next(nint it)
    {
        int rc = Native.ts_iter_next(it);
        if (rc == 2) return null;
        Check(rc, "iterate");
        nuint kl, vl;
        byte* k = (byte*)Native.ts_iter_key(it, &kl);
        var key = new ReadOnlySpan<byte>(k, (int)kl).ToArray();
        int vrc;
        byte* v = (byte*)Native.ts_iter_value(it, &vl, &vrc);
        if (v == null)
        {
            Check(vrc, "iterator value");
            return new KeyValuePair<byte[], byte[]>(key, []);
        }
        return new KeyValuePair<byte[], byte[]>(key, new ReadOnlySpan<byte>(v, (int)vl).ToArray());
    }

    public void Checkpoint() => Check(Native.ts_checkpoint(Db), "checkpoint");

    public TesseraStats Stats()
    {
        Native.ts_stats s;
        Check(Native.ts_get_stats(Db, &s), "stats");
        return new TesseraStats(s.keys, s.txn_id, s.page_size, s.pages, s.free_pages, s.tree_height, s.wal_frames,
            s.cache_hits, s.cache_misses, s.checkpoints, s.commits);
    }

    /// <summary>Runs Tessera's full structural integrity check. Returns the problems found (empty = healthy).</summary>
    public List<string> CheckIntegrity()
    {
        var problems = new List<string>();
        var handle = GCHandle.Alloc(problems);
        try
        {
            ulong count;
            Check(Native.ts_check(Db, &Report, GCHandle.ToIntPtr(handle), &count), "check");
        }
        finally
        {
            handle.Free();
        }
        return problems;
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Report(nint ctx, byte* msg)
    {
        if (GCHandle.FromIntPtr(ctx).Target is List<string> list)
            list.Add(Marshal.PtrToStringUTF8((nint)msg) ?? "");
    }

    public void Dispose()
    {
        if (_db == 0) return;
        int rc = Native.ts_close(_db);
        _db = 0;
        Check(rc, "close");
    }

    private static partial class Native
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct ts_options
        {
            public nuint cache_pages;
            public uint checkpoint_pages;
            public int sync;
            public int create;
            public nint vfs;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct ts_stats
        {
            public ulong keys;
            public ulong txn_id;
            public uint page_size;
            public uint pages;
            public uint free_pages;
            public uint tree_height;
            public uint wal_frames;
            public ulong cache_hits;
            public ulong cache_misses;
            public ulong checkpoints;
            public ulong commits;
        }

        [DllImport(Lib)] public static extern nint ts_errstr(int rc);
        [DllImport(Lib)] public static extern void ts_options_default(ts_options* opt);
        [DllImport(Lib)] public static extern int ts_open(byte* path, ts_options* opt, nint* db);
        [DllImport(Lib)] public static extern int ts_close(nint db);
        [DllImport(Lib)] public static extern int ts_begin(nint db);
        [DllImport(Lib)] public static extern int ts_commit(nint db);
        [DllImport(Lib)] public static extern int ts_rollback(nint db);
        [DllImport(Lib)] public static extern int ts_in_transaction(nint db);
        [DllImport(Lib)] public static extern int ts_put(nint db, byte* key, nuint klen, byte* val, nuint vlen);
        [DllImport(Lib)] public static extern int ts_get(nint db, byte* key, nuint klen, void** val, nuint* vlen);
        [DllImport(Lib)] public static extern int ts_delete(nint db, byte* key, nuint klen);
        [DllImport(Lib)] public static extern void ts_free(void* p);
        [DllImport(Lib)] public static extern int ts_iter_open(nint db, byte* lo, nuint lolen, byte* hi, nuint hilen, nint* it);
        [DllImport(Lib)] public static extern int ts_iter_next(nint it);
        [DllImport(Lib)] public static extern void* ts_iter_key(nint it, nuint* len);
        [DllImport(Lib)] public static extern void* ts_iter_value(nint it, nuint* len, int* rc);
        [DllImport(Lib)] public static extern void ts_iter_close(nint it);
        [DllImport(Lib)] public static extern int ts_checkpoint(nint db);
        [DllImport(Lib)] public static extern int ts_get_stats(nint db, ts_stats* stats);
        [DllImport(Lib)] public static extern int ts_check(nint db, delegate* unmanaged[Cdecl]<nint, byte*, void> report, nint ctx, ulong* problems);
    }
}
