using System.Text;
using Quorum.Core;
using Quorum.Storage;

namespace Quorum.Integration.Tests;

public sealed class StorageTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quorum-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void TesseraRoundTripsKeysValuesAndScans()
    {
        Directory.CreateDirectory(_dir);
        using var db = Tessera.Open(Path.Combine(_dir, "t.tdb"), TesseraSync.Off);
        db.Begin();
        for (int i = 0; i < 1000; i++) db.Put(B($"key{i:D4}"), B($"value{i}"));
        db.Put(B("empty"), []);
        db.Commit();
        Assert.Equal("value7", Encoding.UTF8.GetString(db.Get(B("key0007"))!));
        Assert.Empty(db.Get(B("empty"))!);
        Assert.Null(db.Get(B("missing")));
        Assert.True(db.Delete(B("key0001")));
        Assert.False(db.Delete(B("key0001")));
        var range = db.Scan(B("key0010"), B("key0020")).ToList();
        Assert.Equal(10, range.Count);
        Assert.Equal("key0010", Encoding.UTF8.GetString(range[0].Key));
        Assert.Empty(db.CheckIntegrity());
    }

    [Fact]
    public void NodeStoreReplacesConflictingEntriesAndSurvivesReopen()
    {
        using (var store = NodeStore.Open(_dir, TesseraSync.Off))
        {
            store.Bootstrap([new Member(1, "a", false), new Member(2, "b", false), new Member(3, "c", false)]);
            store.Begin();
            store.AppendEntries([.. Enumerable.Range(4, 10).Select(i => new Entry { Index = (ulong)i, Term = 2, Data = B($"e{i}") })]);
            // A new leader overwrites the tail from index 8.
            store.AppendEntries([new Entry { Index = 8, Term = 3, Data = B("new8") }]);
            store.SetHardState(new HardState(3, 2, 7));
            store.Commit();
        }
        using (var store = NodeStore.Open(_dir, TesseraSync.Off))
        {
            var init = store.LoadInitialState();
            Assert.Equal(new HardState(3, 2, 7), init.HardState);
            Assert.Equal(8, init.Entries.Count);
            Assert.Equal(3UL, init.Entries[^1].Term);
            Assert.Equal("new8", Encoding.UTF8.GetString(init.Entries[^1].Data));
            // The bootstrap configuration is replayed by the Raft node.
            var node = new RaftNode(new RaftOptions { Id = 1 }, init);
            Assert.Equal(3, node.Conf.Voters.Count);
        }
    }

    [Fact]
    public void SessionsMakeRetriesExactlyOnce()
    {
        using var store = NodeStore.Open(_dir, TesseraSync.Off);
        store.Begin();
        var kv = store.Kv;
        var first = kv.Apply(10, new KvCommand(KvOp.Cas, 42, 1, B("x"), B("1"), ExpectAbsent: true));
        Assert.Equal(KvStatus.Ok, first.Status);
        // The client timed out and retried the same request; it must not fail as "already exists".
        var retry = kv.Apply(11, new KvCommand(KvOp.Cas, 42, 1, B("x"), B("1"), ExpectAbsent: true));
        Assert.Equal(KvStatus.Ok, retry.Status);
        Assert.Equal(10UL, retry.Index);
        // An older request arriving late has no effect.
        kv.Apply(12, new KvCommand(KvOp.Put, 42, 2, B("x"), B("2")));
        var late = kv.Apply(13, new KvCommand(KvOp.Put, 42, 1, B("x"), B("stale")));
        Assert.Equal(KvStatus.Stale, late.Status);
        Assert.Equal("2", Encoding.UTF8.GetString(kv.Get(B("x"))!));
        var cas = kv.Apply(14, new KvCommand(KvOp.Cas, 7, 1, B("x"), B("3"), Expected: B("1")));
        Assert.Equal(KvStatus.CasFailed, cas.Status);
        Assert.Equal("2", Encoding.UTF8.GetString(cas.Current!));
        store.Commit();
    }

    [Fact]
    public void SnapshotRestoresTheStateOnAnotherNode()
    {
        string a = Path.Combine(_dir, "a"), b = Path.Combine(_dir, "b");
        SnapshotMeta meta;
        string digest;
        string path;
        using (var src = NodeStore.Open(a, TesseraSync.Off))
        {
            src.Begin();
            for (int i = 0; i < 5000; i++) src.Kv.Apply((ulong)i + 1, new KvCommand(KvOp.Put, (ulong)(i % 50) + 1, (ulong)i + 1, B($"k{i}"), B(new string('v', i % 300))));
            src.SetApplied(5000);
            src.SetAppliedConf(ConfState.FromMembers([new Member(1, "a", false), new Member(2, "b", true)]));
            src.Commit();
            meta = src.CreateSnapshot(term: 4);
            digest = src.Kv.Digest();
            path = src.SnapshotPath(meta.Index);
        }
        Assert.Equal(meta, NodeStore.VerifySnapshotFile(path));

        using var dst = NodeStore.Open(b, TesseraSync.Off);
        dst.Begin();
        dst.Kv.Apply(1, new KvCommand(KvOp.Put, 0, 0, B("old"), B("gone")));
        dst.RestoreSnapshot(path, meta);
        dst.Commit();
        Assert.Equal(digest, dst.Kv.Digest());
        Assert.Null(dst.Kv.Get(B("old")));
        Assert.Equal(5000UL, dst.Applied);
        Assert.Equal(50, dst.Kv.SessionCount);
        Assert.Equal(meta.Conf, dst.AppliedConf);
        var init = dst.LoadInitialState();
        Assert.Equal(5000UL, init.LogOffset);
        Assert.Empty(init.Entries);
    }

    [Fact]
    public void CorruptedSnapshotIsRejected()
    {
        string path;
        using (var src = NodeStore.Open(_dir, TesseraSync.Off))
        {
            src.Begin();
            src.Kv.Apply(1, new KvCommand(KvOp.Put, 0, 0, B("k"), B("v")));
            src.SetApplied(1);
            src.Commit();
            path = src.SnapshotPath(src.CreateSnapshot(1).Index);
        }
        var bytes = File.ReadAllBytes(path);
        bytes[bytes.Length / 2] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        Assert.Throws<InvalidDataException>(() => NodeStore.VerifySnapshotFile(path));
    }
}
