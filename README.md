# Quorum

**A replicated key-value store built on Raft, written from scratch in C#, together with a Jepsen-style chaos
tester that proves (or disproves) that it is linearizable.**

Three to seven node processes keep one copy each of a key-value map and agree on every change through the Raft
consensus algorithm. Kill any minority of them, cut the network in half, freeze the leader, make its clock run
slow: clients keep getting correct answers, and when the faults are healed every replica holds exactly the same
data. The chaos tester checks this the way [Jepsen](https://jepsen.io) does: it records every client operation
with its real-time interval, then searches for a single order of all operations that explains every result.

Each node stores its log and its data in [Tessera](https://github.com/YUStulinu/Tessera), my B+tree storage
engine in C, called from .NET through P/Invoke.

```
quorum chaos --nodes 5 --time 60 --nemesis all

[   4.0s] nemesis: pause node 4 (the leader) for 3200 ms
[  10.1s] nemesis: partition {4} | {3,1,5,2}
[  16.1s] nemesis: kill -9 node(s) 1 (incl. the leader)
[  22.7s] nemesis: membership: removed node 5
[  29.1s] nemesis: clock skew: node 3 runs at 0.1x, the others faster; node 3 isolated
[  35.1s] nemesis: network: 26 % loss, 52±52 ms delay on every link
[  47.7s] nemesis: bridge: {2} - 3 - {1,4}
...
  operations       28671 (28665 ok, 0 failed, 6 unknown outcome)
  linearizable     PASS
  convergence      PASS (4 replicas at index 14370 with identical data (8 keys, digest EA13444D5FAC4F93))
  election safety  PASS (at most one leader per term observed)
```

## Contents

- [Running it](#running-it)
- [What it does](#what-it-does)
- [How it is built](#how-it-is-built)
- [How correctness is tested](#how-correctness-is-tested)
- [A real bug the chaos tester finds: lease reads under clock drift](#a-real-bug-the-chaos-tester-finds-lease-reads-under-clock-drift)
- [Performance](#performance)
- [Project layout](#project-layout)
- [Design decisions and limits](#design-decisions-and-limits)

## Running it

Requirements: the [.NET 9 SDK](https://dotnet.microsoft.com/download) and, to build Tessera, a C compiler:
Visual Studio 2022+ or the Build Tools with *Desktop development with C++* on Windows, `cc` (GCC/Clang)
on Linux and macOS. The first build compiles Tessera automatically (`native/build.ps1` or `native/build.sh`).

### In VS Code

Open the folder, accept the recommended extensions (C# Dev Kit), then pick a configuration in
*Run and Debug* (Ctrl+Shift+D):

| Configuration | What happens |
|---|---|
| **Cluster + dashboard (3 nodes)** | Starts three node processes and opens the live dashboard at http://127.0.0.1:7000 |
| **Cluster + dashboard (5 nodes)** | The same with five nodes |
| **Chaos test: all faults, 60 s** | The full Jepsen-style test; expected result: PASS |
| **Chaos test: lease reads + clock skew** | Shows a real consistency violation; expected result: FAIL (see below) |
| **Benchmark (3 nodes, release)** | Throughput and latency of writes and of each read mode |
| **Single node** | One node under the debugger, for stepping through the Raft loop |

Tests appear in the Testing panel (152 tests), or run *Terminal → Run Task → test: all*.

### From a terminal

```bash
dotnet build
dotnet test

# a 3-node cluster with the dashboard (Ctrl+C stops it)
dotnet run --project src/Quorum.Cli -- cluster --nodes 3 --clean

# in another terminal
dotnet run --project src/Quorum.Cli -- kv put greeting hello
dotnet run --project src/Quorum.Cli -- kv get greeting
dotnet run --project src/Quorum.Cli -- kv cas greeting hello salut
dotnet run --project src/Quorum.Cli -- kv scan

# the chaos test and the benchmark start their own clusters
dotnet run --project src/Quorum.Cli -- chaos --nodes 5 --time 60
dotnet run --project src/Quorum.Cli -c Release -- bench --clients 256
```

Each node also answers plain HTTP: `curl http://127.0.0.1:7201/status`, `curl -X PUT -d hi http://127.0.0.1:7201/kv/x`.

## What it does

### The dashboard

`quorum cluster` starts the nodes as separate OS processes and serves a dashboard that refreshes several times a
second. It draws the cluster as a ring: the leader pulses with its heartbeats, Raft messages travel along the
links as dots, and a partitioned link turns into a broken red line. Next to it, each node's log shows how far it has
replicated, committed, applied and compacted.

Every fault the chaos tester knows is a button: crash a node (`kill -9`), cut it off, freeze it for three
seconds, slow its clock to 0.1×, move the leadership to it, remove it, add a new node, split the network or make
it lossy. A background load with a chosen read mode shows the effect of each fault on throughput and latency, and
the event log tells the story (`node 3: started an election for term 7`, `node 3: became leader at term 7`, ...).

### The API

| Operation | Meaning |
|---|---|
| `put key value` | Write. Executed exactly once even if the client retries (see sessions below). |
| `delete key` | Delete. |
| `cas key expected new` | Compare-and-set. `expected` may be "absent", `new` may be "delete". |
| `get key` | Read, in one of three modes (below). |
| `scan prefix` | Keys with a prefix, in order. |

Reads come in three consistency levels, chosen per request:

| Mode | How | Guarantee |
|---|---|---|
| **ReadIndex** (default) | The leader notes its commit index, confirms with one heartbeat round that it is still the leader, waits until it has applied that index, then reads locally. Concurrent reads share one round. | Linearizable, always |
| **Lease** | The leader reads locally while a quorum acknowledged a heartbeat less than an election timeout ago. No round trip. | Linearizable only if clocks run at the same rate |
| **Stale** | Any node reads its local copy. | May return old data |

## How it is built

```
            clients (gRPC: Execute)                  dashboard / admin (HTTP JSON)
                     |                                          |
  +------------------v------------------------------------------v-------------+
  |  Quorum.Node        RaftHost: one loop thread owns everything below       |
  |                                                                           |
  |   inbox --> RaftNode.Step / Tick / Propose / ReadIndex                    |
  |                 |                                                         |
  |               Ready --> 1 install snapshot   2 append entries + hard state |
  |                         3 apply committed entries   (one Tessera txn,     |
  |                                                      one fsync)           |
  |                         4 send messages  5 answer clients  6 serve reads  |
  |                                                                           |
  |   PeerTransport: per-peer queue -> batched gRPC stream; snapshot streams  |
  |   FaultInjector (partitions, loss, delay)   SkewClock (clock drift)       |
  +---------------------------------------------------------------------------+
  |  Quorum.Storage     NodeStore: log (l...), hard state (m/...), data (k...),|
  |                     sessions (s...), snapshot files                       |
  +---------------------------------------------------------------------------+
  |  Tessera (C11)      B+tree + write-ahead log, crash-safe transactions     |
  +---------------------------------------------------------------------------+
```

### The Raft core: a deterministic state machine

[`RaftNode`](src/Quorum.Core/RaftNode.cs) implements Raft as a pure state machine, in the style of etcd's raft
library: no threads, no I/O, no clock. The host feeds it ticks, messages and requests, and collects a
[`Ready`](src/Quorum.Core/Ready.cs) listing what to persist, apply and send. The same inputs always produce the
same outputs, which is what makes the simulation tests below possible.

It implements:

- **Leader election** with randomized timeouts, plus **pre-vote** (a node returning from a partition does not
  bump the term and depose a healthy leader) and **check-quorum** (a leader that loses its majority steps down).
- **Log replication** with pipelining (a window of in-flight appends per follower), probing, flow control by bytes,
  and **fast backtracking**: a rejecting follower reports the conflicting term so the leader skips it in one step.
- **The commitment rule** of section 5.4.2 of the paper: entries of earlier terms are committed only together with an
  entry of the current term (the Figure 8 scenario has its own test).
- **Snapshots and log compaction**, with a tail of recent entries kept so that slightly lagging followers do not
  need a snapshot; snapshots are streamed in chunks and checksummed.
- **Membership changes** one server at a time (dissertation 4.1), with **learners** that catch up before they vote;
  configurations take effect when appended and are rolled back if the entry is truncated; a removed leader steps
  down once the change commits.
- **Leadership transfer** (TimeoutNow).
- **Linearizable reads** by ReadIndex and by leader lease, including the follower side of the lease (a node that
  heard from a leader recently refuses to vote for another one, and so does a node that has just restarted).

### The node

[`RaftHost`](src/Quorum.Node/RaftHost.cs) runs one node. A single loop thread owns the Raft core, the storage and
the state machine, so none of them needs a lock. Requests that arrive while the disk is busy accumulate in the inbox
and go into the next batch: under load, one fsync covers dozens of writes. Two optimizations from the dissertation
(10.2.1) shorten the critical path: the leader sends its appends while it writes them to its own disk, and it
answers clients as soon as an entry it already had on disk is committed.

The log, the hard state and the key-value data live in one Tessera database, so persisting a Ready and applying the
committed entries is one transaction: after a crash, the applied index can never disagree with the data.

**Exactly-once writes.** Every write carries a client id and a sequence number. The state machine remembers the
last answer per client ([`KvStateMachine`](src/Quorum.Storage/KvStateMachine.cs)), so when a client retries after a
timeout (because it cannot know whether the first attempt was applied) it gets the original answer instead of
applying the write twice. Without this, a retried compare-and-set could fail against its own earlier success, and
the chaos test would rightly flag it.

### The transport

Messages are encoded with a compact binary codec ([`Codec`](src/Quorum.Core/Codec.cs)) and sent in batches over
one long-lived gRPC stream per peer. Snapshots use a separate stream so they never hold up heartbeats. Raft tolerates
lost, duplicated and reordered messages, so on any error the transport simply reconnects with backoff and tells the
Raft core that the peer is unreachable.

## How correctness is tested

### 1. Deterministic simulation (thousands of failure schedules)

[`SimCluster`](tests/Quorum.Core.Tests/Simulation/SimCluster.cs) runs a whole cluster in one thread on a seeded
random generator. Its network drops, delays and reorders messages and can be partitioned; nodes crash at random and
lose everything they had not persisted; snapshots, compaction, leadership transfers and membership changes happen
along the way. After every step it checks Raft's safety properties:

- **election safety**: at most one leader per term;
- **leader completeness**: a new leader has every entry ever applied anywhere;
- **state machine safety**: no two nodes apply different entries at the same index;
- **read linearizability**: every read returns at least everything committed before it started;
- **liveness**: once the faults stop, the cluster elects a leader and every member converges.

A failing seed replays the exact same run.

### 2. Mutation testing: do the tests catch real bugs?

Tests that pass prove little unless they fail when the code is wrong. I broke the implementation on purpose in five
classic ways and checked that the suite catches each one:

| Bug introduced | Caught by |
|---|---|
| Vote for any candidate, ignoring the election restriction | 109 of 111 tests (election safety, leader completeness) |
| Commit entries of old terms by counting replicas (Figure 8) | the Figure 8 scenario, reproduced message by message |
| Keep a truncated configuration entry | the truncated-configuration scenario |
| Serve reads before the new leader's no-op commits | 28 tests |
| Send a heartbeat commit index beyond the follower's log | 51 tests |
| Followers ignore the leader lease | the lease scenario |

The first version of the suite missed three of these; the directed scenarios in
[`PaperScenarioTests`](tests/Quorum.Core.Tests/PaperScenarioTests.cs) and
[`RaftScenarioTests`](tests/Quorum.Core.Tests/RaftScenarioTests.cs) were added until it caught all of them.

### 3. Integration tests

[`ClusterTests`](tests/Quorum.Integration.Tests/ClusterTests.cs) run real nodes (gRPC, Tessera on disk) inside the
test process: failover after the leader stops, a new node that joins through a snapshot and ends with an identical
digest, a crashed node that recovers its state from disk.

### 4. The chaos tester (Jepsen-style)

`quorum chaos` starts a fresh cluster of real processes and runs concurrent clients doing reads, writes and
compare-and-sets on a few keys, so that operations collide. Each client is attached to one node, like a client
behind a load balancer, so a node that wrongly believes it is the leader keeps receiving requests. Meanwhile a
nemesis injects one fault every few seconds:

| Fault | What it does |
|---|---|
| `partition` | splits the nodes into a majority and a minority, often with the leader in the minority |
| `bridge` | two halves that cannot talk, joined by one node that talks to both (a non-transitive network) |
| `kill` | `kill -9` on a minority of processes, often the leader; they restart later from disk |
| `pause` | freezes the leader for most of the interval (like a long garbage collection pause) |
| `clock` | the leader's clock runs at 0.1×, the others faster, and the leader is cut off |
| `member` | removes a node, or adds a new one as a learner and promotes it |
| `network` | 10–30% message loss and random delay on every link |

Then it heals everything and checks three things:

1. **Linearizability.** [`Checker`](src/Quorum.Linearizability/Checker.cs) implements the Wing–Gong algorithm with
   Lowe's memoization, the approach of Knossos and Porcupine: a depth-first search for a total order consistent with
   real time in which every result is what a single register would return. Keys are checked independently and in
   parallel. A write whose outcome is unknown (a timeout) may take effect at any point after its call, or never.
2. **Convergence.** Every member reaches the same applied index with an identical hash of its data.
3. **Election safety**, as observed by sampling every node four times a second.

It writes `history.jsonl` (re-checkable with `quorum check`), `summary.json` and a self-contained HTML report with the
throughput, the faults and each node's role over time. When a key is not linearizable, the report draws its
operations per client, marks the ones that could not be placed, and explains the violation in plain words.

## A real bug the chaos tester finds: lease reads under clock drift

Lease reads are faster than ReadIndex because they skip a round trip, and the chaos tester shows why that speed is
not free:

```bash
quorum chaos --nodes 5 --time 40 --interval 5 --read lease --nemesis clock --seed 7
```

```
    k1       2775 ops  NOT LINEARIZABLE
    k2       2848 ops  NOT LINEARIZABLE
  linearizable     FAIL - a client saw a result no sequential execution could produce
```

What happens: the leader's clock runs ten times slower than real time, so in its own time its lease still has
seconds left when, in real time, it has long expired. The other nodes, whose clocks run normally, wait out their
election timeout, elect a new leader and accept writes. A client still talking to the old leader then gets a lease
read of a value that had already been overwritten. The report explains it like this:

> Read #1755 (client 3, served by node 4) started at 9.794s and returned '7-149'. That value was written by #1688,
> but #1752 (client 8) wrote '8-253' after it and finished at 9.791s, before the read began. A linearizable read
> cannot return the older value: node 4 answered from a stale state.

The same run with `--read linearizable` passes, because ReadIndex never trusts the clock: the isolated leader cannot
get a heartbeat round acknowledged, so its reads time out instead of returning stale data. Both reports are in
[`docs/examples`](docs/examples). Leases are only as safe as the bound on clock drift, which is why etcd and
CockroachDB treat that bound as part of their configuration.

## Performance

`quorum bench` on a laptop (Intel Core i7-10870H, NVMe SSD, Windows 11), three nodes on the same machine, every commit
fsynced on a majority, 256 clients, 100-byte values:

| Workload | ops/s | p50 | p99 |
|---|---:|---:|---:|
| writes | 12,000 | 16 ms | 58 ms |
| reads, ReadIndex | 49,000 | 3.1 ms | 17 ms |
| reads, lease | 56,000 | 2.3 ms | 16 ms |
| reads, local (stale) | 64,000 | 2.1 ms | 13 ms |
| 90% reads / 10% writes | 16,000 | 14 ms | 46 ms |

The leader averaged 4 ms per fsync with 33 writes per batch; its own commit latency (request in, response out)
was 9 ms at the median. All four processes share the laptop's CPU, the benchmark clients included.

Two things are visible in the numbers. A lease read is cheaper than a ReadIndex read because it skips the heartbeat
round, which is exactly the speed the clock-drift anomaly above pays for. And the mixed workload is slower than
either pure one: the loop thread blocks while it fsyncs, so reads that arrive meanwhile wait for the disk too. Moving
the fsync to its own thread (a pipelined storage stage) would remove that wait; it is the next optimization I would
make.

`chaos` and `bench` start their clusters on their own ports (7300+ and 7500+), so they can run while the dashboard
cluster (7100+) is open.

## Project layout

```
src/
  Quorum.Core/            Raft as a deterministic state machine; binary codec
  Quorum.Storage/         Tessera P/Invoke binding, node storage, KV state machine, snapshots
  Quorum.Protocol/        gRPC definitions (peer transport and client API)
  Quorum.Node/            the node: loop thread, transport, fault injection, HTTP admin API
  Quorum.Client/          client library: leader discovery, retries, exactly-once sessions
  Quorum.Linearizability/ the linearizability checker
  Quorum.Cli/             the `quorum` program: cluster + dashboard, chaos, bench, kv, check, node
tests/
  Quorum.Core.Tests/            simulation, paper scenarios, directed scenarios
  Quorum.Linearizability.Tests/ checker tests, including large generated histories
  Quorum.Integration.Tests/     storage and real in-process clusters
native/
  tessera/                Tessera sources (C11), vendored from github.com/YUStulinu/Tessera
  build.ps1, build.sh     build it as a shared library
docs/examples/            chaos reports: lease reads failing, ReadIndex passing
```

## Design decisions and limits

- **Why C#.** The Raft core has no dependencies, and .NET gives gRPC, Kestrel and a good debugger on Windows. The
  storage engine stays in C, where it was written, and the boundary is a small P/Invoke layer.
- **One loop thread per node.** Simpler and easier to reason about than fine-grained locking; the cost is that a
  large snapshot pauses the node while it is written (tens of milliseconds for the sizes tested here).
- **Single-server membership changes** rather than joint consensus: simpler and enough when changes go one at a time,
  which the leader enforces.
- **Snapshots are full copies** of the state machine. Fine up to a few hundred thousand keys; beyond that an
  incremental or copy-on-write snapshot would be needed.
- **Followers redirect** clients to the leader instead of proxying their requests.
- **Local only.** The cluster tooling runs every node on one machine; the nodes themselves take any host and port.
- **Not a production database.** No authentication, no TLS, no resource limits. It is a study of how consensus fails
  and how to show that it does not.

## License

MIT. Tessera (in `native/tessera`) is also MIT.
