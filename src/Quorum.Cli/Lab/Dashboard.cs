using System.Diagnostics;
using System.Net;
using System.Reflection;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Quorum.Client;
using Quorum.Protocol;

namespace Quorum.Cli.Lab;

/// <summary>
/// The live dashboard served by <c>quorum cluster</c>: it polls every node a few times a second, merges their
/// event logs, runs an optional background workload, and exposes buttons for every fault the chaos tester knows.
/// </summary>
public sealed class Dashboard(LocalCluster cluster, int port)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public sealed record ClusterEvent(DateTimeOffset Time, ulong Node, string Text);

    private readonly Dictionary<ulong, JsonElement> _status = [];
    private readonly Dictionary<ulong, long> _eventSeq = [];
    private readonly Dictionary<ulong, DateTime> _startSeen = [];
    private readonly List<ClusterEvent> _events = [];
    private readonly object _lock = new();
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private Workload? _workload;
    private CancellationTokenSource? _workloadCts;
    private QuorumClient? _console;

    public string Url => $"http://127.0.0.1:{port}";

    public void Note(string text)
    {
        lock (_lock)
        {
            _events.Add(new ClusterEvent(DateTimeOffset.UtcNow, 0, text));
            if (_events.Count > 400) _events.RemoveRange(0, _events.Count - 400);
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { ApplicationName = "Quorum.Dashboard" });
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(k => k.Listen(IPAddress.Loopback, port));
        await using var app = builder.Build();
        Map(app);
        await app.StartAsync(ct);
        var poll = PollAsync(ct);
        try
        {
            await Task.Delay(Timeout.Infinite, ct);
        }
        catch (OperationCanceledException)
        {
        }
        StopWorkload();
        await app.StopAsync(CancellationToken.None);
        try { await poll; } catch (OperationCanceledException) { }
        if (_console is not null) await _console.DisposeAsync();
    }

    // ------------------------------------------------------------------
    // Polling
    // ------------------------------------------------------------------

    private async Task PollAsync(CancellationToken ct)
    {
        int round = 0;
        while (!ct.IsCancellationRequested)
        {
            var nodes = cluster.Nodes;
            var statuses = await Task.WhenAll(nodes.Select(async n => (n.Id, Status: await cluster.StatusAsync(n.Id, ct))));
            lock (_lock)
            {
                foreach (var (id, s) in statuses)
                {
                    if (s is { } st) _status[id] = st;
                    else _status.Remove(id);
                }
            }
            if (round++ % 3 == 0)
            {
                foreach (var n in nodes.Where(n => n.Running))
                {
                    long after;
                    lock (_lock)
                    {
                        // A restarted process numbers its events from 1 again.
                        if (n.StartedAt is { } started && _startSeen.GetValueOrDefault(n.Id) != started)
                        {
                            _startSeen[n.Id] = started;
                            _eventSeq[n.Id] = 0;
                        }
                        after = _eventSeq.GetValueOrDefault(n.Id);
                    }
                    var evs = await cluster.GetJsonAsync(n.Id, $"/events?after={after}", ct);
                    if (evs is not { ValueKind: JsonValueKind.Array } arr) continue;
                    lock (_lock)
                    {
                        foreach (var e in arr.EnumerateArray())
                        {
                            long seq = e.GetProperty("seq").GetInt64();
                            if (seq <= after) continue;
                            _events.Add(new ClusterEvent(e.GetProperty("time").GetDateTimeOffset(), n.Id, e.GetProperty("text").GetString() ?? ""));
                            _eventSeq[n.Id] = Math.Max(_eventSeq.GetValueOrDefault(n.Id), seq);
                        }
                        _events.Sort((x, y) => x.Time.CompareTo(y.Time));
                        if (_events.Count > 400) _events.RemoveRange(0, _events.Count - 400);
                    }
                }
            }
            await Task.Delay(250, ct);
        }
    }

    private object Snapshot()
    {
        lock (_lock)
        {
            return new
            {
                time = _clock.Elapsed.TotalSeconds,
                nodes = cluster.Nodes.Select(n => new
                {
                    id = n.Id,
                    running = n.Running,
                    removed = n.Removed,
                    restarts = n.Restarts,
                    grpc = n.GrpcAddress,
                    http = n.HttpAddress,
                    status = _status.TryGetValue(n.Id, out var s) && n.Running ? (JsonElement?)s : null,
                }),
                events = _events.TakeLast(150).Select(e => new { time = e.Time, node = e.Node, text = e.Text }),
                workload = _workload is null ? null : new
                {
                    running = _workload.Running,
                    options = new
                    {
                        clients = _workload.Options.Clients,
                        rate = _workload.Options.Rate,
                        readRatio = _workload.Options.ReadRatio,
                        consistency = _workload.Options.Consistency.ToString(),
                    },
                    ok = _workload.TotalOk,
                    failed = _workload.TotalFailed,
                    unknown = _workload.TotalUnknown,
                    seconds = _workload.RecentSeconds.TakeLast(90).Select(x => new object[] { x.Second, x.Ok, x.Failed, x.Unknown, x.P50Ms, x.P99Ms }),
                },
            };
        }
    }

    // ------------------------------------------------------------------
    // Workload
    // ------------------------------------------------------------------

    public sealed record WorkloadBody(int Clients = 8, int Rate = 300, double ReadRatio = 0.5, string Consistency = "linearizable", int Keys = 16);

    private void StartWorkload(WorkloadBody b)
    {
        StopWorkload();
        _workloadCts = new CancellationTokenSource();
        _workload = new Workload(() => cluster.Endpoints, new WorkloadOptions
        {
            Clients = Math.Clamp(b.Clients, 1, 128),
            Rate = Math.Max(0, b.Rate),
            ReadRatio = Math.Clamp(b.ReadRatio, 0, 1),
            CasRatio = Math.Min(0.2, 1 - Math.Clamp(b.ReadRatio, 0, 1)),
            Consistency = b.Consistency.ToLowerInvariant() switch
            {
                "lease" => ReadConsistency.Lease,
                "stale" => ReadConsistency.Stale,
                _ => ReadConsistency.Linearizable,
            },
            Keys = Math.Clamp(b.Keys, 1, 100_000),
            KeyPrefix = "load:",
            PinClients = true,
        });
        _ = _workload.RunAsync(_workloadCts.Token);
        Note($"workload started: {b.Clients} clients, {(b.Rate == 0 ? "max" : b.Rate + " ops/s")}, {b.ReadRatio:P0} reads ({b.Consistency})");
    }

    private void StopWorkload()
    {
        if (_workloadCts is null) return;
        _workloadCts.Cancel();
        _workloadCts = null;
        Note("workload stopped");
    }

    // ------------------------------------------------------------------
    // HTTP API
    // ------------------------------------------------------------------

    public sealed record PartitionBody(ulong[][] Groups);
    public sealed record NetworkBody(double Drop, int Delay);
    public sealed record KvBody(string Op, string Key, string? Value, string? Consistency);

    private void Map(WebApplication app)
    {
        app.MapGet("/", () => Results.Content(Asset("index.html"), "text/html; charset=utf-8"));

        app.MapGet("/api/state", () => Results.Json(Snapshot(), Json));

        app.MapGet("/api/stream", async (HttpContext ctx) =>
        {
            ctx.Response.Headers.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            var ct = ctx.RequestAborted;
            while (!ct.IsCancellationRequested)
            {
                string json = JsonSerializer.Serialize(Snapshot(), Json);
                await ctx.Response.WriteAsync($"data: {json}\n\n", ct);
                await ctx.Response.Body.FlushAsync(ct);
                await Task.Delay(300, ct);
            }
        });

        app.MapPost("/api/nodes/{id}/kill", (ulong id) =>
        {
            cluster.Kill(id);
            Note($"node {id} killed (kill -9)");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/restart", (ulong id) =>
        {
            var n = cluster[id];
            if (n.Removed) return Results.BadRequest(new { error = "node was removed from the cluster" });
            if (n.Running) cluster.Restart(id);
            else cluster.Start(n);
            Note($"node {id} (re)started");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/pause", async (ulong id, int? ms) =>
        {
            int d = ms ?? 3000;
            await cluster.PauseAsync(id, d);
            Note($"node {id} paused for {d} ms");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/isolate", async (ulong id) =>
        {
            await cluster.PartitionAsync([[id], cluster.Nodes.Where(n => n.Id != id).Select(n => n.Id).ToList()]);
            Note($"node {id} isolated from the others");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/clock", async (ulong id, double rate) =>
        {
            await cluster.SetClockRateAsync(id, rate);
            Note($"node {id} clock rate set to {rate}x");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/lead", async (ulong id) =>
        {
            var leader = await cluster.LeaderAsync();
            if (leader is null) return Results.BadRequest(new { error = "no leader" });
            await cluster.PostAsync(leader.Value, "/admin/transfer", new { target = id });
            Note($"asked leader {leader} to hand leadership to node {id}");
            return Results.Ok();
        });

        app.MapPost("/api/nodes/{id}/remove", async (ulong id) =>
        {
            var (ok, msg) = await cluster.RemoveNodeAsync(id);
            Note(msg);
            return ok ? Results.Ok() : Results.BadRequest(new { error = msg });
        });

        app.MapPost("/api/members/add", async () =>
        {
            var (ok, msg) = await cluster.AddNodeAsync();
            Note(msg);
            return ok ? Results.Ok() : Results.BadRequest(new { error = msg });
        });

        app.MapPost("/api/partition", async (PartitionBody body) =>
        {
            await cluster.PartitionAsync(body.Groups.Select(g => (IReadOnlyCollection<ulong>)g).ToList());
            Note("partition " + string.Join(" | ", body.Groups.Select(g => "{" + string.Join(",", g) + "}")));
            return Results.Ok();
        });

        app.MapPost("/api/partition/random", async () =>
        {
            var ids = cluster.Nodes.Where(n => !n.Removed).Select(n => n.Id).OrderBy(_ => Random.Shared.Next()).ToList();
            int minority = Math.Max(1, (ids.Count - 1) / 2);
            var a = ids.Take(minority).ToList();
            var b = ids.Skip(minority).ToList();
            await cluster.PartitionAsync([a, b]);
            Note($"partition {{{string.Join(",", a)}}} | {{{string.Join(",", b)}}}");
            return Results.Ok();
        });

        app.MapPost("/api/heal", async () =>
        {
            await cluster.HealAsync();
            foreach (var n in cluster.Nodes.Where(n => !n.Removed && !n.Running)) cluster.Start(n);
            Note("healed: network restored, clocks reset, dead nodes restarted");
            return Results.Ok();
        });

        app.MapPost("/api/network", async (NetworkBody body) =>
        {
            foreach (var n in cluster.Nodes.Where(n => n.Running))
                await cluster.SetNetworkAsync(n.Id, Math.Clamp(body.Drop, 0, 1), Math.Max(0, body.Delay), Math.Max(0, body.Delay));
            Note($"network: {body.Drop:P0} loss, {body.Delay} ms delay");
            return Results.Ok();
        });

        app.MapPost("/api/workload/start", (WorkloadBody body) =>
        {
            StartWorkload(body);
            return Results.Ok();
        });

        app.MapPost("/api/workload/stop", () =>
        {
            StopWorkload();
            return Results.Ok();
        });

        app.MapPost("/api/kv", async (KvBody body) =>
        {
            _console ??= new QuorumClient(new QuorumClientOptions { Endpoints = cluster.Endpoints, OperationTimeout = TimeSpan.FromSeconds(5) });
            var sw = Stopwatch.StartNew();
            try
            {
                KvResult r = body.Op switch
                {
                    "put" => await _console.PutAsync(body.Key, body.Value ?? ""),
                    "delete" => await _console.DeleteAsync(body.Key),
                    "scan" => await _console.ScanAsync(body.Key, 50),
                    _ => await _console.GetAsync(body.Key, body.Consistency switch
                    {
                        "lease" => ReadConsistency.Lease,
                        "stale" => ReadConsistency.Stale,
                        _ => ReadConsistency.Linearizable,
                    }),
                };
                return Results.Json(new
                {
                    status = r.Status.ToString(),
                    value = r.ValueString,
                    found = r.Found,
                    index = r.Index,
                    node = r.Node,
                    ms = Math.Round(sw.Elapsed.TotalMilliseconds, 1),
                    pairs = r.Pairs.Select(p => new { key = System.Text.Encoding.UTF8.GetString(p.Key), value = System.Text.Encoding.UTF8.GetString(p.Value) }),
                }, Json);
            }
            catch (QuorumException ex)
            {
                return Results.Json(new { status = "Error", error = ex.Message, ms = Math.Round(sw.Elapsed.TotalMilliseconds, 1) }, Json);
            }
        });
    }

    private static string Asset(string name)
    {
        using var s = Assembly.GetExecutingAssembly().GetManifestResourceStream($"wwwroot/{name}")
                      ?? throw new FileNotFoundException(name);
        using var r = new StreamReader(s);
        return r.ReadToEnd();
    }
}
