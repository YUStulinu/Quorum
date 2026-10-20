using System.Net;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quorum.Core;
using Quorum.Protocol;

namespace Quorum.Node;

/// <summary>Hosts a node: gRPC (peers and clients) on one port, the JSON status/admin API on another.</summary>
public static class NodeServer
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = false };

    public sealed record FaultsBody(ulong[]? Blocked, double DropRate, int DelayMs, int JitterMs);
    public sealed record ClockBody(double Rate);
    public sealed record PauseBody(int Ms);
    public sealed record TransferBody(ulong Target);
    public sealed record AddMemberBody(ulong Id, string Address, bool Learner = true);
    public sealed record RemoveMemberBody(ulong Id);

    public static async Task RunAsync(NodeOptions options, CancellationToken cancellation)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            ApplicationName = "Quorum.Node",
            ContentRootPath = AppContext.BaseDirectory,
        });
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss.fff ";
        });
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
        builder.Logging.AddFilter("Grpc", LogLevel.Warning);

        builder.WebHost.ConfigureKestrel(k =>
        {
            void Listen(int port, HttpProtocols protocols)
            {
                if (IPAddress.TryParse(options.Host, out var ip)) k.Listen(ip, port, o => o.Protocols = protocols);
                else k.ListenAnyIP(port, o => o.Protocols = protocols);
            }
            Listen(options.GrpcPort, HttpProtocols.Http2);
            Listen(options.HttpPort, HttpProtocols.Http1);
            k.Limits.Http2.MaxStreamsPerConnection = 1000;
            k.Limits.MaxRequestBodySize = 64 << 20;
        });

        // Peer streams never end on their own; do not wait long for them when shutting down.
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(2));
        builder.Services.AddGrpc(o =>
        {
            o.MaxReceiveMessageSize = 64 << 20;
            o.MaxSendMessageSize = 64 << 20;
            o.EnableDetailedErrors = true;
        });

        var loggerFactory = LoggerFactory.Create(l => l.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss.fff ";
        }).SetMinimumLevel(LogLevel.Information));
        var log = loggerFactory.CreateLogger($"node-{options.Id}");
        using var host = new RaftHost(options, log);
        await using var transport = new PeerTransport(host, log);
        builder.Services.AddSingleton(host);
        builder.Services.AddSingleton(transport);

        await using var app = builder.Build();
        app.MapGrpcService<RaftTransportService>();
        app.MapGrpcService<KvService>();
        MapHttpApi(app, host);

        host.Start(transport);
        log.LogInformation("node {Id}: gRPC on {Grpc}, HTTP on {Http}, data in {Dir}", options.Id, options.GrpcAddress,
            options.HttpAddress, Path.GetFullPath(options.DataDir));
        await app.StartAsync(CancellationToken.None);
        try
        {
            await Task.Delay(Timeout.Infinite, cancellation);
        }
        catch (OperationCanceledException)
        {
        }
        log.LogInformation("node {Id}: shutting down", options.Id);
        await transport.DisposeAsync();
        using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await app.StopAsync(stopTimeout.Token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static void MapHttpApi(WebApplication app, RaftHost host)
    {
        app.MapGet("/", () => Results.Text($"Quorum node {host.Id}. Try /status, /events, /digest, /kv/{{key}}.\n"));

        app.MapGet("/status", async () => Results.Json(await host.StatusAsync(), Json));

        app.MapGet("/events", (long? after) => Results.Json(host.Events.After(after ?? 0), Json));

        app.MapGet("/digest", async () => Results.Json(await host.DigestAsync(), Json));

        app.MapPost("/admin/faults", (FaultsBody body) =>
        {
            host.Faults.Settings = new FaultSettings
            {
                Blocked = body.Blocked ?? [],
                DropRate = body.DropRate,
                DelayMs = body.DelayMs,
                JitterMs = body.JitterMs,
            };
            host.Events.Add($"faults: blocked [{string.Join(",", body.Blocked ?? [])}] drop {body.DropRate:P0} delay {body.DelayMs}±{body.JitterMs} ms");
            return Results.Json(host.Faults.Settings, Json);
        });

        app.MapPost("/admin/heal", () =>
        {
            host.Faults.Settings = FaultSettings.None;
            host.Clock.Rate = 1.0;
            host.Events.Add("faults cleared, clock rate 1.0");
            return Results.Ok();
        });

        app.MapPost("/admin/clock", (ClockBody body) =>
        {
            host.Clock.Rate = body.Rate;
            host.Events.Add($"clock rate set to {body.Rate}");
            return Results.Ok(new { rate = body.Rate });
        });

        app.MapPost("/admin/pause", (PauseBody body) =>
        {
            host.Pause(Math.Clamp(body.Ms, 1, 60_000));
            return Results.Ok();
        });

        app.MapPost("/admin/transfer", (TransferBody body) =>
        {
            host.TransferLeadership(body.Target);
            return Results.Ok();
        });

        app.MapPost("/admin/members/add", async (AddMemberBody body) =>
        {
            var error = await host.ChangeMembershipAsync(new ConfChange(
                body.Learner ? ConfChangeType.AddLearner : ConfChangeType.AddVoter, body.Id, body.Address));
            return error is null ? Results.Ok() : Results.BadRequest(new { error });
        });

        app.MapPost("/admin/members/remove", async (RemoveMemberBody body) =>
        {
            var error = await host.ChangeMembershipAsync(new ConfChange(ConfChangeType.Remove, body.Id, ""));
            return error is null ? Results.Ok() : Results.BadRequest(new { error });
        });

        // A convenience HTTP front end for the key-value API (curl-friendly). No client session: at-least-once.
        app.MapGet("/kv/{key}", async (string key, string? consistency) =>
        {
            var r = await host.ExecuteAsync(new KvRequest
            {
                Op = KvOperation.Get,
                Key = ByteString.CopyFromUtf8(key),
                Consistency = ParseConsistency(consistency),
            });
            return KvHttpResult(r);
        });

        app.MapPut("/kv/{key}", async (string key, HttpRequest request) =>
        {
            using var reader = new StreamReader(request.Body, Encoding.UTF8);
            string value = await reader.ReadToEndAsync();
            var r = await host.ExecuteAsync(new KvRequest
            {
                Op = KvOperation.Put,
                Key = ByteString.CopyFromUtf8(key),
                Value = ByteString.CopyFromUtf8(value),
                HasValue = true,
            });
            return KvHttpResult(r);
        });

        app.MapDelete("/kv/{key}", async (string key) =>
        {
            var r = await host.ExecuteAsync(new KvRequest { Op = KvOperation.Delete, Key = ByteString.CopyFromUtf8(key) });
            return KvHttpResult(r);
        });
    }

    private static ReadConsistency ParseConsistency(string? s) => s?.ToLowerInvariant() switch
    {
        "lease" => ReadConsistency.Lease,
        "stale" => ReadConsistency.Stale,
        _ => ReadConsistency.Linearizable,
    };

    private static IResult KvHttpResult(KvResponse r)
    {
        var body = new
        {
            status = r.Status.ToString(),
            value = r.Found && r.Value.Length > 0 ? r.Value.ToStringUtf8() : (r.Status == KvStatusCode.CasFailed ? r.Value.ToStringUtf8() : null),
            found = r.Found,
            index = r.Index,
            node = r.NodeId,
            leader = r.LeaderId,
            leaderAddress = r.LeaderAddress,
            error = string.IsNullOrEmpty(r.Error) ? null : r.Error,
        };
        int code = r.Status switch
        {
            KvStatusCode.Ok => 200,
            KvStatusCode.NotFound => 404,
            KvStatusCode.CasFailed => 409,
            KvStatusCode.NotLeader => 421,
            KvStatusCode.Timeout => 504,
            _ => 500,
        };
        return Results.Json(body, Json, statusCode: code);
    }
}
