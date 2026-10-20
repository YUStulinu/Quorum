using System.Text;
using Quorum.Client;
using Quorum.Protocol;

namespace Quorum.Cli.Commands;

/// <summary><c>quorum kv ...</c>: one-shot operations against a running cluster.</summary>
public static class KvCliCommand
{
    public static string[] DefaultEndpoints(int nodes = 5) =>
        Enumerable.Range(1, nodes).Select(i => $"http://127.0.0.1:{7100 + i}").ToArray();

    public static ReadConsistency ParseConsistency(string? s) => s?.ToLowerInvariant() switch
    {
        null or "linearizable" or "readindex" => ReadConsistency.Linearizable,
        "lease" => ReadConsistency.Lease,
        "stale" => ReadConsistency.Stale,
        _ => throw new CliException($"--read must be linearizable, lease or stale, not '{s}'"),
    };

    public static async Task<int> RunAsync(CliArgs a, CancellationToken ct)
    {
        var endpoints = a.Get("endpoints")?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        ?? DefaultEndpoints();
        var consistency = ParseConsistency(a.Get("read"));
        int limit = a.GetInt("limit", 100);
        a.RejectUnknown();
        var p = a.Positional;
        if (p.Count == 0) throw new CliException("usage: quorum kv put|get|del|cas|scan ...");

        await using var client = new QuorumClient(new QuorumClientOptions
        {
            Endpoints = endpoints,
            OperationTimeout = TimeSpan.FromSeconds(10),
        });

        KvResult r;
        string Arg(int i) => i < p.Count ? p[i] : throw new CliException($"'{p[0]}' needs more arguments");
        try
        {
            switch (p[0])
            {
                case "put":
                    r = await client.PutAsync(Arg(1), Arg(2), ct);
                    Console.WriteLine($"OK (index {r.Index}, node {r.Node}{(r.Found ? ", replaced a value" : "")})");
                    break;
                case "get":
                    r = await client.GetAsync(Arg(1), consistency, ct);
                    Console.WriteLine(r.Found ? r.ValueString : "(not found)");
                    Console.Error.WriteLine($"  read at index {r.Index} from node {r.Node} ({consistency})");
                    break;
                case "del":
                case "delete":
                    r = await client.DeleteAsync(Arg(1), ct);
                    Console.WriteLine(r.Found ? "deleted" : "(not found)");
                    break;
                case "cas":
                    string? expected = Arg(2) == "-" ? null : Arg(2);
                    string? next = Arg(3) == "-" ? null : Arg(3);
                    r = await client.CasAsync(Arg(1), expected, next, ct);
                    Console.WriteLine(r.Status == KvStatusCode.Ok
                        ? "OK"
                        : $"CAS failed: current value is {(r.Found ? $"'{r.ValueString}'" : "(absent)")}");
                    break;
                case "scan":
                    r = await client.ScanAsync(p.Count > 1 ? p[1] : "", limit, consistency, ct);
                    foreach (var kv in r.Pairs) Console.WriteLine($"{Encoding.UTF8.GetString(kv.Key)} = {Encoding.UTF8.GetString(kv.Value)}");
                    Console.Error.WriteLine($"  ({r.Pairs.Count} keys, index {r.Index})");
                    break;
                default:
                    throw new CliException($"unknown kv command '{p[0]}'");
            }
        }
        catch (QuorumTimeoutException ex)
        {
            Console.Error.WriteLine($"timeout: {ex.Message}{(ex.MayHaveSucceeded ? " (the write may have been applied)" : "")}");
            return 1;
        }
        return 0;
    }
}
