using Quorum.Cli;
using Quorum.Cli.Commands;

const string Usage = """
Quorum - a replicated key-value store built on Raft, with a chaos tester.

Usage: quorum <command> [options]

Commands:
  cluster   Start a local cluster of node processes and the live dashboard
              --nodes 3|5           number of nodes (default 3)
              --data <dir>          data directory (default ./data)
              --dashboard <port>    dashboard port (default 7000)
              --clean               wipe the data directory first
              --no-browser          do not open the dashboard in a browser

  chaos     Run a Jepsen-style test: clients + faults, then a linearizability check
              --nodes 5             cluster size (default 5)
              --time 60             seconds of workload (default 60)
              --clients 10          concurrent clients (default 10)
              --keys 8              number of keys (default 8)
              --nemesis <list>      partition,bridge,kill,pause,clock,member,all (default all)
              --interval 5          seconds between faults (default 5)
              --read lease|linearizable|stale   read consistency (default linearizable)
              --rate 0              target ops/s over all clients, 0 = as fast as possible
              --seed <n>            random seed (default: time based)
              --out <dir>           where to write results (default ./chaos-results)

  bench     Measure throughput and latency on a fresh local cluster
              --nodes 3 --clients 64 --time 10 --value-size 100 --read-ratio 0

  kv        Talk to a running cluster
              quorum kv put <key> <value>    quorum kv get <key> [--read lease|stale]
              quorum kv del <key>            quorum kv cas <key> <expected|-> <new|->
              quorum kv scan [prefix] [--limit n]
              --endpoints http://127.0.0.1:7101,...   (default: the ports `quorum cluster` uses)

  check     Check a saved history for linearizability: quorum check <history.jsonl>

  node      Run one node (normally started for you by `cluster` and `chaos`)
              --id 1 --data ./data/node-1 --grpc 7101 --http 7201
              --peers 1=http://127.0.0.1:7101,2=http://127.0.0.1:7102,...
              --join                start empty and wait to be added
              --tick 20 --election 15 --heartbeat 3 --snapshot-every 20000
""";

Console.OutputEncoding = System.Text.Encoding.UTF8;

if (args.Length == 0 || args[0] is "help" or "--help" or "-h")
{
    Console.WriteLine(Usage);
    return 0;
}

var rest = new CliArgs(args.Skip(1));
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    return args[0] switch
    {
        "node" => await NodeCommand.RunAsync(rest, cts.Token),
        "cluster" => await ClusterCommand.RunAsync(rest, cts.Token),
        "chaos" => await ChaosCommand.RunAsync(rest, cts.Token),
        "bench" => await BenchCommand.RunAsync(rest, cts.Token),
        "kv" => await KvCliCommand.RunAsync(rest, cts.Token),
        "check" => CheckCommand.Run(rest),
        _ => throw new CliException($"unknown command '{args[0]}'. Run `quorum help`."),
    };
}
catch (CliException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 2;
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    return 130;
}
