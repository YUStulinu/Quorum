using Quorum.Cli.Lab;

namespace Quorum.Cli.Chaos;

/// <summary>A fault that was injected, for the timeline.</summary>
public sealed record FaultEvent(double StartSec, double EndSec, string Kind, string Description);

/// <summary>
/// Injects faults into a <see cref="LocalCluster"/>, one at a time: each fault lasts one interval, then the
/// cluster is healed for a short while before the next one, so the history contains both broken and
/// recovering periods.
/// </summary>
public sealed class Nemesis(LocalCluster cluster, IReadOnlyList<string> kinds, Random rand, Func<double> now, Action<string> log)
{
    public static readonly string[] AllKinds = ["partition", "bridge", "kill", "pause", "clock", "member", "network"];

    public List<FaultEvent> Events { get; } = [];

    private List<ulong> Members => cluster.Nodes.Where(n => !n.Removed).Select(n => n.Id).ToList();

    public async Task RunAsync(TimeSpan interval, CancellationToken ct)
    {
        // Let the workload warm up on a healthy cluster first.
        await Task.Delay(TimeSpan.FromSeconds(Math.Min(3, interval.TotalSeconds)), ct);
        while (!ct.IsCancellationRequested)
        {
            string kind = kinds[rand.Next(kinds.Count)];
            double start = now();
            string description;
            try
            {
                description = await StartFault(kind, interval, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            log($"[{start,6:0.0}s] nemesis: {description}");
            try
            {
                await Task.Delay(interval, ct);
            }
            catch (OperationCanceledException)
            {
                Events.Add(new FaultEvent(start, now(), kind, description));
                break;
            }
            await HealAsync(CancellationToken.None);
            Events.Add(new FaultEvent(start, now(), kind, description));
            log($"[{now(),6:0.0}s] nemesis: healed");
            try
            {
                await Task.Delay(interval / 2, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task<string> StartFault(string kind, TimeSpan interval, CancellationToken ct)
    {
        var members = Members;
        var leader = await cluster.LeaderAsync(ct);
        switch (kind)
        {
            case "partition":
            {
                var shuffled = members.OrderBy(_ => rand.Next()).ToList();
                // Half the time, make sure the current leader ends up in the minority: the interesting case.
                if (leader is { } l && rand.Next(2) == 0)
                {
                    shuffled.Remove(l);
                    shuffled.Insert(0, l);
                }
                int minority = (members.Count - 1) / 2;
                minority = Math.Max(1, rand.Next(1, minority + 1));
                var a = shuffled.Take(minority).ToList();
                var b = shuffled.Skip(minority).ToList();
                await cluster.PartitionAsync([a, b], ct);
                return $"partition {{{string.Join(",", a)}}} | {{{string.Join(",", b)}}}";
            }
            case "bridge":
            {
                // Non-transitive: two halves that cannot talk, joined by one node that talks to both.
                if (members.Count < 3) return await StartFault("partition", interval, ct);
                var shuffled = members.OrderBy(_ => rand.Next()).ToList();
                ulong bridge = shuffled[0];
                var left = shuffled.Skip(1).Take((shuffled.Count - 1) / 2).ToList();
                var right = shuffled.Skip(1 + left.Count).ToList();
                var blocked = new Dictionary<ulong, ulong[]> { [bridge] = [] };
                foreach (var x in left) blocked[x] = [.. right];
                foreach (var x in right) blocked[x] = [.. left];
                await cluster.BlockAsync(blocked, ct);
                return $"bridge: {{{string.Join(",", left)}}} - {bridge} - {{{string.Join(",", right)}}}";
            }
            case "kill":
            {
                int maxKill = (members.Count - 1) / 2;
                var victims = members.OrderBy(_ => rand.Next()).Take(rand.Next(1, maxKill + 1)).ToList();
                if (leader is { } l && rand.Next(2) == 0 && !victims.Contains(l)) victims[0] = l;
                foreach (var v in victims) cluster.Kill(v);
                _killed.AddRange(victims);
                return $"kill -9 node(s) {string.Join(",", victims)}{(victims.Contains(leader ?? 0) ? " (incl. the leader)" : "")}";
            }
            case "pause":
            {
                ulong victim = leader ?? members[rand.Next(members.Count)];
                int ms = (int)(interval.TotalMilliseconds * 0.8);
                await cluster.PauseAsync(victim, ms, ct);
                return $"pause node {victim}{(victim == leader ? " (the leader)" : "")} for {ms} ms";
            }
            case "clock":
            {
                // The recipe that breaks lease reads: the leader's clock runs slow, so its lease seems to last
                // longer than the followers' promise not to elect anyone else; then cut it off.
                ulong victim = leader ?? members[rand.Next(members.Count)];
                double rate = 0.1;
                await cluster.SetClockRateAsync(victim, rate, ct);
                foreach (var other in members.Where(m => m != victim)) await cluster.SetClockRateAsync(other, 1.5 + rand.NextDouble(), ct);
                await cluster.PartitionAsync([[victim], members.Where(m => m != victim).ToList()], ct);
                return $"clock skew: node {victim} runs at {rate}x, the others faster; node {victim} isolated";
            }
            case "member":
            {
                if (members.Count > 3 && (members.Count >= 5 || rand.Next(2) == 0))
                {
                    ulong victim = members[rand.Next(members.Count)];
                    var (ok, msg) = await cluster.RemoveNodeAsync(victim, ct);
                    return ok ? $"membership: removed node {victim}" : $"membership: removing {victim} failed ({msg})";
                }
                var (added, text) = await cluster.AddNodeAsync(ct);
                return added ? $"membership: {text}" : $"membership: add failed ({text})";
            }
            case "network":
            {
                double drop = 0.1 + rand.NextDouble() * 0.2;
                int delay = rand.Next(10, 80);
                foreach (var m in members) await cluster.SetNetworkAsync(m, drop, delay, delay, ct);
                return $"network: {drop:P0} loss, {delay}±{delay} ms delay on every link";
            }
            default:
                throw new CliException($"unknown nemesis '{kind}'");
        }
    }

    private readonly List<ulong> _killed = [];

    public async Task HealAsync(CancellationToken ct)
    {
        foreach (var id in _killed.Distinct())
        {
            var n = cluster[id];
            if (!n.Removed && !n.Running) cluster.Restart(id);
        }
        _killed.Clear();
        await cluster.HealAsync(ct);
    }
}
