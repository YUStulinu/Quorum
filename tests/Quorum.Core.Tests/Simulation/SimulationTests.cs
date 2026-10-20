using Quorum.Core;
using Xunit.Abstractions;

namespace Quorum.Core.Tests.Simulation;

/// <summary>
/// Randomized, deterministic whole-cluster runs. Each seed is a different schedule of crashes, partitions,
/// message loss, snapshots and membership changes; a failure prints the seed so it can be replayed exactly.
/// </summary>
public sealed class SimulationTests(ITestOutputHelper output)
{
    private void Run(int seed, int ticks, SimOptions? template = null)
    {
        var opt = new SimOptions
        {
            Seed = seed,
            InitialNodes = template?.InitialNodes ?? (seed % 2 == 0 ? 3 : 5),
            ReadMode = template?.ReadMode ?? ReadMode.ReadIndex,
            DropRate = template?.DropRate ?? 0.02,
        };
        var sim = new SimCluster(opt);
        try
        {
            for (int i = 0; i < ticks; i++) sim.ChaosStep();
            sim.Stabilize();
        }
        catch (Exception ex) when (ex is SafetyViolation or RaftInvariantException)
        {
            output.WriteLine(string.Join("\n", sim.Log.TakeLast(60)));
            throw new Exception($"seed {seed} failed at tick {sim.Now}: {ex.Message}", ex);
        }
        Assert.True(sim.Elections > 0);
    }

    [Theory]
    [MemberData(nameof(Seeds), 0, 60)]
    public void ChaosReadIndex(int seed) => Run(seed, 3000);

    [Theory]
    [MemberData(nameof(Seeds), 1000, 30)]
    public void ChaosLeaseReads(int seed) => Run(seed, 3000, new SimOptions { ReadMode = ReadMode.Lease });

    [Theory]
    [MemberData(nameof(Seeds), 2000, 20)]
    public void ChaosHeavyLoss(int seed) => Run(seed, 2000, new SimOptions { DropRate = 0.25 });

    [Fact]
    public void ManySeedsSmoke()
    {
        int failures = 0;
        for (int seed = 5000; seed < 5150; seed++)
        {
            try { Run(seed, 1500); }
            catch (Exception ex)
            {
                failures++;
                output.WriteLine(ex.Message);
            }
        }
        Assert.Equal(0, failures);
    }

    public static IEnumerable<object[]> Seeds(int start, int count) =>
        Enumerable.Range(start, count).Select(s => new object[] { s });
}
