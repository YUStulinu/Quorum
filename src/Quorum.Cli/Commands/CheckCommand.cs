using Quorum.Linearizability;

namespace Quorum.Cli.Commands;

/// <summary><c>quorum check history.jsonl</c>: re-checks a history saved by a chaos run.</summary>
public static class CheckCommand
{
    public static int Run(CliArgs a)
    {
        a.RejectUnknown();
        if (a.Positional.Count != 1) throw new CliException("usage: quorum check <history.jsonl>");
        var ops = HistoryFile.Load(a.Positional[0]);
        var result = Checker.Check(ops);
        foreach (var k in result.Keys)
        {
            string verdict = k.Linearizable switch { true => "linearizable", false => "NOT LINEARIZABLE", null => "unknown (timed out)" };
            Console.WriteLine($"  {k.Key,-12} {k.Operations,7} ops  {verdict}  ({k.Elapsed.TotalMilliseconds:0} ms, {k.StatesExplored} states)");
        }
        Console.WriteLine(result.Linearizable switch
        {
            true => "History is linearizable.",
            false => "History is NOT linearizable.",
            null => "Could not decide within the time limit.",
        });
        return result.Linearizable == false ? 1 : 0;
    }
}
