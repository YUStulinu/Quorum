using System.Text.Json;
using System.Text.Json.Serialization;

namespace Quorum.Linearizability;

public enum OpKind : byte
{
    Read,
    Write,
    Delete,
    Cas,
}

/// <summary>How an operation ended, from the client's point of view.</summary>
public enum OpOutcome : byte
{
    /// <summary>It completed and we know its result.</summary>
    Ok,
    /// <summary>It definitely did not take effect (rejected before being executed). Dropped before checking.</summary>
    Fail,
    /// <summary>We do not know whether it took effect (a timeout). It may be linearized at any point after its call.</summary>
    Unknown,
}

/// <summary>
/// One client operation on one key of a register map, with its real-time interval
/// <c>[Call, Return]</c> measured on a single clock.
/// </summary>
public sealed record Operation
{
    public required int Id { get; init; }
    public required int Client { get; init; }
    public required string Key { get; init; }
    public required OpKind Kind { get; init; }
    /// <summary>Write/Cas: the value written (null for a Cas that deletes).</summary>
    public string? Value { get; init; }
    /// <summary>Cas: the expected value (null = the key must be absent).</summary>
    public string? Expected { get; init; }
    /// <summary>Read: the value observed (null = absent).</summary>
    public string? Observed { get; init; }
    /// <summary>Cas: whether the condition held.</summary>
    public bool CasSucceeded { get; init; }
    public required long Call { get; init; }
    public required long Return { get; init; }
    public required OpOutcome Outcome { get; init; }
    /// <summary>The node that answered, for reports.</summary>
    public ulong Node { get; init; }

    public override string ToString()
    {
        string what = Kind switch
        {
            OpKind.Read => $"read {Key} -> {Show(Observed)}",
            OpKind.Write => $"write {Key} := {Show(Value)}",
            OpKind.Delete => $"delete {Key}",
            _ => $"cas {Key} {Show(Expected)} -> {Show(Value)}" + (Outcome == OpOutcome.Ok ? (CasSucceeded ? " ok" : " failed") : ""),
        };
        return $"#{Id} c{Client} {what}{(Outcome == OpOutcome.Unknown ? " (unknown)" : "")}";
    }

    internal static string Show(string? v) => v is null ? "∅" : $"'{v}'";
}

/// <summary>Saving and loading histories as JSON (one operation per line).</summary>
public static class HistoryFile
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static void Save(string path, IEnumerable<Operation> ops)
    {
        using var w = new StreamWriter(path);
        foreach (var op in ops) w.WriteLine(JsonSerializer.Serialize(op, Options));
    }

    public static List<Operation> Load(string path) =>
        File.ReadLines(path).Where(l => l.Length > 0).Select(l => JsonSerializer.Deserialize<Operation>(l, Options)!).ToList();
}
