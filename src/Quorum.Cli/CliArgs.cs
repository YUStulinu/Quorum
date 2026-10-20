using System.Globalization;

namespace Quorum.Cli;

/// <summary>A minimal parser for <c>--name value</c> options, <c>--flag</c> switches and positional arguments.</summary>
public sealed class CliArgs
{
    private readonly Dictionary<string, string?> _options = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _used = new(StringComparer.OrdinalIgnoreCase);

    public List<string> Positional { get; } = [];

    public CliArgs(IEnumerable<string> args)
    {
        var list = args.ToList();
        for (int i = 0; i < list.Count; i++)
        {
            string a = list[i];
            if (a.StartsWith("--", StringComparison.Ordinal))
            {
                string name = a[2..];
                string? value = null;
                int eq = name.IndexOf('=');
                if (eq >= 0)
                {
                    value = name[(eq + 1)..];
                    name = name[..eq];
                }
                else if (i + 1 < list.Count && !list[i + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    value = list[++i];
                }
                _options[name] = value;
            }
            else
            {
                Positional.Add(a);
            }
        }
    }

    public bool Has(string name)
    {
        _used.Add(name);
        return _options.ContainsKey(name);
    }

    public string? Get(string name)
    {
        _used.Add(name);
        return _options.TryGetValue(name, out var v) ? v : null;
    }

    public string Get(string name, string fallback) => Get(name) ?? fallback;

    public int GetInt(string name, int fallback)
    {
        var v = Get(name);
        if (v is null) return fallback;
        return int.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out int n)
            ? n : throw new CliException($"--{name} expects an integer, got '{v}'");
    }

    public double GetDouble(string name, double fallback)
    {
        var v = Get(name);
        if (v is null) return fallback;
        return double.TryParse(v, NumberStyles.Float, CultureInfo.InvariantCulture, out double d)
            ? d : throw new CliException($"--{name} expects a number, got '{v}'");
    }

    public ulong GetULong(string name, ulong fallback)
    {
        var v = Get(name);
        if (v is null) return fallback;
        return ulong.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong n)
            ? n : throw new CliException($"--{name} expects a positive integer, got '{v}'");
    }

    /// <summary>Fails on options nobody asked for (catches typos).</summary>
    public void RejectUnknown()
    {
        var unknown = _options.Keys.Where(k => !_used.Contains(k)).ToList();
        if (unknown.Count > 0) throw new CliException($"unknown option(s): {string.Join(", ", unknown.Select(u => "--" + u))}");
    }
}

public sealed class CliException(string message) : Exception(message);
