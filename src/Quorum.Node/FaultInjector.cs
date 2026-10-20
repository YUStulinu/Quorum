namespace Quorum.Node;

/// <summary>The network faults currently applied to a node, as set through the admin API.</summary>
public sealed record FaultSettings
{
    /// <summary>Peers this node cannot talk to, in either direction (a partition is this set on both sides).</summary>
    public IReadOnlyCollection<ulong> Blocked { get; init; } = [];
    /// <summary>Probability of dropping each outgoing batch of messages.</summary>
    public double DropRate { get; init; }
    /// <summary>Extra latency added to every outgoing batch.</summary>
    public int DelayMs { get; init; }
    /// <summary>Random extra latency in [0, JitterMs).</summary>
    public int JitterMs { get; init; }

    public static readonly FaultSettings None = new();
}

/// <summary>Applies <see cref="FaultSettings"/> to node-to-node traffic. Thread-safe.</summary>
public sealed class FaultInjector
{
    private volatile FaultSettings _settings = FaultSettings.None;
    private volatile HashSet<ulong> _blocked = [];

    public FaultSettings Settings
    {
        get => _settings;
        set
        {
            if (value.DropRate is < 0 or > 1) throw new ArgumentOutOfRangeException(nameof(value), "drop rate must be in [0, 1]");
            if (value.DelayMs < 0 || value.JitterMs < 0) throw new ArgumentOutOfRangeException(nameof(value), "delays must be >= 0");
            _blocked = [.. value.Blocked];
            _settings = value;
        }
    }

    public bool IsBlocked(ulong peer) => _blocked.Contains(peer);

    public bool ShouldDrop() => _settings.DropRate > 0 && Random.Shared.NextDouble() < _settings.DropRate;

    public int NextDelayMs()
    {
        var s = _settings;
        return s.DelayMs + (s.JitterMs > 0 ? Random.Shared.Next(s.JitterMs) : 0);
    }

    public bool Active => _settings != FaultSettings.None &&
                          (_blocked.Count > 0 || _settings.DropRate > 0 || _settings.DelayMs > 0 || _settings.JitterMs > 0);
}
