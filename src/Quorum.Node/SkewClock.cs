using System.Diagnostics;

namespace Quorum.Node;

/// <summary>
/// The node's notion of elapsed time, in milliseconds. Normally it follows the machine's monotonic clock,
/// but the chaos tester can make it run faster or slower (<see cref="Rate"/>), which is how clock drift is
/// injected: election timers, heartbeats and the leader lease all run on this clock.
/// </summary>
public sealed class SkewClock
{
    private readonly object _lock = new();
    private readonly Stopwatch _real = Stopwatch.StartNew();
    private double _baseReal;
    private double _baseSkewed;
    private double _rate = 1.0;

    public double Rate
    {
        get
        {
            lock (_lock) return _rate;
        }
        set
        {
            if (value <= 0 || double.IsNaN(value) || value > 100) throw new ArgumentOutOfRangeException(nameof(value));
            lock (_lock)
            {
                double now = _real.Elapsed.TotalMilliseconds;
                _baseSkewed += (now - _baseReal) * _rate;
                _baseReal = now;
                _rate = value;
            }
        }
    }

    public double NowMs
    {
        get
        {
            lock (_lock) return _baseSkewed + (_real.Elapsed.TotalMilliseconds - _baseReal) * _rate;
        }
    }

    /// <summary>Real milliseconds until the skewed clock reaches <paramref name="skewedTarget"/>.</summary>
    public int RealMsUntil(double skewedTarget)
    {
        double now = NowMs;
        if (skewedTarget <= now) return 0;
        double rate = Rate;
        return (int)Math.Ceiling((skewedTarget - now) / rate);
    }
}
