using System.Diagnostics;
using System.Text.Json;
using Quorum.Cli.Commands;
using Quorum.Cli.Lab;
using Quorum.Linearizability;

namespace Quorum.Cli.Chaos;

/// <summary>Writes a self-contained HTML report of a chaos run (no external files or CDNs needed).</summary>
public static class ChaosReport
{
    public sealed record Input(
        string Name,
        int Seed,
        int Nodes,
        int Clients,
        int Keys,
        int Seconds,
        string Consistency,
        IReadOnlyList<string> Nemesis,
        IReadOnlyList<Operation> History,
        CheckResult Result,
        IReadOnlyList<FaultEvent> Faults,
        IReadOnlyList<WorkloadSecond> PerSecond,
        IReadOnlyList<ChaosCommand.Sample> Samples,
        bool Converged,
        string ConvergenceMessage,
        IReadOnlyList<string> ElectionIssues,
        IReadOnlyList<string> Log,
        (double P50, double P90, double P99, double Max) Latency);

    private static double Sec(long ticks) => ticks == long.MaxValue ? -1 : Math.Round((double)ticks / Stopwatch.Frequency, 4);

    /// <summary>
    /// For a read the checker could not place, looks for the simplest proof that it was stale: the write that
    /// produced the value it returned, and a later write that had already completed when the read began.
    /// </summary>
    private static string? ExplainStaleRead(Operation read, List<Operation> ops)
    {
        var source = ops.FirstOrDefault(o => o.Kind is OpKind.Write or OpKind.Cas && o.Value == read.Observed && read.Observed is not null);
        if (source is null) return null;
        bool sourceDone = source.Outcome == OpOutcome.Ok;
        var overwrite = ops
            .Where(o => o.Outcome == OpOutcome.Ok && o.Return < read.Call && o.Id != source.Id &&
                        (o.Kind == OpKind.Write || o.Kind == OpKind.Delete || (o.Kind == OpKind.Cas && o.CasSucceeded)) &&
                        o.Call > (sourceDone ? source.Return : source.Call))
            .OrderByDescending(o => o.Return)
            .FirstOrDefault();
        if (overwrite is null) return null;
        string Show(Operation o) => o.Kind == OpKind.Delete ? "deleted the key" : $"wrote '{o.Value}'";
        return $"Read #{read.Id} (client {read.Client}, served by node {read.Node}) started at {Sec(read.Call):0.000}s and returned '{read.Observed}'. " +
               $"That value was written by #{source.Id}, but #{overwrite.Id} (client {overwrite.Client}) {Show(overwrite)} after it and " +
               $"finished at {Sec(overwrite.Return):0.000}s, before the read began. A linearizable read cannot return the older value: " +
               $"node {read.Node} answered from a stale state.";
    }

    public static void Write(string path, Input r)
    {
        var failing = r.Result.Keys.Where(k => k.Linearizable == false).ToList();
        var anomalies = failing.Select(k =>
        {
            var ops = r.Result.Operations.Where(o => o.Key == k.Key).OrderBy(o => o.Call).ToList();
            // Show the neighbourhood of the operations that could not be placed.
            var stuckOps = ops.Where(o => k.Stuck.Contains(o.Id)).ToList();
            long from = stuckOps.Count > 0 ? stuckOps.Min(o => o.Call) : ops[0].Call;
            long to = stuckOps.Count > 0 ? stuckOps.Max(o => o.Return == long.MaxValue || o.Outcome == OpOutcome.Unknown ? o.Call : o.Return) : ops[^1].Return;
            long margin = Math.Max((to - from) / 2, Stopwatch.Frequency / 2);
            var window = ops.Where(o => o.Call <= to + margin && (o.Outcome == OpOutcome.Unknown || o.Return >= from - margin)).Take(400).ToList();
            var orderSet = k.BestOrder.ToHashSet();
            var lastOrdered = k.BestOrder.TakeLast(12).ToList();
            return new
            {
                key = k.Key,
                explanations = stuckOps.Where(o => o.Kind == OpKind.Read).Select(o => ExplainStaleRead(o, ops)).Where(e => e is not null).ToList(),
                operations = k.Operations,
                bestOrderLength = k.BestOrder.Count,
                lastOrdered = lastOrdered.Select(id => ops.First(o => o.Id == id).ToString()).ToList(),
                stuck = stuckOps.Select(o => o.ToString()).ToList(),
                ops = window.Select(o => new
                {
                    id = o.Id,
                    client = o.Client,
                    kind = o.Kind.ToString(),
                    text = o.ToString(),
                    call = Sec(o.Call),
                    ret = o.Outcome == OpOutcome.Unknown ? -1 : Sec(o.Return),
                    unknown = o.Outcome == OpOutcome.Unknown,
                    stuck = k.Stuck.Contains(o.Id),
                    ordered = orderSet.Contains(o.Id),
                    node = o.Node,
                }),
            };
        }).ToList();

        var nodesTimeline = r.Samples.GroupBy(s => s.Node).OrderBy(g => g.Key).Select(g => new
        {
            node = g.Key,
            points = g.OrderBy(s => s.T).Select(s => new object[] { Math.Round(s.T, 2), s.Role, s.Term }),
        });

        var data = new
        {
            name = r.Name,
            seed = r.Seed,
            nodes = r.Nodes,
            clients = r.Clients,
            keys = r.Keys,
            seconds = r.Seconds,
            consistency = r.Consistency,
            nemesis = r.Nemesis,
            linearizable = r.Result.Linearizable,
            converged = r.Converged,
            convergence = r.ConvergenceMessage,
            electionIssues = r.ElectionIssues,
            total = r.History.Count,
            ok = r.History.Count(o => o.Outcome == OpOutcome.Ok),
            failed = r.History.Count(o => o.Outcome == OpOutcome.Fail),
            unknown = r.History.Count(o => o.Outcome == OpOutcome.Unknown),
            latency = new { p50 = Math.Round(r.Latency.P50, 1), p90 = Math.Round(r.Latency.P90, 1), p99 = Math.Round(r.Latency.P99, 1), max = Math.Round(r.Latency.Max, 0) },
            perSecond = r.PerSecond.Select(s => new object[] { s.Second, s.Ok, s.Failed, s.Unknown, s.P50Ms, s.P99Ms }),
            faults = r.Faults.Select(f => new { start = Math.Round(f.StartSec, 2), end = Math.Round(f.EndSec, 2), kind = f.Kind, text = f.Description }),
            keyResults = r.Result.Keys.Select(k => new
            {
                key = k.Key,
                ops = k.Operations,
                verdict = k.Linearizable switch { true => "linearizable", false => "NOT linearizable", null => "unknown" },
                ms = Math.Round(k.Elapsed.TotalMilliseconds, 0),
                states = k.StatesExplored,
            }),
            anomalies,
            timeline = nodesTimeline,
            log = r.Log,
        };
        string json = JsonSerializer.Serialize(data, new JsonSerializerOptions(JsonSerializerDefaults.Web)).Replace("</", "<\\/");
        File.WriteAllText(path, Template.Replace("/*DATA*/null", json));
    }

    private const string Template = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quorum chaos report</title>
<style>
:root {
  --bg: #f6f7f9; --panel: #ffffff; --ink: #1b1f24; --muted: #5d6673; --line: #dfe3e8;
  --ok: #1f8a4c; --bad: #c9352b; --warn: #b7791f; --accent: #2f5bd3;
  --leader: #2f5bd3; --follower: #9aa7b8; --candidate: #d08a1a; --down: #c9352b; --fault: rgba(201,53,43,0.10);
  --read: #2f5bd3; --write: #1f8a4c; --cas: #8a3fc7; --delete: #6b7280;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0f1216; --panel: #171b21; --ink: #e6e9ee; --muted: #9aa4b2; --line: #2a313b;
    --ok: #3fbf74; --bad: #ef6b60; --warn: #e2a53b; --accent: #7aa2ff;
    --leader: #7aa2ff; --follower: #556170; --candidate: #e2a53b; --down: #ef6b60; --fault: rgba(239,107,96,0.13);
    --read: #7aa2ff; --write: #3fbf74; --cas: #c084fc; --delete: #9aa4b2;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1180px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 22px; margin: 0 0 4px; }
h2 { font-size: 16px; margin: 28px 0 10px; }
.sub { color: var(--muted); margin-bottom: 18px; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
.card .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.card .value { font-size: 20px; font-weight: 650; margin-top: 2px; }
.pass { color: var(--ok); } .fail { color: var(--bad); } .unk { color: var(--warn); }
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 14px; overflow-x: auto; }
svg text { fill: var(--muted); font-size: 11px; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); font-variant-numeric: tabular-nums; }
th { color: var(--muted); font-weight: 600; font-size: 12px; }
code, .mono { font-family: ui-monospace, "Cascadia Code", Consolas, monospace; font-size: 12.5px; }
.legend { display: flex; gap: 14px; flex-wrap: wrap; color: var(--muted); font-size: 12px; margin-top: 8px; }
.sw { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 5px; vertical-align: -1px; }
.anomaly { border-left: 3px solid var(--bad); }
.log { max-height: 320px; overflow: auto; white-space: pre-wrap; }
.tip { position: fixed; pointer-events: none; background: var(--panel); border: 1px solid var(--line); border-radius: 6px; padding: 6px 8px; font-size: 12px; display: none; max-width: 420px; box-shadow: 0 4px 16px rgba(0,0,0,.15); }
</style>
</head>
<body>
<main id="app"></main>
<div class="tip" id="tip"></div>
<script>
const D = /*DATA*/null;
const $ = (h) => { const t = document.createElement('template'); t.innerHTML = h.trim(); return t.content.firstChild; };
const esc = (s) => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const tip = document.getElementById('tip');
function showTip(e, html) { tip.innerHTML = html; tip.style.display = 'block'; tip.style.left = (e.clientX + 12) + 'px'; tip.style.top = (e.clientY + 12) + 'px'; }
function hideTip() { tip.style.display = 'none'; }

const app = document.getElementById('app');
const verdict = (v) => v === true ? '<span class="pass">PASS</span>' : v === false ? '<span class="fail">FAIL</span>' : '<span class="unk">UNKNOWN</span>';
const elections = D.electionIssues.length === 0;
app.append($(`<div><h1>Quorum chaos report</h1>
<div class="sub">${esc(D.name)} · seed ${D.seed} · ${D.nodes} nodes · ${D.clients} clients · ${D.keys} keys · ${D.seconds} s · reads: ${esc(D.consistency)} · nemesis: ${esc(D.nemesis.join(', ') || 'none')}</div></div>`));
app.append($(`<div class="cards">
  <div class="card"><div class="label">Linearizable</div><div class="value">${verdict(D.linearizable)}</div></div>
  <div class="card"><div class="label">Replicas converged</div><div class="value">${verdict(D.converged)}</div></div>
  <div class="card"><div class="label">Election safety</div><div class="value">${verdict(elections)}</div></div>
  <div class="card"><div class="label">Operations</div><div class="value">${D.total.toLocaleString()}</div><div class="sub" style="margin:0">${D.ok.toLocaleString()} ok · ${D.failed} failed · ${D.unknown} unknown</div></div>
  <div class="card"><div class="label">Latency p50 / p99</div><div class="value">${D.latency.p50} / ${D.latency.p99} ms</div><div class="sub" style="margin:0">max ${D.latency.max} ms</div></div>
  <div class="card"><div class="label">Faults injected</div><div class="value">${D.faults.length}</div></div>
</div>`));

// ---------------------------------------------------------------- timeline
const T = Math.max(D.seconds + 5, ...D.perSecond.map(p => p[0] + 1), ...D.timeline.flatMap(n => n.points.map(p => p[0])));
function timeline() {
  const W = 1140, left = 56, right = 12, plotW = W - left - right;
  const H1 = 150, laneH = 16, lanes = D.timeline.length, top2 = H1 + 34, H = top2 + lanes * (laneH + 6) + 26;
  const x = (t) => left + (t / T) * plotW;
  const maxOps = Math.max(1, ...D.perSecond.map(p => p[1] + p[2] + p[3]));
  let s = `<svg viewBox="0 0 ${W} ${H}" width="100%" style="min-width:720px">`;
  for (const f of D.faults) {
    s += `<rect x="${x(f.start)}" y="0" width="${Math.max(2, x(f.end) - x(f.start))}" height="${H - 20}" fill="${css('--fault')}" data-tip="${esc(f.text)}"/>`;
  }
  for (const p of D.perSecond) {
    const [sec, ok, failed, unknown] = p;
    const bw = Math.max(1, plotW / T - 1);
    let y = H1;
    const seg = (v, color) => { if (!v) return ''; const h = v / maxOps * (H1 - 10); y -= h; return `<rect x="${x(sec)}" y="${y}" width="${bw}" height="${h}" fill="${color}"/>`; };
    s += `<g data-tip="${sec}s: ${ok} ok, ${failed} failed, ${unknown} unknown · p50 ${p[4]} ms, p99 ${p[5]} ms">` + seg(ok, css('--ok')) + seg(failed, css('--bad')) + seg(unknown, css('--warn')) + `</g>`;
  }
  s += `<text x="${left - 8}" y="14" text-anchor="end">${maxOps}</text><text x="${left - 8}" y="${H1}" text-anchor="end">0</text>`;
  s += `<text x="4" y="${H1 / 2}">ops/s</text>`;
  const roleColor = { Leader: css('--leader'), Follower: css('--follower'), Candidate: css('--candidate'), PreCandidate: css('--candidate'), Down: css('--down'), Removed: 'transparent' };
  D.timeline.forEach((n, i) => {
    const y = top2 + i * (laneH + 6);
    s += `<text x="${left - 8}" y="${y + 12}" text-anchor="end">node ${n.node}</text>`;
    for (let j = 0; j < n.points.length; j++) {
      const [t, role, term] = n.points[j];
      const t2 = j + 1 < n.points.length ? n.points[j + 1][0] : t + 0.25;
      s += `<rect x="${x(t)}" y="${y}" width="${Math.max(1, x(t2) - x(t))}" height="${laneH}" fill="${roleColor[role] || css('--follower')}" data-tip="node ${n.node} at ${t}s: ${role}${term ? ' (term ' + term + ')' : ''}"/>`;
    }
  });
  for (let t = 0; t <= T; t += T > 120 ? 20 : 10) s += `<text x="${x(t)}" y="${H - 6}" text-anchor="middle">${t}s</text>`;
  s += `</svg>`;
  return s;
}
app.append($(`<div><h2>Timeline</h2><div class="panel">${timeline()}
<div class="legend"><span><i class="sw" style="background:var(--ok)"></i>ok</span><span><i class="sw" style="background:var(--bad)"></i>failed</span><span><i class="sw" style="background:var(--warn)"></i>unknown outcome</span><span><i class="sw" style="background:var(--fault)"></i>fault active</span><span><i class="sw" style="background:var(--leader)"></i>leader</span><span><i class="sw" style="background:var(--follower)"></i>follower</span><span><i class="sw" style="background:var(--candidate)"></i>candidate</span><span><i class="sw" style="background:var(--down)"></i>down</span></div></div></div>`));

// ---------------------------------------------------------------- faults
if (D.faults.length) {
  app.append($(`<div><h2>Faults</h2><div class="panel"><table><tr><th>from</th><th>to</th><th>fault</th></tr>${D.faults.map(f => `<tr><td>${f.start.toFixed(1)}s</td><td>${f.end.toFixed(1)}s</td><td>${esc(f.text)}</td></tr>`).join('')}</table></div></div>`));
}

// ---------------------------------------------------------------- keys
app.append($(`<div><h2>Linearizability per key</h2><div class="panel"><table><tr><th>key</th><th>operations</th><th>verdict</th><th>search</th></tr>${D.keyResults.map(k => `<tr><td class="mono">${esc(k.key)}</td><td>${k.ops}</td><td class="${k.verdict === 'linearizable' ? 'pass' : k.verdict === 'unknown' ? 'unk' : 'fail'}">${k.verdict}</td><td>${k.ms} ms · ${k.states.toLocaleString()} states</td></tr>`).join('')}</table>
<div class="sub" style="margin:8px 0 0">${esc(D.convergence)}${D.electionIssues.length ? ' · ' + esc(D.electionIssues.join('; ')) : ''}</div></div></div>`));

// ---------------------------------------------------------------- anomalies
function swimlanes(a) {
  const ops = a.ops; if (!ops.length) return '';
  const clients = [...new Set(ops.map(o => o.client))].sort((p, q) => p - q);
  const t0 = Math.min(...ops.map(o => o.call));
  const t1 = Math.max(...ops.map(o => o.ret < 0 ? o.call + 0.05 : o.ret));
  const W = 1140, left = 70, plotW = W - left - 12, rowH = 18, H = clients.length * (rowH + 4) + 30;
  const x = (t) => left + ((t - t0) / Math.max(1e-6, t1 - t0)) * plotW;
  const color = { Read: css('--read'), Write: css('--write'), Cas: css('--cas'), Delete: css('--delete') };
  let s = `<svg viewBox="0 0 ${W} ${H}" width="100%" style="min-width:720px">`;
  clients.forEach((c, i) => { s += `<text x="${left - 8}" y="${i * (rowH + 4) + 13}" text-anchor="end">client ${c < 0 ? 'final' : c}</text>`; });
  for (const o of ops) {
    const row = clients.indexOf(o.client);
    const y = row * (rowH + 4);
    const end = o.ret < 0 ? x(t1) : x(o.ret);
    const w = Math.max(3, end - x(o.call));
    s += `<rect x="${x(o.call)}" y="${y}" width="${w}" height="${rowH}" rx="3" fill="${color[o.kind]}" fill-opacity="${o.ordered ? 0.35 : 0.85}" stroke="${o.stuck ? css('--bad') : 'none'}" stroke-width="${o.stuck ? 2.5 : 0}" ${o.unknown ? 'stroke-dasharray="4 3"' : ''} data-tip="${esc(o.text)}<br>${o.call.toFixed(4)}s → ${o.ret < 0 ? 'unknown' : o.ret.toFixed(4) + 's'} · node ${o.node}${o.stuck ? '<br><b>could not be linearized</b>' : ''}"/>`;
  }
  s += `<text x="${left}" y="${H - 6}">${t0.toFixed(3)}s</text><text x="${W - 12}" y="${H - 6}" text-anchor="end">${t1.toFixed(3)}s</text></svg>`;
  return s;
}
for (const a of D.anomalies) {
  app.append($(`<div><h2>Anomaly on key <code>${esc(a.key)}</code></h2><div class="panel anomaly">
${a.explanations.map(e => `<p><b>In plain words:</b> ${esc(e)}</p>`).join('')}
<p>The checker linearized ${a.bestOrderLength} of ${a.operations} operations on this key in a valid order, then got stuck: none of the operations below can come next without contradicting a result some client saw.</p>
<p><b>Could not be placed:</b></p><ul class="mono">${a.stuck.map(t => `<li>${esc(t)}</li>`).join('')}</ul>
<p><b>Last operations of the longest valid order:</b></p><ol class="mono">${a.lastOrdered.map(t => `<li>${esc(t)}</li>`).join('')}</ol>
${swimlanes(a)}
<div class="legend"><span><i class="sw" style="background:var(--read)"></i>read</span><span><i class="sw" style="background:var(--write)"></i>write</span><span><i class="sw" style="background:var(--cas)"></i>cas</span><span>faded: in the valid order · red outline: stuck · dashed: unknown outcome</span></div>
</div></div>`));
}

// ---------------------------------------------------------------- log
app.append($(`<div><h2>Run log</h2><div class="panel log mono">${D.log.map(esc).join('\n')}</div></div>`));

document.addEventListener('mousemove', (e) => {
  const t = e.target.closest('[data-tip]');
  if (t) showTip(e, t.getAttribute('data-tip')); else hideTip();
});
</script>
</body>
</html>
""";
}
