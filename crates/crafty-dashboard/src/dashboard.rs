//! The embedded, read-only live dashboard UI (observability §6).
//!
//! A single self-contained HTML page (no build step, no external assets) served
//! at `GET /dashboard`. It polls the introspection JSON endpoints for cluster
//! and actor state and tails the telemetry SSE feed at `/dashboard/events` for
//! a live event log. Read-only: it never mutates the cluster (observability §6).

/// The dashboard page markup + inline script.
pub(crate) const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>crafty · dashboard</title>
<style>
  :root { color-scheme: dark; --bg:#0d1117; --panel:#161b22; --line:#30363d; --fg:#e6edf3; --muted:#8b949e; --accent:#58a6ff; --ok:#3fb950; --warn:#d29922; }
  * { box-sizing: border-box; }
  body { margin:0; font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace; background:var(--bg); color:var(--fg); }
  header { padding:14px 20px; border-bottom:1px solid var(--line); display:flex; align-items:baseline; gap:12px; }
  header h1 { font-size:16px; margin:0; letter-spacing:.5px; }
  header .dot { width:8px; height:8px; border-radius:50%; background:var(--muted); display:inline-block; }
  header .dot.live { background:var(--ok); }
  main { display:grid; grid-template-columns:1fr 1fr; gap:16px; padding:16px 20px; }
  section { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px 16px; }
  section h2 { font-size:12px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:0 0 10px; }
  .events { grid-column:1 / -1; }
  table { width:100%; border-collapse:collapse; }
  th,td { text-align:left; padding:5px 8px; border-bottom:1px solid var(--line); }
  th { color:var(--muted); font-weight:600; }
  .kv { display:flex; justify-content:space-between; padding:3px 0; }
  .kv span:first-child { color:var(--muted); }
  #log { max-height:320px; overflow:auto; margin:0; padding:0; list-style:none; }
  #log li { padding:4px 8px; border-bottom:1px solid var(--line); white-space:pre-wrap; font-size:13px; }
  #log li .t { color:var(--muted); font-size:12px; margin-right:8px; }
  #log li .k { color:var(--accent); }
  #log li .q { color:var(--warn); }
  #log li .r { color:var(--ok); }
  td.warn { color:var(--warn); }
  .badge { padding:1px 7px; border-radius:10px; font-size:12px; background:#21262d; }
  .badge.leader { color:var(--ok); }
</style>
</head>
<body>
<header>
  <h1>crafty</h1>
  <span class="dot" id="livedot"></span>
  <span style="color:var(--muted)" id="livetext">connecting…</span>
</header>
<main>
  <section>
    <h2>Cluster</h2>
    <div id="cluster"></div>
    <table><thead><tr><th>node</th><th>role</th><th>member</th></tr></thead><tbody id="nodes"></tbody></table>
  </section>
  <section>
    <h2>Actors</h2>
    <table><thead><tr><th>id</th><th>type</th><th>node</th><th>mailbox</th><th>uptime</th><th>msg/s</th><th>gen</th></tr></thead><tbody id="actors"></tbody></table>
  </section>
  <section>
    <h2>Job queues</h2>
    <table><thead><tr><th>stream</th><th>pending</th><th>leased</th><th>dead letter</th><th>oldest (ms)</th><th title="Jobs that failed an attempt and will be delivered again — handlers must be idempotent">redelivered</th></tr></thead><tbody id="queues"></tbody></table>
  </section>
  <section>
    <h2>Workflows</h2>
    <table><thead><tr><th>saga id</th><th>phase</th><th>steps</th><th>failed</th></tr></thead><tbody id="sagas"></tbody></table>
  </section>
  <section class="events">
    <h2>Event feed</h2>
    <ul id="log"></ul>
  </section>
</main>
<script>
const $ = (id) => document.getElementById(id);
async function refresh() {
  try {
    const [c, a, q, s] = await Promise.all([
      fetch('/introspect/cluster').then(r => r.json()),
      fetch('/introspect/actors').then(r => r.json()),
      fetch('/introspect/queues').then(r => r.json()),
      fetch('/introspect/sagas').then(r => r.json()),
    ]);
    $('cluster').innerHTML =
      `<div class="kv"><span>leader</span><span class="badge leader">${c.leader ?? '—'}</span></div>` +
      `<div class="kv"><span>term</span><span>${c.term}</span></div>` +
      `<div class="kv"><span>commit index</span><span>${c.commit_index}</span></div>`;
    $('nodes').innerHTML = (c.nodes||[]).map(n =>
      `<tr><td>${n.id}</td><td>${n.role}</td><td>${n.member}</td></tr>`).join('');
    $('actors').innerHTML = (a||[]).map(x =>
      `<tr><td>${x.id}</td><td>${x.actor_type}</td><td>${x.node}</td><td>${x.mailbox_depth}</td><td>${x.uptime_secs}s</td><td>${(x.messages_per_sec ?? 0).toFixed(1)}</td><td>${x.generation}</td></tr>`).join('');
    $('queues').innerHTML = ((q && q.streams) || []).map(x =>
      `<tr><td>${x.stream}</td><td>${x.pending}</td><td>${x.leased}</td><td>${x.dead_letter ?? 0}</td><td>${x.oldest_pending_age_ms}</td><td${(x.redelivered ?? 0) > 0 ? ' class="warn" title="idempotency smell: these jobs are being re-run"' : ''}>${x.redelivered ?? 0}</td></tr>`).join('');
    $('sagas').innerHTML = (s||[]).map(x =>
      `<tr><td>${x.saga_id.slice(0,16)}…</td><td>${x.phase}</td><td>${x.completed_steps}</td><td>${x.failed_step ?? '—'}</td></tr>`).join('');
  } catch (e) { /* transient during elections */ }
}
function formatEvent(raw) {
  try {
    const e = JSON.parse(raw);
    switch (e.event) {
      case 'job_enqueued': return `<span class="k">queue</span> ${e.stream}: enqueued job <b>${e.job_id}</b>`;
      case 'job_leased': return `<span class="q">queue</span> ${e.stream}: leased job <b>${e.job_id}</b> → worker ${e.worker_node}#${e.worker_instance}`;
      case 'job_acked': return `<span class="r">queue</span> ${e.stream}: ack lease <b>${e.lease_id}</b> (worker ${e.worker_node})`;
      case 'raft_committed': return `<span class="k">raft</span> commit index <b>${e.commit_index}</b> (term ${e.term})`;
      case 'leader_changed': return `<span class="k">raft</span> leader → node <b>${e.leader}</b> (term ${e.term})`;
      case 'node_joined': return `node <b>${e.node_id}</b> joined cluster`;
      case 'node_left': return `node <b>${e.node_id}</b> left (${e.graceful ? 'graceful' : 'ungraceful'})`;
      case 'actor_spawned': return `actor spawned: ${e.id}`;
      case 'actor_stopped': return `actor stopped: ${e.id} (${e.reason})`;
      case 'message_handled': return `${e.id}: message handled (${e.latency_ms} ms)`;
      default: return `<span class="k">${e.event ?? 'event'}</span> ${raw}`;
    }
  } catch (_) { return raw; }
}
function connect() {
  const es = new EventSource('/dashboard/events');
  es.onopen = () => { $('livedot').classList.add('live'); $('livetext').textContent = 'live'; };
  es.onerror = () => { $('livedot').classList.remove('live'); $('livetext').textContent = 'reconnecting…'; };
  es.onmessage = (m) => {
    const li = document.createElement('li');
    const now = new Date().toLocaleTimeString();
    li.innerHTML = `<span class="t">${now}</span>${formatEvent(m.data)}`;
    const log = $('log');
    log.prepend(li);
    while (log.childElementCount > 200) log.removeChild(log.lastChild);
  };
}
refresh(); setInterval(refresh, 2000); connect();
</script>
</body>
</html>
"#;
