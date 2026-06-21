"""Single-page HTML dashboard — inline CSS + JS, no external dependencies."""

HTML_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dramatiq Streams Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         background: #f5f6fa; color: #2d3436; line-height: 1.5; }
  a { color: #0984e3; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .container { max-width: 1100px; margin: 0 auto; padding: 16px; }
  header { background: #2d3436; color: #fff; padding: 12px 0; margin-bottom: 20px; }
  header .container { display: flex; align-items: center; justify-content: space-between; }
  header h1 { font-size: 18px; font-weight: 600; }
  header nav a { color: #dfe6e9; margin-left: 18px; font-size: 14px; }
  header nav a:hover { color: #fff; text-decoration: none; }
  .controls { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; font-size: 13px; }
  .controls label { cursor: pointer; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px;
           font-weight: 600; color: #fff; }
  .badge-blue { background: #0984e3; }
  .badge-red { background: #d63031; }
  .badge-gray { background: #636e72; }
  table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 6px;
          overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 20px; }
  th, td { text-align: left; padding: 10px 14px; font-size: 13px; }
  th { background: #dfe6e9; font-weight: 600; font-size: 12px; text-transform: uppercase;
       letter-spacing: .3px; color: #636e72; }
  tr:not(:last-child) td { border-bottom: 1px solid #f0f0f0; }
  tr:hover td { background: #f8f9fa; }
  .mono { font-family: "SF Mono", "Fira Code", "Consolas", monospace; font-size: 12px; }
  .btn { display: inline-block; padding: 5px 12px; border: none; border-radius: 4px;
         font-size: 12px; cursor: pointer; font-weight: 500; }
  .btn-primary { background: #0984e3; color: #fff; }
  .btn-danger { background: #d63031; color: #fff; }
  .btn-light { background: #dfe6e9; color: #2d3436; }
  .btn:hover { opacity: .85; }
  .btn + .btn { margin-left: 6px; }
  .pending-more { display: flex; align-items: center; gap: 10px; margin-top: 6px; }
  .pending-count { font-size: 12px; color: #636e72; }
  .empty { text-align: center; padding: 40px; color: #636e72; }
  h2 { font-size: 16px; margin-bottom: 12px; }
  .breadcrumb { font-size: 13px; margin-bottom: 12px; color: #636e72; }
  .stats { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
  .stat-card { background: #fff; border-radius: 6px; padding: 16px 20px;
               box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 140px; }
  .stat-card .value { font-size: 28px; font-weight: 700; }
  .stat-card .label { font-size: 12px; color: #636e72; text-transform: uppercase; }
  .badge-green { background: #00b894; }
  .badge-orange { background: #e17055; }
  .worker-card { background: #fff; border-radius: 6px; padding: 16px 20px;
                 box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 16px; }
  .worker-card h3 { font-size: 14px; margin-bottom: 8px; display: flex;
                    align-items: center; gap: 8px; }
  .worker-meta { display: flex; gap: 20px; flex-wrap: wrap; font-size: 13px;
                 color: #636e72; margin-bottom: 10px; }
  .worker-meta span { display: inline-flex; align-items: center; gap: 4px; }
  .pending-table { margin-top: 8px; }
  .pending-table table { margin-bottom: 0; }
  .help { display: inline-flex; align-items: center; justify-content: center;
          width: 14px; height: 14px; line-height: 1; border-radius: 50%;
          background: #cdd6da; color: #485460; font-size: 10px; font-weight: 700;
          cursor: help; margin-left: 4px; font-style: normal; vertical-align: middle; }
  .stat-card .label .help { background: #e6ebee; }
</style>
</head>
<body>
<header>
  <div class="container">
    <h1><a href="#/" style="color:inherit;text-decoration:none">Dramatiq Streams</a></h1>
    <nav>
      <a href="#/">Overview</a>
      <a href="#/workers">Workers</a>
      <a href="#/delayed">Scheduled</a>
    </nav>
  </div>
</header>
<div class="container">
  <div class="controls">
    <label><input type="checkbox" id="autoRefresh" checked> Auto-refresh (5s)</label>
    <span id="lastUpdate" style="color:#636e72"></span>
  </div>
  <div id="app"></div>
</div>
<script>
(function() {
  var BASE = (document.currentScript && document.currentScript.dataset.base) || '';
  // Strip trailing slash from base for consistent joining
  if (BASE.endsWith('/')) BASE = BASE.slice(0, -1);

  // Detect base from page URL: everything before the hash
  if (!BASE) {
    var path = location.pathname;
    if (path.endsWith('/')) path = path.slice(0, -1);
    BASE = path;
  }

  var app = document.getElementById('app');
  var autoRefresh = document.getElementById('autoRefresh');
  var lastUpdate = document.getElementById('lastUpdate');
  var timer = null;
  // How many pending rows the user has expanded per worker, so an auto-refresh
  // restores that depth instead of collapsing back to the first page.
  var pendingExpanded = {};
  // Rows per "Load more" click — matches the first page so each click adds a
  // consistent chunk (the initial page is the server's pending_limit, also 20).
  var PENDING_PAGE = 20;

  function api(path, opts) {
    return fetch(BASE + path, opts || {}).then(function(r) { return r.json(); });
  }

  function esc(s) {
    var d = document.createElement('div');
    d.appendChild(document.createTextNode(s));
    return d.innerHTML;
  }

  // Escape for use inside a double-quoted HTML attribute. esc() leaves quotes
  // intact, so it is NOT safe for attribute values — use this for data-* attrs.
  function escAttr(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  // A small hoverable "?" carrying an explanatory tooltip.
  function help(tip) {
    return ' <span class="help" title="' + escAttr(tip) + '">?</span>';
  }

  function fmtTime(ts) {
    if (!ts) return '-';
    return new Date(ts).toLocaleString();
  }

  // Per-queue history of cumulative-processed samples, used to derive a
  // processing rate smoothed over the last minute. { queue: [{processed, t}] }
  var RATE_WINDOW_MS = 60000;
  var rateHistory = {};

  function computeRate(name, processed, now) {
    var hist = rateHistory[name] || (rateHistory[name] = []);
    hist.push({ processed: processed, t: now });
    // Keep roughly the last minute of samples (always keep at least 2).
    var cutoff = now - RATE_WINDOW_MS;
    while (hist.length > 2 && hist[0].t < cutoff) hist.shift();
    var first = hist[0];
    if (now <= first.t) return null;
    var dp = processed - first.processed;
    var dt = (now - first.t) / 1000;
    if (dp < 0 || dt <= 0) return null;  // counter reset (flush / new group)
    return dp / dt;
  }

  function fmtRate(r) {
    if (r === null) return '<span style="color:#b2bec3">—</span>';
    if (r === 0) return '0/s';
    if (r < 10) return r.toFixed(1) + '/s';
    return Math.round(r) + '/s';
  }

  // Backlog (consumer-group lag) is the count of messages enqueued but not yet
  // delivered to any worker. Redis reports it as null right after a flush.
  function fmtBacklog(n) {
    if (n === null || n === undefined) return '<span style="color:#b2bec3">—</span>';
    return n;
  }

  function renderOverview(data) {
    var now = Date.now();
    var totalRate = null;
    data.queues.forEach(function(q) {
      var rate = computeRate(q.name, q.processed, now);
      q._rate = rate;
      if (rate !== null) totalRate = (totalRate || 0) + rate;
    });

    var h = '<div class="stats">';
    var totalDlq = 0;
    var totalBacklog = null;
    data.queues.forEach(function(q) {
      totalDlq += q.dlq_length;
      if (q.lag !== null && q.lag !== undefined) totalBacklog = (totalBacklog || 0) + q.lag;
    });
    h += '<div class="stat-card"><div class="value">' + data.queues.length + '</div><div class="label">Queues</div></div>';
    h += '<div class="stat-card"><div class="value">' + fmtBacklog(totalBacklog) + '</div><div class="label">Waiting' + help('Messages enqueued but not yet picked up by any worker.') + '</div></div>';
    h += '<div class="stat-card"><div class="value">' + fmtRate(totalRate) + '</div><div class="label">Throughput' + help('Tasks completed per second, averaged over the last minute.') + '</div></div>';
    h += '<div class="stat-card"><div class="value">' + data.delayed_count + '</div><div class="label">Scheduled' + help('Messages waiting for a future time — delays and retry backoffs.') + '</div></div>';
    h += '<div class="stat-card"><div class="value">' + totalDlq + '</div><div class="label">Failed' + help('Messages that gave up after exhausting retries (in the dead-letter queue).') + '</div></div>';
    h += '</div>';
    if (!data.queues.length) {
      h += '<div class="empty">No queues found.</div>';
      return h;
    }
    h += '<table><tr>';
    h += '<th>Queue</th>';
    h += '<th>Total' + help('Messages in the queue right now (waiting + in progress).') + '</th>';
    h += '<th>Waiting' + help('Enqueued but not yet picked up by a worker.') + '</th>';
    h += '<th>In&nbsp;progress' + help('Picked up by a worker, not yet completed.') + '</th>';
    h += '<th>Rate' + help('Tasks completed per second (1-minute average).') + '</th>';
    h += '<th>Failed' + help('Messages in the dead-letter queue. Click the count to inspect.') + '</th>';
    h += '<th>Workers' + help('Worker processes currently consuming this queue.') + '</th>';
    h += '<th>Actions</th></tr>';
    data.queues.forEach(function(q) {
      h += '<tr>';
      h += '<td><a href="#/queue/' + encodeURIComponent(q.name) + '">' + esc(q.name) + '</a></td>';
      h += '<td>' + q.stream_length + '</td>';
      h += '<td>' + fmtBacklog(q.lag) + '</td>';
      h += '<td>' + q.pending + '</td>';
      h += '<td>' + fmtRate(q._rate) + '</td>';
      h += '<td>' + (q.dlq_length > 0 ? '<a href="#/queue/' + encodeURIComponent(q.name) + '/dlq"><span class="badge badge-red">' + q.dlq_length + '</span></a>' : '<span class="badge badge-gray">0</span>') + '</td>';
      h += '<td>' + q.consumers + '</td>';
      // Empty queues can be removed entirely; non-empty ones can be flushed.
      if (q.stream_length === 0 && q.dlq_length === 0) {
        h += '<td><button class="btn btn-danger" data-action="remove" data-queue="' + escAttr(q.name) + '">Remove</button></td>';
      } else {
        h += '<td><button class="btn btn-danger" data-action="flush" data-queue="' + escAttr(q.name) + '">Flush</button></td>';
      }
      h += '</tr>';
    });
    h += '</table>';
    return h;
  }

  function renderMessages(queue, msgs, isDlq) {
    var h = '<div class="breadcrumb"><a href="#/">Queues</a> &rsaquo; ';
    if (isDlq) {
      h += '<a href="#/queue/' + encodeURIComponent(queue) + '">' + esc(queue) + '</a> &rsaquo; Failed';
    } else {
      h += esc(queue);
    }
    h += '</div>';
    h += '<h2>' + esc(queue) + (isDlq ? ' — Failed messages' : ' — Messages') + '</h2>';
    if (isDlq) {
      h += '<p style="font-size:13px;color:#636e72;margin:-4px 0 12px">Tasks that exhausted their retries. Requeue to try again, or delete to discard.</p>';
    }
    if (isDlq) {
      h += '<div style="margin-bottom:12px">';
      h += '<button class="btn btn-primary" data-action="requeueAll" data-queue="' + escAttr(queue) + '">Requeue All</button>';
      h += '<button class="btn btn-danger" data-action="purge" data-queue="' + escAttr(queue) + '">Purge All</button>';
      h += '</div>';
    }
    if (!msgs.length) {
      h += '<div class="empty">No messages.</div>';
      return h;
    }
    h += '<table><tr>';
    h += '<th>ID' + help('Redis stream entry ID for this message.') + '</th>';
    h += '<th>Task' + help('The dramatiq actor (task function) to run.') + '</th>';
    h += '<th>Args</th><th>Kwargs</th>';
    h += '<th>Enqueued' + help('When the message was created.') + '</th>';
    if (isDlq) h += '<th>Error' + help('Why it failed — the last line is shown; hover for the full traceback. The count is how many times it was retried before giving up.') + '</th>';
    if (isDlq) h += '<th>Actions</th>';
    h += '</tr>';
    msgs.forEach(function(m) {
      h += '<tr>';
      h += '<td class="mono">' + esc(m.id) + '</td>';
      h += '<td>' + esc(m.actor) + '</td>';
      h += '<td class="mono">' + esc(JSON.stringify(m.args)) + '</td>';
      h += '<td class="mono">' + esc(JSON.stringify(m.kwargs)) + '</td>';
      h += '<td>' + fmtTime(m.timestamp) + '</td>';
      if (isDlq) {
        var err = (m.error || '').replace(/\\s+$/, '');
        var lines = err ? err.split('\\n') : [];
        var summary = lines.length ? lines[lines.length - 1] : '';
        var rc = (m.retries !== null && m.retries !== undefined)
          ? ' <span style="color:#636e72">(retried ' + m.retries + '&times;)</span>' : '';
        h += '<td class="mono" style="max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"' +
             (err ? ' title="' + escAttr(err) + '"' : '') + '>' +
             (summary ? '<span style="color:#d63031">' + esc(summary) + '</span>'
                      : '<span style="color:#b2bec3">&mdash;</span>') + rc + '</td>';
        h += '<td>';
        h += '<button class="btn btn-primary" data-action="requeue" data-queue="' + escAttr(queue) + '" data-id="' + escAttr(m.id) + '">Requeue</button>';
        h += '<button class="btn btn-danger" data-action="delete" data-queue="' + escAttr(queue) + '" data-id="' + escAttr(m.id) + '">Delete</button>';
        h += '</td>';
      }
      h += '</tr>';
    });
    h += '</table>';
    return h;
  }

  function renderDelayed(msgs) {
    var h = '<div class="breadcrumb"><a href="#/">Overview</a> &rsaquo; Scheduled</div>';
    h += '<h2>Scheduled messages</h2>';
    h += '<p style="font-size:13px;color:#636e72;margin:-4px 0 12px">Messages waiting for a future time — explicit delays and retry backoffs.</p>';
    if (!msgs.length) {
      h += '<div class="empty">No scheduled messages.</div>';
      return h;
    }
    h += '<table><tr><th>Task</th><th>Queue</th><th>Args</th>';
    h += '<th>Runs at' + help('When this message becomes available to run.') + '</th></tr>';
    msgs.forEach(function(m) {
      h += '<tr>';
      h += '<td>' + esc(m.actor) + '</td>';
      h += '<td>' + esc(m.queue) + '</td>';
      h += '<td class="mono">' + esc(JSON.stringify(m.args)) + '</td>';
      h += '<td>' + fmtTime(m.eta_ms) + '</td>';
      h += '</tr>';
    });
    h += '</table>';
    return h;
  }

  function fmtIdle(ms) {
    if (ms < 1000) return ms + 'ms';
    var s = Math.floor(ms / 1000);
    if (s < 60) return s + 's';
    var m = Math.floor(s / 60);
    if (m < 60) return m + 'm ' + (s % 60) + 's';
    var h = Math.floor(m / 60);
    return h + 'h ' + (m % 60) + 'm';
  }

  // Messages abandoned at shutdown are stamped with a ~31-year idle so the next
  // worker reclaims them; show that, not an absurd duration.
  function fmtHeld(ms) {
    if (ms >= 1e11) {
      return '<span style="color:#636e72" title="Abandoned at shutdown — awaiting reclaim by another worker">abandoned</span>';
    }
    return fmtIdle(ms);
  }

  function statusBadge(status) {
    var cls = status === 'active' ? 'badge-green' : status === 'idle' ? 'badge-orange' : 'badge-gray';
    var tip = status === 'active' ? 'Checked in within the last minute.'
            : status === 'idle' ? 'Quiet for 1–5 minutes.'
            : 'No check-in for over 5 minutes — may have stopped.';
    return '<span class="badge ' + cls + '" title="' + escAttr(tip) + '">' + status + '</span>';
  }

  function renderWorkerCard(w) {
    var h = '<div class="worker-card" data-worker="' + escAttr(w.name) + '">';
    h += '<h3><span class="mono">' + esc(w.name) + '</span> ' + statusBadge(w.status) + '</h3>';
    h += '<div class="worker-meta">';
    h += '<span>Last seen: <strong>' + fmtIdle(w.idle_ms) + ' ago</strong>' + help('Time since this worker last contacted Redis (a liveness heartbeat). Stays low while the worker is alive, even when it is busy or has a large backlog.') + '</span>';
    h += '<span>In progress: <strong>' + w.total_pending + '</strong>' + help('Tasks this worker has picked up but not yet completed.') + '</span>';
    h += '<span>Queues: ' + w.queues.map(function(q) {
      var det = w.queue_details[q];
      var pending = det ? det.pending : 0;
      var label = esc(q);
      if (pending > 0) label += '&nbsp;<span class="badge badge-blue" title="' + escAttr(pending + ' task(s) in progress on ' + q) + '">' + pending + '</span>';
      return '<a href="#/queue/' + encodeURIComponent(q) + '">' + label + '</a>';
    }).join(', ') + '</span>';
    h += '</div>';
    if (w.pending_messages.length) {
      h += '<div class="pending-table">';
      h += '<div style="font-size:12px;color:#636e72;margin:8px 0 4px">Tasks in progress on this worker:</div>';
      h += '<table><thead><tr>';
      h += '<th>ID' + help('Redis stream entry ID for this message.') + '</th>';
      h += '<th>Queue</th><th>Task</th>';
      h += '<th>Running for' + help('Time since this task was delivered to the worker — roughly how long it has been running.') + '</th>';
      h += '<th>Attempts' + help('How many times this task has been delivered to a worker. More than 1 means it was retried after a worker failed to finish it in time — look for slow tasks or crashes.') + '</th></tr></thead><tbody>';
      w.pending_messages.forEach(function(pm) { h += pendingRow(pm); });
      h += '</tbody></table>';
      if (w.total_pending > w.pending_messages.length) {
        h += '<div class="pending-more">';
        if (w.pending_cursor) {
          h += '<button class="btn btn-light" data-action="loadPending" data-worker="' +
               escAttr(w.name) + '" data-cursor="' + escAttr(w.pending_cursor) +
               '" data-total="' + w.total_pending + '">Load more</button>';
        }
        h += '<span class="pending-count">Showing ' + w.pending_messages.length +
             ' of ' + w.total_pending + ' in-progress tasks.</span>';
        h += '</div>';
      }
      h += '</div>';
    }
    h += '</div>';
    return h;
  }

  // One row of a worker's pending-task table. Shared by the initial render and
  // by the "Load more" append, so both stay identical.
  function pendingRow(pm) {
    return '<tr>' +
      '<td class="mono">' + esc(pm.id) + '</td>' +
      '<td>' + esc(pm.queue) + '</td>' +
      '<td>' + esc(pm.actor) + '</td>' +
      '<td>' + fmtHeld(pm.idle_ms) + '</td>' +
      '<td>' + (pm.deliveries > 1 ? '<strong style="color:#e17055">' + pm.deliveries + '</strong>' : pm.deliveries) + '</td>' +
      '</tr>';
  }

  function fetchPending(name, cursor, count) {
    var qs = '?after=' + encodeURIComponent(cursor) + (count ? '&count=' + count : '');
    return api('/api/workers/' + encodeURIComponent(name) + '/pending' + qs);
  }

  // Find a worker's pending-table wrapper by name without building a CSS
  // selector from the (untrusted) name.
  function findPendingWrap(name) {
    var cards = app.querySelectorAll('.worker-card');
    for (var i = 0; i < cards.length; i++) {
      if (cards[i].dataset.worker === name) return cards[i].querySelector('.pending-table');
    }
    return null;
  }

  // Append one fetched page into a worker's table; advance or retire the button
  // and refresh the "Showing N of M" counter.
  function appendPendingPage(wrap, data) {
    var tbody = wrap.querySelector('tbody');
    var btn = wrap.querySelector('button[data-action="loadPending"]');
    if (tbody && data.messages) {
      data.messages.forEach(function(pm) { tbody.insertAdjacentHTML('beforeend', pendingRow(pm)); });
    }
    if (btn) {
      if (data.next_cursor) { btn.dataset.cursor = data.next_cursor; btn.disabled = false; }
      else { btn.remove(); }  // reached the end
      var countEl = wrap.querySelector('.pending-count');
      if (countEl && tbody) {
        countEl.textContent = 'Showing ' + tbody.querySelectorAll('tr').length +
          ' of ' + btn.dataset.total + ' in-progress tasks.';
      }
    }
  }

  // Manual "Load more": one page, and remember the new depth so an auto-refresh
  // re-expands to here instead of collapsing.
  function loadMorePending(btn) {
    var wrap = btn.closest('.pending-table'), name = btn.dataset.worker;
    if (!wrap || !btn.dataset.cursor) return;
    btn.disabled = true;
    fetchPending(name, btn.dataset.cursor, PENDING_PAGE).then(function(data) {
      appendPendingPage(wrap, data);
      pendingExpanded[name] = wrap.querySelectorAll('tbody tr').length;
    }).catch(function() { btn.disabled = false; });
  }

  // Restore a previously-expanded depth after a full re-render, paging until the
  // table holds `target` rows again (or the worker has no more pending tasks).
  function fillPendingTo(wrap, name, target) {
    var tbody = wrap.querySelector('tbody');
    var btn = wrap.querySelector('button[data-action="loadPending"]');
    if (!tbody || !btn || !btn.dataset.cursor) return;
    var before = tbody.querySelectorAll('tr').length;
    if (before >= target) return;
    btn.disabled = true;
    fetchPending(name, btn.dataset.cursor, Math.min(target - before, 200)).then(function(data) {
      appendPendingPage(wrap, data);
      // Continue only while making progress and a cursor remains (no infinite loop).
      if (tbody.querySelectorAll('tr').length > before &&
          wrap.querySelector('button[data-action="loadPending"]')) {
        fillPendingTo(wrap, name, target);
      }
    }).catch(function() {});
  }

  // After (re)rendering the workers view, re-open whatever the user had expanded.
  function applyPendingExpansions() {
    Object.keys(pendingExpanded).forEach(function(name) {
      var wrap = findPendingWrap(name);
      if (wrap) fillPendingTo(wrap, name, pendingExpanded[name]);
    });
  }

  function renderWorkers(workers) {
    var h = '<div class="breadcrumb"><a href="#/">Overview</a> &rsaquo; Workers</div>';
    h += '<h2>Workers</h2>';
    if (!workers.length) {
      h += '<div class="empty">No workers found.</div>';
      return h;
    }

    // Active workers first, sorted by name; everyone else sorted by idle ascending.
    var active = workers.filter(function(w) { return w.status === 'active'; })
                        .sort(function(a, b) { return a.name < b.name ? -1 : a.name > b.name ? 1 : 0; });
    var others = workers.filter(function(w) { return w.status !== 'active'; })
                        .sort(function(a, b) { return a.idle_ms - b.idle_ms; });

    h += '<h3 style="margin:8px 0 12px">Active <span class="badge badge-green">' + active.length + '</span>' +
         help('Workers that checked in within the last minute.') + '</h3>';
    if (active.length) {
      active.forEach(function(w) { h += renderWorkerCard(w); });
    } else {
      h += '<div class="empty">No active workers.</div>';
    }

    if (others.length) {
      h += '<h3 style="margin:20px 0 12px">Inactive <span class="badge badge-gray">' + others.length + '</span>' +
           help('Workers quiet for over a minute — idle, or possibly stopped.') + '</h3>';
      others.forEach(function(w) { h += renderWorkerCard(w); });
    }
    return h;
  }

  function route() {
    var hash = location.hash || '#/';
    var m;
    if (m = hash.match(/^#\\/queue\\/([^/]+)\\/dlq$/)) {
      var q = decodeURIComponent(m[1]);
      api('/api/queues/' + encodeURIComponent(q) + '/dlq').then(function(data) {
        app.innerHTML = renderMessages(q, data, true);
      });
    } else if (m = hash.match(/^#\\/queue\\/([^/]+)$/)) {
      var q = decodeURIComponent(m[1]);
      api('/api/queues/' + encodeURIComponent(q) + '/messages').then(function(data) {
        app.innerHTML = renderMessages(q, data, false);
      });
    } else if (hash === '#/workers') {
      api('/api/workers').then(function(data) {
        app.innerHTML = renderWorkers(data);
        applyPendingExpansions();
      });
    } else if (hash === '#/delayed') {
      api('/api/delayed').then(function(data) {
        app.innerHTML = renderDelayed(data);
      });
    } else {
      api('/api/overview').then(function(data) {
        app.innerHTML = renderOverview(data);
      });
    }
    lastUpdate.textContent = 'Updated: ' + new Date().toLocaleTimeString();
  }

  window.flushQueue = function(name) {
    if (!confirm('Flush all messages from "' + name + '"?')) return;
    api('/api/queues/' + encodeURIComponent(name) + '/flush', {method:'POST'}).then(route);
  };
  window.removeQueue = function(name) {
    if (!confirm('Remove empty queue "' + name + '" from the dashboard?')) return;
    api('/api/queues/' + encodeURIComponent(name) + '/remove', {method:'POST'}).then(route);
  };
  window.purgeDlq = function(name) {
    if (!confirm('Purge all DLQ messages for "' + name + '"?')) return;
    api('/api/queues/' + encodeURIComponent(name) + '/dlq/purge', {method:'POST'}).then(route);
  };
  window.requeueAllDlq = function(name) {
    if (!confirm('Requeue all DLQ messages for "' + name + '" back to the main queue?')) return;
    api('/api/queues/' + encodeURIComponent(name) + '/dlq/requeue-all', {method:'POST'}).then(route);
  };
  window.requeueMsg = function(queue, id) {
    api('/api/queues/' + encodeURIComponent(queue) + '/dlq/' + encodeURIComponent(id) + '/requeue', {method:'POST'}).then(route);
  };
  window.deleteMsg = function(queue, id) {
    api('/api/queues/' + encodeURIComponent(queue) + '/dlq/' + encodeURIComponent(id) + '/delete', {method:'POST'}).then(route);
  };

  // Delegated handler: action buttons carry data-* attributes instead of inline
  // onclick, so queue names/ids never enter a JS-string context (no injection).
  app.addEventListener('click', function(e) {
    var btn = e.target.closest && e.target.closest('button[data-action]');
    if (!btn) return;
    var q = btn.dataset.queue, id = btn.dataset.id;
    switch (btn.dataset.action) {
      case 'flush': flushQueue(q); break;
      case 'remove': removeQueue(q); break;
      case 'purge': purgeDlq(q); break;
      case 'requeueAll': requeueAllDlq(q); break;
      case 'requeue': requeueMsg(q, id); break;
      case 'delete': deleteMsg(q, id); break;
      case 'loadPending': loadMorePending(btn); break;
    }
  });

  function scheduleRefresh() {
    clearTimeout(timer);
    if (autoRefresh.checked) {
      timer = setTimeout(function() { route(); scheduleRefresh(); }, 5000);
    }
  }
  autoRefresh.addEventListener('change', scheduleRefresh);

  window.addEventListener('hashchange', function() { route(); scheduleRefresh(); });
  route();
  scheduleRefresh();
})();
</script>
</body>
</html>
"""
