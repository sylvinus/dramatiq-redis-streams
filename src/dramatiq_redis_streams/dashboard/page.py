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
  .btn:hover { opacity: .85; }
  .btn + .btn { margin-left: 6px; }
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
</style>
</head>
<body>
<header>
  <div class="container">
    <h1>Dramatiq Streams</h1>
    <nav>
      <a href="#/">Overview</a>
      <a href="#/workers">Workers</a>
      <a href="#/delayed">Delayed</a>
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

  function api(path, opts) {
    return fetch(BASE + path, opts || {}).then(function(r) { return r.json(); });
  }

  function esc(s) {
    var d = document.createElement('div');
    d.appendChild(document.createTextNode(s));
    return d.innerHTML;
  }

  function fmtTime(ts) {
    if (!ts) return '-';
    return new Date(ts).toLocaleString();
  }

  function renderOverview(data) {
    var h = '<div class="stats">';
    var totalDlq = 0;
    data.queues.forEach(function(q) { totalDlq += q.dlq_length; });
    h += '<div class="stat-card"><div class="value">' + data.queues.length + '</div><div class="label">Queues</div></div>';
    h += '<div class="stat-card"><div class="value">' + data.delayed_count + '</div><div class="label">Delayed</div></div>';
    h += '<div class="stat-card"><div class="value">' + totalDlq + '</div><div class="label">Dead Letters</div></div>';
    h += '</div>';
    if (!data.queues.length) {
      h += '<div class="empty">No queues found.</div>';
      return h;
    }
    h += '<table><tr><th>Queue</th><th>Stream Length</th><th>Consumers</th><th>Pending</th><th>DLQ</th><th>Actions</th></tr>';
    data.queues.forEach(function(q) {
      h += '<tr>';
      h += '<td><a href="#/queue/' + encodeURIComponent(q.name) + '">' + esc(q.name) + '</a></td>';
      h += '<td>' + q.stream_length + '</td>';
      h += '<td>' + q.consumers + '</td>';
      h += '<td>' + q.pending + '</td>';
      h += '<td>' + (q.dlq_length > 0 ? '<a href="#/queue/' + encodeURIComponent(q.name) + '/dlq"><span class="badge badge-red">' + q.dlq_length + '</span></a>' : '<span class="badge badge-gray">0</span>') + '</td>';
      h += '<td><button class="btn btn-danger" onclick="flushQueue(\\'' + esc(q.name) + '\\')">Flush</button></td>';
      h += '</tr>';
    });
    h += '</table>';
    return h;
  }

  function renderMessages(queue, msgs, isDlq) {
    var h = '<div class="breadcrumb"><a href="#/">Queues</a> &rsaquo; ';
    if (isDlq) {
      h += '<a href="#/queue/' + encodeURIComponent(queue) + '">' + esc(queue) + '</a> &rsaquo; DLQ';
    } else {
      h += esc(queue);
    }
    h += '</div>';
    h += '<h2>' + esc(queue) + (isDlq ? ' — Dead Letter Queue' : ' — Messages') + '</h2>';
    if (isDlq) {
      h += '<div style="margin-bottom:12px"><button class="btn btn-danger" onclick="purgeDlq(\\'' + esc(queue) + '\\')">Purge All</button></div>';
    }
    if (!msgs.length) {
      h += '<div class="empty">No messages.</div>';
      return h;
    }
    h += '<table><tr><th>ID</th><th>Actor</th><th>Args</th><th>Kwargs</th><th>Timestamp</th>';
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
        h += '<td>';
        h += '<button class="btn btn-primary" onclick="requeueMsg(\\'' + esc(queue) + '\\',\\'' + esc(m.id) + '\\')">Requeue</button>';
        h += '<button class="btn btn-danger" onclick="deleteMsg(\\'' + esc(queue) + '\\',\\'' + esc(m.id) + '\\')">Delete</button>';
        h += '</td>';
      }
      h += '</tr>';
    });
    h += '</table>';
    return h;
  }

  function renderDelayed(msgs) {
    var h = '<div class="breadcrumb"><a href="#/">Overview</a> &rsaquo; Delayed Messages</div>';
    h += '<h2>Delayed Messages</h2>';
    if (!msgs.length) {
      h += '<div class="empty">No delayed messages.</div>';
      return h;
    }
    h += '<table><tr><th>Actor</th><th>Queue</th><th>Args</th><th>ETA</th></tr>';
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

  function statusBadge(status) {
    var cls = status === 'active' ? 'badge-green' : status === 'idle' ? 'badge-orange' : 'badge-gray';
    return '<span class="badge ' + cls + '">' + status + '</span>';
  }

  function renderWorkers(workers) {
    var h = '<div class="breadcrumb"><a href="#/">Overview</a> &rsaquo; Workers</div>';
    h += '<h2>Workers</h2>';
    if (!workers.length) {
      h += '<div class="empty">No active workers found.</div>';
      return h;
    }
    workers.forEach(function(w) {
      h += '<div class="worker-card">';
      h += '<h3><span class="mono">' + esc(w.name) + '</span> ' + statusBadge(w.status) + '</h3>';
      h += '<div class="worker-meta">';
      h += '<span>Idle: <strong>' + fmtIdle(w.idle_ms) + '</strong></span>';
      h += '<span>Pending: <strong>' + w.total_pending + '</strong></span>';
      h += '<span>Queues: ' + w.queues.map(function(q) {
        var det = w.queue_details[q];
        var pending = det ? det.pending : 0;
        var label = esc(q);
        if (pending > 0) label += '&nbsp;<span class="badge badge-blue">' + pending + '</span>';
        return '<a href="#/queue/' + encodeURIComponent(q) + '">' + label + '</a>';
      }).join(', ') + '</span>';
      h += '</div>';
      if (w.pending_messages.length) {
        h += '<div class="pending-table">';
        h += '<table><tr><th>Stream ID</th><th>Queue</th><th>Actor</th><th>Idle</th><th>Deliveries</th></tr>';
        w.pending_messages.forEach(function(pm) {
          h += '<tr>';
          h += '<td class="mono">' + esc(pm.id) + '</td>';
          h += '<td>' + esc(pm.queue) + '</td>';
          h += '<td>' + esc(pm.actor) + '</td>';
          h += '<td>' + fmtIdle(pm.idle_ms) + '</td>';
          h += '<td>' + pm.deliveries + '</td>';
          h += '</tr>';
        });
        h += '</table></div>';
      }
      h += '</div>';
    });
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
  window.purgeDlq = function(name) {
    if (!confirm('Purge all DLQ messages for "' + name + '"?')) return;
    api('/api/queues/' + encodeURIComponent(name) + '/dlq/purge', {method:'POST'}).then(route);
  };
  window.requeueMsg = function(queue, id) {
    api('/api/queues/' + encodeURIComponent(queue) + '/dlq/' + encodeURIComponent(id) + '/requeue', {method:'POST'}).then(route);
  };
  window.deleteMsg = function(queue, id) {
    api('/api/queues/' + encodeURIComponent(queue) + '/dlq/' + encodeURIComponent(id) + '/delete', {method:'POST'}).then(route);
  };

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
