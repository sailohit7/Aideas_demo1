// scheduler.js
// Handles job create / list / start / stop / delete + live logs

const $ = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));

async function api(path, opts = {}) {
  const res = await fetch(path, Object.assign({headers: {'Content-Type': 'application/json'}}, opts));
  return res.json();
}

// UI references
const jobName = $('#job-name');
const jobDb = $('#job-db');
const createBtn = $('#create-job-btn');
const jobsList = $('#jobs-list');
const logsBox = $('#live-logs');
const clearLogsBtn = $('#clear-logs');
const refreshDbs = $('#refresh-dbs');

function appendLog(line) {
  const p = document.createElement('div');
  p.textContent = line;
  logsBox.appendChild(p);
  logsBox.scrollTop = logsBox.scrollHeight;
}

clearLogsBtn?.addEventListener('click', () => logsBox.innerHTML = '');

// read radio
function getSelectedType() {
  const r = document.querySelector('input[name="job-type"]:checked');
  return r ? r.value : 'interval';
}

async function refreshDatabases() {
  try {
    const res = await api('/get_databases');
    if (res.databases) {
      jobDb.innerHTML = '<option value="">(Default connection)</option>';
      res.databases.forEach(d => {
        const o = document.createElement('option'); o.value = d; o.textContent = d;
        jobDb.appendChild(o);
      });
    }
  } catch (e) {
    appendLog('⚠ Could not refresh databases: ' + e);
  }
}

refreshDbs?.addEventListener('click', async () => { await refreshDatabases(); appendLog('Databases refreshed'); });

// Create job
createBtn?.addEventListener('click', async e => {
  e.preventDefault();
  const payload = {
    name: jobName.value || undefined,
    db: jobDb.value || undefined,
    type: getSelectedType(),
    interval: document.getElementById('job-interval').value,
    time: document.getElementById('job-time').value,
    day: document.getElementById('job-day').value,
    date: document.getElementById('job-date').value,
    auto_start: document.getElementById('job-autostart').checked
  };
  const res = await api('/jobs/create', { method: 'POST', body: JSON.stringify(payload) });
  appendLog('➕ Job created: ' + (res.job?.name || 'unknown'));
  jobName.value = '';
  loadJobs();
});

// List jobs
async function loadJobs() {
  jobsList.innerHTML = '<div class="muted">Loading jobs…</div>';
  const res = await api('/jobs');
  const list = res.jobs || [];
  jobsList.innerHTML = '';
  if (!list.length) {
    jobsList.innerHTML = '<div class="muted">No jobs yet. Create one above.</div>';
    return;
  }
  list.forEach(job => {
    const card = document.createElement('div');
    card.className = 'job-card';
    card.innerHTML = `
      <div class="job-row">
        <div>
          <div style="font-weight:700; color: var(--warn,#ffd400);">${escapeHtml(job.name)}</div>
          <div class="muted">${escapeHtml(job.type)} • DB: ${escapeHtml(job.db || '(default)')}</div>
          <div class="muted" style="margin-top:8px">Next run: <strong>${job.next_run || '—'}</strong></div>
        </div>
        <div style="text-align:right">
          <div id="status-${job.id}" class="status-badge ${job.status === 'running' ? 'status-running' : 'status-idle'}">${job.status}</div>
          <div style="margin-top:12px" class="job-actions">
            <button class="btn btn-outline" data-action="edit" data-id="${job.id}">Edit</button>
            <button class="btn btn-primary" data-action="start" data-id="${job.id}">Start</button>
            <button class="btn btn-outline" data-action="stop" data-id="${job.id}">Stop</button>
            <button class="btn btn-outline" data-action="delete" data-id="${job.id}">Delete</button>
          </div>
        </div>
      </div>
    `;
    jobsList.appendChild(card);
  });
  // attach listeners
  $$('.job-actions button, .job-actions .btn').forEach(btn => {
    btn.addEventListener('click', async (ev) => {
      const id = ev.currentTarget.dataset.id;
      const action = ev.currentTarget.dataset.action;
      if (action === 'start') {
        await api(`/jobs/${id}/start`, { method: 'POST' });
        appendLog(`▶ Job start requested: ${id}`);
      } else if (action === 'stop') {
        await api(`/jobs/${id}/stop`, { method: 'POST' });
        appendLog(`⏹ Job stop requested: ${id}`);
      } else if (action === 'delete') {
        if (!confirm('Delete this job?')) return;
        await api(`/jobs/${id}/delete`, { method: 'POST' });
        appendLog(`🗑 Job deleted: ${id}`);
      } else if (action === 'edit') {
        return openEditDialog(id);
      }
      setTimeout(loadJobs, 800);
    });
  });
}

// simple edit prompt (lightweight)
async function openEditDialog(jobId) {
  const res = await api('/jobs');
  const job = (res.jobs || []).find(j => j.id === jobId);
  if (!job) { alert('Job not found'); return; }
  const newName = prompt('Job name', job.name) || job.name;
  const newInterval = prompt('Interval minutes (for interval type)', job.interval || 15) || job.interval;
  // minimal: only editing name + interval/time/day/date via prompts to keep UI simple
  const payload = { name: newName, interval: newInterval };
  await api(`/jobs/${jobId}/update`, { method: 'POST', body: JSON.stringify(payload) });
  appendLog(`✏️ Job updated: ${newName}`);
  loadJobs();
}

// helpers
function escapeHtml(s) {
  if (!s && s !== 0) return '';
  return String(s).replace(/[&<>"'`]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','`':'&#x60;'}[c]));
}

// live logs poller (fetches /get_logs)
async function pollLogs() {
  try {
    const res = await api('/get_logs');
    if (res.logs) {
      logsBox.innerHTML = '';
      res.logs.slice(-150).forEach(l => appendLog(l));
      logsBox.scrollTop = logsBox.scrollHeight;
    }
  } catch (e) {
    appendLog('⚠ Could not fetch logs: ' + e);
  }
}

// periodic refresh
setInterval(() => { loadJobs(); pollLogs(); }, 6000);

// initial load
refreshDatabases();
loadJobs();
pollLogs();
