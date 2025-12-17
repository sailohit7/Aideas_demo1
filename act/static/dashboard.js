async function runMode(mode) {
  document.getElementById('statusText').innerText = 'Running ' + mode + '...';
  await fetch(`/run/${mode}`);
}

async function refreshLogs() {
  const res = await fetch('/logs');
  const data = await res.json();
  const logDiv = document.getElementById('logbox');
  logDiv.innerHTML = data.logs.join('<br>');
  logDiv.scrollTop = logDiv.scrollHeight;
}

setInterval(refreshLogs, 1000);
