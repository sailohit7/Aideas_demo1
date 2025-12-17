#!/usr/bin/env python3
"""
Flask web app for Tally → SQL Sync Portal
- One-click login for now (session-based)
- Dashboard, Scheduler, Jobs, Downloads, Logs
- Integrates with main_sync.py backend

Adds multi-job scheduler:
- create / edit / delete jobs
- start / stop jobs independently
- jobs saved in-memory (jobs list)

Also adds Connections checks:
- /connections page
- /api/check_sql
- /api/check_tally
"""

from flask import Flask, render_template, jsonify, request, redirect, url_for, session, flash
import threading
import logging
import pyodbc
import main_sync  # backend sync implementation
from datetime import datetime, timedelta
import time
from functools import wraps
import os
import uuid
import requests  # used for Tally HTTP probe

# ---------------- FLASK APP CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
app.secret_key = os.environ.get("SYNC_APP_SECRET") or "dev-secret-change-me"

# ---------------- GLOBAL STATE ----------------
log_text = []
selected_masters = []
download_history = []
# JOBS: list of dicts {id, name, db, type, interval, time, day, date, status}
jobs = []
# job threads state: job_id -> {"thread": Thread, "stop": Event}
job_threads = {}

# Tally probe URL (fallback); can be overridden by env var TALLY_URL
TALLY_URL = os.environ.get("TALLY_URL", "http://localhost:9000")

# ---------------- HELPERS ----------------
def add_log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full = f"[{ts}] {msg}"
    print(full)
    log_text.append(full)
    if len(log_text) > 2000:
        log_text.pop(0)

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get("user"):
            return fn(*args, **kwargs)
        flash("Please login to continue.", "warning")
        return redirect(url_for("login"))
    return wrapper

# ---------------- AUTH ROUTES ----------------
@app.route("/", methods=["GET"])
def login():
    if session.get("user"):
        return redirect(url_for("dashboard"))
    return render_template("login.html", title="Login")

@app.route("/do_login", methods=["POST"])
def do_login():
    username = "admin"
    session["user"] = username
    session.permanent = True
    add_log(f"🔐 User '{username}' logged in (auto-login).")
    return redirect(url_for("dashboard"))

@app.route("/logout")
def logout():
    user = session.pop("user", None)
    add_log(f"🔓 User '{user}' logged out." if user else "🔓 Logout called")
    flash("Logged out.", "info")
    return redirect(url_for("login"))

# ---------------- PAGES ----------------
@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", title="Dashboard")

@app.route("/interactive")
@login_required
def interactive():
    return render_template("interactive.html", title="Interactive Run")

@app.route("/runonce")
@login_required
def runonce():
    return render_template("runonce.html", title="Run Once")

# Scheduler page now shows jobs and the scheduler form (same template)
@app.route("/scheduler")
@login_required
def scheduler():
    """Render scheduler page — includes jobs list for client JS."""
    try:
        conn = main_sync.connect_sql_default()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
        databases = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        add_log(f"⚠ Error fetching databases for scheduler: {e}")
        databases = []
    masters = list(getattr(main_sync, "MASTERS", {}).keys())
    return render_template("scheduler.html", title="Scheduler", databases=databases, masters=masters)

@app.route("/downloads")
@login_required
def downloads():
    return render_template("downloads.html", title="Downloads")

@app.route("/logs")
@login_required
def logs_page():
    return render_template("logs.html", title="Logs")

@app.route("/history")
@login_required
def history():
    return render_template("history.html", title="History")

@app.route("/masters")
@login_required
def masters():
    return render_template("masters.html", title="Masters")

# ---------------- CONNECTIONS PAGE + API ----------------
@app.route("/connections")
@login_required
def connections_page():
    """UI page for connection checks."""
    return render_template("connections.html", title="Connections")

@app.route("/api/check_sql")
@login_required
def api_check_sql():
    """Check MS SQL Server connectivity using main_sync.connect_sql_default()."""
    try:
        conn = None
        try:
            conn = main_sync.connect_sql_default()
        except Exception as e:
            # If main_sync doesn't provide it or it fails, raise to be handled below
            raise RuntimeError(f"main_sync.connect_sql_default() error: {e}")

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        conn.close()
        ok = (row is not None)
        msg = "SQL OK (SELECT 1 returned)" if ok else "SQL responded but unexpected result"
        add_log(f"🧪 SQL check: {msg}")
        return jsonify({"ok": ok, "msg": msg})
    except Exception as e:
        add_log(f"⚠ SQL check failed: {e}")
        return jsonify({"ok": False, "msg": str(e)}), 500

@app.route("/api/check_tally")
@login_required
def api_check_tally():
    """
    Check Tally connection.
    First call main_sync.check_tally_connection() if present, otherwise do an HTTP GET to TALLY_URL.
    """
    try:
        # Try to use backend helper if present
        if hasattr(main_sync, "check_tally_connection"):
            try:
                # expected to return (bool, "message") or boolean / raise on error
                res = main_sync.check_tally_connection()
                # normalize
                if isinstance(res, tuple) and len(res) >= 2:
                    ok, details = bool(res[0]), str(res[1])
                elif isinstance(res, bool):
                    ok, details = res, "Tally reported status"
                else:
                    # unknown return, coerce to string
                    ok, details = True, str(res)
                add_log(f"🧪 Tally check (via main_sync): {details}")
                return jsonify({"ok": ok, "msg": details})
            except Exception as e:
                add_log(f"⚠ main_sync.check_tally_connection() raised: {e}; falling back to HTTP probe.")

        # Fallback: HTTP GET to TALLY_URL
        try:
            resp = requests.get(TALLY_URL, timeout=4)
            if resp.status_code < 400:
                msg = f"Tally HTTP OK ({resp.status_code}) at {TALLY_URL}"
                add_log(f"🧪 Tally check: {msg}")
                return jsonify({"ok": True, "msg": msg})
            else:
                msg = f"Tally HTTP returned {resp.status_code}"
                add_log(f"⚠ Tally check failed: {msg}")
                return jsonify({"ok": False, "msg": msg}), 502
        except Exception as e:
            add_log(f"⚠ Tally HTTP probe error: {e}")
            return jsonify({"ok": False, "msg": str(e)}), 500

    except Exception as e:
        add_log(f"⚠ Tally check unexpected error: {e}")
        return jsonify({"ok": False, "msg": str(e)}), 500

# ---------------- BASIC SYNC ENDPOINTS ----------------
@app.route("/save_masters", methods=["POST"])
@login_required
def save_masters():
    global selected_masters
    data = request.get_json()
    selected_masters = data.get("masters", [])
    add_log(f"💾 Saved master selection: {selected_masters}")
    return jsonify({"message": f"Saved {len(selected_masters)} masters selected."})

@app.route("/run/<mode>")
@login_required
def run_mode(mode):
    db_name = request.args.get("db")
    add_log(f"▶ Starting mode: {mode} (DB: {db_name or 'Default'})")
    def run_task():
        try:
            if mode == "interactive":
                main_sync.run_interactive(db_name=db_name)
            elif mode == "runonce":
                main_sync.run_once_all(db_name=db_name)
            elif mode == "scheduler":
                main_sync.run_once_all(db_name=db_name)
            elif mode == "selected":
                if not selected_masters:
                    add_log("⚠ No masters selected for sync.")
                    return
                add_log(f"🔄 Running selected masters: {selected_masters}")
                main_sync.run_selected(selected_masters, db_name=db_name)
        except Exception as e:
            add_log(f"⚠ Error during {mode}: {e}")
        finally:
            add_log(f"✅ Finished mode: {mode}")
    threading.Thread(target=run_task, daemon=True).start()
    return jsonify({"status": "started", "mode": mode, "database": db_name})

@app.route("/get_logs")
@login_required
def get_logs():
    return jsonify({"logs": log_text})

# ---------------- DATABASE OPS ----------------
def get_sql_connection():
    return main_sync.connect_sql_default()

@app.route("/get_databases")
@login_required
def get_databases():
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
        dbs = [row[0] for row in cursor.fetchall()]
        conn.close()
        return jsonify({"databases": dbs})
    except Exception as e:
        add_log(f"⚠ Database fetch failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/create_database", methods=["POST"])
@login_required
def create_database():
    data = request.get_json()
    db_name = data.get("name")
    if not db_name:
        return jsonify({"error": "Database name required"}), 400
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]")
        conn.commit()
        conn.close()
        add_log(f"✅ Database '{db_name}' created successfully.")
        return jsonify({"message": f"Database '{db_name}' created successfully."})
    except Exception as e:
        add_log(f"⚠ Error creating database: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------- DOWNLOADS ----------------
@app.route("/get_download_history")
@login_required
def get_download_history():
    return jsonify({"history": list(reversed(download_history))})

@app.route("/download_now", methods=["POST"])
@login_required
def download_now():
    data = request.get_json() or {}
    note = data.get("note", "")
    db_name = data.get("db") or None
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    add_log(f"⬇️ Download requested (DB: {db_name}) — note: {note}")
    def dl_task():
        entry = {"ts": ts, "status": "started", "notes": note, "db": db_name}
        download_history.append(entry)
        try:
            main_sync.run_once_all(db_name=db_name)
            entry["status"] = "success"
            entry["notes"] = (entry.get("notes", "") + " | Completed")
            add_log(f"✅ Download completed (DB: {db_name})")
        except Exception as e:
            entry["status"] = "failed"
            entry["notes"] = (entry.get("notes", "") + f" | Error: {e}")
            add_log(f"⚠ Download failed: {e}")
    threading.Thread(target=dl_task, daemon=True).start()
    return jsonify({"status": "started", "ts": ts})

# ---------------- SCHEDULER: shared helpers ----------------
def _next_run_time_now(s_type, interval=None, time_str=None, day=None, date_iso=None):
    now = datetime.now()
    if s_type == "interval":
        return now + timedelta(minutes=max(1, int(interval or 15)))
    if s_type == "daily":
        hh, mm = map(int, (time_str or "02:00").split(":"))
        next_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if next_dt <= now:
            next_dt += timedelta(days=1)
        return next_dt
    if s_type == "monthly":
        dd = int(day or 1)
        hh, mm = map(int, (time_str or "02:00").split(":"))
        year = now.year
        month = now.month
        try:
            candidate = datetime(year, month, dd, hh, mm)
        except Exception:
            candidate = (datetime(year, month, 1) + timedelta(days=31)).replace(day=1, hour=hh, minute=mm)
        if candidate <= now:
            month += 1
            if month > 12:
                month = 1
                year += 1
            try:
                candidate = datetime(year, month, dd, hh, mm)
            except Exception:
                candidate = (datetime(year, month, 1) + timedelta(days=31)).replace(day=1, hour=hh, minute=mm)
        return candidate
    if s_type == "yearly":
        if not date_iso:
            mmday = (1, 1)
        else:
            parts = date_iso.split("-")
            mmday = (int(parts[1]), int(parts[2])) if len(parts) >= 3 else (1, 1)
        mm, dd = mmday
        hh, minu = map(int, (time_str or "02:00").split(":"))
        year = now.year
        try:
            candidate = datetime(year, mm, dd, hh, minu)
        except Exception:
            candidate = datetime(year, 1, 1, hh, minu)
        if candidate <= now:
            candidate = candidate.replace(year=year + 1)
        return candidate
    return now + timedelta(minutes=15)

def _job_loop(job):
    """
    Background loop for a single job dict.
    job dict: id, name, db, type, interval, time, day, date, status
    """
    jid = job["id"]
    stop_ev = job_threads[jid]["stop"]
    add_log(f"🕒 Job '{job.get('name') or jid}' started (type={job['type']})")
    while not stop_ev.is_set():
        try:
            next_run = _next_run_time_now(job["type"], job.get("interval"), job.get("time"), job.get("day"), job.get("date"))
            job["next_run"] = next_run.strftime("%Y-%m-%d %H:%M:%S")
            add_log(f"⏳ Job '{job.get('name')}' next run at {job['next_run']}")
            # wait until next_run or stop
            while not stop_ev.is_set():
                now = datetime.now()
                if now >= next_run:
                    break
                sleep_seconds = min(10, (next_run - now).total_seconds())
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
            if stop_ev.is_set():
                break
            # execute
            job["status"] = "running"
            add_log(f"🟢 Job '{job.get('name')}' executing run_once_all (DB: {job.get('db')})")
            try:
                main_sync.run_once_all(db_name=job.get("db"))
                add_log(f"✅ Job '{job.get('name')}' run completed.")
            except Exception as ex:
                add_log(f"⚠ Job '{job.get('name')}' run failed: {ex}")
            job["status"] = "idle"
            # small pause before next schedule loop
            time.sleep(1)
        except Exception as e:
            add_log(f"⚠ Job loop '{job.get('name')}' error: {e}")
            time.sleep(5)
    add_log(f"🛑 Job '{job.get('name')}' stopped.")

# ---------------- JOB MANAGEMENT ENDPOINTS ----------------
@app.route("/jobs")
@login_required
def jobs_page():
    """Return job list JSON (front-end uses it)."""
    # Provide a serializable copy
    return jsonify({"jobs": jobs})

@app.route("/jobs/create", methods=["POST"])
@login_required
def create_job():
    """
    Create a job.
    Expected JSON:
      - name
      - db
      - type: interval|daily|monthly|yearly
      - interval (minutes) [for interval]
      - time (HH:MM) [for daily/monthly/yearly]
      - day (int) [for monthly]
      - date (YYYY-MM-DD) [for yearly]
      - auto_start: true/false
    """
    data = request.get_json() or {}
    job = {
        "id": str(uuid.uuid4()),
        "name": data.get("name") or f"Job-{len(jobs)+1}",
        "db": data.get("db"),
        "type": data.get("type", "interval"),
        "interval": int(data.get("interval") or 15),
        "time": data.get("time"),
        "day": data.get("day"),
        "date": data.get("date"),
        "status": "idle",
        "next_run": None
    }
    jobs.append(job)
    add_log(f"➕ Job created: {job['name']} (id={job['id']})")
    auto_start = data.get("auto_start", False)
    if auto_start:
        _start_job_internal(job["id"])
    return jsonify({"job": job})

@app.route("/jobs/<job_id>/start", methods=["POST"])
@login_required
def start_job(job_id):
    res = _start_job_internal(job_id)
    if not res:
        return jsonify({"error": "start failed or job not found"}), 400
    return jsonify({"status": "started", "job_id": job_id})

def _start_job_internal(job_id):
    job = next((j for j in jobs if j["id"] == job_id), None)
    if not job:
        add_log(f"⚠ Start requested but job not found: {job_id}")
        return False
    if job_id in job_threads and job_threads[job_id]["thread"].is_alive():
        add_log(f"ℹ️ Job already running: {job['name']}")
        return True
    stop_ev = threading.Event()
    t = threading.Thread(target=_job_loop, args=(job,), daemon=True)
    job_threads[job_id] = {"thread": t, "stop": stop_ev}
    # pass stop event into job_threads dict so loop can access it via job_threads[job_id]["stop"]
    job_threads[job_id]["stop"] = stop_ev
    t.start()
    add_log(f"▶ Job started: {job['name']}")
    return True

@app.route("/jobs/<job_id>/stop", methods=["POST"])
@login_required
def stop_job(job_id):
    if job_id not in job_threads:
        return jsonify({"error": "job not running"}), 400
    ev = job_threads[job_id]["stop"]
    ev.set()
    thread = job_threads[job_id]["thread"]
    thread.join(timeout=5)
    job_threads.pop(job_id, None)
    add_log(f"⏹ Job stopped: {job_id}")
    return jsonify({"status": "stopped", "job_id": job_id})

@app.route("/jobs/<job_id>/delete", methods=["POST"])
@login_required
def delete_job(job_id):
    # stop if running
    if job_id in job_threads:
        job_threads[job_id]["stop"].set()
        job_threads[job_id]["thread"].join(timeout=3)
        job_threads.pop(job_id, None)
    global jobs
    jobs = [j for j in jobs if j["id"] != job_id]
    add_log(f"🗑 Job deleted: {job_id}")
    return jsonify({"status": "deleted", "job_id": job_id})

@app.route("/jobs/<job_id>/update", methods=["POST"])
@login_required
def update_job(job_id):
    data = request.get_json() or {}
    job = next((j for j in jobs if j["id"] == job_id), None)
    if not job:
        return jsonify({"error": "job not found"}), 404
    # update fields (simple)
    job["name"] = data.get("name", job["name"])
    job["db"] = data.get("db", job.get("db"))
    job["type"] = data.get("type", job["type"])
    job["interval"] = int(data.get("interval") or job.get("interval") or 15)
    job["time"] = data.get("time", job.get("time"))
    job["day"] = data.get("day", job.get("day"))
    job["date"] = data.get("date", job.get("date"))
    add_log(f"✏️ Job updated: {job['name']} (id={job_id})")
    return jsonify({"job": job})

# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5000, debug=True)
