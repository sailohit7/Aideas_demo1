#!/usr/bin/env python3
"""
TallyPrime → SQL Server Sync
- Supports Licensed (full) and Edu/Trial (NAME-only) modes
- Modes: [1] Interactive [2] Run once (all masters) [3] Scheduler
- Auto table creation & UPSERT by NAME with incremental sync
- Tracks _HASH / _ALTERID / _GUID / _MASTERID / _SYNCED_AT
"""

import requests
import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
import re
import logging
import hashlib
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

# ---------------- CONFIG ----------------
# TallyPrime server URL and request headers
TALLY_URL = "http://localhost:9000"
HEADERS = {"Content-Type": "text/xml"}

# SQL connection settings
SQL_SERVERS_TO_TRY = [r"LOHITH\\SQLEXPRESS", "127.0.0.1,1433"]  # possible SQL servers to try
ODBC_DRIVER = "{ODBC Driver 17 for SQL Server}"                 # ODBC driver
DEFAULT_DB = "lohit"                                            # default database name
UPSERT_BATCH_SIZE = 200                                         # batch size for inserts/updates

# Logging configuration (both file + console output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("tally_sync.log"), logging.StreamHandler()]
)

# ---------------- SQL UTILS ----------------
def connect_sql_interactive():
    """
    Connect to SQL Server interactively.
    - Tries multiple server options until one connects.
    - Lists available databases and lets user choose or create a new one.
    - Returns a connection object.
    """
    for server in SQL_SERVERS_TO_TRY:
        try:
            # First connect to 'master' to list databases
            conn = pyodbc.connect(
                f"DRIVER={ODBC_DRIVER};SERVER={server};DATABASE=master;Trusted_Connection=yes;",
                timeout=5
            )
            logging.info(f"Connected to SQL Server: {server}")
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases")
            dbs = [r[0] for r in cursor.fetchall()]

            # Show DB list to user
            print("\nDatabases on server:")
            for i, db in enumerate(dbs, 1):
                print(f"  {i}. {db}")

            # Ask for choice or create new DB
            choice = input(f"Choose DB number or type a new database name [default: {DEFAULT_DB}]: ").strip()
            if not choice:
                db_name = DEFAULT_DB
            elif choice.isdigit() and 1 <= int(choice) <= len(dbs):
                db_name = dbs[int(choice) - 1]
            else:
                db_name = choice
                cursor.execute(f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]")
                conn.commit()
                logging.info(f"Created new database: {db_name}")

            # Reconnect directly to chosen DB
            conn.close()
            conn = pyodbc.connect(
                f"DRIVER={ODBC_DRIVER};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;",
                timeout=5
            )
            logging.info(f"Connected to database {db_name} on server {server}")
            return conn
        except Exception as e:
            logging.warning(f"Failed SQL connection to {server}: {e}")
    raise Exception("All SQL Server connection attempts failed.")

def connect_sql_default():
    """
    Connect directly to the default SQL database.
    Used for non-interactive modes.
    """
    for server in SQL_SERVERS_TO_TRY:
        try:
            conn = pyodbc.connect(
                f"DRIVER={ODBC_DRIVER};SERVER={server};DATABASE={DEFAULT_DB};Trusted_Connection=yes;",
                timeout=5
            )
            logging.info(f"Connected to default DB {DEFAULT_DB} on server {server}")
            return conn
        except Exception as e:
            logging.warning(f"Failed SQL connection to {server}: {e}")
    raise Exception("All SQL Server connection attempts failed.")

# ---------------- HASH UTILS ----------------
def compute_row_hash(row: pd.Series) -> str:
    """
    Compute a stable SHA1 hash for a row.
    - Excludes metadata columns (those starting with '_').
    - Used for change detection to avoid unnecessary updates.
    """
    content = "|".join(str(v) if v is not None else "" for k, v in row.items() if not k.startswith("_"))
    return hashlib.sha1(content.encode("utf-8")).hexdigest()

# ---------------- UPSERT ----------------
def upsert_dataframe(df: pd.DataFrame, table: str, conn):
    """
    UPSERT (insert/update) a pandas DataFrame into SQL Server.
    - Creates table if not exists.
    - Adds missing columns dynamically.
    - Uses NAME as primary key (unique index).
    - Compares hashes to only update changed rows.
    """
    if df.empty:
        logging.warning(f"Empty DataFrame for {table}, skipping.")
        return

    # Add hash + metadata columns
    df["_HASH"] = df.apply(compute_row_hash, axis=1)
    df["_SYNCED_AT"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "_ALTERID" not in df.columns: df["_ALTERID"] = None
    if "_GUID" not in df.columns: df["_GUID"] = None
    if "_MASTERID" not in df.columns: df["_MASTERID"] = None

    cursor = conn.cursor()
    safe_table = f"[{table}]"

    # Create table if it doesn’t exist
    columns = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    cursor.execute(f"IF OBJECT_ID(N'{table}', 'U') IS NULL CREATE TABLE {safe_table} ({columns})")
    conn.commit()

    # Add missing columns if schema changed
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
    existing_cols = {r[0] for r in cursor.fetchall()}
    missing_cols = [c for c in df.columns if c not in existing_cols]
    for col in missing_cols:
        cursor.execute(f"ALTER TABLE {safe_table} ADD [{col}] NVARCHAR(MAX)")
        logging.info(f"Altering {table}: added new column [{col}]")
    conn.commit()

    # Create indexes for performance
    try:
        cursor.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{table}_NAME')
            CREATE UNIQUE INDEX IX_{table}_NAME ON {safe_table} ([NAME]);
        """)
        cursor.execute(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{table}_HASH')
            CREATE INDEX IX_{table}_HASH ON {safe_table} ([_HASH]);
        """)
        conn.commit()
        logging.info(f"Ensured indexes on {table}: [NAME] (unique), [_HASH]")
    except Exception as e:
        logging.warning(f"Could not create indexes on {table}: {e}")

    # Upsert rows one by one
    for _, row in df.iterrows():
        cursor.execute(f"SELECT _HASH FROM {safe_table} WHERE [NAME] = ?", row["NAME"])
        existing = cursor.fetchone()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not existing:
            # Insert new row
            placeholders = ", ".join(["?"] * len(df.columns))
            cursor.execute(
                f"INSERT INTO {safe_table} ({', '.join(f'[{c}]' for c in df.columns)}) VALUES ({placeholders})",
                row.tolist()
            )
        else:
            if existing[0] != row["_HASH"]:
                # Update row if data changed
                assignments = ", ".join([f"[{col}] = ?" for col in df.columns if col != "_SYNCED_AT"])
                cursor.execute(
                    f"UPDATE {safe_table} SET {assignments}, [_SYNCED_AT] = ? WHERE [NAME] = ?",
                    [row[c] for c in df.columns if c != "_SYNCED_AT"] + [now, row["NAME"]]
                )
            else:
                logging.debug(f"Skipped unchanged row: {row['NAME']}")

    conn.commit()
    logging.info(f"Synced {len(df)} rows into {table}")

# ---------------- TALLY UTILS ----------------
def send_request(xml: str) -> str:
    """
    Send XML request to Tally server and return raw response text.
    """
    try:
        resp = requests.post(TALLY_URL, data=xml.encode("utf-8"), headers=HEADERS)
        return resp.text
    except Exception as e:
        logging.error(f"Tally request failed: {e}")
        return ""

def parse_xml_to_df(xml: str, tags: list) -> pd.DataFrame:
    """
    Parse Tally XML response into a pandas DataFrame.
    - Extracts only requested tags.
    - Cleans illegal characters.
    """
    try:
        xml = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", xml)  # remove control chars
        xml = re.sub(r"&(?!(amp;|lt;|gt;|apos;|quot;))", "&amp;", xml)  # fix bad ampersands
        root = ET.fromstring(xml)
        rows = []
        for obj in root.iter("COLLECTION"):
            for child in obj:
                row = {}
                for tag in tags:
                    el = child.find(tag)
                    row[tag] = el.text if el is not None else None
                rows.append(row)
        return pd.DataFrame(rows)
    except Exception as e:
        logging.error(f"Parse error: {e}")
        return pd.DataFrame(columns=tags)

def fetch_master(master: str, full_fields: list):
    """
    Fetch a master from Tally.
    - First tries Licensed mode (full fields).
    - Falls back to Edu mode (NAME + IDs only).
    Returns a DataFrame.
    """
    tally_fields = full_fields + ["_ALTERID", "_GUID", "_MASTERID"]

    # Licensed/full mode request
    xml_full = f"""
    <ENVELOPE>
      <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Collection</TYPE>
        <ID>List of {master}s</ID>
      </HEADER>
      <BODY>
        <DESC>
          <STATICVARIABLES>
            <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
          </STATICVARIABLES>
          <TDL>
            <TDLMESSAGE>
              <COLLECTION NAME="List of {master}s" TYPE="{master}">
                {''.join(f"<FETCH>{f}</FETCH>" for f in tally_fields)}
              </COLLECTION>
            </TDLMESSAGE>
          </TDL>
        </DESC>
      </BODY>
    </ENVELOPE>
    """
    resp = send_request(xml_full)
    df = parse_xml_to_df(resp, tally_fields)
    if not df.empty:
        logging.info(f"{master}: Licensed mode (full fields)")
        return df

    # Edu fallback request
    xml_edu = f"""
    <ENVELOPE>
      <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Edu{master}List</ID>
      </HEADER>
      <BODY>
        <DESC>
          <TDL>
            <TDLMESSAGE>
              <COLLECTION NAME="Edu{master}List" TYPE="{master}">
                <FETCH>NAME</FETCH>
                <FETCH>_ALTERID</FETCH>
                <FETCH>_GUID</FETCH>
                <FETCH>_MASTERID</FETCH>
              </COLLECTION>
            </TDLMESSAGE>
          </TDL>
        </DESC>
      </BODY>
    </ENVELOPE>
    """
    resp = send_request(xml_edu)
    df = parse_xml_to_df(resp, ["NAME", "_ALTERID", "_GUID", "_MASTERID"])
    logging.info(f"{master}: Edu mode (NAME + IDs)")
    return df

# ---------------- MASTERS ----------------
# Dictionary of all Tally masters with their fields
MASTERS = {
    "Ledger": ["NAME", "PARENT", "OPENINGBALANCE"],
    "Group": ["NAME", "PARENT", "ISSUBLEDGER"],
    "VoucherType": ["NAME", "PARENT", "NUMBERINGMETHOD"],
    "Currency": ["NAME", "ORIGINALNAME", "ISDAILYRATE"],
    "Budget": ["NAME", "PARENT"],
    "Scenario": ["NAME", "PARENT"],
    "CostCentre": ["NAME", "PARENT"],
    "CostCategory": ["NAME"],
    "InterestCollection": ["NAME"],
    "StockGroup": ["NAME", "PARENT"],
    "StockCategory": ["NAME", "PARENT"],
    "StockItem": ["NAME", "PARENT", "BASEUNITS"],
    "Unit": ["NAME", "GSTREPUOM"],
    "Godown": ["NAME", "PARENT"],
    "Batch": ["NAME"],
    "VoucherClass": ["NAME"],
    "EmployeeGroup": ["NAME"],
    "Employee": ["NAME", "EMPLOYEEID"],
    "AttendanceType": ["NAME", "ATTENDANCETYPE"],
    "PayHead": ["NAME", "PARENT"],
    "SalaryDetails": ["NAME"],
    "Company": ["NAME", "STARTINGFROM"],
    "SecurityControl": ["NAME"],
    "StatutoryFeature": ["NAME"]
}

# ---------------- RUN MODES ----------------
def run_interactive():
    """
    Interactive mode:
    - Lets user pick DB.
    - Lets user pick specific masters (or all).
    - Fetch + sync each master.
    """
    conn = connect_sql_interactive()
    print("\nAvailable masters:")
    for i, master in enumerate(MASTERS.keys(), 1):
        print(f"  {i}. {master}")
    choice = input("Enter master numbers (e.g. 1,3) or 'all' [default all]: ").strip()
    if not choice or choice.lower() == "all":
        selected = list(MASTERS.keys())
    else:
        nums = [int(x) for x in choice.split(",") if x.strip().isdigit()]
        selected = [list(MASTERS.keys())[i - 1] for i in nums if 1 <= i <= len(MASTERS)]

    for master in selected:
        fields = MASTERS[master]
        logging.info(f"Fetching {master}...")
        df = fetch_master(master, fields)
        logging.info(f"{master}: Parsed {len(df)} rows")
        upsert_dataframe(df, master, conn)

    conn.close()
    logging.info("Interactive sync complete.")

def run_once_all():
    """
    One-time mode:
    - Connects to default DB.
    - Fetches all masters and syncs them.
    """
    conn = connect_sql_default()
    for master, fields in MASTERS.items():
        logging.info(f"Fetching {master}...")
        df = fetch_master(master, fields)
        logging.info(f"{master}: Parsed {len(df)} rows")
        upsert_dataframe(df, master, conn)
    conn.close()
    logging.info("One-time sync (all masters) complete.")

def run_scheduler():
    """
    Scheduler mode:
    - Option 1: Run immediately.
    - Option 2: Run every N seconds (interval).
    - Option 3: Run daily at given time.
    """
    scheduler = BlockingScheduler()
    print("\nScheduler options:")
    print("  1. Run now")
    print("  2. Interval (every N seconds)")
    print("  3. Daily (at HH:MM)")
    choice = input("Choose option [1]: ").strip() or "1"

    if choice == "1":
        run_once_all()
    elif choice == "2":
        secs = int(input("Interval seconds [60]: ") or "60")
        scheduler.add_job(run_once_all, "interval", seconds=secs)
        scheduler.start()
    elif choice == "3":
        t = input("Enter time HH:MM [02:00]: ").strip() or "02:00"
        hh, mm = map(int, t.split(":"))
        scheduler.add_job(run_once_all, "cron", hour=hh, minute=mm)
        scheduler.start()

# ---------------- MAIN ----------------
def main():
    """
    Main entry point.
    Lets user choose mode: [1] Interactive, [2] Run once, [3] Scheduler.
    """
    print("Tally -> SQL Sync (all masters).")
    print("Modes: [1] Interactive run  [2] Run once (all masters)  [3] Scheduler")
    mode = input("Choose mode [1]: ").strip() or "1"
    if mode == "1":
        run_interactive()
    elif mode == "2":
        run_once_all()
    elif mode == "3":
        run_scheduler()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
