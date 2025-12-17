#!/usr/bin/env python3
"""
TallyPrime → SQL Server Sync
- Supports Licensed (full) and Edu/Trial (NAME-only) modes
- Modes: [1] Interactive [2] Run once (all masters) [3] Scheduler
- Auto table creation & UPSERT by NAME
"""

import requests
import xml.etree.ElementTree as ET
import pyodbc
import pandas as pd
import re
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

# ---------------- CONFIG ----------------
TALLY_URL = "http://localhost:9000"
HEADERS = {"Content-Type": "text/xml"}

SQL_SERVERS_TO_TRY = [r"LOHITH\\SQLEXPRESS", "127.0.0.1,1433"]
ODBC_DRIVER = "{ODBC Driver 17 for SQL Server}"
DEFAULT_DB = "lohit"
UPSERT_BATCH_SIZE = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("tally_sync.log"), logging.StreamHandler()]
)

# ---------------- SQL UTILS ----------------
def connect_sql_interactive():
    for server in SQL_SERVERS_TO_TRY:
        try:
            conn = pyodbc.connect(
                f"DRIVER={ODBC_DRIVER};SERVER={server};DATABASE=master;Trusted_Connection=yes;",
                timeout=5
            )
            logging.info(f"Connected to SQL Server: {server}")
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases")
            dbs = [r[0] for r in cursor.fetchall()]
            print("\nDatabases on server:")
            for i, db in enumerate(dbs, 1):
                print(f"  {i}. {db}")
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

def upsert_dataframe(df: pd.DataFrame, table: str, conn):
    if df.empty:
        logging.warning(f"Empty DataFrame for {table}, skipping.")
        return
    cursor = conn.cursor()
    safe_table = f"[{table}]"

    # Create table if not exists
    columns = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    cursor.execute(f"IF OBJECT_ID(N'{table}', 'U') IS NULL CREATE TABLE {safe_table} ({columns})")
    conn.commit()

    # --- NEW: Ensure schema has all columns ---
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
    existing_cols = {r[0] for r in cursor.fetchall()}
    missing_cols = [c for c in df.columns if c not in existing_cols]
    for col in missing_cols:
        cursor.execute(f"ALTER TABLE {safe_table} ADD [{col}] NVARCHAR(MAX)")
        logging.info(f"Altering {table}: added new column [{col}]")
    conn.commit()
    # --- END NEW ---

    # Upsert rows
    for _, row in df.iterrows():
        assignments = ", ".join([f"[{col}] = ?" for col in df.columns])
        placeholders = ", ".join(["?"] * len(df.columns))
        sql = f"""
        MERGE {safe_table} AS target
        USING (SELECT ? AS [NAME]) AS source
        ON target.[NAME] = source.[NAME]
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({", ".join(f"[{c}]" for c in df.columns)})
        VALUES ({placeholders});
        """
        values = [row["NAME"]] + row.tolist() + row.tolist()
        cursor.execute(sql, values)
    conn.commit()
    logging.info(f"Upserted {len(df)} rows into {table}")


# ---------------- TALLY UTILS ----------------
def send_request(xml: str) -> str:
    try:
        resp = requests.post(TALLY_URL, data=xml.encode("utf-8"), headers=HEADERS)
        return resp.text
    except Exception as e:
        logging.error(f"Tally request failed: {e}")
        return ""

def parse_xml_to_df(xml: str, tags: list) -> pd.DataFrame:
    try:
        xml = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", xml)
        xml = re.sub(r"&(?!(amp;|lt;|gt;|apos;|quot;))", "&amp;", xml)
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
        </DESC>
      </BODY>
    </ENVELOPE>
    """
    resp = send_request(xml_full)
    df = parse_xml_to_df(resp, full_fields)
    if not df.empty:
        logging.info(f"{master}: Licensed mode (full fields)")
        return df

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
              </COLLECTION>
            </TDLMESSAGE>
          </TDL>
        </DESC>
      </BODY>
    </ENVELOPE>
    """
    resp = send_request(xml_edu)
    df = parse_xml_to_df(resp, ["NAME"])
    logging.info(f"{master}: Edu mode (NAME only)")
    return df

# ---------------- MASTERS ----------------
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
    conn = connect_sql_default()
    for master, fields in MASTERS.items():
        logging.info(f"Fetching {master}...")
        df = fetch_master(master, fields)
        logging.info(f"{master}: Parsed {len(df)} rows")
        upsert_dataframe(df, master, conn)
    conn.close()
    logging.info("One-time sync (all masters) complete.")

def run_scheduler():
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
 

def run_selected(selected_masters):
    conn = connect_sql_default()
    for master in selected_masters:
        fields = MASTERS.get(master, ["NAME"])
        logging.info(f"Fetching {master}...")
        df = fetch_master(master, fields)
        logging.info(f"{master}: Parsed {len(df)} rows")
        upsert_dataframe(df, master, conn)
    conn.close()
    logging.info("Selected masters sync complete.")
def run_interactive(db_name=None):
    print(f"Running interactive sync on database: {db_name}")
    # your existing logic...
def run_once_all(db_name=None):
    print(f"Running full one-time sync on database: {db_name}")
    # your actual sync logic...

