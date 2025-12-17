#!/usr/bin/env python3
"""
TallyPrime EDU → Export all masters to CSV (safe XML cleaning)
"""

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import re

TALLY_URL = "http://localhost:9000"

xml_req = """<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>All Masters</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>"""

print("🔗 Connecting to Tally at", TALLY_URL)
try:
    resp = requests.post(TALLY_URL, data=xml_req.encode(), timeout=15)
    print("HTTP status:", resp.status_code)
    if resp.status_code != 200:
        raise Exception("Invalid response")
    xml_data = resp.text
    print("\n--- First 500 chars of response ---\n", xml_data[:500])
except Exception as e:
    print("❌ Error connecting:", e)
    exit()

# 🧹 Clean invalid XML characters
xml_data_clean = re.sub(r"[^\x09\x0A\x0D\x20-\x7F]+", "", xml_data)

# 🧩 Parse cleaned XML safely
try:
    root = ET.fromstring(xml_data_clean)
except Exception as e:
    print("❌ Still failed to parse XML:", e)
    with open("raw_tally_response.xml", "w", encoding="utf-8") as f:
        f.write(xml_data)
    print("⚠️ Saved full XML to raw_tally_response.xml for review.")
    exit()

# 🗂️ Collect all master data
records = []
for tag in [
    "COMPANY", "CURRENCY", "GROUP", "LEDGER",
    "STOCKGROUP", "STOCKCATEGORY", "STOCKITEM",
    "UNIT", "GODOWN", "COSTCATEGORY", "COSTCENTRE"
]:
    for node in root.findall(f".//{tag}"):
        name = node.findtext("NAME")
        parent = node.findtext("PARENT")
        records.append({"MASTER_TYPE": tag, "NAME": name, "PARENT": parent})

df = pd.DataFrame(records)
if df.empty:
    print("⚠️ No records found — please ensure a company is open in Tally.")
else:
    df.to_csv("tally_all_masters.csv", index=False, encoding="utf-8-sig")
    print(f"✅ Exported {len(df)} records to tally_all_masters.csv")
    print(df.head())
