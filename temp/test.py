import requests
import xml.etree.ElementTree as ET
import pandas as pd

# --- XML Request to Tally to get Ledgers ---
xml_req = """<ENVELOPE>
    <HEADER>
        <TALLYREQUEST>Export Data</TALLYREQUEST>
    </HEADER>
    <BODY>
        <EXPORTDATA>
            <REQUESTDESC>
                <REPORTNAME>List of Ledgers</REPORTNAME>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                </STATICVARIABLES>
            </REQUESTDESC>
        </EXPORTDATA>
    </BODY>
</ENVELOPE>"""

# Send XML to Tally
url = "http://localhost:9000"
response = requests.post(url, data=xml_req.encode())

# Check for success
if response.status_code == 200:
    xml_data = response.text
    root = ET.fromstring(xml_data)
    
    # Extract ledger names and parent groups
    ledgers = []
    for ledger in root.findall(".//LEDGER"):
        name = ledger.findtext("NAME")
        parent = ledger.findtext("PARENT")
        currency = ledger.findtext("CURRENCYNAME")
        opening = ledger.findtext("OPENINGBALANCE")
        ledgers.append({
            "NAME": name,
            "PARENT": parent,
            "CURRENCY": currency,
            "OPENING_BALANCE": opening
        })

    # Save to CSV
    df = pd.DataFrame(ledgers)
    df.to_csv("ledgers.csv", index=False, encoding="utf-8-sig")
    print("✅ Ledger data exported successfully to ledgers.csv")
    print(df.head())  # show sample
else:
    print("❌ Failed to connect:", response.status_code)
    print(response.text)
