import requests

TALLY_URL = "http://localhost:9000"

reports_to_test = [
    "List of Accounts",
    "List of Ledgers",
    "List of Groups",
    "Chart of Accounts",
    "All Masters"
]

for rep in reports_to_test:
    xml_req = f"""<ENVELOPE>
     <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
     <BODY><EXPORTDATA>
      <REQUESTDESC><REPORTNAME>{rep}</REPORTNAME>
       <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
      </REQUESTDESC>
     </EXPORTDATA></BODY></ENVELOPE>"""

    print(f"\n🔍 Testing report: {rep}")
    try:
        r = requests.post(TALLY_URL, data=xml_req.encode(), timeout=10)
        print("HTTP", r.status_code)
        if "<LINEERROR>" in r.text:
            start = r.text.find("<LINEERROR>")
            print("❌", r.text[start:start+150])
        else:
            print("✅ Seems valid — first 200 chars:\n", r.text[:200])
    except Exception as e:
        print("Error:", e)
