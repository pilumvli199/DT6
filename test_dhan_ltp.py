import os, requests, json, sys
CLIENT_ID = os.getenv('DHAN_CLIENT_ID') or "<PUT_CLIENT_ID_HERE>"
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN') or "<PUT_ACCESS_TOKEN_HERE>"
ENDPOINT = os.getenv('DHAN_LTP_ENDPOINT') or "https://api.dhan.co/v2/marketfeed/ltp"
payload = {"NSE_EQ": [11536, 2885], "NSE_INDEX": [13]}
headers = {"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID, "Accept": "application/json", "Content-Type": "application/json"}
r = requests.post(ENDPOINT, json=payload, headers=headers, timeout=15)
print("HTTP", r.status_code)
print("Body (first 2000 chars):")
print(r.text[:2000])
try:
    j = r.json()
    print("Parsed JSON keys:", list(j.keys()) if isinstance(j, dict) else type(j))
except Exception as e:
    print("JSON parse failed:", e)
