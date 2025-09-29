import os, requests, json
CLIENT_ID = os.getenv('DHAN_CLIENT_ID') or "<PUT_CLIENT_ID>"
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN') or "<PUT_ACCESS_TOKEN>"
ENDPOINT = "https://api.dhan.co/v2/marketfeed/ltp"
payload = {"NSE_EQ":[11536,2885],"NSE_INDEX":[13]}
headers = {"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID, "Accept": "application/json", "Content-Type": "application/json"}
r = requests.post(ENDPOINT, json=payload, headers=headers, timeout=15)
print("HTTP", r.status_code)
print("Body (first 1000 chars):")
print(r.text[:1000])
try:
    print("Parsed keys:", list(r.json().keys()) if isinstance(r.json(), dict) else type(r.json()))
except Exception as e:
    print("JSON parse failed:", e)
