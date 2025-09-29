import requests, csv, io
CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
names_to_find = ["RELIANCE", "TCS", "TATAMOTORS", "NIFTY", "BANKNIFTY", "SENSEX"]
r = requests.get(CSV_URL, timeout=20); r.raise_for_status()
reader = csv.DictReader(io.StringIO(r.text))
found = {}
for row in reader:
    symbol = row.get('SM_SYMBOL_NAME') or row.get('SYMBOL') or row.get('SYMBOL_NAME') or row.get('TRADING_SYMBOL')
    sid = row.get('SECURITY_ID') or row.get('SM_INSTRUMENT_ID') or row.get('EXCH_TOKEN')
    if not symbol or not sid: continue
    s = symbol.strip().upper()
    if s in names_to_find:
        found[s] = sid
print("Found mapping:")
for k,v in found.items():
    print(k, "->", v)
