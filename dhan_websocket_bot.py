#!/usr/bin/env python3
# dhan_websocket_bot.py (fixed poller diagnostics + retries)
import os
import time
import json
import threading
import traceback
import requests

try:
    import websocket
except Exception:
    raise RuntimeError("Missing dependency 'websocket-client'. Install with: pip install websocket-client")

# ----------------------------
# Security ID mapping (sample)
INDICES_NSE = {
    "NIFTY 50": "13",
    "NIFTY BANK": "25",
    "BANKNIFTY": "25",
}
INDICES_BSE = {
    "SENSEX": "51",
}
NIFTY50_STOCKS = {
    "TATAMOTORS": "3456",
    "RELIANCE": "2885",
    "TCS": "11536",
}
def get_security_id(symbol: str):
    s = symbol.upper().strip()
    return NIFTY50_STOCKS.get(s) or INDICES_NSE.get(s) or INDICES_BSE.get(s)

# ----------------------------
# Config (from env)
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
WS_URL = os.getenv('WS_URL', 'wss://dhan.websocket.placeholder/marketfeed')
SYMBOLS = os.getenv('SYMBOLS', 'NIFTY,BANKNIFTY,SENSEX,RELIANCE,TCS,TATAMOTORS')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '60'))   # seconds
RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', '3'))
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '25'))

INSTRUMENT_CSV_URL = os.getenv('INSTRUMENT_CSV_URL', 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv')
DHAN_LTP_ENDPOINT = os.getenv('DHAN_LTP_ENDPOINT', 'https://api.dhan.co/v2/marketfeed/ltp')

# ----------------------------
# Telegram helper
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured, would have sent:\n", text)
        return
    url = TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
        print("Telegram sent")
    except Exception as e:
        print("Telegram send error:", e)

# ----------------------------
# REST poller helpers with retries and diagnostics
def build_security_payload(symbols):
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print(f"[poller-warn] security id not found for symbol: {s}")
            continue
        if s.upper() in ('NIFTY','NIFTY 50','BANKNIFTY','NIFTY BANK'):
            seg = 'NSE_INDEX'
        elif s.upper() == 'SENSEX':
            seg = 'BSE_INDEX'
        else:
            seg = 'NSE_EQ'
        try:
            seg_map.setdefault(seg, []).append(int(sid))
        except Exception:
            seg_map.setdefault(seg, []).append(sid)
    return seg_map

def call_dhan_ltp_with_retries(payload, retries=3, backoff=1.5):
    headers = {
        'access-token': ACCESS_TOKEN or '',
        'client-id': CLIENT_ID or '',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            r = requests.post(DHAN_LTP_ENDPOINT, json=payload, headers=headers, timeout=10)
            print(f"[poller] HTTP {r.status_code} (attempt {attempt})")
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception as e:
                    print("[poller] JSON decode failed:", e, "response text:", r.text[:500])
                    return None
            else:
                print("[poller] Non-200 response text (truncated):", r.text[:800])
        except Exception as e:
            print(f"[poller] exception on attempt {attempt}:", e)
            last_exc = e
        time.sleep(backoff * attempt)
    print("[poller] All retries failed. Last exception:", last_exc)
    return None

def download_and_build_map(symbols):
    try:
        r = requests.get(INSTRUMENT_CSV_URL, timeout=15)
        r.raise_for_status()
        text = r.text.splitlines()
        from csv import DictReader
        reader = DictReader(text)
        mapping = {}
        for row in reader:
            symbol_name = row.get('SM_SYMBOL_NAME') or row.get('SYMBOL') or row.get('SYMBOL_NAME') or row.get('TRADING_SYMBOL')
            sid = row.get('SECURITY_ID') or row.get('SM_INSTRUMENT_ID') or row.get('EXCH_TOKEN')
            if not symbol_name or not sid:
                continue
            mapping[symbol_name.strip().upper()] = sid
        patched = {}
        for s in symbols:
            val = mapping.get(s.strip().upper())
            if val:
                patched[s.strip().upper()] = val
        return patched
    except Exception as e:
        print("[poller] failed to download instrument CSV:", e)
        return {}

def format_poll_message(json_resp, symbol_map):
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    lines = []
    if not json_resp:
        return f"⏱ {now} — Poller: no response from Dhan"
    data = json_resp.get('data') if isinstance(json_resp, dict) else None
    if not data:
        data = json_resp
    if isinstance(data, dict):
        for seg, bucket in data.items():
            if not isinstance(bucket, dict):
                continue
            for secid, info in bucket.items():
                if not isinstance(info, dict):
                    continue
                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice') or info.get('price')
                if ltp is None:
                    continue
                readable = None
                try:
                    sid_str = str(secid)
                    for k,v in symbol_map.items():
                        if str(v) == sid_str:
                            readable = k
                            break
                except Exception:
                    pass
                lines.append(f"<b>{readable or secid}</b>  LTP: <code>{ltp}</code>  ({seg})")
    if not lines:
        return f"⏱ {now} — Poller: response received but no LTP fields found (see logs)"
    return f"⏱ {now} — Poller snapshot:\n" + "\n".join(lines)

# ----------------------------
# Poller thread (improved)
class PollerThread(threading.Thread):
    def __init__(self, symbols, interval=POLL_INTERVAL):
        super().__init__(daemon=True)
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.interval = interval
        self.sym_map = {}
        for s in self.symbols:
            sid = get_security_id(s)
            if sid:
                self.sym_map[s.upper()] = sid
        self.failed_count = 0
        self.last_alert_time = 0

    def run(self):
        payload_map = build_security_payload(self.symbols)
        if not payload_map:
            print("[poller] payload empty, attempting to download instrument CSV and remap...")
            patched = download_and_build_map(self.symbols)
            print("[poller] patched mapping from CSV (sample):", dict(list(patched.items())[:10]))
            for k,v in patched.items():
                self.sym_map[k.upper()] = v
        while True:
            try:
                payload_map = build_security_payload(self.symbols)
                print("[poller] Using payload:", payload_map)
                resp = call_dhan_ltp_with_retries(payload_map, retries=3)
                msg = format_poll_message(resp, self.sym_map)
                if resp is None:
                    self.failed_count += 1
                    print(f"[poller] failed_count={self.failed_count}")
                    if self.failed_count % 5 == 1:
                        send_telegram(msg + "\n[poller diagnostic] check logs for HTTP status and response text.")
                else:
                    if self.failed_count > 0:
                        send_telegram(f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — Poller recovered. Sending snapshot now.")
                        self.failed_count = 0
                    send_telegram(msg)
            except Exception as e:
                print("[poller] Exception:", e)
                traceback.print_exc()
                send_telegram(f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — Poller exception: {e}")
            time.sleep(self.interval)

# ----------------------------
# WebSocket client (unchanged)
def build_auth_payload():
    return {
        'action': 'auth',
        'client_id': CLIENT_ID,
        'access_token': ACCESS_TOKEN,
    }

def build_subscribe_msg(security_ids):
    return {
        'action': 'subscribe',
        'instruments': [{'segment': seg, 'instrument_id': sid} for seg, sid in security_ids]
    }

def build_security_list(symbols):
    out = []
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print(f"[ws-warn] security id not found for: {s}")
            continue
        if s.upper() in ('NIFTY','NIFTY 50','BANKNIFTY','NIFTY BANK'):
            seg = 'NSE_INDEX'
        elif s.upper() == 'SENSEX':
            seg = 'BSE_INDEX'
        else:
            seg = 'NSE_EQ'
        out.append((seg, str(sid)))
    return out

class DhanWebsocketClient:
    def __init__(self, ws_url, symbols):
        self.ws_url = ws_url
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.ws = None
        self._stop = threading.Event()
        self.last_ltps = {}

    def on_open(self, ws):
        print("WebSocket opened")
        try:
            auth = build_auth_payload()
            if auth:
                ws.send(json.dumps(auth))
                print("Auth payload sent")
            sec_list = build_security_list(self.symbols)
            if not sec_list:
                print("No security ids to subscribe to. Check mapping.")
                return
            sub = build_subscribe_msg(sec_list)
            ws.send(json.dumps(sub))
            print("Subscribe message sent:", sub)
        except Exception as e:
            print("Error during on_open actions:", e)

    def on_message(self, ws, message):
        try:
            if isinstance(message, bytes):
                try:
                    message = message.decode('utf-8')
                except Exception:
                    print("Received binary message - parser not implemented")
                    return
            data = json.loads(message)
        except Exception as e:
            print("Failed to parse incoming message as JSON:", e)
            return

        ltp = None
        symbol = None
        if isinstance(data, dict):
            payload = data.get('data') if 'data' in data else data
            if isinstance(payload, dict):
                for k, v in payload.items():
                    if isinstance(v, dict):
                        for instid, info in v.items():
                            if isinstance(info, dict):
                                ltp = info.get('last_price') or info.get('ltp') or info.get('lastPrice')
                                if ltp is not None:
                                    symbol = instid
                                    break
                        if ltp is not None:
                            break
            if ltp is None:
                ltp = data.get('ltp') or data.get('last_price') or data.get('lastPrice') or data.get('price')
                symbol = data.get('symbol') or data.get('instrument') or symbol

        if ltp is not None:
            readable = symbol
            sid = None
            try:
                sid = str(symbol)
                for k, v in NIFTY50_STOCKS.items():
                    if str(v) == sid:
                        readable = k
                        break
                for k, v in INDICES_NSE.items():
                    if str(v) == sid:
                        readable = k
                        break
                for k, v in INDICES_BSE.items():
                    if str(v) == sid:
                        readable = k
                        break
            except Exception:
                pass

            key = f"{readable}:{sid}" if sid else readable
            prev = self.last_ltps.get(key)
            self.last_ltps[key] = ltp
            text = f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — <b>{readable}</b> LTP: <code>{ltp}</code>"
            if prev != ltp:
                send_telegram(text)
                print("Sent telegram (ws):", text)

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)

    def run_forever(self):
        while not self._stop.is_set():
            try:
                print("Connecting to websocket:", self.ws_url)
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=HEARTBEAT_INTERVAL, ping_timeout=10)
            except Exception as e:
                print("Exception in run_forever loop:", e)
                traceback.print_exc()
            if not self._stop.is_set():
                print(f"Reconnecting after {RECONNECT_DELAY}s...")
                time.sleep(RECONNECT_DELAY)

    def stop(self):
        self._stop.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

# ----------------------------
if __name__ == '__main__':
    print("Starting Dhan WebSocket + Poller bot (fixed). Poll interval (s):", POLL_INTERVAL)
    poller = PollerThread(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()
    client = DhanWebsocketClient(WS_URL, SYMBOLS)
    try:
        client.run_forever()
    except KeyboardInterrupt:
        print("Stopping...")
        client.stop()
