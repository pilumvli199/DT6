#!/usr/bin/env python3
# dhan_websocket_bot.py
# WebSocket + 1-minute poller to ensure Telegram LTP updates
# Replace WS_URL and ensure DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN are set.

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
# Replace or extend with real IDs from Dhan CSV if needed.
# ----------------------------
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
# ----------------------------
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
WS_URL = os.getenv('WS_URL', 'wss://dhan.websocket.placeholder/marketfeed')
SYMBOLS = os.getenv('SYMBOLS', 'NIFTY,BANKNIFTY,SENSEX,RELIANCE,TCS,TATAMOTORS')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '60'))   # seconds
RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', '3'))
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '25'))

# Basic sanity check
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set (env). Telegram will not work without them.")

if not CLIENT_ID or not ACCESS_TOKEN:
    print("WARNING: DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN missing — REST poller and WS auth may fail.")

# ----------------------------
# Telegram helper
# ----------------------------
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured, would have sent:", text)
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
# Helpers to build REST payload for /marketfeed/ltp
# ----------------------------
DHAN_LTP_ENDPOINT = "https://api.dhan.co/v2/marketfeed/ltp"

def build_security_payload(symbols):
    """
    Returns dict grouped by segment: {'NSE_EQ':[id1,id2], 'NSE_INDEX':[id3]}
    Uses get_security_id() mapping above.
    """
    seg_map = {}
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print(f"[poller-warn] security id not found for symbol: {s}")
            continue
        # choose likely segment
        if s.upper() in ('NIFTY','NIFTY 50','BANKNIFTY','NIFTY BANK'):
            seg = 'NSE_INDEX'
        elif s.upper() == 'SENSEX':
            seg = 'BSE_INDEX'
        else:
            seg = 'NSE_EQ'
        seg_map.setdefault(seg, []).append(int(sid))
    return seg_map

def call_dhan_ltp(payload):
    """
    POST to Dhan LTP endpoint with required headers.
    Returns parsed JSON or None.
    """
    headers = {
        'access-token': ACCESS_TOKEN or '',
        'client-id': CLIENT_ID or '',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    try:
        r = requests.post(DHAN_LTP_ENDPOINT, json=payload, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("Dhan LTP REST call failed:", e)
        return None

def format_poll_message(json_resp, symbol_map):
    """
    Build a readable Telegram message from Dhan LTP response.
    This logic is defensive and will try common response shapes.
    """
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    lines = []
    if not json_resp:
        return f"⏱ {now} — Poller: no response from Dhan"
    data = json_resp.get('data') if isinstance(json_resp, dict) else None
    if not data:
        # maybe flat map
        data = json_resp
    # Attempt to iterate segments -> ids -> info
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
                # map secid to readable symbol if we can
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
        return f"⏱ {now} — Poller: response received but no LTP fields found"
    return f"⏱ {now} — Poller snapshot:\\n" + "\\n".join(lines)

# ----------------------------
# Poller thread (every POLL_INTERVAL seconds)
# ----------------------------
class PollerThread(threading.Thread):
    def __init__(self, symbols, interval=POLL_INTERVAL):
        super().__init__(daemon=True)
        self.symbols = [s.strip() for s in symbols.split(',') if s.strip()]
        self.interval = interval
        # build a reverse symbol->id map for pretty output
        self.sym_map = {}
        for s in self.symbols:
            sid = get_security_id(s)
            if sid:
                self.sym_map[s.upper()] = sid

    def run(self):
        payload_map = build_security_payload(self.symbols)
        if not payload_map:
            print("[poller] No security IDs mapped — poller will still try if mapping changes.")
        while True:
            try:
                print("[poller] Calling Dhan LTP endpoint with payload:", payload_map)
                resp = call_dhan_ltp(payload_map)
                msg = format_poll_message(resp, self.sym_map)
                send_telegram(msg)
            except Exception as e:
                print("[poller] Exception:", e)
                traceback.print_exc()
                # send a small error message to telegram to alert you
                send_telegram(f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — Poller error: {e}")
            time.sleep(self.interval)

# ----------------------------
# WebSocket client (unchanged behavior) - also sends telegram on tick changes
# ----------------------------
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
# Launcher: start poller thread then start ws client
# ----------------------------
if __name__ == '__main__':
    print("Starting Dhan WebSocket + Poller bot. Poll interval (s):", POLL_INTERVAL)
    # Start poller thread
    poller = PollerThread(SYMBOLS, interval=POLL_INTERVAL)
    poller.start()
    # Start websocket client in main thread (Railway process keeps running)
    client = DhanWebsocketClient(WS_URL, SYMBOLS)
    try:
        client.run_forever()
    except KeyboardInterrupt:
        print("Stopping...")
        client.stop()
