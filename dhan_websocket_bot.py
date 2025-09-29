# dhan_websocket_bot.py
# WebSocket-based Dhan -> Telegram LTP streamer (Railway-ready)
# NOTE: Make sure to set environment variables on Railway: DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, WS_URL, SYMBOLS
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
# Config
# ----------------------------
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
WS_URL = os.getenv('WS_URL', 'wss://dhan.websocket.placeholder/marketfeed')
SYMBOLS = os.getenv('SYMBOLS', 'NIFTY,BANKNIFTY,SENSEX,RELIANCE,TCS,TATAMOTORS')
RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', '3'))
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '25'))

if not CLIENT_ID or not ACCESS_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("Warning: some environment variables are not set. Set DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID on Railway.")

# ----------------------------
# Telegram helper
# ----------------------------
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"
def send_telegram(text: str):
    url = TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}
    try:
        r = requests.post(url, data=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send error:", e)

# ----------------------------
# Build auth & subscribe payloads
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

# ----------------------------
# Helper to map symbols to (segment,id)

# ----------------------------
def build_security_list(symbols):
    out = []
    for s in symbols:
        sid = get_security_id(s)
        if not sid:
            print(f"[warn] security id not found for symbol: {s}")
            continue
        if s.upper() in ('NIFTY', 'NIFTY 50', 'BANKNIFTY', 'NIFTY BANK'):
            seg = 'NSE_INDEX'
        elif s.upper() == 'SENSEX':
            seg = 'BSE_INDEX'
        else:
            seg = 'NSE_EQ'
        out.append((seg, str(sid)))
    return out

# ----------------------------
# WebSocket client

# ----------------------------
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

            key = f"{readable}:{sid}" if symbol else readable
            prev = self.last_ltps.get(key)
            self.last_ltps[key] = ltp
            text = f"⏱ {time.strftime('%Y-%m-%d %H:%M:%S')} — <b>{readable}</b> LTP: <code>{ltp}</code>"
            if prev != ltp:
                send_telegram(text)
                print("Sent telegram:", text)

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

if __name__ == '__main__':
    print("Starting Dhan WebSocket -> Telegram bot (Railway-ready). Configure env vars in Railway project settings.")
    client = DhanWebsocketClient(WS_URL, SYMBOLS)
    try:
        client.run_forever()
    except KeyboardInterrupt:
        print("Stopping...")
        client.stop()
