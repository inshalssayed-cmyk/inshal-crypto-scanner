import os, sys, time, json, threading, requests
from flask import Flask, jsonify

app = Flask(__name__)

# CONFIG
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

KUCOIN = "https://api.kucoin.com/api/v1"
TICKERS_URL = f"{KUCOIN}/market/allTickers"
PRICE_URL = f"{KUCOIN}/market/orderbook/level1"
KLINES_URL = f"{KUCOIN}/market/candles"

THRESHOLD = 85
TP1, TP2, SL = 6.0, 12.0, 5.0
SCAN_SEC = 900
TRACK_SEC = 180

# STATE
scan_count = 0
scan_lock = threading.Lock()
running = False
positions = {}
pos_lock = threading.Lock()
watchlist = {}
wl_lock = threading.Lock()
last_alerted = {}

CARTEL = {"ORCA", "GUN", "ACE", "METIS", "SOLV", "ZAMA", "APE", "TNSR", "RARE", "PHA", "CHR"}
SMART = {"ORCA", "GUN", "ACE", "METIS", "SOLV", "ZAMA", "APE", "TNSR", "RARE", "PHA", "CHR", "JUP", "RAY"}
SECTORS = {"GUN": "Gaming", "ACE": "Gaming", "ORCA": "Solana", "METIS": "L2", "SOLV": "DeFi"}
HOT = {"Gaming", "Solana", "L2", "DeFi"}

# TELEGRAM
def tg(msg):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(msg), 3900):
        try:
            requests.post(url, data={"chat_id": int(CHAT_ID), "text": msg[i:i+3900], "parse_mode": "HTML"}, timeout=15)
        except: pass

def fmt_price(p):
    if p >= 1: return f"${p:.4f}"
    if p >= 0.01: return f"${p:.5f}"
    return f"${p:.8f}"

def base(s): return s.replace("-USDT", "")
def get_sector(s): return SECTORS.get(base(s), "Unknown")

# HTTP
def get_price(symbol):
    try:
        r = requests.get(PRICE_URL, params={"symbol": symbol}, timeout=10)
        if r.json().get("code") == "200000": return float(r.json()["data"]["price"])
    except: pass
    return None

def get_klines(symbol, tf="1hour", limit=12):
    try:
        r = requests.get(KLINES_URL, params={"symbol": symbol, "type": tf, "startAt": int(time.time()) - limit*3600}, timeout=10)
        if r.json().get("code") == "200000": return r.json().get("data", [])
    except: pass
    return []

def get_support(symbol):
    klines = get_klines(symbol, "1hour", 24)
    if not klines: return None
    lows = [float(k[4]) for k in klines[-12:]]
    return min(lows) if lows else None

# MOMENTUM
def analyze_momentum(symbol):
    klines = get_klines(symbol, "1hour", 12)
    if len(klines) < 3: return 0, "No data"
    
    closes = [float(k[2]) for k in klines[-6:]]
    volumes = [float(k[5]) for k in klines[-6:]]
    
    score = 0
    if closes[-1] > closes[0] * 1.01: score += 30
    if sum(volumes[-3:]) > sum(volumes[-6:-3]) * 1.3: score += 30
    if max(closes[-3:]) - min(closes[-3:]) < max(closes[-6:-3]) - min(closes[-6:-3]): score += 20
    
    return min(score, 100), f"Mom: {score}"

# SCORING
def score_coin(change, vol, rs, sector, b, symbol):
    score = 0
    if -2 <= change <= 5: score += 25
    elif 5 < change <= 8: score += 10
    if vol >= 10_000_000: score += 25
    elif vol >= 5_000_000: score += 20
    elif vol >= 1_000_000: score += 12
    if rs >= 4: score += 20
    elif rs >= 2: score += 15
    elif rs >= 0: score += 8
    if sector in HOT: score += 15
    if b in CARTEL: score += 10
    if b in SMART: score += 8
    return min(score, 100)

# WATCHLIST
def add_wl(symbol, price, score, sector):
    with wl_lock:
        watchlist[symbol] = {"price": price, "score": score, "sector": sector, "added": time.time()}

def check_wl():
    with wl_lock:
        syms = list(watchlist.keys())
    
    for sym in syms:
        try:
            cur = get_price(sym)
            if not cur: continue
            support = get_support(sym)
            if not support: continue
            if (cur - support) / support * 100 > 2: continue
            
            mom, reason = analyze_momentum(sym)
            if mom < 70: continue
            
            tp1 = cur * 1.06
            tp2 = cur * 1.12
            sl = cur * 0.95
            
            tg(f"🚀 <b>BUY SIGNAL</b>\n\n<b>{sym}</b>\n💰 Entry: {fmt_price(cur)}\n🎯 TP1: {fmt_price(tp1)}\n🎯 TP2: {fmt_price(tp2)}\n🛑 SL: {fmt_price(sl)}\n\n⚡ Momentum: {mom}/100\n✅ All factors aligned. Enter now.")
            
            with pos_lock:
                positions[sym] = {"entry": cur, "tp1": tp1, "tp2": tp2, "sl": sl, "time": time.time(), "hit": 0}
            
            with wl_lock:
                del watchlist[sym]
        except: pass

# POSITIONS
def check_pos():
    with pos_lock:
        syms = list(positions.keys())
    
    to_del = []
    for sym in syms:
        p = positions[sym]
        cur = get_price(sym)
        if not cur: continue
        
        pnl = (cur - p["entry"]) / p["entry"] * 100
        
        if cur >= p["tp2"] and p["hit"] != 2:
            with pos_lock: positions[sym]["hit"] = 2
            tg(f"🚀 <b>TARGET 2 HIT!</b>\n\n<b>{sym}</b>\n💰 Entry: {fmt_price(p['entry'])}\n📍 Exit: {fmt_price(cur)}\n📈 Profit: +{pnl:.2f}%")
            to_del.append(sym)
        elif cur >= p["tp1"] and p["hit"] != 1:
            with pos_lock: positions[sym]["hit"] = 1
            tg(f"✅ <b>TARGET 1 HIT!</b>\n\n<b>{sym}</b>\n💰 Entry: {fmt_price(p['entry'])}\n📍 Current: {fmt_price(cur)}\n📈 Profit: +{pnl:.2f}%")
        elif cur <= p["sl"]:
            tg(f"🛑 <b>STOP LOSS HIT</b>\n\n<b>{sym}</b>\n💰 Entry: {fmt_price(p['entry'])}\n📍 Exit: {fmt_price(cur)}\n📉 Loss: {pnl:.2f}%")
            to_del.append(sym)
        elif time.time() - p["time"] > 172800:
            tg(f"⏰ <b>EXPIRED (48h)</b>\n\n<b>{sym}</b>\n{pnl:.2f}%")
            to_del.append(sym)
    
    with pos_lock:
        for s in to_del:
            if s in positions: del positions[s]

# SCAN
def scan():
    global scan_count
    with scan_lock:
        scan_count += 1
        current = scan_count
    
    try:
        r = requests.get(TICKERS_URL, timeout=20)
        if r.json().get("code") != "200000": return
        
        data = r.json()["data"]["ticker"]
        market = {}
        for coin in data:
            sym = coin.get("symbol", "")
            if not sym.endswith("-USDT"): continue
            try:
                market[sym] = {"price": float(coin["last"]), "change": float(coin["changeRate"])*100, "volume": float(coin["volValue"])}
            except: continue
        
        btc_ch = market.get("BTC-USDT", {}).get("change", 0)
        candidates = []
        
        for sym, info in market.items():
            b = base(sym)
            if any(x in b for x in ["3L", "3S", "UP", "DOWN"]): continue
            p, ch, vol = info["price"], info["change"], info["volume"]
            if p <= 0 or vol < 3_000_000 or not (-3 <= ch <= 8): continue
            
            rs = ch - btc_ch
            sector = get_sector(sym)
            score = score_coin(ch, vol, rs, sector, b, sym)
            
            if score >= 72:
                candidates.append({"symbol": sym, "score": score, "price": p, "sector": sector})
        
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        strict = [c for c in candidates[:15] if c["score"] >= THRESHOLD]
        
        for c in strict:
            if c["symbol"] not in last_alerted:
                tg(f"🔍 <b>COIN ADDED TO WATCHLIST</b>\n\n<b>{c['symbol']}</b>\n⭐ Score: {c['score']}/100\n💰 Price: {fmt_price(c['price'])}\n🧩 Sector: {c['sector']}\n\n⏰ Waiting for optimal entry (support + momentum)...")
                add_wl(c["symbol"], c["price"], c["score"], c["sector"])
                last_alerted[c["symbol"]] = time.time()
        
        print(f"Scan {current} | Elite: {len(strict)}")
    except Exception as e:
        print(f"Scan error: {e}")

def handle_command(cmd):
    if cmd == "/status":
        with pos_lock: active = len(positions)
        with wl_lock: wl = len(watchlist)
        tg(f"🤖 <b>Inshal Crypto Scanner v9-Minimal</b>\n\n✅ Running: True\n🔢 Scans: {scan_count}\n📡 Active Positions: {active}\n👁 Coins in Watchlist: {wl}\n⚡ Momentum Detection: ON\n🎯 TP1: +6%  TP2: +12%  SL: -5%")
    elif cmd == "/help":
        tg("🤖 <b>Commands</b>\n\n/status — Scanner health\n/help — This message")

def telegram_thread():
    if not BOT_TOKEN or not CHAT_ID: return
    print("Telegram listener started")
    last_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": last_id + 1, "timeout": 30}, timeout=35)
            updates = r.json().get("result", [])
            for upd in updates:
                last_id = upd["update_id"]
                msg = upd.get("message", {})
                text = msg.get("text", "").strip()
                cid = str(msg.get("chat", {}).get("id", ""))
                if cid == str(CHAT_ID) and text.startswith("/"):
                    print(f"Command: {text}")
                    handle_command(text)
        except: pass
        time.sleep(10)

def scanner_thread():
    print("Scanner started")
    tg("🟢 <b>Inshal Crypto Scanner v9-Minimal — STARTED</b>\n\n✅ Two-stage alerting active\n🎯 Score ≥85 + Breakout Momentum\n⚡ Momentum Threshold: 70/100\n🎯 TP1: +6%  TP2: +12%  SL: -5%\n\nAlert #1: Coin added to watchlist\nAlert #2: Price + momentum = BUY NOW\n\n95% accuracy target. In Sha Allah. 🚀")
    while True:
        try:
            scan()
            check_wl()
            check_pos()
        except: pass
        time.sleep(SCAN_SEC)

# FLASK
@app.route("/")
def home():
    with pos_lock: active = len(positions)
    with wl_lock: wl = len(watchlist)
    return f"v9-Minimal | Scans: {scan_count} | Active: {active} | Watchlist: {wl}"

@app.route("/health")
def health():
    global running
    if not running:
        running = True
        threading.Thread(target=scanner_thread, daemon=True).start()
        threading.Thread(target=telegram_thread, daemon=True).start()
    return "OK"

@app.route("/status")
def status():
    with pos_lock: active = len(positions)
    with wl_lock: wl = len(watchlist)
    return jsonify({"running": running, "scans": scan_count, "active": active, "watchlist": wl})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
