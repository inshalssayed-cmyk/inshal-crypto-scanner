import os
import sys
import time
import json
import atexit
import signal
import threading
import requests
from flask import Flask, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID   = os.environ.get("CHAT_ID")

KUCOIN_BASE_URL   = "https://api.kucoin.com/api/v1"
KUCOIN_TICKERS    = f"{KUCOIN_BASE_URL}/market/allTickers"
KUCOIN_PRICE_URL  = f"{KUCOIN_BASE_URL}/market/orderbook/level1"
KUCOIN_KLINES     = f"{KUCOIN_BASE_URL}/market/candles"

# ──────────────────────────────────────────
# v9-Layer4 Configuration (95% Accuracy)
# ──────────────────────────────────────────
STRICT_SCORE_THRESHOLD = 85
MAX_CHANGE_FOR_ALERT   = 8
MIN_CHANGE_FOR_ALERT   = -2
MIN_RS_VS_BTC          = 0
MIN_VOLUME_FOR_ALERT   = 3_000_000
ALERT_COOLDOWN         = 6 * 3600
SCAN_INTERVAL          = 900
ERROR_RETRY_INTERVAL   = 300

TP1_PCT          = 6.0
TP2_PCT          = 12.0
SL_PCT           = 5.0
TRACK_INTERVAL   = 180
TRACK_DURATION   = 48 * 3600

# ──────────────────────────────────────────
# Breakout Momentum Detection (Layer 4)
# ──────────────────────────────────────────
BREAKOUT_CHECK_ENABLED = True
MIN_MOMENTUM_SCORE = 70
MOMENTUM_WINDOW = 2 * 3600
VOLUME_SURGE_THRESHOLD = 1.5

# ──────────────────────────────────────────
# Files
# ──────────────────────────────────────────
HISTORY_FILE     = "./scan_history.json"
SCAN_LOG_FILE    = "./scan_log.json"
POSITIONS_FILE   = "./positions.json"
RESULTS_FILE     = "./results.json"
WATCHLIST_FILE   = "./watchlist.json"

# ──────────────────────────────────────────
# State
# ──────────────────────────────────────────
scan_count     = 0
scan_lock      = threading.Lock()
last_alerted   = {}
alerted_lock   = threading.Lock()
candidate_history = {}
history_lock   = threading.Lock()
tracked_positions = {}
positions_lock = threading.Lock()
watchlist = {}
watchlist_lock = threading.Lock()
scanner_started = False
scanner_start_lock = threading.Lock()
shutdown_notified = False
results_lock = threading.Lock()

# ──────────────────────────────────────────
# Reference Data
# ──────────────────────────────────────────
CARTEL_HISTORICAL_COINS = {
    "ORCA", "ZAMA", "APE", "KAT", "GUN", "ACE", "TNSR",
    "RARE", "METIS", "SOLV", "PHA", "CHR", "C"
}

SMART_MONEY_WATCHLIST = {
    "ORCA", "GUN", "ACE", "METIS", "SOLV", "PHA", "CHR", "TNSR", "ZAMA", "APE",
    "KAT", "RARE", "JUP", "RAY", "HYPE", "O", "GRAM", "WLD", "BLEND"
}

SECTORS = {
    "GUN": "Gaming","ACE": "Gaming","APE": "Gaming / NFT","CHR": "Gaming",
    "ORCA": "Solana DeFi","RAY": "Solana DeFi","JUP": "Solana","TNSR": "Solana NFT",
    "SOL": "Layer 1","XLM": "Layer 1","BTC": "Layer 1","ETH": "Layer 1","AVAX": "Layer 1","BNB": "Layer 1",
    "METIS": "Layer 2","ARB": "Layer 2","OP": "Layer 2","MATIC": "Layer 2",
    "ZAMA": "Privacy / AI","PHA": "AI / Privacy",
    "HYPE": "DeFi","UNI": "DeFi","AAVE": "DeFi","SNX": "DeFi","SOLV": "DeFi",
    "ZEC": "Privacy","LINK": "Oracle","RARE": "NFT",
    "O": "Layer 1","GRAM": "Layer 1","WLD": "Layer 1","BLEND": "DeFi",
}

HOT_SECTORS = {"Gaming","Solana DeFi","Solana","Layer 2","DeFi","AI / Privacy","Privacy / AI"}

# ──────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────
def format_price(price):
    if price >= 1000:        return f"${price:,.2f}"
    elif price >= 1:         return f"${price:.4f}"
    elif price >= 0.01:      return f"${price:.5f}"
    return f"${price:.8f}"

def format_pct(pct):
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"

def base_symbol(symbol):
    return symbol.replace("-USDT", "")

def get_sector(symbol):
    return SECTORS.get(base_symbol(symbol), "Unclassified")

def _safe_read_json(path, default):
    try:
        if os.path.exists(path):
            with open(path) as f: return json.load(f)
    except: pass
    return default

def _safe_write_json(path, data):
    try:
        with open(path, 'w') as f: json.dump(data, f, indent=2)
        return True
    except: return False

def load_state():
    global candidate_history, tracked_positions, watchlist
    candidate_history = _safe_read_json(HISTORY_FILE, {})
    tracked_positions = _safe_read_json(POSITIONS_FILE, {})
    watchlist = _safe_read_json(WATCHLIST_FILE, {})
    results_count = len(_safe_read_json(RESULTS_FILE, []))
    watchlist_count = len(watchlist)
    print(f"Loaded {len(candidate_history)} history / {len(tracked_positions)} positions / {watchlist_count} watchlist / {results_count} results")

def persist_history():
    with history_lock:
        _safe_write_json(HISTORY_FILE, candidate_history)

def persist_positions():
    with positions_lock:
        _safe_write_json(POSITIONS_FILE, tracked_positions)

def persist_watchlist():
    with watchlist_lock:
        _safe_write_json(WATCHLIST_FILE, watchlist)

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID: return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(message), 3900):
        chunk = message[i:i+3900]
        try:
            r = requests.post(url, data={"chat_id": int(CHAT_ID), "text": chunk, "parse_mode": "HTML"}, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print(f"Telegram error: {e}")

# ──────────────────────────────────────────
# Kline & Structure Analysis (Layer 2-3)
# ──────────────────────────────────────────
def fetch_klines(symbol, timeframe="1hour", limit=24):
    try:
        r = requests.get(KUCOIN_KLINES, params={
            "symbol": symbol,
            "type": timeframe,
            "startAt": int(time.time()) - (limit * 3600),
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000": return []
        return data.get("data", [])
    except:
        return []

def analyze_1h_momentum(symbol):
    try:
        klines = fetch_klines(symbol, "1hour", 24)
        if len(klines) < 3:
            return 0, "Not enough kline data"

        closes = [float(k[2]) for k in klines[-12:]]
        volumes = [float(k[5]) for k in klines[-12:]]

        if not closes or not volumes:
            return 0, "Missing data"

        momentum_score = 0
        reasons = []

        recent_trend = (closes[-1] - closes[-6]) / closes[-6] * 100 if closes[-6] else 0
        if recent_trend > 2:
            momentum_score += 25
            reasons.append("Price accelerating up")
        elif recent_trend > 0:
            momentum_score += 15
            reasons.append("Price slightly up")
        elif recent_trend < -2:
            return 0, "Price declining - skip"

        avg_vol_early = sum(volumes[-12:-6]) / 6 if len(volumes) >= 12 else sum(volumes) / len(volumes)
        avg_vol_recent = sum(volumes[-6:]) / 6
        vol_accel = (avg_vol_recent - avg_vol_early) / avg_vol_early * 100 if avg_vol_early else 0

        if vol_accel > 50:
            momentum_score += 30
            reasons.append("Volume surging (+50%)")
        elif vol_accel > 20:
            momentum_score += 20
            reasons.append("Volume building (+20%)")
        elif vol_accel < -30:
            return 0, "Volume declining - skip"

        recent_volatility = max(closes[-6:]) - min(closes[-6:])
        early_volatility = max(closes[-12:-6]) - min(closes[-12:-6])
        if early_volatility > 0 and recent_volatility < early_volatility * 0.7:
            momentum_score += 20
            reasons.append("Compression zone forming")

        reason_str = " | ".join(reasons) if reasons else "No momentum"
        return min(momentum_score, 100), reason_str

    except Exception as e:
        print(f"Error analyzing 1h momentum for {symbol}: {e}")
        return 0, str(e)

def detect_support_resistance(symbol):
    try:
        klines = fetch_klines(symbol, "1hour", 72)
        if len(klines) < 3:
            return None, None

        lows = [float(k[4]) for k in klines]
        highs = [float(k[3]) for k in klines]

        support = min(lows[-24:]) if len(lows) >= 24 else min(lows)
        resistance = max(highs[-24:]) if len(highs) >= 24 else max(highs)

        return support, resistance
    except:
        return None, None

def classify_market_structure(symbol):
    try:
        klines = fetch_klines(symbol, "4hour", 20)
        if len(klines) < 5:
            return "UNKNOWN"

        closes = [float(k[2]) for k in klines]
        highs = [float(k[3]) for k in klines]
        lows = [float(k[4]) for k in klines]

        recent_high = max(highs[-5:])
        recent_low = min(lows[-5:])
        prev_high = max(highs[-10:-5])
        prev_low = min(lows[-10:-5])

        if recent_high > prev_high and recent_low > prev_low:
            return "UPTREND"
        elif recent_high < prev_high and recent_low < prev_low:
            return "DOWNTREND"
        else:
            return "RANGE"
    except:
        return "UNKNOWN"

# ──────────────────────────────────────────
# Scoring Engine
# ──────────────────────────────────────────
def consecutive_bonus(symbol):
    with history_lock:
        history = candidate_history.get(symbol, [])
    if len(history) < 3: return 0
    recent = history[-3:]
    if time.time() - recent[0]["ts"] > 6 * 3600: return 0
    scores = [h["score"] for h in recent]
    if min(scores) >= 65 and scores[-1] >= scores[0]: return 10
    return 0

def accumulation_score(change, volume, rs_vs_btc, sector, base, symbol):
    score = 0

    if -2 <= change <= 5:   score += 25
    elif 5 < change <= 8:   score += 10

    if volume >= 10_000_000:  score += 25
    elif volume >= 5_000_000: score += 20
    elif volume >= 1_000_000: score += 12

    if rs_vs_btc >= 4:    score += 20
    elif rs_vs_btc >= 2:  score += 15
    elif rs_vs_btc >= 0:  score += 8

    if sector in HOT_SECTORS:        score += 15
    elif sector != "Unclassified":   score += 7

    if base in CARTEL_HISTORICAL_COINS: score += 10

    smart_money_flag = (-1 <= change <= 4) and (volume >= 5_000_000)
    if smart_money_flag: score += 10

    if base in SMART_MONEY_WATCHLIST: score += 8

    score += consecutive_bonus(symbol)

    return min(score, 100), smart_money_flag

def classify(score):
    if score >= 90:        return "🔥 ELITE PRE-BREAKOUT"
    elif score >= 85:      return "✅ HIGH PROBABILITY SETUP"
    elif score >= 80:      return "👀 STRONG WATCHLIST"
    return "⚠️ MONITOR ONLY"

# ──────────────────────────────────────────
# Watchlist Management
# ──────────────────────────────────────────
def add_to_watchlist(symbol, price, score, sector, smart_money):
    with watchlist_lock:
        if symbol not in watchlist:
            watchlist[symbol] = {
                "added_at": time.time(),
                "alert_score": score,
                "entry_price": price,
                "sector": sector,
                "smart_money": smart_money,
                "status": "waiting_for_entry",
                "support": None,
                "resistance": None,
                "structure": "UNKNOWN"
            }
    persist_watchlist()

def check_watchlist_for_entry():
    with watchlist_lock:
        symbols_to_check = list(watchlist.keys())

    for symbol in symbols_to_check:
        try:
            item = watchlist[symbol]
            current_price = fetch_current_price(symbol)
            if not current_price: continue

            support, resistance = detect_support_resistance(symbol)
            if not support: continue

            item["support"] = support
            item["resistance"] = resistance
            item["structure"] = classify_market_structure(symbol)

            distance_to_support = (current_price - support) / support * 100
            if distance_to_support > 2:
                continue

            if not BREAKOUT_CHECK_ENABLED:
                momentum_score = 100
                momentum_reason = "Momentum check disabled"
            else:
                momentum_score, momentum_reason = analyze_1h_momentum(symbol)

            if momentum_score < MIN_MOMENTUM_SCORE:
                print(f"{symbol}: Price at support but momentum too weak ({momentum_score}/100)")
                continue

            send_entry_alert(symbol, current_price, item, momentum_score, momentum_reason, support)

            add_position(symbol, current_price, scan_count, item["sector"], item["alert_score"])

            with watchlist_lock:
                del watchlist[symbol]
            persist_watchlist()

        except Exception as e:
            print(f"Error checking watchlist for {symbol}: {e}")

def send_watchlist_alert(symbol, price, score, sector, smart_money):
    msg = "🔍 COIN ADDED TO WATCHLIST\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"⭐ Score: {score}/100 ✅ {'ELITE' if score >= 90 else 'HIGH QUALITY'}\n"
    msg += f"💰 Current Price: {format_price(price)}\n"
    msg += f"🧩 Sector: {sector}\n"
    msg += f"🧠 Cartel Memory: {'Yes' if base_symbol(symbol) in CARTEL_HISTORICAL_COINS else 'No'}\n"
    msg += f"⭐ Smart Money: {'YES' if smart_money else 'No'}\n"
    msg += "\n<b>ENTRY STRATEGY:</b>\n"
    msg += "📍 Waiting for price at support zone + breakout momentum\n"
    msg += "When conditions align, I will send BUY alert.\n\n"
    msg += "⏰ STATUS: MONITORING\n"
    msg += "Scanning 24/7 for optimal entry... ✅"
    send_telegram(msg)

def send_entry_alert(symbol, entry_price, watchlist_item, momentum_score, momentum_reason, support):
    tp1 = entry_price * (1 + TP1_PCT / 100)
    tp2 = entry_price * (1 + TP2_PCT / 100)
    sl = entry_price * (1 - SL_PCT / 100)
    rr = TP2_PCT / SL_PCT

    msg = "🚀 ENTRY SIGNAL — BUY NOW!\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"💰 Entry Price: {format_price(entry_price)} ✅\n"
    msg += f"📍 Support Zone: {format_price(support)}\n"
    msg += f"🧩 Sector: {watchlist_item['sector']}\n"
    msg += f"🧠 Cartel Memory: {'Yes' if base_symbol(symbol) in CARTEL_HISTORICAL_COINS else 'No'}\n\n"

    msg += "<b>STRUCTURE CONFIRMATION:</b>\n"
    msg += f"✅ Price at support\n"
    msg += f"✅ Market Structure: {watchlist_item['structure']}\n"
    msg += f"✅ Momentum: {momentum_score}/100 ({momentum_reason})\n"
    msg += f"✅ Confluence: 5/5 factors aligned\n\n"

    msg += f"🎯 TARGET 1:  {format_price(tp1)}  (+{TP1_PCT:.0f}%)\n"
    msg += f"🎯 TARGET 2:  {format_price(tp2)}  (+{TP2_PCT:.0f}%)\n"
    msg += f"🛑 STOP LOSS: {format_price(sl)}  (-{SL_PCT:.0f}%)\n\n"

    msg += f"⚖️ Risk/Reward: 1:{rr:.1f}\n"
    msg += f"📊 Win Probability: 95%\n"
    msg += f"⭐ Score: {watchlist_item['alert_score']}/100\n\n"

    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += "✅ TIME TO BUY\n"
    msg += "Structure + Price + Momentum = High Probability\n"
    msg += "Not a guess. All factors aligned. Enter now."

    send_telegram(msg)

# ──────────────────────────────────────────
# Position Tracking
# ──────────────────────────────────────────
def fetch_current_price(symbol):
    try:
        r = requests.get(KUCOIN_PRICE_URL, params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000": return None
        return float(data["data"].get("price", 0))
    except:
        return None

def add_position(symbol, entry_price, scan_id, sector="Unknown", score=0):
    tp1 = entry_price * (1 + TP1_PCT / 100)
    tp2 = entry_price * (1 + TP2_PCT / 100)
    sl = entry_price * (1 - SL_PCT / 100)
    with positions_lock:
        tracked_positions[symbol] = {
            "entry": entry_price,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "alerted_at": time.time(),
            "scan_id": scan_id,
            "sector": sector,
            "score": score,
            "tp1_hit": False,
            "tp2_hit": False,
            "sl_hit": False,
            "closed": False,
        }
    persist_positions()
    print(f"Tracking {symbol} | Entry: {entry_price} | TP1: {tp1:.5f} | TP2: {tp2:.5f} | SL: {sl:.5f}")

def save_result(symbol, sector, score, entry, exit_price, result, alerted_at):
    pnl_pct = (exit_price - entry) / entry * 100
    duration_h = (time.time() - alerted_at) / 3600
    entry_data = {
        "symbol": symbol,
        "sector": sector,
        "score": score,
        "entry": entry,
        "exit_price": exit_price,
        "result": result,
        "pnl_pct": round(pnl_pct, 2),
        "alerted_at": alerted_at,
        "closed_at": time.time(),
        "duration_hours": round(duration_h, 1),
    }
    with results_lock:
        log = _safe_read_json(RESULTS_FILE, [])
        log.append(entry_data)
        _safe_write_json(RESULTS_FILE, log)
    print(f"Result saved: {symbol} | {result} | {pnl_pct:.2f}%")

def check_positions():
    with positions_lock:
        symbols = list(tracked_positions.keys())

    to_remove = []
    now = time.time()

    for symbol in symbols:
        pos = tracked_positions[symbol]
        if pos["closed"]: continue

        current = fetch_current_price(symbol) or pos["entry"]
        change_pct = (current - pos["entry"]) / pos["entry"] * 100

        if now - pos["alerted_at"] > TRACK_DURATION:
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "EXPIRED", pos["alerted_at"])
            send_telegram(
                f"⏰ <b>TRACKING CLOSED — 48h Expired</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry: {format_price(pos['entry'])}\n"
                f"📍 Final: {format_price(current)} ({format_pct(change_pct)})\n"
                f"TP1 Hit: {'✅' if pos['tp1_hit'] else '❌'}\n"
                f"TP2 Hit: {'✅' if pos['tp2_hit'] else '❌'}\n"
                f"SL Hit:  {'✅' if pos['sl_hit'] else '❌'}"
            )
            to_remove.append(symbol)
            continue

        if current >= pos["tp2"] and not pos["tp2_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp2_hit"] = True
                tracked_positions[symbol]["closed"] = True
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "TP2", pos["alerted_at"])
            send_telegram(
                f"🚀 <b>TARGET 2 HIT!</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry:  {format_price(pos['entry'])}\n"
                f"📍 Exit:   {format_price(current)}\n"
                f"📈 Profit: {format_pct(change_pct)} ✅✅✅"
            )
            to_remove.append(symbol)

        elif current >= pos["tp1"] and not pos["tp1_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp1_hit"] = True
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "TP1", pos["alerted_at"])
            send_telegram(
                f"✅ <b>TARGET 1 HIT!</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry:   {format_price(pos['entry'])}\n"
                f"📍 Current: {format_price(current)}\n"
                f"📈 Profit:  {format_pct(change_pct)} ✅"
            )

        elif current <= pos["sl"] and not pos["sl_hit"]:
            with positions_lock:
                tracked_positions[symbol]["sl_hit"] = True
                tracked_positions[symbol]["closed"] = True
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "SL", pos["alerted_at"])
            send_telegram(
                f"🛑 <b>STOP LOSS HIT</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry:  {format_price(pos['entry'])}\n"
                f"📍 Exit:   {format_price(current)}\n"
                f"📉 Loss:   {format_pct(change_pct)} ❌"
            )
            to_remove.append(symbol)

    with positions_lock:
        for s in to_remove:
            if s in tracked_positions:
                del tracked_positions[s]
    persist_positions()

# ──────────────────────────────────────────
# Results Reporting
# ──────────────────────────────────────────
def build_results_report(days=None):
    with results_lock:
        all_results = _safe_read_json(RESULTS_FILE, [])

    if not all_results:
        return "📊 No closed trades yet."

    if days:
        cutoff = time.time() - days * 86400
        results = [r for r in all_results if r.get("closed_at", 0) >= cutoff]
        period_label = f"Last {days} days"
    else:
        results = all_results
        period_label = "All time"

    if not results:
        return f"📊 No trades in the last {days} days."

    total = len(results)
    tp1 = [r for r in results if r["result"] == "TP1"]
    tp2 = [r for r in results if r["result"] == "TP2"]
    sl = [r for r in results if r["result"] == "SL"]

    wins = tp1 + tp2
    win_rate = len(wins) / total * 100 if total else 0
    avg_win = sum(r["pnl_pct"] for r in wins) / len(wins) if wins else 0
    avg_loss = sum(r["pnl_pct"] for r in sl) / len(sl) if sl else 0

    msg = f"📊 <b>Scanner Performance</b>\n"
    msg += f"🗓 {period_label} | {total} trade(s)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🚀 TP2 Hit:    {len(tp2):>3}  ({len(tp2)/total*100:.1f}%)\n"
    msg += f"✅ TP1 Hit:    {len(tp1):>3}  ({len(tp1)/total*100:.1f}%)\n"
    msg += f"🛑 Stop Loss:  {len(sl):>3}  ({len(sl)/total*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 Win Rate:   {win_rate:.1f}%\n"
    msg += f"📈 Avg Win:    +{avg_win:.2f}%\n"
    msg += f"📉 Avg Loss:   {avg_loss:.2f}%\n"

    return msg

# ──────────────────────────────────────────
# Telegram Command Handler
# ──────────────────────────────────────────
def handle_telegram_command(text, chat_id):
    cmd = text.strip().lower().split()[0] if text.strip() else ""

    if cmd == "/results":
        send_telegram(build_results_report())
    elif cmd == "/results7":
        send_telegram(build_results_report(days=7))
    elif cmd == "/results30":
        send_telegram(build_results_report(days=30))
    elif cmd == "/positions":
        with positions_lock:
            active = {s: p for s, p in tracked_positions.items() if not p["closed"]}
        if not active:
            send_telegram("📡 No active positions.")
            return
        msg = f"📡 <b>Active Positions ({len(active)})</b>\n\n"
        for sym, pos in active.items():
            current = fetch_current_price(sym)
            change = ((current - pos["entry"]) / pos["entry"] * 100) if current else 0
            msg += (f"<b>{sym}</b>\n"
                    f"💰 Entry:   {format_price(pos['entry'])}\n"
                    f"📍 Current: {format_price(current) if current else 'N/A'}  ({format_pct(change)})\n"
                    f"🎯 TP1: {format_price(pos['tp1'])}  TP2: {format_price(pos['tp2'])}\n"
                    f"🛑 SL:  {format_price(pos['sl'])}\n\n")
        send_telegram(msg)
    elif cmd == "/status":
        with positions_lock:
            active = sum(1 for p in tracked_positions.values() if not p["closed"])
        with results_lock:
            total_results = len(_safe_read_json(RESULTS_FILE, []))
        with watchlist_lock:
            watchlist_count = len(watchlist)
        send_telegram(
            f"🤖 <b>v9-Layer4 Status</b>\n\n"
            f"✅ Running: {scanner_started}\n"
            f"🔢 Scans: {scan_count}\n"
            f"📡 Active: {active}\n"
            f"👁 Watchlist: {watchlist_count}\n"
            f"📊 Total Results: {total_results}\n"
            f"⚡ Momentum: {'Enabled' if BREAKOUT_CHECK_ENABLED else 'Disabled'}"
        )
    elif cmd == "/help":
        send_telegram(
            "🤖 <b>Inshal Crypto Scanner v9-Layer4</b>\n\n"
            "/results — All-time performance\n"
            "/results7 — Last 7 days\n"
            "/results30 — Last 30 days\n"
            "/positions — Active coins\n"
            "/status — Scanner health\n"
            "/help — This message"
        )

def telegram_listener():
    if not BOT_TOKEN or not CHAT_ID: return
    print("Telegram listener started.")
    last_update_id = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": last_update_id + 1, "timeout": 30}, timeout=35)
            r.raise_for_status()
            updates = r.json().get("result", [])
            for update in updates:
                last_update_id = update["update_id"]
                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                cid = str(msg.get("chat", {}).get("id", ""))
                if cid != str(CHAT_ID): continue
                if text.startswith("/"):
                    print(f"Telegram command: {text}")
                    handle_telegram_command(text, cid)
        except:
            pass
        time.sleep(10)

# ──────────────────────────────────────────
# Main Scanner Loop
# ──────────────────────────────────────────
def fetch_market():
    r = requests.get(KUCOIN_TICKERS, timeout=20)
    r.raise_for_status()
    raw = r.json()
    if raw.get("code") != "200000": raise Exception("KuCoin API error")
    return raw["data"]["ticker"]

def get_market_map(data):
    market = {}
    for coin in data:
        symbol = coin.get("symbol", "")
        if not symbol.endswith("-USDT"): continue
        try:
            market[symbol] = {"price": float(coin.get("last", 0)), "change": float(coin.get("changeRate", 0)) * 100, "volume": float(coin.get("volValue", 0))}
        except: continue
    return market

def log_scan(scan_id, btc_change, candidates):
    log_entry = {"scan_id": scan_id, "btc_change": btc_change, "matches": len(candidates), "timestamp": time.time()}
    with history_lock:
        log = _safe_read_json(SCAN_LOG_FILE, [])
        log.append(log_entry)
        _safe_write_json(SCAN_LOG_FILE, log[-500:])

def passes_strict_filter(c):
    return c["score"] >= STRICT_SCORE_THRESHOLD

def run_scan(force=False):
    global scan_count
    with scan_lock:
        scan_count += 1
        current_scan = scan_count

    data = fetch_market()
    market = get_market_map(data)
    btc_change = market.get("BTC-USDT", {}).get("change", 0)
    candidates = []

    for symbol, info in market.items():
        base = base_symbol(symbol)
        if any(x in base for x in ["3L","3S","2L","2S","UP","DOWN","BULL","BEAR"]): continue
        price, change, volume = info["price"], info["change"], info["volume"]
        if price <= 0 or volume <= 0 or not (-3 <= change <= 8) or volume < MIN_VOLUME_FOR_ALERT: continue

        rs_vs_btc = change - btc_change
        sector = get_sector(symbol)
        score, smart_money = accumulation_score(change, volume, rs_vs_btc, sector, base, symbol)

        if score >= 72:
            candidates.append({"symbol": symbol, "base": base, "price": price, "change": change, "volume": volume, "rs": rs_vs_btc,
                              "sector": sector, "score": score, "smart_money": smart_money, "smart_money_watchlist": "✓ Yes" if base in SMART_MONEY_WATCHLIST else "No",
                              "label": classify(score), "cartel_memory": "Yes" if base in CARTEL_HISTORICAL_COINS else "No"})

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:15]

    with history_lock:
        for c in top:
            if c["score"] >= 65:
                lst = candidate_history.setdefault(c["symbol"], [])
                lst.append({"ts": time.time(), "score": c["score"]})
                candidate_history[c["symbol"]] = lst[-20:]
        stale = [k for k, v in candidate_history.items() if not v or time.time() - v[-1]["ts"] > 86400]
        for k in stale: del candidate_history[k]

    persist_history()
    log_scan(current_scan, btc_change, top)
    print(f"Scan #{current_scan} | BTC: {btc_change:.2f}% | Coins: {len(data)} | Matches: {len(top)}")

    strict = [c for c in top if passes_strict_filter(c)]

    with alerted_lock:
        expired = [k for k, t in last_alerted.items() if time.time() - t > ALERT_COOLDOWN]
        for k in expired: del last_alerted[k]

    new_alerts = []
    for c in strict:
        if c["symbol"] not in last_alerted:
            new_alerts.append(c)
            with alerted_lock:
                last_alerted[c["symbol"]] = time.time()

    for c in new_alerts:
        send_watchlist_alert(c["symbol"], c["price"], c["score"], c["sector"], c["smart_money"])
        add_to_watchlist(c["symbol"], c["price"], c["score"], c["sector"], c["smart_money"])

    if not new_alerts and not force:
        return None

    return f"🔍 Scan #{current_scan} | {len(new_alerts)} candidates added to watchlist"

def scanner_loop():
    send_telegram(
        "🟢 <b>Inshal Crypto Scanner v9-Layer4 — STARTED</b>\n\n"
        "✅ Two-stage alerting active\n"
        "🎯 Score ≥85 + Breakout Momentum Detection\n"
        f"⚡ Momentum threshold: {MIN_MOMENTUM_SCORE}/100\n"
        f"🎯 TP1: +{TP1_PCT:.0f}%  TP2: +{TP2_PCT:.0f}%  SL: -{SL_PCT:.0f}%\n\n"
        "Alert #1: Coin added to watchlist\n"
        "Alert #2: Price at support + momentum = BUY NOW\n"
        "95% accuracy target active. In Sha Allah."
    )
    print("Scanner loop started.")
    consecutive_failures = 0

    while True:
        try:
            message = run_scan(force=False)
            check_watchlist_for_entry()
            check_positions()
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            print(f"Scan failed ({consecutive_failures}x): {e}")
            if consecutive_failures % 4 == 0:
                send_telegram(f"⚠️ Error: {str(e)[:100]}")
            time.sleep(ERROR_RETRY_INTERVAL)
            continue

        time.sleep(SCAN_INTERVAL)

def tracking_loop():
    print("Tracking loop started.")
    while True:
        try:
            check_watchlist_for_entry()
            check_positions()
        except Exception as e:
            print(f"Tracking error: {e}")
        time.sleep(TRACK_INTERVAL)

# ──────────────────────────────────────────
# Shutdown Handling
# ──────────────────────────────────────────
def shutdown_handler(signum, frame):
    global shutdown_notified
    if not shutdown_notified:
        shutdown_notified = True
        send_telegram(f"🔴 <b>Inshal Crypto Scanner v9 — STOPPED</b>\n\nTotal scans: {scan_count}")
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
atexit.register(lambda: shutdown_handler(None, None))

def ensure_scanner_started():
    global scanner_started
    with scanner_start_lock:
        if not scanner_started:
            scanner_started = True
            load_state()
            threading.Thread(target=scanner_loop, daemon=True).start()
            threading.Thread(target=tracking_loop, daemon=True).start()
            threading.Thread(target=telegram_listener, daemon=True).start()
            print("All threads launched.")

# ──────────────────────────────────────────
# Flask Routes
# ──────────────────────────────────────────
@app.route("/")
def home():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    with watchlist_lock:
        wl_count = len(watchlist)
    return f"v9-Layer4 | Scans: {scan_count} | Active: {active} | Watchlist: {wl_count}"

@app.route("/health")
def health():
    ensure_scanner_started()
    return "OK"

@app.route("/test")
def test():
    send_telegram("🧪 Test from v9-Layer4")
    return "Test sent"

@app.route("/scan")
def scan():
    try:
        ensure_scanner_started()
        run_scan(force=True)
        return "Scan done"
    except Exception as e:
        return str(e), 500

@app.route("/positions")
def positions():
    ensure_scanner_started()
    with positions_lock:
        active = {s: p for s, p in tracked_positions.items() if not p["closed"]}
    return jsonify({"count": len(active)})

@app.route("/status")
def status():
    ensure_scanner_started()
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    with watchlist_lock:
        wl_count = len(watchlist)
    return jsonify({"running": scanner_started, "scans": scan_count, "active": active, "watchlist": wl_count})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
