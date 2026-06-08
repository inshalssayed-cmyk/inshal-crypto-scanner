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

KUCOIN_URL        = "https://api.kucoin.com/api/v1/market/allTickers"
KUCOIN_PRICE_URL  = "https://api.kucoin.com/api/v1/market/orderbook/level1"

# ──────────────────────────────────────────
# Scanner Configuration
# ──────────────────────────────────────────
STRICT_SCORE_THRESHOLD = 80
MAX_CHANGE_FOR_ALERT   = 5
MIN_CHANGE_FOR_ALERT   = -2
MIN_RS_VS_BTC          = 0
MIN_VOLUME_FOR_ALERT   = 3_000_000
ALERT_COOLDOWN         = 6 * 3600
SCAN_INTERVAL          = 900
ERROR_RETRY_INTERVAL   = 300
HISTORY_RETENTION      = 20
SCAN_LOG_MAX_ENTRIES   = 500

# ──────────────────────────────────────────
# Target & Stop Loss Configuration
# ──────────────────────────────────────────
TP1_PCT          = 6.0    # Target 1: +6%
TP2_PCT          = 12.0   # Target 2: +12%
SL_PCT           = 4.0    # Stop Loss: -4%
TRACK_INTERVAL   = 180    # Check tracked coins every 3 minutes
TRACK_DURATION   = 48 * 3600  # Track each coin for max 48 hours

# ──────────────────────────────────────────
# Files
# ──────────────────────────────────────────
HISTORY_FILE    = "./scan_history.json"
SCAN_LOG_FILE   = "./scan_log.json"
POSITIONS_FILE  = "./positions.json"
RESULTS_FILE    = "./results.json"

# ──────────────────────────────────────────
# State
# ──────────────────────────────────────────
scan_count     = 0
scan_lock      = threading.Lock()
last_alerted   = {}
alerted_lock   = threading.Lock()
candidate_history = {}
history_lock   = threading.Lock()

# Tracked positions after alert
tracked_positions = {}
positions_lock    = threading.Lock()

scanner_started   = False
scanner_start_lock = threading.Lock()
shutdown_notified = False
results_lock      = threading.Lock()


# ──────────────────────────────────────────
# Reference Data
# ──────────────────────────────────────────
CARTEL_HISTORICAL_COINS = {
    "ORCA", "ZAMA", "APE", "KAT", "GUN", "ACE", "TNSR",
    "RARE", "METIS", "SOLV", "PHA", "CHR", "C"
}

# Coins known for silent accumulation patterns (v11 smart money proxy)
SMART_MONEY_WATCHLIST = {
    "ORCA", "GUN", "ACE", "METIS", "SOLV", "PHA", "CHR", "TNSR", "ZAMA", "APE",
    "KAT", "RARE", "JUP", "RAY", "HYPE"
}

SECTORS = {
    "GUN": "Gaming",     "ACE": "Gaming",      "APE": "Gaming / NFT", "CHR": "Gaming",
    "ORCA": "Solana DeFi","RAY": "Solana DeFi","JUP": "Solana",       "TNSR": "Solana NFT",
    "SOL": "Layer 1",    "XLM": "Layer 1",     "BTC": "Layer 1",      "ETH": "Layer 1",
    "AVAX": "Layer 1",   "BNB": "Layer 1",
    "METIS": "Layer 2",  "ARB": "Layer 2",     "OP": "Layer 2",       "MATIC": "Layer 2",
    "ZAMA": "Privacy / AI","PHA": "AI / Privacy",
    "HYPE": "DeFi",      "UNI": "DeFi",        "AAVE": "DeFi",        "SNX": "DeFi",   "SOLV": "DeFi",
    "ZEC": "Privacy",    "LINK": "Oracle",      "RARE": "NFT",
}

HOT_SECTORS = {"Gaming","Solana DeFi","Solana","Layer 2","DeFi","AI / Privacy","Privacy / AI"}


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────
def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials missing")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    success = True
    for i in range(0, len(message), 3900):
        chunk = message[i:i + 3900]
        try:
            r = requests.post(
                url,
                data={"chat_id": int(CHAT_ID), "text": chunk, "parse_mode": "HTML"},
                timeout=15
            )
            r.raise_for_status()
            print("Telegram sent")
        except Exception as e:
            print("Telegram error:", e)
            success = False
    return success


def format_price(price):
    if price >= 1000:   return f"${price:,.2f}"
    elif price >= 1:    return f"${price:.4f}"
    elif price >= 0.01: return f"${price:.5f}"
    return f"${price:.8f}"


def format_pct(pct):
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def base_symbol(symbol):
    return symbol.replace("-USDT", "")


def get_sector(symbol):
    return SECTORS.get(base_symbol(symbol), "Unclassified")


# ──────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────
def _safe_read_json(path, default):
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception as e:
        print(f"Read error {path}:", e)
    return default


def _safe_write_json(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Write error {path}:", e)


def load_state():
    global candidate_history, tracked_positions
    candidate_history  = _safe_read_json(HISTORY_FILE,   {})
    tracked_positions  = _safe_read_json(POSITIONS_FILE, {})
    results_count = len(_safe_read_json(RESULTS_FILE, []))
    print(f"Loaded {len(candidate_history)} history / {len(tracked_positions)} positions / {results_count} results")


def save_result(symbol, sector, score, entry, exit_price, result, alerted_at):
    """
    Save a closed trade to results.json for performance tracking.
    result: "TP1" | "TP2" | "SL" | "EXPIRED"
    """
    pnl_pct = (exit_price - entry) / entry * 100
    duration_h = (time.time() - alerted_at) / 3600
    entry = {
        "symbol":       symbol,
        "sector":       sector,
        "score":        score,
        "entry":        entry,
        "exit_price":   exit_price,
        "result":       result,
        "pnl_pct":      round(pnl_pct, 2),
        "alerted_at":   alerted_at,
        "closed_at":    time.time(),
        "duration_hours": round(duration_h, 1),
    }
    with results_lock:
        log = _safe_read_json(RESULTS_FILE, [])
        log.append(entry)
        _safe_write_json(RESULTS_FILE, log)
    print(f"Result saved: {symbol} | {result} | {pnl_pct:.2f}%")


def build_results_report(days=None):
    """Build a performance report string. days=None means all-time."""
    with results_lock:
        all_results = _safe_read_json(RESULTS_FILE, [])

    if not all_results:
        return (
            "📊 <b>Scanner Performance Report</b>\n\n"
            "No closed trades yet.\n"
            "Results will appear here after TP/SL/Expiry events."
        )

    # Filter by days if requested
    if days:
        cutoff = time.time() - days * 86400
        results = [r for r in all_results if r.get("closed_at", 0) >= cutoff]
        period_label = f"Last {days} days"
    else:
        results = all_results
        period_label = "All time"

    if not results:
        return f"📊 No trades closed in the last {days} days."

    total      = len(results)
    tp1_trades = [r for r in results if r["result"] == "TP1"]
    tp2_trades = [r for r in results if r["result"] == "TP2"]
    sl_trades  = [r for r in results if r["result"] == "SL"]
    exp_trades = [r for r in results if r["result"] == "EXPIRED"]

    wins  = tp1_trades + tp2_trades
    win_rate = len(wins) / total * 100 if total else 0

    avg_win  = sum(r["pnl_pct"] for r in wins)  / len(wins)  if wins  else 0
    avg_loss = sum(r["pnl_pct"] for r in sl_trades) / len(sl_trades) if sl_trades else 0

    best  = max(results, key=lambda r: r["pnl_pct"])
    worst = min(results, key=lambda r: r["pnl_pct"])

    # Best performing sector
    sector_wins = {}
    for r in wins:
        s = r.get("sector", "Unknown")
        sector_wins[s] = sector_wins.get(s, 0) + 1
    best_sector = max(sector_wins, key=sector_wins.get) if sector_wins else "N/A"
    best_sector_wins = sector_wins.get(best_sector, 0)

    # Best score range
    score_buckets = {"80-84": 0, "85-89": 0, "90-100": 0}
    for r in wins:
        sc = r.get("score", 0)
        if sc >= 90:        score_buckets["90-100"] += 1
        elif sc >= 85:      score_buckets["85-89"]  += 1
        else:               score_buckets["80-84"]  += 1
    best_bucket = max(score_buckets, key=score_buckets.get)

    # Avg duration
    avg_dur = sum(r.get("duration_hours", 0) for r in results) / total if total else 0

    msg  = f"📊 <b>Scanner Performance Report</b>\n"
    msg += f"🗓 Period: {period_label} | {total} trade(s)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🚀 TP2 Hit:    {len(tp2_trades):>3}  ({len(tp2_trades)/total*100:.1f}%)\n"
    msg += f"✅ TP1 Hit:    {len(tp1_trades):>3}  ({len(tp1_trades)/total*100:.1f}%)\n"
    msg += f"🛑 Stop Loss:  {len(sl_trades):>3}  ({len(sl_trades)/total*100:.1f}%)\n"
    msg += f"⏰ Expired:    {len(exp_trades):>3}  ({len(exp_trades)/total*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 Win Rate:   {win_rate:.1f}%  (TP1 + TP2)\n"
    msg += f"📈 Avg Win:    +{avg_win:.2f}%\n"
    msg += f"📉 Avg Loss:   {avg_loss:.2f}%\n"
    msg += f"⏱ Avg Duration: {avg_dur:.1f}h\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🥇 Best Trade:   <b>{best['symbol']}</b>  {'+' if best['pnl_pct'] >= 0 else ''}{best['pnl_pct']:.2f}%\n"
    msg += f"💀 Worst Trade:  <b>{worst['symbol']}</b>  {'+' if worst['pnl_pct'] >= 0 else ''}{worst['pnl_pct']:.2f}%\n"
    msg += f"🧩 Best Sector:  {best_sector}  ({best_sector_wins} wins)\n"
    msg += f"⭐ Best Score:   {best_bucket}  ({score_buckets[best_bucket]} wins)\n"

    # Last 5 trades
    recent = sorted(results, key=lambda r: r.get("closed_at", 0), reverse=True)[:5]
    if recent:
        msg += "\n<b>Recent Trades:</b>\n"
        for r in recent:
            icon = "🚀" if r["result"] == "TP2" else "✅" if r["result"] == "TP1" else "🛑" if r["result"] == "SL" else "⏰"
            msg += f"{icon} {r['symbol']}  {'+' if r['pnl_pct'] >= 0 else ''}{r['pnl_pct']:.2f}%  ({r['result']})\n"

    return msg


def persist_history():
    with history_lock:
        _safe_write_json(HISTORY_FILE, candidate_history)


def persist_positions():
    with positions_lock:
        _safe_write_json(POSITIONS_FILE, tracked_positions)


def log_scan(scan_id, btc_change, candidates):
    entry = {
        "scan_id": scan_id, "ts": time.time(),
        "btc_change": btc_change,
        "candidates": [
            {"symbol": c["symbol"], "score": c["score"], "change": c["change"],
             "volume": c["volume"], "rs": c["rs"], "sector": c["sector"]}
            for c in candidates[:10]
        ]
    }
    log = _safe_read_json(SCAN_LOG_FILE, [])
    log.append(entry)
    _safe_write_json(SCAN_LOG_FILE, log[-SCAN_LOG_MAX_ENTRIES:])


# ──────────────────────────────────────────
# Market Fetch
# ──────────────────────────────────────────
def fetch_market(retries=3, backoff=5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(KUCOIN_URL, timeout=20)
            r.raise_for_status()
            raw = r.json()
            if raw.get("code") != "200000":
                raise Exception(f"KuCoin error: {raw.get('msg', '')}")
            return raw["data"]["ticker"]
        except Exception as e:
            last_error = str(e)
            print(f"Fetch attempt {attempt}: {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise Exception(f"Fetch failed after {retries} attempts: {last_error}")


def fetch_current_price(symbol):
    """Fetch live price for a single coin (used by position tracker)."""
    try:
        r = requests.get(KUCOIN_PRICE_URL, params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        raw = r.json()
        if raw.get("code") == "200000" and raw.get("data"):
            return float(raw["data"]["price"])
    except Exception as e:
        print(f"Price fetch error {symbol}: {e}")
    return None


# ──────────────────────────────────────────
# Position Tracker
# ──────────────────────────────────────────
def add_position(symbol, entry_price, scan_id, sector="Unknown", score=0):
    """Register a coin for TP/SL tracking after it's alerted."""
    tp1 = entry_price * (1 + TP1_PCT / 100)
    tp2 = entry_price * (1 + TP2_PCT / 100)
    sl  = entry_price * (1 - SL_PCT  / 100)
    with positions_lock:
        tracked_positions[symbol] = {
            "entry":      entry_price,
            "tp1":        tp1,
            "tp2":        tp2,
            "sl":         sl,
            "alerted_at": time.time(),
            "scan_id":    scan_id,
            "sector":     sector,
            "score":      score,
            "tp1_hit":    False,
            "tp2_hit":    False,
            "sl_hit":     False,
            "closed":     False,
        }
    persist_positions()
    print(f"Tracking {symbol} | Entry: {entry_price} | TP1: {tp1:.5f} | TP2: {tp2:.5f} | SL: {sl:.5f}")


def check_positions():
    """Check all tracked positions against live prices. Called every 3 min."""
    now = time.time()
    to_remove = []

    with positions_lock:
        symbols = list(tracked_positions.keys())

    for symbol in symbols:
        with positions_lock:
            pos = tracked_positions.get(symbol)
        if not pos or pos["closed"]:
            to_remove.append(symbol)
            continue

        # Expired tracking window
        if now - pos["alerted_at"] > TRACK_DURATION:
            current = fetch_current_price(symbol) or pos["entry"]
            change_pct = (current - pos["entry"]) / pos["entry"] * 100
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

        current = fetch_current_price(symbol)
        if not current:
            continue

        change_pct = (current - pos["entry"]) / pos["entry"] * 100

        # ── TP2 Hit ──
        if current >= pos["tp2"] and not pos["tp2_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp2_hit"] = True
                tracked_positions[symbol]["closed"]  = True
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "TP2", pos["alerted_at"])
            send_telegram(
                f"🚀 <b>TARGET 2 HIT!</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry:   {format_price(pos['entry'])}\n"
                f"📍 Current: {format_price(current)}\n"
                f"📈 Gain:    {format_pct(change_pct)}\n\n"
                f"🎯 TP1 ✅  →  TP2 ✅\n"
                f"Position closed. Well done. 💰"
            )
            to_remove.append(symbol)

        # ── TP1 Hit (still open, heading to TP2) ──
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
                f"📈 Gain:    {format_pct(change_pct)}\n\n"
                f"🎯 TP1 ✅  →  TP2 watching...\n"
                f"👉 Consider moving SL to breakeven ({format_price(pos['entry'])})"
            )

        # ── Stop Loss Hit ──
        elif current <= pos["sl"] and not pos["sl_hit"]:
            with positions_lock:
                tracked_positions[symbol]["sl_hit"]  = True
                tracked_positions[symbol]["closed"]  = True
            save_result(symbol, pos.get("sector","Unknown"), pos.get("score",0),
                        pos["entry"], current, "SL", pos["alerted_at"])
            send_telegram(
                f"🛑 <b>STOP LOSS HIT</b>\n\n"
                f"<b>{symbol}</b>\n"
                f"💰 Entry:   {format_price(pos['entry'])}\n"
                f"📍 Current: {format_price(current)}\n"
                f"📉 Loss:    {format_pct(change_pct)}\n\n"
                f"Position closed. Risk managed. ✊"
            )
            to_remove.append(symbol)

    # Clean up closed positions
    if to_remove:
        with positions_lock:
            for s in to_remove:
                tracked_positions.pop(s, None)
        persist_positions()


def tracking_loop():
    """Background thread — checks positions every 3 minutes."""
    print("Position tracking loop started.")
    while True:
        try:
            with positions_lock:
                active = sum(1 for p in tracked_positions.values() if not p["closed"])
            if active > 0:
                print(f"Checking {active} active position(s)...")
                check_positions()
        except Exception as e:
            print(f"Tracking loop error: {e}")
        time.sleep(TRACK_INTERVAL)


# ──────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────
def consecutive_bonus(symbol):
    with history_lock:
        history = candidate_history.get(symbol, [])
    if len(history) < 3:
        return 0
    recent = history[-3:]
    if time.time() - recent[0]["ts"] > 6 * 3600:
        return 0
    scores = [h["score"] for h in recent]
    if min(scores) >= 65 and scores[-1] >= scores[0]:
        return 10
    return 0


def accumulation_score(change, volume, rs_vs_btc, sector, base, symbol):
    score = 0

    # Sideways / not over-pumped
    if -2 <= change <= 5:   score += 25
    elif 5 < change <= 8:   score += 10

    # Volume strength
    if volume >= 10_000_000:  score += 25
    elif volume >= 5_000_000: score += 20
    elif volume >= 1_000_000: score += 12

    # Relative strength vs BTC
    if rs_vs_btc >= 4:    score += 20
    elif rs_vs_btc >= 2:  score += 15
    elif rs_vs_btc >= 0:  score += 8

    # Narrative / sector
    if sector in HOT_SECTORS:        score += 15
    elif sector != "Unclassified":   score += 7

    # Cartel historical memory
    if base in CARTEL_HISTORICAL_COINS:
        score += 10

    # Smart money proxy (high volume + low movement)
    smart_money_flag = (-1 <= change <= 4) and (volume >= 5_000_000)
    if smart_money_flag:
        score += 10

    # Smart Money Watchlist boost (v11 integration)
    if base in SMART_MONEY_WATCHLIST:
        score += 8

    # Consecutive appearance bonus (sustained accumulation)
    score += consecutive_bonus(symbol)

    return min(score, 100), smart_money_flag


def classify(score):
    if score >= 90: return "🔥 ELITE PRE-BREAKOUT"
    if score >= 80: return "✅ READY FOR BREAKOUT"
    if score >= 70: return "👀 STRONG WATCHLIST"
    return "⚠️ LOW PRIORITY"


def passes_strict_filter(c):
    return (
        c["score"]  >= STRICT_SCORE_THRESHOLD and
        MIN_CHANGE_FOR_ALERT <= c["change"] <= MAX_CHANGE_FOR_ALERT and
        c["rs"]     >= MIN_RS_VS_BTC and
        c["volume"] >= MIN_VOLUME_FOR_ALERT
    )


# ──────────────────────────────────────────
# Core Scan
# ──────────────────────────────────────────
def run_scan(force=False):
    global scan_count

    with scan_lock:
        scan_count += 1
        current_scan = scan_count

    data = fetch_market()

    btc_change = 0.0
    for coin in data:
        if coin.get("symbol") == "BTC-USDT":
            try: btc_change = float(coin.get("changeRate", 0)) * 100
            except: btc_change = 0.0
            break

    candidates = []
    now = time.time()

    for coin in data:
        symbol = coin.get("symbol", "")
        if not symbol.endswith("-USDT"): continue
        base = base_symbol(symbol)
        if any(x in base for x in ["3L","3S","2L","2S","UP","DOWN","BULL","BEAR"]): continue

        try:
            change = float(coin.get("changeRate", 0)) * 100
            volume = float(coin.get("volValue", 0))
            price  = float(coin.get("last", 0))
        except (ValueError, TypeError):
            continue

        if price <= 0 or volume <= 0: continue
        if not (-3 <= change <= 8):   continue
        if volume < 1_000_000:        continue

        rs_vs_btc = change - btc_change
        sector    = get_sector(symbol)
        score, smart_money = accumulation_score(change, volume, rs_vs_btc, sector, base, symbol)

        candidates.append({
            "symbol": symbol, "base": base, "price": price,
            "change": change, "volume": volume, "rs": rs_vs_btc,
            "sector": sector, "score": score,
            "smart_money": smart_money,
            "smart_money_watchlist": "✓ Yes" if base in SMART_MONEY_WATCHLIST else "No",
            "label": classify(score),
            "cartel_memory": "Yes" if base in CARTEL_HISTORICAL_COINS else "No",
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:15]

    with history_lock:
        for c in top:
            if c["score"] >= 65:
                lst = candidate_history.setdefault(c["symbol"], [])
                lst.append({"ts": now, "score": c["score"]})
                candidate_history[c["symbol"]] = lst[-HISTORY_RETENTION:]
        stale = [k for k, v in candidate_history.items() if not v or now - v[-1]["ts"] > 86400]
        for k in stale: del candidate_history[k]

    persist_history()
    log_scan(current_scan, btc_change, top)

    print(f"Scan #{current_scan} | BTC: {btc_change:.2f}% | Coins: {len(data)} | Matches: {len(top)}")

    strict = [c for c in top if passes_strict_filter(c)]

    with alerted_lock:
        expired = [k for k, t in last_alerted.items() if now - t > ALERT_COOLDOWN]
        for k in expired: del last_alerted[k]
        if not force:
            strict = [c for c in strict if c["symbol"] not in last_alerted]
        for c in strict:
            last_alerted[c["symbol"]] = now

    # Register alerted coins for TP/SL tracking
    for c in strict:
        add_position(c["symbol"], c["price"], current_scan, c["sector"], c["score"])

    if not strict and not force:
        print(f"Scan #{current_scan}: silent.")
        return None

    if not strict and force:
        msg = f"🔍 <b>Manual Scan #{current_scan}</b>\n"
        msg += f"₿ BTC 24h: {btc_change:.2f}%\n\n"
        msg += "No coin meets strict pre-breakout criteria.\n\n"
        if top:
            msg += "<b>Closest candidates:</b>\n\n"
            for c in top[:5]:
                msg += f"<b>{c['symbol']}</b> — {c['score']}/100 | {c['change']:.2f}% | ${c['volume']:,.0f}\n"
        return msg

    # ── Build alert with TP/SL ──
    msg = "🚨 <b>PRE-BREAKOUT WATCHLIST</b>\n"
    msg += "🎯 Inshal Crypto Scanner v8\n"
    msg += f"📊 KuCoin | Scan #{current_scan} | ₿ BTC: {btc_change:.2f}%\n\n"

    for c in strict:
        tp1 = c["price"] * (1 + TP1_PCT / 100)
        tp2 = c["price"] * (1 + TP2_PCT / 100)
        sl  = c["price"] * (1 - SL_PCT  / 100)
        rr  = TP2_PCT / SL_PCT

        smart_line  = "🐋 Smart Money Proxy: TRIGGERED\n" if c["smart_money"] else ""
        watchlist_line = "⭐ Smart Money Watchlist: YES\n" if c["smart_money_watchlist"] == "✓ Yes" else ""

        msg += (
            f"{c['label']}\n"
            f"<b>{c['symbol']}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 Entry Price: {format_price(c['price'])}\n"
            f"📈 24h Change:  {c['change']:.2f}% (sideways)\n"
            f"₿ RS vs BTC:   {c['rs']:.2f}%\n"
            f"💵 Volume:      ${c['volume']:,.0f}\n"
            f"🧩 Sector:      {c['sector']}\n"
            f"🧠 Cartel Memory: {c['cartel_memory']}\n"
            f"{smart_line}"
            f"{watchlist_line}"
            f"━━━━━━━━━━━━━━━\n"
            f"🎯 Target 1:  {format_price(tp1)}  (+{TP1_PCT:.0f}%)\n"
            f"🎯 Target 2:  {format_price(tp2)}  (+{TP2_PCT:.0f}%)\n"
            f"🛑 Stop Loss: {format_price(sl)}  (-{SL_PCT:.0f}%)\n"
            f"⚖️ R/R Ratio: 1:{rr:.1f}\n"
            f"⭐ Score: {c['score']}/100\n\n"
        )

    msg += (
        "📡 Tracking active — you will be notified when TP1, TP2, or SL is hit.\n\n"
        "⚠️ Not a buy signal. Confirm chart manually before trading."
    )
    return msg


# ──────────────────────────────────────────
# Background Scanner Loop
# ──────────────────────────────────────────
def scanner_loop():
    send_telegram(
        "🟢 <b>Inshal Crypto Scanner v8 (Enhanced with v11)</b> — STARTED\n\n"
        "✅ Auto-scanning every 15 minutes\n"
        "🎯 Mode: Pre-Breakout Accumulation + Smart Money Watchlist\n"
        f"📏 Score threshold: ≥{STRICT_SCORE_THRESHOLD}/100\n"
        f"🎯 TP1: +{TP1_PCT:.0f}%  |  TP2: +{TP2_PCT:.0f}%  |  SL: -{SL_PCT:.0f}%\n"
        f"📡 Auto-tracking enabled after each alert\n"
        f"🐋 Smart Money Proxy + Cartel DNA + v11 Watchlist\n\n"
        "Silent scans are normal — alert fires only on strict pre-breakout conditions."
    )
    print("Scanner loop started.")
    consecutive_failures = 0

    while True:
        try:
            message = run_scan(force=False)
            if message:
                send_telegram(message)
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            print(f"Scan failed ({consecutive_failures}x): {e}")
            if consecutive_failures == 1 or consecutive_failures % 4 == 0:
                send_telegram(
                    f"⚠️ <b>Scanner Error</b>\n\n"
                    f"Failures: {consecutive_failures}\n{str(e)[:300]}\n\n"
                    f"Retrying in {ERROR_RETRY_INTERVAL // 60} min."
                )
            time.sleep(ERROR_RETRY_INTERVAL)
            continue
        time.sleep(SCAN_INTERVAL)


# ──────────────────────────────────────────
# Telegram Command Listener
# ──────────────────────────────────────────
def handle_telegram_command(text, chat_id):
    """Process commands sent to the bot in Telegram."""
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
            send_telegram("📡 No positions currently being tracked.")
            return
        msg = f"📡 <b>Active Positions ({len(active)})</b>\n\n"
        for sym, pos in active.items():
            current = fetch_current_price(sym)
            change_pct = ((current - pos["entry"]) / pos["entry"] * 100) if current else 0
            msg += (
                f"<b>{sym}</b>\n"
                f"💰 Entry:   {format_price(pos['entry'])}\n"
                f"📍 Current: {format_price(current) if current else 'N/A'}  ({format_pct(change_pct)})\n"
                f"🎯 TP1: {format_price(pos['tp1'])}  TP2: {format_price(pos['tp2'])}\n"
                f"🛑 SL:  {format_price(pos['sl'])}\n"
                f"TP1 Hit: {'✅' if pos['tp1_hit'] else '❌'}\n"
                f"⏱ Tracking: {round((time.time()-pos['alerted_at'])/3600, 1)}h\n\n"
            )
        send_telegram(msg)

    elif cmd == "/status":
        with positions_lock:
            active = sum(1 for p in tracked_positions.values() if not p["closed"])
        with results_lock:
            total_results = len(_safe_read_json(RESULTS_FILE, []))
        send_telegram(
            f"🤖 <b>Scanner Status</b>\n\n"
            f"✅ Running: {scanner_started}\n"
            f"🔢 Scans completed: {scan_count}\n"
            f"📡 Active positions: {active}\n"
            f"📊 Total trades logged: {total_results}\n"
            f"📏 Score threshold: ≥{STRICT_SCORE_THRESHOLD}\n"
            f"🎯 TP1: +{TP1_PCT:.0f}%  TP2: +{TP2_PCT:.0f}%  SL: -{SL_PCT:.0f}%"
        )

    elif cmd == "/help":
        send_telegram(
            "🤖 <b>Inshal Crypto Scanner — Commands</b>\n\n"
            "/results     — Full performance report (all time)\n"
            "/results7    — Last 7 days performance\n"
            "/results30   — Last 30 days performance\n"
            "/positions   — Active tracked positions\n"
            "/status      — Scanner health & stats\n"
            "/help        — This help message\n\n"
            "ℹ️ The scanner runs silently and only alerts when a coin meets strict pre-breakout criteria."
        )


def telegram_listener():
    """
    Background thread — polls Telegram for incoming messages
    and responds to commands like /results, /positions, /status.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram listener: credentials missing, skipping.")
        return

    print("Telegram command listener started.")
    last_update_id = 0

    while True:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            r = requests.get(
                url,
                params={"offset": last_update_id + 1, "timeout": 30},
                timeout=35
            )
            r.raise_for_status()
            updates = r.json().get("result", [])

            for update in updates:
                last_update_id = update["update_id"]
                msg  = update.get("message", {})
                text = msg.get("text", "").strip()
                cid  = str(msg.get("chat", {}).get("id", ""))

                # Only respond to your own chat
                if cid != str(CHAT_ID):
                    continue

                if text.startswith("/"):
                    print(f"Telegram command received: {text}")
                    handle_telegram_command(text, cid)

        except requests.exceptions.Timeout:
            pass  # Normal for long polling
        except Exception as e:
            print(f"Telegram listener error: {e}")
            time.sleep(10)


def ensure_scanner_started():
    global scanner_started
    with scanner_start_lock:
        if not scanner_started:
            scanner_started = True
            threading.Thread(target=scanner_loop,     daemon=True).start()
            threading.Thread(target=tracking_loop,    daemon=True).start()
            threading.Thread(target=telegram_listener, daemon=True).start()
            print("Scanner + tracker + Telegram listener threads launched.")


# ──────────────────────────────────────────
# Shutdown Notifications
# ──────────────────────────────────────────
def notify_shutdown(reason="Render restart or shutdown"):
    global shutdown_notified
    if shutdown_notified: return
    shutdown_notified = True
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    try:
        send_telegram(
            f"🔴 <b>Inshal Crypto Scanner v8 — STOPPED</b>\n\n"
            f"Reason: {reason}\n"
            f"Total scans completed: {scan_count}\n"
            f"Active tracked positions: {active}\n\n"
            f"⚠️ Scanner is offline. Check Render dashboard."
        )
    except Exception as e:
        print("Shutdown notify error:", e)


def handle_sigterm(signum, frame):
    notify_shutdown("SIGTERM (Render redeploy / shutdown)")
    sys.exit(0)

def handle_sigint(signum, frame):
    notify_shutdown("SIGINT (manual interrupt)")
    sys.exit(0)

atexit.register(notify_shutdown, "Process exiting")
signal.signal(signal.SIGTERM, handle_sigterm)
try:
    signal.signal(signal.SIGINT, handle_sigint)
except Exception:
    pass


# ──────────────────────────────────────────
# Flask Routes
# ──────────────────────────────────────────
@app.route("/")
def home():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    return (
        f"Inshal Crypto Scanner v8 running.\n"
        f"Scans: {scan_count} | Tracking: {active} position(s)"
    )

@app.route("/health")
def health():
    return "OK"

@app.route("/test")
def test():
    ok = send_telegram("🧪 Test message from Inshal Crypto Scanner v8.")
    return ("Test sent." if ok else "Test failed — check logs."), (200 if ok else 500)

@app.route("/scan")
def manual_scan():
    try:
        message = run_scan(force=True)
        if message: send_telegram(message)
        return "Manual scan completed."
    except Exception as e:
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{str(e)}")
        return str(e), 500

@app.route("/positions")
def positions():
    """See all currently tracked positions and their status."""
    with positions_lock:
        snap = {}
        for sym, pos in tracked_positions.items():
            if pos["closed"]: continue
            current = fetch_current_price(sym)
            change_pct = ((current - pos["entry"]) / pos["entry"] * 100) if current else None
            snap[sym] = {
                "entry":    format_price(pos["entry"]),
                "tp1":      format_price(pos["tp1"]),
                "tp2":      format_price(pos["tp2"]),
                "sl":       format_price(pos["sl"]),
                "current":  format_price(current) if current else "N/A",
                "change":   f"{change_pct:.2f}%" if change_pct is not None else "N/A",
                "tp1_hit":  pos["tp1_hit"],
                "tp2_hit":  pos["tp2_hit"],
                "tracking_hours": round((time.time() - pos["alerted_at"]) / 3600, 1),
            }
    return jsonify({"active_positions": len(snap), "positions": snap})

@app.route("/status")
def status():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    return jsonify({
        "scanner_running":    scanner_started,
        "scans_completed":    scan_count,
        "active_positions":   active,
        "coins_in_cooldown":  len(last_alerted),
        "coins_tracked":      len(candidate_history),
        "strict_threshold":   STRICT_SCORE_THRESHOLD,
        "tp1_pct":            TP1_PCT,
        "tp2_pct":            TP2_PCT,
        "sl_pct":             SL_PCT,
    })

@app.route("/history")
def history():
    with history_lock:
        snap = {k: v[-5:] for k, v in candidate_history.items()}
    return jsonify({"tracked_coins": len(snap), "data": snap})

@app.route("/log")
def log():
    data = _safe_read_json(SCAN_LOG_FILE, [])
    return jsonify({"total_scans_logged": len(data), "recent": data[-10:]})


@app.route("/results")
def results_route():
    """Send performance report to Telegram AND return as JSON."""
    report = build_results_report()
    send_telegram(report)
    with results_lock:
        all_r = _safe_read_json(RESULTS_FILE, [])
    return jsonify({"total_trades": len(all_r), "report_sent_to_telegram": True})


# ──────────────────────────────────────────
# Boot
# ──────────────────────────────────────────
load_state()
ensure_scanner_started()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
