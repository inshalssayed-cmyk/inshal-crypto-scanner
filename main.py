import os
import time
import json
import threading
import requests
from flask import Flask, jsonify

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

KUCOIN_URL = "https://api.kucoin.com/api/v1/market/allTickers"

# ──────────────────────────────────────────
# Strict Alert Configuration
# ──────────────────────────────────────────
STRICT_SCORE_THRESHOLD = 80         # Only alert when score >= 80
MAX_CHANGE_FOR_ALERT  = 5            # Reject already-pumped coins
MIN_CHANGE_FOR_ALERT  = -2           # Reject big dumps
MIN_RS_VS_BTC         = 0            # Must be stronger or equal to BTC
MIN_VOLUME_FOR_ALERT  = 3_000_000    # Strict liquidity floor

ALERT_COOLDOWN        = 6 * 3600     # 6 hours per coin (rare, high-quality alerts)
HISTORY_RETENTION     = 20           # Per-coin: keep last 20 score entries
SCAN_LOG_MAX_ENTRIES  = 500          # Rolling scan log size

# Files (best-effort persistence — Render free tier wipes on redeploy)
HISTORY_FILE  = "./scan_history.json"
SCAN_LOG_FILE = "./scan_log.json"

scan_count = 0
scan_lock = threading.Lock()

last_alerted = {}             # { symbol: timestamp_of_last_alert }
alerted_lock = threading.Lock()

candidate_history = {}        # { symbol: [{"ts": ts, "score": score}, ...] }
history_lock = threading.Lock()


# ──────────────────────────────────────────
# Reference Data
# ──────────────────────────────────────────

CARTEL_HISTORICAL_COINS = {
    "ORCA", "ZAMA", "APE", "KAT", "GUN", "ACE", "TNSR",
    "RARE", "METIS", "SOLV", "PHA", "CHR", "C"
}

SECTORS = {
    "GUN": "Gaming", "ACE": "Gaming", "APE": "Gaming / NFT", "CHR": "Gaming",
    "ORCA": "Solana DeFi", "RAY": "Solana DeFi", "JUP": "Solana", "TNSR": "Solana NFT",
    "SOL": "Layer 1", "XLM": "Layer 1", "BTC": "Layer 1", "ETH": "Layer 1",
    "AVAX": "Layer 1", "BNB": "Layer 1",
    "METIS": "Layer 2", "ARB": "Layer 2", "OP": "Layer 2", "MATIC": "Layer 2",
    "ZAMA": "Privacy / AI", "PHA": "AI / Privacy",
    "HYPE": "DeFi", "UNI": "DeFi", "AAVE": "DeFi", "SNX": "DeFi", "SOLV": "DeFi",
    "ZEC": "Privacy", "LINK": "Oracle", "RARE": "NFT",
}

HOT_SECTORS = {
    "Gaming", "Solana DeFi", "Solana", "Layer 2",
    "DeFi", "AI / Privacy", "Privacy / AI"
}


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
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
        except requests.exceptions.HTTPError as e:
            print(f"Telegram HTTP error: {e} | {e.response.text}")
        except Exception as e:
            print("Telegram error:", e)


def format_price(price):
    if price >= 1000:   return f"${price:,.2f}"
    elif price >= 1:    return f"${price:.4f}"
    elif price >= 0.01: return f"${price:.5f}"
    return f"${price:.8f}"


def base_symbol(symbol):
    return symbol.replace("-USDT", "")


def get_sector(symbol):
    return SECTORS.get(base_symbol(symbol), "Unclassified")


# ──────────────────────────────────────────
# Persistence (best-effort, won't crash if file write fails)
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
    global candidate_history
    candidate_history = _safe_read_json(HISTORY_FILE, {})
    print(f"Loaded history for {len(candidate_history)} coins")


def persist_history():
    with history_lock:
        _safe_write_json(HISTORY_FILE, candidate_history)


def log_scan(scan_id, btc_change, candidates):
    """Append scan results to log file for future ML training."""
    entry = {
        "scan_id": scan_id,
        "ts": time.time(),
        "btc_change": btc_change,
        "candidates": [
            {
                "symbol": c["symbol"],
                "score": c["score"],
                "change": c["change"],
                "volume": c["volume"],
                "rs": c["rs"],
                "sector": c["sector"],
                "smart_money": c["smart_money_flag"]
            }
            for c in candidates[:10]
        ]
    }
    log = _safe_read_json(SCAN_LOG_FILE, [])
    log.append(entry)
    log = log[-SCAN_LOG_MAX_ENTRIES:]
    _safe_write_json(SCAN_LOG_FILE, log)


# ──────────────────────────────────────────
# Market Fetch (retry + backoff)
# ──────────────────────────────────────────

def fetch_market(retries=3, backoff=5):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(KUCOIN_URL, timeout=20)
            r.raise_for_status()
            raw = r.json()
            if raw.get("code") != "200000":
                raise Exception(f"KuCoin API error: {raw.get('code')} | {raw.get('msg', '')}")
            return raw["data"]["ticker"]
        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP (attempt {attempt}/{retries}): {e}"
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection (attempt {attempt}/{retries}): {e}"
        except Exception as e:
            last_error = f"Fetch (attempt {attempt}/{retries}): {e}"
        print(last_error)
        if attempt < retries:
            time.sleep(backoff * attempt)
    raise Exception(f"KuCoin fetch failed after {retries} attempts. Last: {last_error}")


# ──────────────────────────────────────────
# Consecutive Appearance Bonus
# ──────────────────────────────────────────

def consecutive_bonus(symbol):
    """
    Reward coins showing sustained accumulation across recent scans.
    If a coin scores ≥65 in 3+ recent scans within 6 hours → +10 bonus.
    This is the strongest pre-breakout signal: persistent strength.
    """
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


# ──────────────────────────────────────────
# Scoring (Pre-Breakout Accumulation Logic)
# ──────────────────────────────────────────

def accumulation_score(change, volume, rs_vs_btc, sector, base, symbol):
    score = 0

    # 1. Sideways / not over-pumped
    if -2 <= change <= 5:   score += 25
    elif 5 < change <= 8:   score += 10

    # 2. Volume strength
    if volume >= 10_000_000:  score += 25
    elif volume >= 5_000_000: score += 20
    elif volume >= 1_000_000: score += 12

    # 3. Relative strength vs BTC
    if rs_vs_btc >= 4:    score += 20
    elif rs_vs_btc >= 2:  score += 15
    elif rs_vs_btc >= 0:  score += 8

    # 4. Narrative / sector
    if sector in HOT_SECTORS:        score += 15
    elif sector != "Unclassified":   score += 7

    # 5. Cartel historical memory
    if base in CARTEL_HISTORICAL_COINS:
        score += 10

    # 6. Smart Money Proxy
    smart_money_flag = (-1 <= change <= 4) and (volume >= 5_000_000)
    if smart_money_flag:
        score += 10

    # 7. Consecutive appearance bonus (sustained accumulation)
    score += consecutive_bonus(symbol)

    return min(score, 100), smart_money_flag


def classify(score):
    if score >= 90: return "🔥 ELITE PRE-BREAKOUT"
    if score >= 80: return "✅ READY FOR BREAKOUT"
    if score >= 70: return "👀 STRONG WATCHLIST"
    return "⚠️ LOW PRIORITY"


# ──────────────────────────────────────────
# Strict Alert Filter
# ──────────────────────────────────────────

def passes_strict_filter(c):
    """All conditions must be true to be considered ready for breakout."""
    return (
        c["score"] >= STRICT_SCORE_THRESHOLD and
        MIN_CHANGE_FOR_ALERT <= c["change"] <= MAX_CHANGE_FOR_ALERT and
        c["rs"] >= MIN_RS_VS_BTC and
        c["volume"] >= MIN_VOLUME_FOR_ALERT
    )


# ──────────────────────────────────────────
# Core Scan
# ──────────────────────────────────────────

def run_scan(force=False):
    """
    force=True → always send a message (manual testing via /scan)
    force=False → silent unless strict criteria met (cron via /cron)
    """
    global scan_count

    with scan_lock:
        scan_count += 1
        current_scan = scan_count

    data = fetch_market()

    btc_change = 0.0
    for coin in data:
        if coin.get("symbol") == "BTC-USDT":
            try:
                btc_change = float(coin.get("changeRate", 0)) * 100
            except (ValueError, TypeError):
                btc_change = 0.0
            break

    candidates = []
    now = time.time()

    for coin in data:
        symbol = coin.get("symbol", "")
        if not symbol.endswith("-USDT"):
            continue
        base = base_symbol(symbol)
        if any(x in base for x in ["3L", "3S", "2L", "2S", "UP", "DOWN", "BULL", "BEAR"]):
            continue

        try:
            change = float(coin.get("changeRate", 0)) * 100
            volume = float(coin.get("volValue", 0))
            price  = float(coin.get("last", 0))
        except (ValueError, TypeError):
            continue

        if price <= 0 or volume <= 0:
            continue

        # Pre-breakout pre-filter (broad, before scoring)
        if not (-3 <= change <= 8):
            continue
        if volume < 1_000_000:
            continue

        rs_vs_btc = change - btc_change
        sector = get_sector(symbol)
        score, smart_money_flag = accumulation_score(change, volume, rs_vs_btc, sector, base, symbol)

        candidates.append({
            "symbol": symbol,
            "base": base,
            "price": price,
            "change": change,
            "volume": volume,
            "rs": rs_vs_btc,
            "sector": sector,
            "score": score,
            "smart_money_flag": smart_money_flag,
            "label": classify(score),
            "cartel_memory": "Yes" if base in CARTEL_HISTORICAL_COINS else "No",
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = candidates[:15]

    # Update per-coin history for top candidates (for consecutive bonus)
    with history_lock:
        for c in top_candidates:
            if c["score"] >= 65:
                lst = candidate_history.setdefault(c["symbol"], [])
                lst.append({"ts": now, "score": c["score"]})
                candidate_history[c["symbol"]] = lst[-HISTORY_RETENTION:]

        # Garbage collect: drop entirely if last entry > 24h old
        stale = [k for k, v in candidate_history.items() if not v or now - v[-1]["ts"] > 86400]
        for k in stale:
            del candidate_history[k]

    persist_history()
    log_scan(current_scan, btc_change, top_candidates)

    print(f"Scan #{current_scan} | BTC: {btc_change:.2f}% | Coins: {len(data)} | Top: {len(top_candidates)}")

    # ── STRICT ALERT FILTER ──
    strict_alerts = [c for c in top_candidates if passes_strict_filter(c)]

    # Dedup (drop coins still in cooldown)
    with alerted_lock:
        expired = [k for k, t in last_alerted.items() if now - t > ALERT_COOLDOWN]
        for k in expired:
            del last_alerted[k]
        if not force:
            strict_alerts = [c for c in strict_alerts if c["symbol"] not in last_alerted]
        for c in strict_alerts:
            last_alerted[c["symbol"]] = now

    # ── SILENT MODE: no alerts to send, no force → return None ──
    if not strict_alerts and not force:
        print(f"Scan #{current_scan}: no breakout-ready setups. Staying silent.")
        return None

    # ── FORCED (manual) but nothing strict-passing → show diagnostic ──
    if not strict_alerts and force:
        msg = "🔍 <b>Inshal Scanner v8 — Manual Scan</b>\n"
        msg += f"Scan #{current_scan} | ₿ BTC 24h: {btc_change:.2f}%\n\n"
        msg += "No coin currently meets strict pre-breakout criteria.\n\n"
        if top_candidates:
            msg += "<b>Top current candidates (below strict bar):</b>\n\n"
            for c in top_candidates[:5]:
                msg += (
                    f"<b>{c['symbol']}</b> | {c['score']}/100\n"
                    f"Change: {c['change']:.2f}% | RS: {c['rs']:.2f}% | "
                    f"Vol: ${c['volume']:,.0f}\n\n"
                )
        return msg

    # ── ALERT MODE: real breakout-ready setups ──
    msg = "🚨 <b>PRE-BREAKOUT WATCHLIST</b>\n"
    msg += "🎯 Inshal Crypto Scanner v8 — Silent Accumulation Mode\n"
    msg += "📊 KuCoin Spot | "
    msg += f"Scan #{current_scan} | ₿ BTC: {btc_change:.2f}%\n\n"

    for c in strict_alerts:
        smart_money_line = "🐋 Smart Money Proxy: TRIGGERED\n" if c["smart_money_flag"] else ""
        msg += (
            f"{c['label']}\n"
            f"<b>{c['symbol']}</b>\n"
            f"💰 Price: {format_price(c['price'])}\n"
            f"📈 24h Change: {c['change']:.2f}% (sideways/quiet)\n"
            f"₿ RS vs BTC: {c['rs']:.2f}% (stronger)\n"
            f"💵 Volume: ${c['volume']:,.0f}\n"
            f"🧩 Sector: {c['sector']}\n"
            f"🧠 Cartel Memory: {c['cartel_memory']}\n"
            f"{smart_money_line}"
            f"📦 Status: Possible Accumulation\n"
            f"⭐ Score: {c['score']}/100\n\n"
        )

    msg += (
        "⚠️ Not a buy signal — confirm chart manually.\n"
        "Check: compression, support, volume profile, breakout level."
    )
    return msg


# ──────────────────────────────────────────
# Flask Routes
# ──────────────────────────────────────────

@app.route("/")
def home():
    return f"Inshal Crypto Scanner v8 (Silent Mode) running. Scans completed: {scan_count}"


@app.route("/health")
def health():
    return "OK"


@app.route("/test")
def test():
    send_telegram("🧪 Test message from Inshal Crypto Scanner v8.")
    return "Test sent."


@app.route("/scan")
def manual_scan():
    """Manual scan — always returns a message even if nothing strict-passing."""
    try:
        message = run_scan(force=True)
        if message:
            send_telegram(message)
        return "Manual scan completed."
    except Exception as e:
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{str(e)}")
        return str(e), 500


@app.route("/cron")
def cron_scan():
    """Cron scan — silent unless strict pre-breakout setup found."""
    try:
        message = run_scan(force=False)
        if message:
            send_telegram(message)
            return "Cron scan: alert sent."
        return "Cron scan: silent (no breakout-ready setup)."
    except Exception as e:
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{str(e)}")
        return str(e), 500


@app.route("/startup")
def startup():
    send_telegram(
        "✅ <b>Inshal Crypto Scanner v8 — online</b>\n"
        "🎯 Mode: Silent Pre-Breakout Accumulation\n\n"
        f"⛔ Will NOT alert every 15 min.\n"
        f"✅ Only fires when score ≥ {STRICT_SCORE_THRESHOLD} AND all strict conditions met.\n"
        f"⏱ Cooldown per coin: {ALERT_COOLDOWN // 3600} hours\n\n"
        "Endpoints:\n"
        "• /health → UptimeRobot (5 min)\n"
        "• /cron → cron-job.org (15 min)\n"
        "• /scan → manual diagnostic"
    )
    return "Startup message sent."


@app.route("/history")
def history():
    """Inspect tracked coin history (for ML/debugging)."""
    with history_lock:
        snapshot = {k: v[-5:] for k, v in candidate_history.items()}
    return jsonify({
        "tracked_coins": len(snapshot),
        "data": snapshot
    })


@app.route("/log")
def log():
    """Inspect rolling scan log (ML training data)."""
    log_data = _safe_read_json(SCAN_LOG_FILE, [])
    return jsonify({
        "total_scans_logged": len(log_data),
        "recent": log_data[-10:]
    })


# ──────────────────────────────────────────
# Boot
# ──────────────────────────────────────────

load_state()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
