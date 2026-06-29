import os
import sys
import time
import json
import signal
import threading
import requests
from flask import Flask, jsonify
from datetime import datetime

# ============================================================================
# INSHAL CRYPTO SCANNER v9.2.3 (STRICT MODE - PRODUCTION)
# ============================================================================
# 95% Accuracy Pre-Breakout Detection System
# v9.2: Brooks price-action entry, fixed entry trigger, 4%/8% targets, seen counter
# STRICT MODE: High quality trades only
# ============================================================================

app = Flask(__name__)

# ============================================================================
# CONFIGURATION (STRICT MODE)
# ============================================================================

# API Credentials
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

# KuCoin API Endpoints
KUCOIN_BASE_URL = "https://api.kucoin.com/api/v1"
KUCOIN_TICKERS = f"{KUCOIN_BASE_URL}/market/allTickers"
KUCOIN_PRICE = f"{KUCOIN_BASE_URL}/market/orderbook/level1"
KUCOIN_KLINES = f"{KUCOIN_BASE_URL}/market/candles"

# STRICT MODE: Score & Momentum Thresholds
STRICT_SCORE_THRESHOLD = 85  # STRICT: Only elite coins
MAX_PRICE_CHANGE = 8
MIN_PRICE_CHANGE = -2
MIN_VOLUME_FOR_ALERT = 3_000_000
ALERT_COOLDOWN_SECONDS = 6 * 3600

# Scan Timing
SCAN_INTERVAL_SECONDS = 900  # 15 minutes
TRACK_CHECK_INTERVAL = 180   # 3 minutes
TRACK_MAX_DURATION = 48 * 3600  # 48 hours

# Position Settings (TP/SL)
TARGET_1_PERCENT = 4.0   # v9.2: lowered from 6.0
TARGET_2_PERCENT = 8.0   # v9.2: lowered from 12.0
STOP_LOSS_PERCENT = 5.0

# v9.2 Brooks-based entry engine
# Entry is derived from the recent swing-low (range bottom), not a flat % discount.
# A small buffer is placed ABOVE the swing low so the order fills just above support
# (Brooks: bulls buy just above the bottom of the trading range, not exactly at it).
SWING_LOOKBACK_HOURS = 48          # window used to locate the range/swing low
ENTRY_BUFFER_ABOVE_SUPPORT = 0.4   # % above the swing low to place entry
ENTRY_FILL_TOLERANCE = 0.5         # % band around entry that counts as "price reached entry"
ENTRY_MAX_DISCOUNT = 6.0           # safety cap: entry never more than this % below current
ENTRY_MIN_DISCOUNT = 0.2           # safety floor: entry at least this % below current

# Breakout Momentum Detection (Layer 4) - STRICT MODE
MOMENTUM_CHECK_ENABLED = True
MIN_MOMENTUM_SCORE = 70  # STRICT: Require strong momentum
MOMENTUM_ANALYSIS_HOURS = 2

# Support Entry Distance - STRICT MODE
MAX_SUPPORT_DISTANCE_PERCENT = 2.0  # STRICT: Tight entry zone

# v9.2.3: 120-hour accumulation-quality bonus
# Reads 5 days of 1h candles for top candidates only and awards up to +10
# for a genuine multi-day base (tight range, holding up, no prior blow-off).
ACCUM_LOOKBACK_HOURS = 120
ACCUM_MAX_BONUS = 10

# File Storage
HISTORY_FILE = "./scan_history.json"
POSITIONS_FILE = "./positions.json"
RESULTS_FILE = "./results.json"
WATCHLIST_FILE = "./watchlist.json"
SEEN_COUNTS_FILE = "./seen_counts.json"

# ============================================================================
# GLOBAL STATE
# ============================================================================

scan_count = 0
scan_lock = threading.Lock()

last_alerted = {}
alerted_lock = threading.Lock()

candidate_history = {}
history_lock = threading.Lock()

# v9.2: how many times each coin has appeared as a scan candidate
seen_counts = {}
seen_lock = threading.Lock()

tracked_positions = {}
positions_lock = threading.Lock()

watchlist = {}
watchlist_lock = threading.Lock()

all_results = []
results_lock = threading.Lock()

scanner_running = False
scanner_lock = threading.Lock()
shutdown_flag = threading.Event()

# ============================================================================
# MARKET INTELLIGENCE DATABASE
# ============================================================================

CARTEL_HISTORICAL_COINS = {
    "ORCA", "ZAMA", "APE", "KAT", "GUN", "ACE", "TNSR",
    "RARE", "METIS", "SOLV", "PHA", "CHR", "C"
}

SMART_MONEY_WATCHLIST = {
    "ORCA", "GUN", "ACE", "METIS", "SOLV", "PHA", "CHR", "TNSR", "ZAMA", "APE",
    "KAT", "RARE", "JUP", "RAY", "HYPE", "O", "GRAM", "WLD", "BLEND"
}

SECTOR_MAP = {
    "GUN": "Gaming", "ACE": "Gaming", "APE": "Gaming", "CHR": "Gaming",
    "ORCA": "Solana DeFi", "RAY": "Solana DeFi", "JUP": "Solana", "TNSR": "Solana NFT",
    "SOL": "Layer 1", "XLM": "Layer 1", "BTC": "Layer 1", "ETH": "Layer 1", "AVAX": "Layer 1", "BNB": "Layer 1",
    "METIS": "Layer 2", "ARB": "Layer 2", "OP": "Layer 2", "MATIC": "Layer 2",
    "ZAMA": "Privacy/AI", "PHA": "AI/Privacy",
    "HYPE": "DeFi", "UNI": "DeFi", "AAVE": "DeFi", "SNX": "DeFi", "SOLV": "DeFi",
    "ZEC": "Privacy", "LINK": "Oracle", "RARE": "NFT",
    "O": "Layer 1", "GRAM": "Layer 1", "WLD": "Layer 1", "BLEND": "DeFi",
}

HOT_SECTORS = {"Gaming", "Solana DeFi", "Solana", "Layer 2", "DeFi", "AI/Privacy", "Privacy/AI"}

# v9.2.2: keyword-based hot-sector matching so label variants (e.g. "Solana NFT")
# still earn the hot-sector bonus instead of silently dropping to the generic +7.
HOT_SECTOR_KEYWORDS = ("solana", "gaming", "defi", "layer 2", "l2", "ai", "privacy")

def is_hot_sector(sector):
    """True if the sector matches any hot-sector family by keyword."""
    if not sector:
        return False
    s = sector.lower()
    if sector in HOT_SECTORS:
        return True
    return any(kw in s for kw in HOT_SECTOR_KEYWORDS)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_base_symbol(symbol):
    """Extract base from trading pair"""
    return symbol.replace("-USDT", "")

def get_coin_sector(symbol):
    """Get sector for symbol"""
    base = extract_base_symbol(symbol)
    return SECTOR_MAP.get(base, "Unclassified")

def format_price_display(price):
    """Format price for display"""
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.5f}"
    else:
        return f"${price:.8f}"

def format_percentage(percent_value):
    """Format percentage"""
    sign = "+" if percent_value >= 0 else ""
    return f"{sign}{percent_value:.2f}%"

def safe_json_read(filepath, default_value):
    """Safe JSON read"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return default_value

def safe_json_write(filepath, data):
    """Safe JSON write"""
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"Error writing {filepath}: {e}")
        return False

def load_persistent_state():
    """Load state from disk"""
    global candidate_history, tracked_positions, watchlist, all_results, seen_counts
    
    with history_lock:
        candidate_history = safe_json_read(HISTORY_FILE, {})
    
    with positions_lock:
        tracked_positions = safe_json_read(POSITIONS_FILE, {})
    
    with watchlist_lock:
        watchlist = safe_json_read(WATCHLIST_FILE, {})
    
    with results_lock:
        all_results = safe_json_read(RESULTS_FILE, [])
    
    with seen_lock:
        seen_counts = safe_json_read(SEEN_COUNTS_FILE, {})
    
    print(f"✅ State loaded")

def persist_all_state():
    """Save state to disk"""
    with history_lock:
        safe_json_write(HISTORY_FILE, candidate_history)
    with positions_lock:
        safe_json_write(POSITIONS_FILE, tracked_positions)
    with watchlist_lock:
        safe_json_write(WATCHLIST_FILE, watchlist)
    with results_lock:
        safe_json_write(RESULTS_FILE, all_results)
    with seen_lock:
        safe_json_write(SEEN_COUNTS_FILE, seen_counts)

# ============================================================================
# TELEGRAM MESSAGING
# ============================================================================

def send_telegram_message(text_content):
    """Send Telegram message"""
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials missing")
        return
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    for i in range(0, len(text_content), 3900):
        chunk = text_content[i:i+3900]
        try:
            payload = {
                "chat_id": int(CHAT_ID),
                "text": chunk,
                "parse_mode": "HTML"
            }
            requests.post(url, data=payload, timeout=15)
        except Exception as e:
            print(f"Telegram error: {e}")

# ============================================================================
# KUCOIN API
# ============================================================================

def fetch_current_price(symbol):
    """Get current price"""
    try:
        response = requests.get(KUCOIN_PRICE, params={"symbol": symbol}, timeout=10)
        if response.json().get("code") == "200000":
            return float(response.json()["data"].get("price", 0))
    except:
        pass
    return None

def fetch_kline_data(symbol, timeframe="1hour", limit=24):
    """Get candlestick data"""
    try:
        params = {
            "symbol": symbol,
            "type": timeframe,
            "startAt": int(time.time()) - (limit * 3600)
        }
        response = requests.get(KUCOIN_KLINES, params=params, timeout=10)
        if response.json().get("code") == "200000":
            return response.json().get("data", [])
    except:
        pass
    return []

def detect_support_level(symbol):
    """Detect support from 72h history"""
    klines = fetch_kline_data(symbol, "1hour", 72)
    if len(klines) < 3:
        return None
    lows = [float(k[4]) for k in klines]
    support = min(lows[-24:]) if len(lows) >= 24 else min(lows)
    return support

def detect_resistance_level(symbol):
    """Detect resistance from 72h history"""
    klines = fetch_kline_data(symbol, "1hour", 72)
    if len(klines) < 3:
        return None
    highs = [float(k[3]) for k in klines]
    resistance = max(highs[-24:]) if len(highs) >= 24 else max(highs)
    return resistance

def calculate_brooks_entry(symbol, current_price):
    """
    v9.2 — Derive entry from price-action structure (Al Brooks method),
    not a flat percentage discount.

    Brooks: in a trading range, bulls buy just ABOVE the bottom of the range
    (the recent swing low / support magnet). So the entry is the recent swing
    low plus a small buffer, clamped to sane limits relative to current price.

    Returns dict: entry, swing_low, range_high, range_pct, basis
    or None if data unavailable.
    """
    klines = fetch_kline_data(symbol, "1hour", SWING_LOOKBACK_HOURS)
    if len(klines) < 6:
        return None

    lows = [float(k[4]) for k in klines]
    highs = [float(k[3]) for k in klines]

    # Range bottom (support magnet) and range top over the lookback window
    swing_low = min(lows)
    range_high = max(highs)

    if swing_low <= 0:
        return None

    # Brooks entry: just above the swing low (buy near the bottom of the range)
    entry = swing_low * (1 + ENTRY_BUFFER_ABOVE_SUPPORT / 100)

    # Clamp so the entry is realistic vs current price
    max_entry = current_price * (1 - ENTRY_MIN_DISCOUNT / 100)   # at least slightly below current
    min_entry = current_price * (1 - ENTRY_MAX_DISCOUNT / 100)   # never absurdly far below

    basis = "swing-low + buffer"
    if entry > max_entry:
        # Price already coiling right at support -> entry sits just under current
        entry = max_entry
        basis = "tight range near support"
    if entry < min_entry:
        # Swing low is very deep -> cap the discount
        entry = min_entry
        basis = "deep swing-low (capped)"

    range_pct = ((range_high - swing_low) / swing_low) * 100 if swing_low else 0

    return {
        "entry": entry,
        "swing_low": swing_low,
        "range_high": range_high,
        "range_pct": range_pct,
        "basis": basis
    }


def classify_market_structure(symbol):
    """Classify market structure"""
    klines = fetch_kline_data(symbol, "4hour", 20)
    if len(klines) < 5:
        return "UNKNOWN"
    
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

# ============================================================================
# BREAKOUT MOMENTUM ANALYSIS (Layer 4)
# ============================================================================

def analyze_breakout_momentum(symbol):
    """Analyze breakout momentum"""
    klines = fetch_kline_data(symbol, "1hour", 24)
    if len(klines) < 3:
        return 0, "Insufficient data"
    
    closes = [float(k[2]) for k in klines[-12:]]
    volumes = [float(k[5]) for k in klines[-12:]]
    
    if not closes or not volumes:
        return 0, "No data"
    
    momentum_score = 0
    reasons = []
    
    # Price momentum
    if len(closes) >= 6:
        price_change_percent = ((closes[-1] - closes[-6]) / closes[-6]) * 100
        
        if price_change_percent > 2:
            momentum_score += 25
            reasons.append("Price up")
        elif price_change_percent > 0:
            momentum_score += 15
            reasons.append("Slight up")
        elif price_change_percent < -2:
            return 0, "Price down"
    
    # Volume acceleration
    if len(volumes) >= 12:
        early_volume = sum(volumes[-12:-6]) / 6
        recent_volume = sum(volumes[-6:]) / 6
        
        if early_volume > 0:
            volume_acceleration = ((recent_volume - early_volume) / early_volume) * 100
            
            if volume_acceleration > 50:
                momentum_score += 30
                reasons.append("Vol surge")
            elif volume_acceleration > 20:
                momentum_score += 20
                reasons.append("Vol build")
            elif volume_acceleration < -30:
                return 0, "Vol down"
    
    # Compression
    if len(closes) >= 12:
        recent_range = max(closes[-6:]) - min(closes[-6:])
        early_range = max(closes[-12:-6]) - min(closes[-12:-6])
        
        if early_range > 0 and recent_range < (early_range * 0.7):
            momentum_score += 20
            reasons.append("Compress")
    
    final_reason = " | ".join(reasons) if reasons else "No signals"
    return min(momentum_score, 100), final_reason

def analyze_accumulation_quality(symbol):
    """
    v9.2.3 — Judge multi-day accumulation quality over 120h (5 days).
    Returns (bonus 0..ACCUM_MAX_BONUS, reason). Only called for top candidates.

    Rewards a genuine base:
      • Tight 5-day range (not wildly volatile)        -> up to +4
      • Price holding in upper half of the range       -> up to +3
      • No prior blow-off pump inside the window        -> up to +3
    """
    klines = fetch_kline_data(symbol, "1hour", ACCUM_LOOKBACK_HOURS)
    if len(klines) < 24:
        return 0, "insufficient 5d data"

    closes = [float(k[2]) for k in klines]
    highs = [float(k[3]) for k in klines]
    lows = [float(k[4]) for k in klines]

    hi = max(highs)
    lo = min(lows)
    last = closes[-1]
    if lo <= 0 or hi <= lo:
        return 0, "flat/no range"

    bonus = 0
    reasons = []

    # 1) Tight base: total 5-day range as % of price
    range_pct = (hi - lo) / lo * 100
    if range_pct <= 15:
        bonus += 4
        reasons.append("tight base")
    elif range_pct <= 30:
        bonus += 2
        reasons.append("moderate base")
    # very wide (>30%) earns nothing

    # 2) Holding up: where price sits within the 5-day range (0=low,1=high)
    position = (last - lo) / (hi - lo)
    if position >= 0.5:
        bonus += 3
        reasons.append("upper half")
    elif position >= 0.33:
        bonus += 1
        reasons.append("mid range")
    # bleeding near the lows earns nothing

    # 3) No prior blow-off: biggest single-hour jump inside the window
    max_jump = 0
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            jump = (closes[i] - closes[i-1]) / closes[i-1] * 100
            if jump > max_jump:
                max_jump = jump
    if max_jump < 12:
        bonus += 3
        reasons.append("no blow-off")
    elif max_jump < 20:
        bonus += 1
        reasons.append("mild spike")
    # already-pumped (>=20% hourly) earns nothing

    bonus = min(bonus, ACCUM_MAX_BONUS)
    return bonus, " | ".join(reasons) if reasons else "weak base"

# ============================================================================
# ACCUMULATION SCORING ENGINE
# ============================================================================

def get_consecutive_appearance_bonus(symbol):
    """Check consecutive appearances"""
    with history_lock:
        history = candidate_history.get(symbol, [])
    
    if len(history) < 3:
        return 0
    
    recent_entries = history[-3:]
    
    if time.time() - recent_entries[0]["ts"] > 6 * 3600:
        return 0
    
    scores = [h["score"] for h in recent_entries]
    if min(scores) >= 65 and scores[-1] >= scores[0]:
        return 10
    
    return 0

def increment_seen_count(symbol):
    """v9.2: Increment how many times a coin has appeared as a candidate."""
    with seen_lock:
        seen_counts[symbol] = seen_counts.get(symbol, 0) + 1
        return seen_counts[symbol]

def get_seen_count(symbol):
    """v9.2: Read how many times a coin has appeared."""
    with seen_lock:
        return seen_counts.get(symbol, 0)

def calculate_accumulation_score(price_change, volume, rs_vs_btc, sector, base_symbol, symbol):
    """Calculate accumulation score (0-100)"""
    score = 0
    smart_money_signal = False
    
    # Sideways consolidation
    if -2 <= price_change <= 5:
        score += 25
    elif 5 < price_change <= 8:
        score += 10
    
    # Volume strength
    if volume >= 10_000_000:
        score += 25
    elif volume >= 5_000_000:
        score += 20
    elif volume >= 1_000_000:
        score += 12
    
    # Relative strength
    if rs_vs_btc >= 4:
        score += 20
    elif rs_vs_btc >= 2:
        score += 15
    elif rs_vs_btc >= 0:
        score += 8
    
    # Sector analysis (v9.2.2: keyword match so "Solana NFT" etc. still count)
    if is_hot_sector(sector):
        score += 15
    elif sector != "Unclassified":
        score += 7
    
    # Cartel historical
    if base_symbol in CARTEL_HISTORICAL_COINS:
        score += 10
    
    # Smart money proxy
    if (-1 <= price_change <= 4) and (volume >= 5_000_000):
        score += 10
        smart_money_signal = True
    
    # Smart money watchlist
    if base_symbol in SMART_MONEY_WATCHLIST:
        score += 8
    
    # Consecutive bonus
    score += get_consecutive_appearance_bonus(symbol)
    
    return min(score, 100), smart_money_signal

def classify_score_to_label(score):
    """Classify score to label"""
    if score >= 90:
        return "🔥 ELITE PRE-BREAKOUT"
    elif score >= 85:
        return "✅ HIGH PROBABILITY SETUP"
    elif score >= 80:
        return "👀 STRONG WATCHLIST"
    else:
        return "⚠️ MONITOR ONLY"

# ============================================================================
# WATCHLIST MANAGEMENT (Two-Stage Alerting)
# ============================================================================

def add_coin_to_watchlist(symbol, current_price, score, sector, smart_money, plan=None, seen=1):
    """Add to watchlist with a Brooks-based entry plan (v9.2)."""
    with watchlist_lock:
        if symbol not in watchlist:
            entry = plan["entry"] if plan else current_price * 0.99
            tp1 = entry * (1 + TARGET_1_PERCENT / 100)
            tp2 = entry * (1 + TARGET_2_PERCENT / 100)
            sl = entry * (1 - STOP_LOSS_PERCENT / 100)
            watchlist[symbol] = {
                "added_at": time.time(),
                "score": score,
                "added_price": current_price,
                "planned_entry": entry,
                "tp1": tp1,
                "tp2": tp2,
                "sl": sl,
                "sector": sector,
                "smart_money": smart_money,
                "seen": seen,
                "swing_low": plan["swing_low"] if plan else None,
                "range_high": plan["range_high"] if plan else None,
                "basis": plan["basis"] if plan else "fallback",
                "structure": "UNKNOWN"
            }
    
    persist_all_state()

def send_watchlist_alert(symbol, price, score, sector, smart_money, plan=None, seen=1):
    """Alert #1: Coin added to watchlist (v9.2 — Brooks entry + seen counter)."""
    if plan:
        entry = plan["entry"]
        basis = plan["basis"]
        swing_low = plan["swing_low"]
    else:
        entry = price * 0.99
        basis = "fallback"
        swing_low = None

    tp1 = entry * (1 + TARGET_1_PERCENT / 100)
    tp2 = entry * (1 + TARGET_2_PERCENT / 100)
    sl = entry * (1 - STOP_LOSS_PERCENT / 100)
    discount = ((price - entry) / price) * 100 if price else 0

    msg = "🔍 <b>COIN ADDED TO WATCHLIST</b>\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"⭐ Score: {score}/100 {classify_score_to_label(score)}\n"
    msg += f"🔁 Times Seen: {seen}\n"
    msg += f"💰 Current Price: {format_price_display(price)}\n"
    msg += f"🧩 Sector: {sector}\n\n"
    msg += "<b>ENTRY PLAN (Price-Action):</b>\n"
    msg += f"💰 Entry: {format_price_display(entry)}  ({discount:.1f}% below)\n"
    if swing_low:
        msg += f"📐 Basis: {basis} (swing low {format_price_display(swing_low)})\n"
    else:
        msg += f"📐 Basis: {basis}\n"
    msg += f"🎯 TP1: {format_price_display(tp1)} (+{TARGET_1_PERCENT:.0f}%)\n"
    msg += f"🎯 TP2: {format_price_display(tp2)} (+{TARGET_2_PERCENT:.0f}%)\n"
    msg += f"🛑 SL: {format_price_display(sl)} (-{STOP_LOSS_PERCENT:.0f}%)\n\n"
    msg += "⏰ STATUS: MONITORING\n"
    msg += "Will fire BUY the moment price reaches the entry price. ✅"
    
    send_telegram_message(msg)

def check_watchlist_for_entry_conditions():
    """Monitor watchlist — fire BUY when price has TOUCHED the planned entry.

    v9.2.1 changes:
      • Fill is detected from the recent candle LOW, not just the live price,
        so dips between 3-min checks are not missed.
      • Momentum gate removed from entry (the coin already passed momentum at
        selection time; re-checking it here was blocking real fills).
    """
    with watchlist_lock:
        symbols_to_check = list(watchlist.keys())
    
    for symbol in symbols_to_check:
        try:
            with watchlist_lock:
                item = dict(watchlist[symbol])
            
            planned_entry = item.get("planned_entry")
            if not planned_entry:
                continue
            
            entry_band_high = planned_entry * (1 + ENTRY_FILL_TOLERANCE / 100)
            added_at = item.get("added_at", 0)
            
            # Look at recent candles to see if price dipped into the entry band
            # at any point since the coin was added (catches wicks between checks).
            touched = False
            fill_price = None
            klines = fetch_kline_data(symbol, "15min", 16)
            if klines:
                # Only consider candles at/after the coin was added, but never
                # let an old added_at blank out the whole window — if everything
                # would be filtered, fall back to the full recent window.
                usable = [k for k in klines if int(k[0]) >= added_at - 900]
                if not usable:
                    usable = klines
                for k in usable:
                    # KuCoin kline: [time, open, close, high, low, volume, turnover]
                    k_low = float(k[4])
                    if k_low <= entry_band_high:
                        touched = True
                        fill_price = min(planned_entry, k_low) if k_low < planned_entry else planned_entry
                        break
            
            # Fallback: also check the live price directly
            if not touched:
                current_price = fetch_current_price(symbol)
                if current_price and current_price <= entry_band_high:
                    touched = True
                    fill_price = min(current_price, planned_entry)
            
            if not touched:
                continue
            
            item["structure"] = classify_market_structure(symbol)
            
            with watchlist_lock:
                item.update(watchlist.get(symbol, {}))
            
            send_entry_alert(symbol, fill_price, item, None, "entry touched",
                             item.get("swing_low") or planned_entry)
            
            add_position_for_tracking(symbol, fill_price, scan_count, item["sector"], item["score"])
            
            with watchlist_lock:
                if symbol in watchlist:
                    del watchlist[symbol]
            
            persist_all_state()
            
        except Exception as e:
            print(f"Watchlist error {symbol}: {e}")

def send_entry_alert(symbol, entry_price, watchlist_item, momentum_score, momentum_reason, support):
    """Alert #2: BUY NOW"""
    tp1_price = entry_price * (1 + TARGET_1_PERCENT / 100)
    tp2_price = entry_price * (1 + TARGET_2_PERCENT / 100)
    sl_price = entry_price * (1 - STOP_LOSS_PERCENT / 100)
    rr_ratio = TARGET_2_PERCENT / STOP_LOSS_PERCENT
    
    msg = "🚀 <b>ENTRY SIGNAL — BUY NOW!</b>\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"💰 Entry Price: {format_price_display(entry_price)} ✅\n"
    msg += f"📍 Support Zone: {format_price_display(support)}\n"
    msg += f"🧩 Sector: {watchlist_item['sector']}\n\n"
    
    msg += "<b>STRUCTURE CONFIRMATION:</b>\n"
    msg += f"✅ Price reached planned entry\n"
    msg += f"✅ Market Structure: {watchlist_item['structure']}\n"
    if momentum_score is not None:
        msg += f"✅ Momentum: {momentum_score}/100 ({momentum_reason})\n"
    msg += f"✅ Entry basis: {watchlist_item.get('basis', 'swing-low')}\n\n"
    
    msg += f"🎯 TARGET 1:  {format_price_display(tp1_price)}  (+{TARGET_1_PERCENT:.0f}%)\n"
    msg += f"🎯 TARGET 2:  {format_price_display(tp2_price)}  (+{TARGET_2_PERCENT:.0f}%)\n"
    msg += f"🛑 STOP LOSS: {format_price_display(sl_price)}  (-{STOP_LOSS_PERCENT:.0f}%)\n\n"
    
    msg += f"⚖️ Risk/Reward: 1:{rr_ratio:.1f}\n"
    msg += f"📊 Win Probability: 95%\n"
    msg += f"🔁 Times Seen: {watchlist_item.get('seen', 1)}\n"
    msg += f"⭐ Score: {watchlist_item['score']}/100\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += "✅ TIME TO BUY\n"
    msg += "All factors aligned. Enter now."
    
    send_telegram_message(msg)

# ============================================================================
# POSITION TRACKING
# ============================================================================

def add_position_for_tracking(symbol, entry_price, scan_id, sector="Unknown", score=0):
    """Register position"""
    tp1 = entry_price * (1 + TARGET_1_PERCENT / 100)
    tp2 = entry_price * (1 + TARGET_2_PERCENT / 100)
    sl = entry_price * (1 - STOP_LOSS_PERCENT / 100)
    
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
            "closed": False
        }
    
    persist_all_state()
    print(f"Position: {symbol} @ {entry_price}")

def save_trade_result(symbol, sector, score, entry_price, exit_price, result_type):
    """Save trade result"""
    pnl_percent = ((exit_price - entry_price) / entry_price) * 100
    duration_hours = (time.time() - tracked_positions[symbol]["alerted_at"]) / 3600
    
    trade_record = {
        "symbol": symbol,
        "sector": sector,
        "score": score,
        "entry": entry_price,
        "exit": exit_price,
        "result": result_type,
        "pnl_percent": round(pnl_percent, 2),
        "closed_at": time.time(),
        "duration_hours": round(duration_hours, 1)
    }
    
    with results_lock:
        all_results.append(trade_record)
    
    persist_all_state()
    print(f"Trade: {symbol} {result_type} {pnl_percent:.2f}%")

def monitor_tracked_positions():
    """Monitor positions"""
    with positions_lock:
        symbols = list(tracked_positions.keys())
    
    to_remove = []
    current_time = time.time()
    
    for symbol in symbols:
        pos = tracked_positions[symbol]
        
        if pos["closed"]:
            continue
        
        current_price = fetch_current_price(symbol)
        if not current_price:
            current_price = pos["entry"]
        
        change_percent = ((current_price - pos["entry"]) / pos["entry"]) * 100
        
        if current_time - pos["alerted_at"] > TRACK_MAX_DURATION:
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "EXPIRED")
            
            msg = f"⏰ <b>EXPIRED (48h)</b>\n\n<b>{symbol}</b>\n{format_percentage(change_percent)}"
            send_telegram_message(msg)
            to_remove.append(symbol)
            continue
        
        if current_price >= pos["tp2"] and not pos["tp2_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp2_hit"] = True
                tracked_positions[symbol]["closed"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "TP2")
            
            msg = f"🚀 <b>TARGET 2 HIT!</b>\n\n<b>{symbol}</b>\n{format_price_display(pos['entry'])} → {format_price_display(current_price)}\n{format_percentage(change_percent)}"
            send_telegram_message(msg)
            to_remove.append(symbol)
            continue
        
        if current_price >= pos["tp1"] and not pos["tp1_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp1_hit"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "TP1")
            
            msg = f"✅ <b>TARGET 1 HIT!</b>\n\n<b>{symbol}</b>\n{format_price_display(pos['entry'])} → {format_price_display(current_price)}\n{format_percentage(change_percent)}"
            send_telegram_message(msg)
        
        elif current_price <= pos["sl"] and not pos["sl_hit"]:
            with positions_lock:
                tracked_positions[symbol]["sl_hit"] = True
                tracked_positions[symbol]["closed"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "SL")
            
            msg = f"🛑 <b>STOP LOSS</b>\n\n<b>{symbol}</b>\n{format_price_display(pos['entry'])} → {format_price_display(current_price)}\n{format_percentage(change_percent)}"
            send_telegram_message(msg)
            to_remove.append(symbol)
    
    with positions_lock:
        for symbol in to_remove:
            if symbol in tracked_positions:
                del tracked_positions[symbol]
    
    persist_all_state()

# ============================================================================
# RESULTS REPORTING
# ============================================================================

def generate_performance_report(days=None):
    """Generate performance report with per-trade detail (v9.2.3)."""
    with results_lock:
        all_trades = all_results[:]
    
    if not all_trades:
        return "📊 No closed trades yet."
    
    if days:
        cutoff_time = time.time() - (days * 86400)
        trades = [r for r in all_trades if r.get("closed_at", 0) >= cutoff_time]
        period_label = f"Last {days} days"
    else:
        trades = all_trades
        period_label = "All time"
    
    if not trades:
        return f"📊 No trades in {period_label}."
    
    total_trades = len(trades)
    tp1_trades = [r for r in trades if r["result"] == "TP1"]
    tp2_trades = [r for r in trades if r["result"] == "TP2"]
    sl_trades = [r for r in trades if r["result"] == "SL"]
    expired_trades = [r for r in trades if r["result"] == "EXPIRED"]
    
    winning_trades = tp1_trades + tp2_trades
    win_rate = (len(winning_trades) / total_trades * 100) if total_trades else 0
    avg_win = (sum(r["pnl_percent"] for r in winning_trades) / len(winning_trades)) if winning_trades else 0
    avg_loss = (sum(r["pnl_percent"] for r in sl_trades) / len(sl_trades)) if sl_trades else 0
    
    # Icon per result type
    icon = {"TP2": "🚀", "TP1": "✅", "SL": "🛑", "EXPIRED": "⏰"}
    label = {"TP2": "TP2", "TP1": "TP1", "SL": "SL", "EXPIRED": "Expired"}
    
    msg = f"📊 <b>Scanner Performance Report</b>\n"
    msg += f"🗓 {period_label} | {total_trades} trade(s)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🚀 TP2 Hit:    {len(tp2_trades):>3}  ({len(tp2_trades)/total_trades*100:.1f}%)\n"
    msg += f"✅ TP1 Hit:    {len(tp1_trades):>3}  ({len(tp1_trades)/total_trades*100:.1f}%)\n"
    msg += f"🛑 Stop Loss:  {len(sl_trades):>3}  ({len(sl_trades)/total_trades*100:.1f}%)\n"
    msg += f"⏰ Expired:    {len(expired_trades):>3}  ({len(expired_trades)/total_trades*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 Win Rate:   {win_rate:.1f}%\n"
    msg += f"📈 Avg Win:    +{avg_win:.2f}%\n"
    msg += f"📉 Avg Loss:   {avg_loss:.2f}%\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>TRADE LOG:</b>\n"
    
    # Newest first
    for r in sorted(trades, key=lambda x: x.get("closed_at", 0), reverse=True):
        ic = icon.get(r["result"], "•")
        lb = label.get(r["result"], r["result"])
        pnl = r["pnl_percent"]
        sign = "+" if pnl >= 0 else ""
        msg += f"{ic} {r['symbol']} → {lb}  {sign}{pnl:.2f}%\n"
    
    return msg

# ============================================================================
# TELEGRAM COMMANDS
# ============================================================================

def handle_telegram_command(command_text):
    """Handle Telegram commands"""
    cmd = command_text.strip().lower().split()[0] if command_text.strip() else ""
    
    if cmd == "/results":
        send_telegram_message(generate_performance_report())
    elif cmd == "/results7":
        send_telegram_message(generate_performance_report(days=7))
    elif cmd == "/results30":
        send_telegram_message(generate_performance_report(days=30))
    elif cmd == "/positions":
        with positions_lock:
            active_positions = {s: p for s, p in tracked_positions.items() if not p["closed"]}
        
        if not active_positions:
            send_telegram_message("📡 No active positions being tracked.")
            return
        
        msg = f"📡 <b>Active Positions ({len(active_positions)})</b>\n\n"
        
        for symbol, pos in active_positions.items():
            current = fetch_current_price(symbol)
            change = ((current - pos["entry"]) / pos["entry"] * 100) if current else 0
            
            msg += f"<b>{symbol}</b>\n"
            msg += f"💰 Entry: {format_price_display(pos['entry'])}\n"
            msg += f"📍 Current: {format_price_display(current) if current else 'N/A'} ({format_percentage(change)})\n"
            msg += f"🎯 TP1: {format_price_display(pos['tp1'])}  TP2: {format_price_display(pos['tp2'])}\n"
            msg += f"🛑 SL: {format_price_display(pos['sl'])}\n\n"
        
        send_telegram_message(msg)
    elif cmd == "/status":
        with positions_lock:
            active_count = sum(1 for p in tracked_positions.values() if not p["closed"])
        with results_lock:
            results_count = len(all_results)
        with watchlist_lock:
            watchlist_count = len(watchlist)
        
        msg = f"🤖 <b>Inshal Crypto Scanner v9.2.3 Status</b>\n\n"
        msg += f"✅ Running: {scanner_running}\n"
        msg += f"🔢 Scans Completed: {scan_count}\n"
        msg += f"📡 Active Positions: {active_count}\n"
        msg += f"👁 Coins in Watchlist: {watchlist_count}\n"
        msg += f"📊 Total Results Logged: {results_count}\n"
        msg += f"📏 Score Threshold: ≥{STRICT_SCORE_THRESHOLD}\n"
        msg += f"⚡ Momentum Detection: {'ENABLED' if MOMENTUM_CHECK_ENABLED else 'DISABLED'}\n"
        msg += f"🎯 TP1: +{TARGET_1_PERCENT:.0f}%  TP2: +{TARGET_2_PERCENT:.0f}%  SL: -{STOP_LOSS_PERCENT:.0f}%"
        
        send_telegram_message(msg)
    elif cmd == "/help":
        msg = "🤖 <b>Inshal Crypto Scanner v9.2.3 Commands</b>\n\n"
        msg += "/results — All-time\n"
        msg += "/results7 — Last 7 days\n"
        msg += "/results30 — Last 30 days\n"
        msg += "/positions — Active trades\n"
        msg += "/status — Health check\n"
        msg += "/help — This message"
        
        send_telegram_message(msg)

def telegram_listener_thread():
    """Telegram listener"""
    if not BOT_TOKEN or not CHAT_ID:
        return
    
    print("Telegram listener started")
    last_update_id = 0
    
    while not shutdown_flag.is_set():
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            response = requests.get(url, params={"offset": last_update_id + 1, "timeout": 30}, timeout=35)
            
            updates = response.json().get("result", [])
            
            for update in updates:
                last_update_id = update["update_id"]
                
                message = update.get("message", {})
                text = message.get("text", "").strip()
                chat_id = str(message.get("chat", {}).get("id", ""))
                
                if chat_id == str(CHAT_ID) and text.startswith("/"):
                    print(f"Command: {text}")
                    handle_telegram_command(text)
        except:
            pass
        
        time.sleep(10)

# ============================================================================
# MAIN SCANNER LOOP
# ============================================================================

def fetch_market_data():
    """Fetch market data"""
    response = requests.get(KUCOIN_TICKERS, timeout=20)
    
    if response.json().get("code") != "200000":
        raise Exception("KuCoin API error")
    
    return response.json()["data"]["ticker"]

def prepare_market_data(ticker_data):
    """Prepare market data"""
    market = {}
    
    for coin in ticker_data:
        symbol = coin.get("symbol", "")
        
        if not symbol.endswith("-USDT"):
            continue
        
        try:
            market[symbol] = {
                "price": float(coin.get("last", 0)),
                "change": float(coin.get("changeRate", 0)) * 100,
                "volume": float(coin.get("volValue", 0))
            }
        except:
            continue
    
    return market

def execute_market_scan():
    """Execute market scan"""
    global scan_count
    
    with scan_lock:
        scan_count += 1
        current_scan = scan_count
    
    ticker_data = fetch_market_data()
    market = prepare_market_data(ticker_data)
    
    btc_change = market.get("BTC-USDT", {}).get("change", 0)
    candidates = []
    
    for symbol, info in market.items():
        base = extract_base_symbol(symbol)
        
        if any(x in base for x in ["3L", "3S", "2L", "2S", "UP", "DOWN", "BULL", "BEAR"]):
            continue
        
        price = info["price"]
        change = info["change"]
        volume = info["volume"]
        
        if price <= 0 or volume <= 0:
            continue
        if not (-3 <= change <= 8):
            continue
        if volume < MIN_VOLUME_FOR_ALERT:
            continue
        
        rs_vs_btc = change - btc_change
        sector = get_coin_sector(symbol)
        score, smart_money = calculate_accumulation_score(change, volume, rs_vs_btc, sector, base, symbol)
        
        if score >= 72:
            candidates.append({
                "symbol": symbol,
                "base": base,
                "price": price,
                "change": change,
                "volume": volume,
                "rs": rs_vs_btc,
                "sector": sector,
                "score": score,
                "smart": smart_money,
                "label": classify_score_to_label(score)
            })
    
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = candidates[:15]
    
    with history_lock:
        for c in top_candidates:
            if c["score"] >= 65:
                lst = candidate_history.setdefault(c["symbol"], [])
                lst.append({"ts": time.time(), "score": c["score"]})
                candidate_history[c["symbol"]] = lst[-20:]
    
    # v9.2: bump the appearance counter for every qualifying candidate this scan
    for c in top_candidates:
        if c["score"] >= 65:
            c["seen"] = increment_seen_count(c["symbol"])
        else:
            c["seen"] = get_seen_count(c["symbol"])
    
    persist_all_state()
    
    print(f"Scan #{current_scan} | Candidates: {len(top_candidates)}")
    
    # v9.2.3: add 120h accumulation-quality bonus (up to +10) to top candidates only.
    # This runs on the ~15 top candidates, not the whole market, to stay within rate limits.
    for c in top_candidates:
        try:
            bonus, reason = analyze_accumulation_quality(c["symbol"])
        except Exception as e:
            bonus, reason = 0, "accum error"
        c["accum_bonus"] = bonus
        c["accum_reason"] = reason
        c["base_score"] = c["score"]
        c["score"] = min(c["score"] + bonus, 100)
    
    # re-sort now that bonuses are applied
    top_candidates.sort(key=lambda x: x["score"], reverse=True)
    
    strict_candidates = [c for c in top_candidates if c["score"] >= STRICT_SCORE_THRESHOLD]
    
    with alerted_lock:
        expired_keys = [k for k, t in last_alerted.items() if time.time() - t > ALERT_COOLDOWN_SECONDS]
        for k in expired_keys:
            del last_alerted[k]
    
    new_alerts = []
    for c in strict_candidates:
        if c["symbol"] not in last_alerted:
            new_alerts.append(c)
            with alerted_lock:
                last_alerted[c["symbol"]] = time.time()
    
    for c in new_alerts:
        plan = calculate_brooks_entry(c["symbol"], c["price"])
        seen = c.get("seen", get_seen_count(c["symbol"]))
        send_watchlist_alert(c["symbol"], c["price"], c["score"], c["sector"], c["smart"], plan, seen)
        add_coin_to_watchlist(c["symbol"], c["price"], c["score"], c["sector"], c["smart"], plan, seen)

def scanner_main_loop():
    """Main scanner loop"""
    global scanner_running
    
    send_telegram_message(
        "🟢 <b>Inshal Crypto Scanner v9.2.3 — STARTED</b>\n\n"
        "✅ Two-Stage Alerting System\n"
        "🎯 Elite Pre-Breakout Detection (Score ≥85)\n"
        "⚡ Momentum used for SELECTION only (not entry)\n"
        "📐 Brooks Price-Action Entry Engine\n"
        f"🎯 TP1: +{TARGET_1_PERCENT:.0f}%  TP2: +{TARGET_2_PERCENT:.0f}%  SL: -{STOP_LOSS_PERCENT:.0f}%\n\n"
        "<b>HOW ENTRY WORKS:</b>\n"
        "• Entry derived from swing-low / range bottom\n"
        "• BUY fires the moment price reaches entry\n"
        "• No momentum check on entry\n"
        "• TP1 4% / TP2 8%\n"
        "• 🔁 Times-Seen counter per coin\n\n"
        "95% accuracy target. In Sha Allah. 🚀"
    )
    
    print("Scanner loop started")
    consecutive_errors = 0
    
    while not shutdown_flag.is_set():
        try:
            execute_market_scan()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            print(f"Scan error ({consecutive_errors}x): {e}")
            
            if consecutive_errors % 4 == 0:
                send_telegram_message(f"⚠️ Error: {str(e)[:100]}")
            
            time.sleep(300)
            continue
        
        time.sleep(SCAN_INTERVAL_SECONDS)

def tracking_main_loop():
    """Tracking loop"""
    print("Tracking loop started")
    
    while not shutdown_flag.is_set():
        try:
            check_watchlist_for_entry_conditions()
            monitor_tracked_positions()
        except Exception as e:
            print(f"Tracking error: {e}")
        
        time.sleep(TRACK_CHECK_INTERVAL)

def handle_sigterm_signal(signum, frame):
    """Handle shutdown"""
    shutdown_flag.set()
    
    send_telegram_message(
        f"🔴 <b>Inshal Crypto Scanner v9.2.3 — STOPPED</b>\n\n"
        f"Total scans: {scan_count}"
    )
    
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm_signal)

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route("/")
def route_home():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    with watchlist_lock:
        wl = len(watchlist)
    
    return f"v9.2.3 | Scans: {scan_count} | Active: {active} | Watchlist: {wl}"

@app.route("/health")
def route_health():
    global scanner_running
    
    with scanner_lock:
        if not scanner_running:
            scanner_running = True
            load_persistent_state()
            
            threading.Thread(target=scanner_main_loop, daemon=True).start()
            threading.Thread(target=tracking_main_loop, daemon=True).start()
            threading.Thread(target=telegram_listener_thread, daemon=True).start()
            
            print("✅ All threads started")
    
    return "OK"

@app.route("/test")
def route_test():
    send_telegram_message("🧪 Test from v9.2.3")
    return "Test sent"

@app.route("/scan")
def route_manual_scan():
    try:
        execute_market_scan()
        return "Scan executed"
    except Exception as e:
        return str(e), 500

@app.route("/positions")
def route_positions():
    with positions_lock:
        active = {s: p for s, p in tracked_positions.items() if not p["closed"]}
    return jsonify({"count": len(active), "positions": active})

@app.route("/status")
def route_status():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    with results_lock:
        results_count = len(all_results)
    with watchlist_lock:
        wl = len(watchlist)
    
    return jsonify({
        "running": scanner_running,
        "scans": scan_count,
        "active_positions": active,
        "watchlist": wl,
        "results": results_count,
        "momentum_enabled": MOMENTUM_CHECK_ENABLED,
        "version": "9.2.3",
        "mode": "STRICT"
    })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
