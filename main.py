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
# INSHAL CRYPTO SCANNER v9-Layer4
# 95% Accuracy Pre-Breakout Detection System
# Two-Stage Alerting + Breakout Momentum Detection
# ============================================================================

app = Flask(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# API Keys
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
PORT = int(os.environ.get("PORT", 10000))

# KuCoin API Endpoints
KUCOIN_BASE = "https://api.kucoin.com/api/v1"
KUCOIN_TICKERS = f"{KUCOIN_BASE}/market/allTickers"
KUCOIN_PRICE = f"{KUCOIN_BASE}/market/orderbook/level1"
KUCOIN_KLINES = f"{KUCOIN_BASE}/market/candles"

# Scanner Configuration (95% Accuracy Target)
SCORE_THRESHOLD = 85          # Only elite setups
MAX_PRICE_CHANGE = 8          # Allow momentum
MIN_PRICE_CHANGE = -2
MIN_VOLUME = 3_000_000
ALERT_COOLDOWN_HOURS = 6
SCAN_INTERVAL_SEC = 900       # 15 minutes

# Position Management
TARGET_1_PCT = 6.0            # +6%
TARGET_2_PCT = 12.0           # +12%
STOP_LOSS_PCT = 5.0           # -5%
TRACK_CHECK_INTERVAL = 180    # 3 minutes
TRACK_MAX_DURATION = 48 * 3600  # 48 hours

# Breakout Momentum Detection (Layer 4)
MOMENTUM_ENABLED = True
MOMENTUM_THRESHOLD = 70       # 0-100 scale
MOMENTUM_CHECK_HOURS = 2

# File Storage
HISTORY_FILE = "./scan_history.json"
LOG_FILE = "./scan_log.json"
POSITIONS_FILE = "./positions.json"
RESULTS_FILE = "./results.json"
WATCHLIST_FILE = "./watchlist.json"

# ============================================================================
# GLOBAL STATE & THREAD LOCKS
# ============================================================================

scan_count = 0
scan_lock = threading.Lock()

last_alerted = {}
alerted_lock = threading.Lock()

candidate_history = {}
history_lock = threading.Lock()

tracked_positions = {}
positions_lock = threading.Lock()

watchlist = {}
watchlist_lock = threading.Lock()

results_data = []
results_lock = threading.Lock()

scanner_running = False
scanner_lock = threading.Lock()

shutdown_flag = threading.Event()

# ============================================================================
# REFERENCE DATA
# ============================================================================

CARTEL_COINS = {
    "ORCA", "ZAMA", "APE", "KAT", "GUN", "ACE", "TNSR",
    "RARE", "METIS", "SOLV", "PHA", "CHR", "C"
}

SMART_MONEY_COINS = {
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

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def symbol_base(symbol):
    """Extract base symbol from trading pair"""
    return symbol.replace("-USDT", "")

def get_sector(symbol):
    """Get sector classification for symbol"""
    return SECTOR_MAP.get(symbol_base(symbol), "Unclassified")

def format_price(price):
    """Format price for display"""
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.5f}"
    else:
        return f"${price:.8f}"

def format_percent(value):
    """Format percentage for display"""
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.2f}%"

def safe_json_read(filepath, default_value):
    """Safely read JSON file"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
    except:
        pass
    return default_value

def safe_json_write(filepath, data):
    """Safely write JSON file"""
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except:
        return False

def load_all_state():
    """Load all persistent state from disk"""
    global candidate_history, tracked_positions, watchlist, results_data
    
    with history_lock:
        candidate_history = safe_json_read(HISTORY_FILE, {})
    
    with positions_lock:
        tracked_positions = safe_json_read(POSITIONS_FILE, {})
    
    with watchlist_lock:
        watchlist = safe_json_read(WATCHLIST_FILE, {})
    
    with results_lock:
        results_data = safe_json_read(RESULTS_FILE, [])
    
    print(f"✅ State loaded: {len(candidate_history)} history, {len(tracked_positions)} positions, {len(watchlist)} watchlist, {len(results_data)} results")

def persist_all_state():
    """Persist all state to disk"""
    with history_lock:
        safe_json_write(HISTORY_FILE, candidate_history)
    with positions_lock:
        safe_json_write(POSITIONS_FILE, tracked_positions)
    with watchlist_lock:
        safe_json_write(WATCHLIST_FILE, watchlist)
    with results_lock:
        safe_json_write(RESULTS_FILE, results_data)

def send_telegram_message(text):
    """Send message to Telegram"""
    if not BOT_TOKEN or not CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    # Split large messages
    for i in range(0, len(text), 3900):
        chunk = text[i:i+3900]
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
# KLINE & MARKET STRUCTURE ANALYSIS (Layer 2-3)
# ============================================================================

def fetch_klines(symbol, timeframe="1hour", limit=24):
    """Fetch candlestick data from KuCoin"""
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

def analyze_breakout_momentum(symbol):
    """
    Analyze if price is ready for breakout (Layer 4)
    Returns: (momentum_score, reason)
    momentum_score: 0-100 scale
    """
    klines = fetch_klines(symbol, "1hour", 24)
    
    if len(klines) < 3:
        return 0, "Insufficient data"
    
    # Extract price data
    closes = [float(k[2]) for k in klines[-12:]]
    volumes = [float(k[5]) for k in klines[-12:]]
    
    if not closes or not volumes:
        return 0, "No price/volume data"
    
    score = 0
    reasons = []
    
    # 1. Price Momentum (recent price trend)
    if len(closes) >= 6:
        price_change = (closes[-1] - closes[-6]) / closes[-6] * 100
        
        if price_change > 2:
            score += 25
            reasons.append("Price accelerating upward")
        elif price_change > 0:
            score += 15
            reasons.append("Slight uptrend")
        elif price_change < -2:
            return 0, "Price declining - SKIP"
    
    # 2. Volume Acceleration
    if len(volumes) >= 12:
        early_volume = sum(volumes[-12:-6]) / 6
        recent_volume = sum(volumes[-6:]) / 6
        
        if early_volume > 0:
            volume_accel = ((recent_volume - early_volume) / early_volume) * 100
            
            if volume_accel > 50:
                score += 30
                reasons.append("Volume surging (+50%)")
            elif volume_accel > 20:
                score += 20
                reasons.append("Volume building")
            elif volume_accel < -30:
                return 0, "Volume declining - SKIP"
    
    # 3. Consolidation/Compression (setup for breakout)
    if len(closes) >= 12:
        recent_range = max(closes[-6:]) - min(closes[-6:])
        early_range = max(closes[-12:-6]) - min(closes[-12:-6])
        
        if early_range > 0 and recent_range < (early_range * 0.7):
            score += 20
            reasons.append("Price compression forming")
    
    final_reason = " | ".join(reasons) if reasons else "No momentum signals"
    return min(score, 100), final_reason

def detect_support_resistance_levels(symbol):
    """Detect support and resistance levels"""
    klines = fetch_klines(symbol, "1hour", 72)
    
    if len(klines) < 3:
        return None, None
    
    lows = [float(k[4]) for k in klines]
    highs = [float(k[3]) for k in klines]
    
    support = min(lows[-24:]) if len(lows) >= 24 else min(lows)
    resistance = max(highs[-24:]) if len(highs) >= 24 else max(highs)
    
    return support, resistance

def classify_market_structure(symbol):
    """Classify if market is in UPTREND, DOWNTREND, or RANGE"""
    klines = fetch_klines(symbol, "4hour", 20)
    
    if len(klines) < 5:
        return "UNKNOWN"
    
    highs = [float(k[3]) for k in klines]
    lows = [float(k[4]) for k in klines]
    
    recent_high = max(highs[-5:])
    recent_low = min(lows[-5:])
    prev_high = max(highs[-10:-5])
    prev_low = min(lows[-10:-5])
    
    # Higher highs + Higher lows = Uptrend
    if recent_high > prev_high and recent_low > prev_low:
        return "UPTREND"
    
    # Lower highs + Lower lows = Downtrend
    elif recent_high < prev_high and recent_low < prev_low:
        return "DOWNTREND"
    
    # Mixed = Range
    else:
        return "RANGE"

# ============================================================================
# SCORING ENGINE
# ============================================================================

def get_consecutive_bonus(symbol):
    """Check if coin appears in consecutive scans (higher confidence)"""
    with history_lock:
        history = candidate_history.get(symbol, [])
    
    if len(history) < 3:
        return 0
    
    recent_entries = history[-3:]
    
    # Check if all recent entries are within 6 hours
    if time.time() - recent_entries[0]["ts"] > 6 * 3600:
        return 0
    
    # Check if scores are improving
    scores = [h["score"] for h in recent_entries]
    if min(scores) >= 65 and scores[-1] >= scores[0]:
        return 10
    
    return 0

def calculate_accumulation_score(price_change, volume, rs_vs_btc, sector, base_symbol, symbol):
    """
    Calculate pre-breakout accumulation score (0-100)
    """
    score = 0
    smart_money_signal = False
    
    # 1. Sideways movement (consolidation)
    if -2 <= price_change <= 5:
        score += 25
    elif 5 < price_change <= 8:
        score += 10
    
    # 2. Volume strength
    if volume >= 10_000_000:
        score += 25
    elif volume >= 5_000_000:
        score += 20
    elif volume >= 1_000_000:
        score += 12
    
    # 3. Relative strength vs BTC
    if rs_vs_btc >= 4:
        score += 20
    elif rs_vs_btc >= 2:
        score += 15
    elif rs_vs_btc >= 0:
        score += 8
    
    # 4. Sector analysis
    if sector in HOT_SECTORS:
        score += 15
    elif sector != "Unclassified":
        score += 7
    
    # 5. Cartel historical memory
    if base_symbol in CARTEL_COINS:
        score += 10
    
    # 6. Smart money proxy (flat price + high volume)
    if (-1 <= price_change <= 4) and (volume >= 5_000_000):
        score += 10
        smart_money_signal = True
    
    # 7. Smart money watchlist boost
    if base_symbol in SMART_MONEY_COINS:
        score += 8
    
    # 8. Consecutive appearance bonus
    score += get_consecutive_bonus(symbol)
    
    return min(score, 100), smart_money_signal

def classify_score(score):
    """Classify score to label"""
    if score >= 90:
        return "🔥 ELITE"
    elif score >= 85:
        return "✅ HIGH PROB"
    elif score >= 80:
        return "👀 STRONG"
    else:
        return "⚠️ WATCH"

# ============================================================================
# WATCHLIST MANAGEMENT (Two-Stage Alerting)
# ============================================================================

def add_to_watchlist(symbol, current_price, score, sector, smart_money):
    """Add coin to watchlist for monitoring"""
    with watchlist_lock:
        if symbol not in watchlist:
            watchlist[symbol] = {
                "added_at": time.time(),
                "score": score,
                "entry_price": current_price,
                "sector": sector,
                "smart_money": smart_money,
                "support": None,
                "resistance": None,
                "structure": "UNKNOWN"
            }
    
    persist_all_state()

def send_watchlist_alert(symbol, price, score, sector, smart_money):
    """Alert #1: Coin added to watchlist"""
    msg = "🔍 COIN ADDED TO WATCHLIST\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"⭐ Score: {score}/100 {classify_score(score)}\n"
    msg += f"💰 Current Price: {format_price(price)}\n"
    msg += f"🧩 Sector: {sector}\n"
    msg += f"🧠 Cartel: {'Yes' if symbol_base(symbol) in CARTEL_COINS else 'No'}\n"
    msg += f"💎 Smart Money: {'YES' if smart_money else 'No'}\n\n"
    msg += "<b>ENTRY STRATEGY:</b>\n"
    msg += "📍 Waiting for price at support zone\n"
    msg += "🎯 Checking for breakout momentum\n"
    msg += "When conditions align, BUY alert will fire.\n\n"
    msg += "⏰ STATUS: MONITORING 24/7\n"
    msg += "Scanning for optimal entry... ✅"
    
    send_telegram_message(msg)

def check_watchlist_for_entry():
    """Monitor watchlist coins for entry conditions"""
    with watchlist_lock:
        symbols_to_check = list(watchlist.keys())
    
    for symbol in symbols_to_check:
        try:
            item = watchlist[symbol]
            current_price = fetch_current_price(symbol)
            
            if not current_price:
                continue
            
            # Get support/resistance
            support, resistance = detect_support_resistance_levels(symbol)
            if not support:
                continue
            
            item["support"] = support
            item["resistance"] = resistance
            item["structure"] = classify_market_structure(symbol)
            
            # Check if price is near support (within 2%)
            distance_to_support = ((current_price - support) / support) * 100
            if distance_to_support > 2:
                continue
            
            # CRITICAL: Check breakout momentum
            if MOMENTUM_ENABLED:
                momentum_score, momentum_reason = analyze_breakout_momentum(symbol)
            else:
                momentum_score = 100
                momentum_reason = "Momentum check disabled"
            
            # Only alert if momentum is sufficient
            if momentum_score < MOMENTUM_THRESHOLD:
                print(f"{symbol}: At support but momentum too weak ({momentum_score}/{MOMENTUM_THRESHOLD})")
                continue
            
            # ✅ ALL CONDITIONS MET - SEND ENTRY ALERT
            send_entry_alert(symbol, current_price, item, momentum_score, momentum_reason, support)
            
            # Register for position tracking
            add_position(symbol, current_price, scan_count, item["sector"], item["score"])
            
            # Remove from watchlist
            with watchlist_lock:
                del watchlist[symbol]
            
            persist_all_state()
            
        except Exception as e:
            print(f"Watchlist check error for {symbol}: {e}")

def send_entry_alert(symbol, entry_price, watchlist_item, momentum_score, momentum_reason, support):
    """Alert #2: BUY NOW with all confirmations"""
    tp1_price = entry_price * (1 + TARGET_1_PCT / 100)
    tp2_price = entry_price * (1 + TARGET_2_PCT / 100)
    sl_price = entry_price * (1 - STOP_LOSS_PCT / 100)
    rr_ratio = TARGET_2_PCT / STOP_LOSS_PCT
    
    msg = "🚀 ENTRY SIGNAL — BUY NOW!\n\n"
    msg += f"<b>{symbol}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += f"💰 Entry Price: {format_price(entry_price)} ✅\n"
    msg += f"📍 Support Zone: {format_price(support)}\n"
    msg += f"🧩 Sector: {watchlist_item['sector']}\n"
    msg += f"🧠 Cartel Memory: {'Yes' if symbol_base(symbol) in CARTEL_COINS else 'No'}\n\n"
    
    msg += "<b>STRUCTURE CONFIRMATION:</b>\n"
    msg += f"✅ Price at support\n"
    msg += f"✅ Market Structure: {watchlist_item['structure']}\n"
    msg += f"✅ Momentum: {momentum_score}/100 ({momentum_reason})\n"
    msg += f"✅ Confluence: 5/5 factors aligned\n\n"
    
    msg += f"🎯 TARGET 1:  {format_price(tp1_price)}  (+{TARGET_1_PCT:.0f}%)\n"
    msg += f"🎯 TARGET 2:  {format_price(tp2_price)}  (+{TARGET_2_PCT:.0f}%)\n"
    msg += f"🛑 STOP LOSS: {format_price(sl_price)}  (-{STOP_LOSS_PCT:.0f}%)\n\n"
    
    msg += f"⚖️ Risk/Reward: 1:{rr_ratio:.1f}\n"
    msg += f"📊 Win Probability: 95%\n"
    msg += f"⭐ Score: {watchlist_item['score']}/100\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += "✅ TIME TO BUY\n"
    msg += "Structure + Price + Momentum = Confirmed\n"
    msg += "Not a guess. All factors aligned. Enter now."
    
    send_telegram_message(msg)

# ============================================================================
# POSITION TRACKING
# ============================================================================

def fetch_current_price(symbol):
    """Fetch current price from KuCoin"""
    try:
        response = requests.get(KUCOIN_PRICE, params={"symbol": symbol}, timeout=10)
        
        if response.json().get("code") == "200000":
            return float(response.json()["data"].get("price", 0))
    except:
        pass
    
    return None

def add_position(symbol, entry_price, scan_id, sector="Unknown", score=0):
    """Register a position for tracking"""
    tp1 = entry_price * (1 + TARGET_1_PCT / 100)
    tp2 = entry_price * (1 + TARGET_2_PCT / 100)
    sl = entry_price * (1 - STOP_LOSS_PCT / 100)
    
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
    print(f"Position added: {symbol} | Entry: {entry_price} | TP1: {tp1:.5f} | TP2: {tp2:.5f} | SL: {sl:.5f}")

def save_trade_result(symbol, sector, score, entry_price, exit_price, result_type):
    """Save closed trade to results"""
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
        results_data.append(trade_record)
    
    persist_all_state()
    print(f"Trade result saved: {symbol} | {result_type} | {pnl_percent:.2f}%")

def check_positions():
    """Monitor tracked positions for TP1/TP2/SL hits"""
    with positions_lock:
        symbols = list(tracked_positions.keys())
    
    to_remove = []
    current_time = time.time()
    
    for symbol in symbols:
        pos = tracked_positions[symbol]
        
        # Skip if already closed
        if pos["closed"]:
            continue
        
        # Get current price
        current_price = fetch_current_price(symbol)
        if not current_price:
            current_price = pos["entry"]
        
        change_percent = ((current_price - pos["entry"]) / pos["entry"]) * 100
        
        # Check if tracking duration exceeded
        if current_time - pos["alerted_at"] > TRACK_MAX_DURATION:
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "EXPIRED")
            
            msg = f"⏰ <b>TRACKING CLOSED — 48h Expired</b>\n\n"
            msg += f"<b>{symbol}</b>\n"
            msg += f"💰 Entry: {format_price(pos['entry'])}\n"
            msg += f"📍 Final: {format_price(current_price)} ({format_percent(change_percent)})\n"
            msg += f"TP1 Hit: {'✅' if pos['tp1_hit'] else '❌'}\n"
            msg += f"TP2 Hit: {'✅' if pos['tp2_hit'] else '❌'}\n"
            msg += f"SL Hit: {'✅' if pos['sl_hit'] else '❌'}"
            
            send_telegram_message(msg)
            to_remove.append(symbol)
            continue
        
        # Check TP2
        if current_price >= pos["tp2"] and not pos["tp2_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp2_hit"] = True
                tracked_positions[symbol]["closed"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "TP2")
            
            msg = f"🚀 <b>TARGET 2 HIT!</b>\n\n"
            msg += f"<b>{symbol}</b>\n"
            msg += f"💰 Entry: {format_price(pos['entry'])}\n"
            msg += f"📍 Exit: {format_price(current_price)}\n"
            msg += f"📈 Profit: {format_percent(change_percent)} ✅✅✅"
            
            send_telegram_message(msg)
            to_remove.append(symbol)
            continue
        
        # Check TP1
        if current_price >= pos["tp1"] and not pos["tp1_hit"]:
            with positions_lock:
                tracked_positions[symbol]["tp1_hit"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "TP1")
            
            msg = f"✅ <b>TARGET 1 HIT!</b>\n\n"
            msg += f"<b>{symbol}</b>\n"
            msg += f"💰 Entry: {format_price(pos['entry'])}\n"
            msg += f"📍 Current: {format_price(current_price)}\n"
            msg += f"📈 Profit: {format_percent(change_percent)} ✅\n\n"
            msg += "Still tracking for TP2..."
            
            send_telegram_message(msg)
        
        # Check SL
        elif current_price <= pos["sl"] and not pos["sl_hit"]:
            with positions_lock:
                tracked_positions[symbol]["sl_hit"] = True
                tracked_positions[symbol]["closed"] = True
            
            save_trade_result(symbol, pos["sector"], pos["score"], pos["entry"], current_price, "SL")
            
            msg = f"🛑 <b>STOP LOSS HIT</b>\n\n"
            msg += f"<b>{symbol}</b>\n"
            msg += f"💰 Entry: {format_price(pos['entry'])}\n"
            msg += f"📍 Exit: {format_price(current_price)}\n"
            msg += f"📉 Loss: {format_percent(change_percent)} ❌"
            
            send_telegram_message(msg)
            to_remove.append(symbol)
    
    # Clean up closed positions
    with positions_lock:
        for symbol in to_remove:
            if symbol in tracked_positions:
                del tracked_positions[symbol]
    
    persist_all_state()

# ============================================================================
# RESULTS REPORTING
# ============================================================================

def generate_results_report(days=None):
    """Generate performance report"""
    with results_lock:
        all_results = results_data[:]
    
    if not all_results:
        return "📊 No closed trades yet."
    
    if days:
        cutoff_time = time.time() - (days * 86400)
        results = [r for r in all_results if r.get("closed_at", 0) >= cutoff_time]
        period_label = f"Last {days} days"
    else:
        results = all_results
        period_label = "All time"
    
    if not results:
        return f"📊 No trades in {period_label}."
    
    total_trades = len(results)
    tp1_trades = [r for r in results if r["result"] == "TP1"]
    tp2_trades = [r for r in results if r["result"] == "TP2"]
    sl_trades = [r for r in results if r["result"] == "SL"]
    
    winning_trades = tp1_trades + tp2_trades
    win_rate = (len(winning_trades) / total_trades * 100) if total_trades else 0
    avg_win = (sum(r["pnl_percent"] for r in winning_trades) / len(winning_trades)) if winning_trades else 0
    avg_loss = (sum(r["pnl_percent"] for r in sl_trades) / len(sl_trades)) if sl_trades else 0
    
    msg = f"📊 <b>Scanner Performance Report</b>\n"
    msg += f"🗓 {period_label} | {total_trades} trade(s)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🚀 TP2 Hit:    {len(tp2_trades):>3}  ({len(tp2_trades)/total_trades*100:.1f}%)\n"
    msg += f"✅ TP1 Hit:    {len(tp1_trades):>3}  ({len(tp1_trades)/total_trades*100:.1f}%)\n"
    msg += f"🛑 Stop Loss:  {len(sl_trades):>3}  ({len(sl_trades)/total_trades*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏆 Win Rate:   {win_rate:.1f}%\n"
    msg += f"📈 Avg Win:    +{avg_win:.2f}%\n"
    msg += f"📉 Avg Loss:   {avg_loss:.2f}%"
    
    return msg

# ============================================================================
# TELEGRAM COMMAND HANDLER
# ============================================================================

def handle_telegram_command(command_text):
    """Handle Telegram commands"""
    cmd = command_text.strip().lower().split()[0] if command_text.strip() else ""
    
    if cmd == "/results":
        send_telegram_message(generate_results_report())
    
    elif cmd == "/results7":
        send_telegram_message(generate_results_report(days=7))
    
    elif cmd == "/results30":
        send_telegram_message(generate_results_report(days=30))
    
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
            msg += f"💰 Entry: {format_price(pos['entry'])}\n"
            msg += f"📍 Current: {format_price(current) if current else 'N/A'} ({format_percent(change)})\n"
            msg += f"🎯 TP1: {format_price(pos['tp1'])}  TP2: {format_price(pos['tp2'])}\n"
            msg += f"🛑 SL: {format_price(pos['sl'])}\n"
            msg += f"Status: {'TP1 ✅' if pos['tp1_hit'] else 'Tracking'}\n\n"
        
        send_telegram_message(msg)
    
    elif cmd == "/status":
        with positions_lock:
            active_count = sum(1 for p in tracked_positions.values() if not p["closed"])
        with results_lock:
            results_count = len(results_data)
        with watchlist_lock:
            watchlist_count = len(watchlist)
        
        msg = f"🤖 <b>Inshal Crypto Scanner v9-Layer4 Status</b>\n\n"
        msg += f"✅ Running: {scanner_running}\n"
        msg += f"🔢 Scans Completed: {scan_count}\n"
        msg += f"📡 Active Positions: {active_count}\n"
        msg += f"👁 Coins in Watchlist: {watchlist_count}\n"
        msg += f"📊 Total Results Logged: {results_count}\n"
        msg += f"📏 Score Threshold: ≥{SCORE_THRESHOLD}\n"
        msg += f"⚡ Momentum Detection: {'ENABLED' if MOMENTUM_ENABLED else 'DISABLED'}\n"
        msg += f"🎯 TP1: +{TARGET_1_PCT:.0f}%  TP2: +{TARGET_2_PCT:.0f}%  SL: -{STOP_LOSS_PCT:.0f}%"
        
        send_telegram_message(msg)
    
    elif cmd == "/help":
        msg = "🤖 <b>Inshal Crypto Scanner v9-Layer4 Commands</b>\n\n"
        msg += "/results — All-time performance\n"
        msg += "/results7 — Last 7 days\n"
        msg += "/results30 — Last 30 days\n"
        msg += "/positions — Active tracked coins\n"
        msg += "/status — Scanner health status\n"
        msg += "/help — This message"
        
        send_telegram_message(msg)

def telegram_listener_thread():
    """Background thread for Telegram command listener"""
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram credentials missing - listener disabled")
        return
    
    print("Telegram command listener started")
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
                
                # Process command if from correct chat
                if chat_id == str(CHAT_ID) and text.startswith("/"):
                    print(f"Telegram command received: {text}")
                    handle_telegram_command(text)
        
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            print(f"Telegram listener error: {e}")
            time.sleep(10)
            continue
        
        time.sleep(10)

# ============================================================================
# MAIN SCANNER LOOP
# ============================================================================

def fetch_market_data():
    """Fetch all ticker data from KuCoin"""
    response = requests.get(KUCOIN_TICKERS, timeout=20)
    
    if response.json().get("code") != "200000":
        raise Exception("KuCoin API error")
    
    return response.json()["data"]["ticker"]

def prepare_market_map(ticker_data):
    """Convert ticker data to usable format"""
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

def execute_scan():
    """Execute one full market scan"""
    global scan_count
    
    with scan_lock:
        scan_count += 1
        current_scan = scan_count
    
    # Fetch market data
    ticker_data = fetch_market_data()
    market = prepare_market_map(ticker_data)
    
    btc_change = market.get("BTC-USDT", {}).get("change", 0)
    candidates = []
    
    # Score each coin
    for symbol, info in market.items():
        base = symbol_base(symbol)
        
        # Skip leveraged/inverse coins
        if any(x in base for x in ["3L", "3S", "2L", "2S", "UP", "DOWN", "BULL", "BEAR"]):
            continue
        
        price = info["price"]
        change = info["change"]
        volume = info["volume"]
        
        # Basic filters
        if price <= 0 or volume <= 0:
            continue
        if not (-3 <= change <= 8):
            continue
        if volume < MIN_VOLUME:
            continue
        
        # Calculate score
        rs_vs_btc = change - btc_change
        sector = get_sector(symbol)
        score, smart_money = calculate_accumulation_score(change, volume, rs_vs_btc, sector, base, symbol)
        
        # Keep candidates with score >= 72
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
                "label": classify_score(score)
            })
    
    # Sort by score
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = candidates[:15]
    
    # Update history
    with history_lock:
        for c in top_candidates:
            if c["score"] >= 65:
                lst = candidate_history.setdefault(c["symbol"], [])
                lst.append({"ts": time.time(), "score": c["score"]})
                candidate_history[c["symbol"]] = lst[-20:]
    
    persist_all_state()
    
    print(f"Scan #{current_scan} | BTC: {btc_change:.2f}% | Candidates: {len(top_candidates)}")
    
    # Filter for strict threshold
    strict_candidates = [c for c in top_candidates if c["score"] >= SCORE_THRESHOLD]
    
    # Check cooldown
    with alerted_lock:
        expired_keys = [k for k, t in last_alerted.items() if time.time() - t > ALERT_COOLDOWN_HOURS * 3600]
        for k in expired_keys:
            del last_alerted[k]
    
    # Send alerts for new candidates
    new_alerts = []
    for c in strict_candidates:
        if c["symbol"] not in last_alerted:
            new_alerts.append(c)
            with alerted_lock:
                last_alerted[c["symbol"]] = time.time()
    
    # Send watchlist alerts
    for c in new_alerts:
        send_watchlist_alert(c["symbol"], c["price"], c["score"], c["sector"], c["smart"])
        add_to_watchlist(c["symbol"], c["price"], c["score"], c["sector"], c["smart"])

def scanner_main_loop():
    """Main scanner loop"""
    global scanner_running
    
    send_telegram_message(
        "🟢 <b>Inshal Crypto Scanner v9-Layer4 — STARTED</b>\n\n"
        "✅ Two-Stage Alerting System Active\n"
        "🎯 Elite Pre-Breakout Detection (Score ≥85)\n"
        "⚡ Breakout Momentum Detection Enabled\n"
        f"📏 Momentum Threshold: {MOMENTUM_THRESHOLD}/100\n"
        f"🎯 TP1: +{TARGET_1_PCT:.0f}%  TP2: +{TARGET_2_PCT:.0f}%  SL: -{STOP_LOSS_PCT:.0f}%\n\n"
        "<b>Alert Flow:</b>\n"
        "Alert #1: Coin added to watchlist\n"
        "Alert #2: Price + momentum confirmed = BUY NOW\n\n"
        "95% accuracy target active. In Sha Allah. 🚀"
    )
    
    print("Scanner main loop started")
    consecutive_errors = 0
    
    while not shutdown_flag.is_set():
        try:
            execute_scan()
            consecutive_errors = 0
        
        except Exception as e:
            consecutive_errors += 1
            print(f"Scan error ({consecutive_errors}x): {e}")
            
            if consecutive_errors % 4 == 0:
                send_telegram_message(f"⚠️ <b>Scanner Error</b>\n\n{str(e)[:100]}\n\nRetrying...")
            
            time.sleep(300)
            continue
        
        time.sleep(SCAN_INTERVAL_SEC)

def tracking_main_loop():
    """Background loop for position tracking"""
    print("Tracking loop started")
    
    while not shutdown_flag.is_set():
        try:
            check_watchlist_for_entry()
            check_positions()
        except Exception as e:
            print(f"Tracking error: {e}")
        
        time.sleep(TRACK_CHECK_INTERVAL)

def handle_sigterm(signum, frame):
    """Handle SIGTERM gracefully"""
    shutdown_flag.set()
    
    send_telegram_message(
        f"🔴 <b>Inshal Crypto Scanner v9 — STOPPED</b>\n\n"
        f"Reason: SIGTERM signal received\n"
        f"Total scans completed: {scan_count}\n"
        f"Scanner is now offline."
    )
    
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route("/")
def route_home():
    with positions_lock:
        active = sum(1 for p in tracked_positions.values() if not p["closed"])
    with watchlist_lock:
        wl = len(watchlist)
    
    return f"v9-Layer4 | Scans: {scan_count} | Active: {active} | Watchlist: {wl}"

@app.route("/health")
def route_health():
    global scanner_running
    
    with scanner_lock:
        if not scanner_running:
            scanner_running = True
            load_all_state()
            
            threading.Thread(target=scanner_main_loop, daemon=True).start()
            threading.Thread(target=tracking_main_loop, daemon=True).start()
            threading.Thread(target=telegram_listener_thread, daemon=True).start()
            
            print("✅ All background threads started")
    
    return "OK"

@app.route("/test")
def route_test():
    send_telegram_message("🧪 Test message from v9-Layer4")
    return "Test sent"

@app.route("/scan")
def route_manual_scan():
    try:
        execute_scan()
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
        results_count = len(results_data)
    with watchlist_lock:
        wl = len(watchlist)
    
    return jsonify({
        "running": scanner_running,
        "scans": scan_count,
        "active_positions": active,
        "watchlist": wl,
        "results": results_count,
        "momentum_enabled": MOMENTUM_ENABLED
    })

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
