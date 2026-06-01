import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

KUCOIN_URL = "https://api.kucoin.com/api/v1/market/allTickers"

last_alerted = set()
scan_count = 0
scan_lock = threading.Lock()  # Thread-safe scan counter

SECTORS = {
    "GUN": "Gaming",
    "ORCA": "Solana DeFi",
    "RAY": "Solana DeFi",
    "JUP": "Solana",
    "SOL": "Layer 1",
    "METIS": "Layer 2",
    "ARB": "Layer 2",
    "OP": "Layer 2",
    "ZAMA": "Privacy / AI",
    "ACE": "Gaming",
    "APE": "Gaming / NFT",
    "PHA": "AI / Privacy",
    "CHR": "Gaming",
    "TNSR": "Solana NFT",
    "HYPE": "DeFi",
    "XLM": "Layer 1",
    "ZEC": "Privacy",
    "BTC": "Layer 1",
    "ETH": "Layer 1",
    "AVAX": "Layer 1",
    "BNB": "Layer 1",
    "MATIC": "Layer 2",
    "LINK": "Oracle",
    "UNI": "DeFi",
    "AAVE": "DeFi",
    "SNX": "DeFi",
}


# ──────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────

def format_price(price):
    """Smart price formatter — avoids ugly trailing zeros."""
    if price >= 1000:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.5f}"
    else:
        return f"${price:.8f}"


def send_telegram(message):
    """Send a Telegram message, splitting if over 4096 chars."""
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram BOT_TOKEN or CHAT_ID missing")
        return

    # Telegram max message length is 4096 characters
    MAX_LEN = 4000
    chunks = [message[i:i+MAX_LEN] for i in range(0, len(message), MAX_LEN)]

    for chunk in chunks:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            response = requests.post(
                url,
                data={
                    "chat_id": int(CHAT_ID),
                    "text": chunk,
                    "parse_mode": "HTML"
                },
                timeout=15
            )
            response.raise_for_status()
            print("Telegram sent:", response.status_code)
        except requests.exceptions.HTTPError as e:
            print(f"Telegram HTTP Error: {e} | Response: {e.response.text}")
        except Exception as e:
            print("Telegram error:", str(e))


def get_sector(symbol):
    base = symbol.replace("-USDT", "")
    return SECTORS.get(base, "Unclassified")


def calculate_score(change, volume, sector):
    momentum_score = min(change / 25 * 35, 35)
    volume_score = min(volume / 50_000_000 * 25, 25)

    sector_score = 0
    if sector in ["Solana DeFi", "Solana", "Gaming", "AI / Privacy", "Layer 2", "DeFi"]:
        sector_score = 20
    elif sector != "Unclassified":
        sector_score = 10

    liquidity_score = 20 if volume >= 5_000_000 else 10

    total = momentum_score + volume_score + sector_score + liquidity_score
    return round(total, 2)


def classify_setup(score):
    if score >= 80:
        return "🔥 Elite Setup"
    elif score >= 70:
        return "✅ High Probability Candidate"
    elif score >= 55:
        return "👀 Watchlist"
    else:
        return "⚠️ Low Priority"


# ──────────────────────────────────────────
# Core scanner
# ──────────────────────────────────────────

def scan_market(force_alert=False):
    global last_alerted, scan_count

    with scan_lock:
        scan_count += 1
        current_scan = scan_count

    retries = 3
    backoff = 5  # seconds

    for attempt in range(1, retries + 1):
        try:
            print(f"Starting scan #{current_scan} (attempt {attempt})")

            response = requests.get(KUCOIN_URL, timeout=20)
            response.raise_for_status()
            raw = response.json()

            if raw.get("code") != "200000":
                raise Exception(f"KuCoin API error: {raw.get('msg', 'Unknown')}")

            data = raw["data"]["ticker"]
            results = []

            for coin in data:
                symbol = coin.get("symbol", "")

                if not symbol.endswith("-USDT"):
                    continue

                base = symbol.replace("-USDT", "")
                if any(x in base for x in ["3L", "3S", "2L", "2S", "UP", "DOWN", "BULL", "BEAR"]):
                    continue

                try:
                    change = float(coin.get("changeRate", 0)) * 100
                    volume = float(coin.get("volValue", 0))
                    price = float(coin.get("last", 0))
                except (ValueError, TypeError):
                    continue

                if price <= 0 or volume <= 0:
                    continue

                if 2 <= change <= 22 and volume >= 1_000_000:
                    sector = get_sector(symbol)
                    score = calculate_score(change, volume, sector)
                    classification = classify_setup(score)
                    results.append((score, symbol, price, change, volume, sector, classification))

            results = sorted(results, reverse=True)[:15]

            print(f"Coins scanned: {len(data)}")
            print(f"Matches found: {len(results)}")

            # ── No results ──
            if not results:
                last_alerted = set()
                send_telegram(
                    f"🔍 <b>Inshal Crypto Scanner v2</b>\n\n"
                    f"Scan #{current_scan}\n"
                    f"Scanned KuCoin market.\n"
                    f"No strong setup found."
                )
                return

            # ── Deduplicate ──
            new_results = [r for r in results if r[1] not in last_alerted]
            last_alerted = {r[1] for r in results}

            # ── No new coins, send heartbeat ──
            if not new_results and not force_alert:
                top = results[0]
                send_telegram(
                    f"🔄 <b>Scanner Running</b>\n\n"
                    f"Scan #{current_scan}\n"
                    f"No new coin since last alert.\n\n"
                    f"Current Top Coin:\n"
                    f"<b>{top[1]}</b>\n"
                    f"Change: {top[3]:.2f}%\n"
                    f"Score: {top[0]}/100"
                )
                return

            alert_results = new_results if new_results else results

            high_priority = [r for r in alert_results if r[0] >= 70]
            watchlist = [r for r in alert_results if r[0] < 70]

            # ── Build message ──
            message = "🚨 <b>Inshal Crypto Scanner v2</b>\n"
            message += "📊 Source: KuCoin Spot Market\n"
            message += f"🔢 Scan #{current_scan}\n"
            message += "🎯 Style: Momentum / Cartel-Style Screening\n\n"

            if high_priority:
                message += "🔥 <b>HIGH PRIORITY</b>\n\n"
                for score, symbol, price, change, volume, sector, classification in high_priority:
                    message += (
                        f"{classification}\n"
                        f"<b>{symbol}</b>\n"
                        f"💰 Price: {format_price(price)}\n"
                        f"📈 24h Change: {change:.2f}%\n"
                        f"💵 Volume: ${volume:,.0f}\n"
                        f"🧩 Sector: {sector}\n"
                        f"⭐ Cartel Score: {score}/100\n\n"
                    )

            if watchlist:
                message += "👀 <b>WATCHLIST</b>\n\n"
                for score, symbol, price, change, volume, sector, classification in watchlist[:7]:
                    message += (
                        f"<b>{symbol}</b>\n"
                        f"💰 Price: {format_price(price)}\n"
                        f"📈 Change: {change:.2f}%\n"
                        f"💵 Volume: ${volume:,.0f}\n"
                        f"🧩 Sector: {sector}\n"
                        f"⭐ Score: {score}/100\n\n"
                    )

            message += "⚠️ Manual chart confirmation required before any trade."

            send_telegram(message)
            return  # Success — exit retry loop

        except requests.exceptions.HTTPError as e:
            print(f"KuCoin HTTP Error (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                send_telegram(f"❌ <b>Scanner Error</b>\n\nKuCoin HTTP Error after {retries} attempts:\n{e}")

        except requests.exceptions.ConnectionError as e:
            print(f"Connection error (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                send_telegram(f"❌ <b>Scanner Error</b>\n\nConnection error after {retries} attempts:\n{e}")

        except Exception as e:
            print(f"Scanner error (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                send_telegram(f"❌ <b>Scanner Error</b>\n\n{str(e)}")


# ──────────────────────────────────────────
# Background loop
# ──────────────────────────────────────────

def scanner_loop():
    send_telegram(
        "✅ <b>Inshal Crypto Scanner v2 started</b>\n"
        "📊 Source: KuCoin Spot Market\n"
        "⏱ Scanning every 15 minutes.\n"
        "🎯 Cartel-style scoring enabled.\n\n"
        "Note: Render free may sleep if not pinged."
    )

    while True:
        try:
            scan_market()
        except Exception as e:
            print(f"Loop-level crash: {e}")
            send_telegram(f"⚠️ <b>Loop crashed:</b> {e}\n\nRetrying in 5 minutes...")
            time.sleep(300)
            continue

        print("Sleeping 15 minutes...")
        time.sleep(900)


# ──────────────────────────────────────────
# Flask routes
# ──────────────────────────────────────────

@app.route("/")
def home():
    return f"Inshal Crypto Scanner v2 is running. Scans completed: {scan_count}"


@app.route("/health")
def health():
    return "OK"


@app.route("/test")
def test():
    send_telegram("🧪 Test message from Inshal Crypto Scanner v2.")
    return "Test message sent."


@app.route("/scan")
def manual_scan():
    threading.Thread(target=scan_market, kwargs={"force_alert": True}, daemon=True).start()
    return "Manual scan triggered."


# ──────────────────────────────────────────
# Start
# ──────────────────────────────────────────

threading.Thread(target=scanner_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
