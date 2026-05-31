import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
BINANCE_24HR_URL = "https://api.binance.com/api/v3/ticker/24hr"

# Track last alerted coins to avoid duplicate alerts
last_alerted = set()


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram BOT_TOKEN or CHAT_ID missing")
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        response = requests.post(
            url,
            data={
                "chat_id": int(CHAT_ID),
                "text": message,
                "parse_mode": "HTML"
            },
            timeout=15
        )
        response.raise_for_status()
        print("Telegram Status:", response.status_code)
    except requests.exceptions.HTTPError as e:
        print(f"Telegram HTTP Error: {e} | Response: {e.response.text}")
    except Exception as e:
        print("Telegram Error:", str(e))


def scan_market():
    global last_alerted

    try:
        print("===================================")
        print("Starting market scan...")

        response = requests.get(BINANCE_24HR_URL, timeout=20)
        response.raise_for_status()
        data = response.json()

        results = []

        for coin in data:
            symbol = coin.get("symbol", "")

            if not symbol.endswith("USDT"):
                continue

            if any(x in symbol for x in [
                "UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"
            ]):
                continue

            try:
                change = float(coin.get("priceChangePercent", 0))
                volume = float(coin.get("quoteVolume", 0))
            except (ValueError, TypeError):
                continue

            if 1.5 <= change <= 25 and volume >= 1_000_000:
                # Balanced score: 70% weight on momentum, 30% on volume (capped at $1B)
                score = (change * 0.7) + (min(volume, 1_000_000_000) / 1_000_000_000 * 30)
                results.append((score, symbol, change, volume))

        results = sorted(results, reverse=True)[:10]

        print(f"Coins scanned: {len(data)}")
        print(f"Matches found: {len(results)}")

        if results:
            # Filter out coins alerted in the last scan
            new_results = [r for r in results if r[1] not in last_alerted]
            last_alerted = {r[1] for r in results}

            if new_results:
                message = "🚨 <b>Inshal Crypto Scanner v1</b>\n\n"
                message += "Top Binance Spot Momentum Coins:\n\n"
                for score, symbol, change, volume in new_results:
                    message += (
                        f"✅ <b>{symbol}</b>\n"
                        f"24h Change: {change:.2f}%\n"
                        f"Volume: {volume:,.0f} USDT\n"
                        f"Score: {score:.2f}\n\n"
                    )
                message += "⚠️ Note: Check chart manually before any trade."
                send_telegram(message)
                print("Alert sent successfully.")
            else:
                print("All matches already alerted in last scan. Skipping.")
        else:
            last_alerted = set()
            send_telegram(
                "🔍 Scanner checked Binance market.\n\n"
                "No strong setup found in this scan."
            )
            print("No setup message sent.")

    except requests.exceptions.HTTPError as e:
        error_msg = f"Binance API HTTP Error: {e}"
        print(error_msg)
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{error_msg}")
    except requests.exceptions.ConnectionError:
        msg = "Connection error while reaching Binance API."
        print(msg)
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{msg}")
    except Exception as e:
        error_msg = f"Scan Error: {str(e)}"
        print(error_msg)
        send_telegram(f"❌ <b>Scanner Error</b>\n\n{str(e)}")


def scanner_loop():
    send_telegram("✅ <b>Inshal Crypto Scanner v1</b> started successfully.")

    while True:
        try:
            scan_market()
        except Exception as e:
            print(f"Loop-level error: {e}")
            send_telegram(f"⚠️ <b>Loop crashed:</b> {e}\n\nRetrying in 5 minutes...")
            time.sleep(300)
            continue

        print("Sleeping for 15 minutes...")
        time.sleep(900)


@app.route("/")
def home():
    return "Inshal Crypto Scanner v1 is running."


@app.route("/test")
def test():
    send_telegram("🧪 Test message from Inshal Crypto Scanner.")
    return "Test message sent."


threading.Thread(
    target=scanner_loop,
    daemon=True
).start()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
