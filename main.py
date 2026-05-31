import os
import time
import threading
import requests
from flask import Flask

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

BINANCE_24HR_URL = "https://api.binance.com/api/v3/ticker/24hr"


def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram BOT_TOKEN or CHAT_ID missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print("Telegram error:", e)


def scan_market():
    try:
        response = requests.get(BINANCE_24HR_URL, timeout=15)
        data = response.json()

        results = []

        for coin in data:
            symbol = coin.get("symbol", "")

            if not symbol.endswith("USDT"):
                continue

            if any(x in symbol for x in ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]):
                continue

            change = float(coin.get("priceChangePercent", 0))
            volume = float(coin.get("quoteVolume", 0))

            # Core v1 filter
            if 1.5 <= change <= 25 and volume >= 1_000_000:
                score = change + (volume / 100_000_000)
                results.append((score, symbol, change, volume))

        results = sorted(results, reverse=True)[:10]

        if results:
            message = "🚨 Inshal Crypto Scanner v1\n\n"
            message += "Top Binance Spot Momentum Coins:\n\n"

            for score, symbol, change, volume in results:
                message += (
                    f"✅ {symbol}\n"
                    f"24h Change: {change:.2f}%\n"
                    f"Volume: {volume:,.0f} USDT\n"
                    f"Score: {score:.2f}\n\n"
                )

            message += "Note: Check chart manually before any trade."
            send_telegram(message)

    except Exception as e:
        print("Scan error:", e)


def scanner_loop():
    send_telegram("✅ Inshal Crypto Scanner v1 started successfully.")
    while True:
        scan_market()
        time.sleep(900)  # 15 minutes


@app.route("/")
def home():
    return "Inshal Crypto Scanner v1 is running."


threading.Thread(target=scanner_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
