import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, jsonify

from sim_trade_manager import create_sim_trade, update_trade_prices

load_dotenv()

app = Flask(__name__)

WEBHOOK_SECRET = os.getenv("SIM_WEBHOOK_SECRET", "change_me")
LOG_FILE = "strategy5_sim_log.csv"

SIMULATION_ONLY = True
PLACE_ALPACA_ORDERS = False


def log_event(payload, status, message):
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "timestamp_et",
                "status",
                "message",
                "symbol",
                "side",
                "price",
                "strategy",
                "model",
                "raw_payload"
            ])

        writer.writerow([
            datetime.now(ZoneInfo("America/New_York")).isoformat(),
            status,
            message,
            payload.get("symbol"),
            payload.get("side"),
            payload.get("price"),
            payload.get("strategy"),
            payload.get("model"),
            payload
        ])


@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "service": "Strategy 5 Simulator",
        "simulation_only": SIMULATION_ONLY,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True)

    if not payload:
        return jsonify({"ok": False, "error": "Invalid or missing JSON"}), 400

    if payload.get("secret") != WEBHOOK_SECRET:
        log_event(payload, "REJECTED", "Invalid secret")
        return jsonify({"ok": False, "error": "Invalid secret"}), 403

    symbol = str(payload.get("symbol", "")).upper().strip()
    side = str(payload.get("side", "")).lower().strip()
    price = payload.get("price")

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol"}), 400

    if side not in ["buy", "sell"]:
        return jsonify({"ok": False, "error": "Invalid side"}), 400

    if price is None:
        return jsonify({"ok": False, "error": "Missing price"}), 400

    trades, updated = update_trade_prices(symbol, price)

    if updated:
        log_event(payload, "SIM_TRADE_UPDATED", f"Trade updated for {symbol}")

        return jsonify({
            "ok": True,
            "message": "Existing trade updated.",
            "updated": True,
            "trades": trades
        })

    trade = create_sim_trade(symbol, side, price)

    log_event(payload, "SIM_TRADE_CREATED", f"Simulated trade created for {symbol}")

    return jsonify({
        "ok": True,
        "simulation_only": True,
        "message": "Strategy 5 simulated trade created.",
        "trade": trade
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)