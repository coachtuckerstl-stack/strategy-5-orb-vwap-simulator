import csv
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

OPEN_TRADES_FILE = "open_trades.json"
CLOSED_TRADES_FILE = "strategy5_closed_trades.csv"


def now_et():
    return datetime.now(ZoneInfo("America/New_York")).isoformat()


def load_open_trades():
    if not os.path.isfile(OPEN_TRADES_FILE):
        return []

    with open(OPEN_TRADES_FILE, "r") as file:
        return json.load(file)


def save_open_trades(trades):
    with open(OPEN_TRADES_FILE, "w") as file:
        json.dump(trades, file, indent=2)


def log_closed_trade(trade):
    file_exists = os.path.isfile(CLOSED_TRADES_FILE)

    pnl = round(
        float(trade["closed_price"]) - float(trade["entry_price"]),
        2
    )

    risk = round(
        float(trade["entry_price"]) - float(trade["stop_price"]),
        2
    )

    r_multiple = round(pnl / risk, 2) if risk > 0 else 0

    with open(CLOSED_TRADES_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "opened_at",
                "closed_at",
                "symbol",
                "side",
                "entry_price",
                "stop_price",
                "target_price",
                "closed_price",
                "status",
                "pnl_per_share",
                "r_multiple"
            ])

        writer.writerow([
            trade.get("opened_at"),
            trade.get("closed_at"),
            trade.get("symbol"),
            trade.get("side"),
            trade.get("entry_price"),
            trade.get("stop_price"),
            trade.get("target_price"),
            trade.get("closed_price"),
            trade.get("status"),
            pnl,
            r_multiple
        ])


def create_sim_trade(symbol, side, entry_price):
    entry_price = float(entry_price)

    stop_price = round(entry_price * 0.992, 2)
    target_price = round(entry_price * 1.013, 2)

    trade = {
        "symbol": symbol,
        "side": side,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "target_price": target_price,
        "status": "OPEN",
        "opened_at": now_et()
    }

    trades = load_open_trades()
    trades.append(trade)
    save_open_trades(trades)

    return trade


def update_trade_prices(symbol, current_price):
    trades = load_open_trades()
    current_price = float(current_price)

    updated = False

    for trade in trades:
        if trade["symbol"] != symbol:
            continue

        if trade["status"] != "OPEN":
            continue

        if trade["side"] == "buy":
            if current_price >= float(trade["target_price"]):
                trade["status"] = "TARGET_HIT"
                trade["closed_price"] = current_price
                trade["closed_at"] = now_et()
                log_closed_trade(trade)
                updated = True

            elif current_price <= float(trade["stop_price"]):
                trade["status"] = "STOP_HIT"
                trade["closed_price"] = current_price
                trade["closed_at"] = now_et()
                log_closed_trade(trade)
                updated = True

    save_open_trades(trades)

    return trades, updated