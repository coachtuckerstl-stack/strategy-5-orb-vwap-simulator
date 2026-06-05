import csv
import json
import os

MAX_DOLLARS_PER_TRADE = float(os.getenv("MAX_DOLLARS_PER_TRADE", "20"))
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

OPEN_TRADES_FILE = "open_trades.json"
CLOSED_TRADES_FILE = "strategy5_closed_trades.csv"



def calculate_fractional_qty(entry_price):
    """Return fractional simulated qty for fixed $20 notional testing."""
    try:
        entry_price = float(entry_price)
        if entry_price <= 0:
            return 0
        return round(MAX_DOLLARS_PER_TRADE / entry_price, 6)
    except Exception:
        return 0

def now_et():
    return datetime.now(ZoneInfo("America/New_York")).isoformat()


def load_open_trades():
    if not os.path.isfile(OPEN_TRADES_FILE):
        return []

    try:
        with open(OPEN_TRADES_FILE, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return []


def save_open_trades(trades):
    with open(OPEN_TRADES_FILE, "w", encoding="utf-8") as file:
        json.dump(trades, file, indent=2)


def log_closed_trade(trade):
    file_exists = os.path.isfile(CLOSED_TRADES_FILE)

    entry_price = float(trade.get("entry_price", 0))
    closed_price = float(trade.get("closed_price", 0))
    stop_price = float(trade.get("stop_price", 0))
    qty = float(trade.get("qty", 1) or 1)
    side = str(trade.get("side", "buy")).lower()

    if side == "buy":
        pnl_per_share = closed_price - entry_price
        risk = entry_price - stop_price
    else:
        pnl_per_share = entry_price - closed_price
        risk = stop_price - entry_price

    pnl_total = round(pnl_per_share * qty, 2)
    pnl_per_share = round(pnl_per_share, 2)
    risk = round(risk, 2)
    r_multiple = round(pnl_per_share / risk, 2) if risk > 0 else 0

    with open(CLOSED_TRADES_FILE, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "trade_id",
                "opened_at",
                "closed_at",
                "symbol",
                "side",
                "qty",
                "entry_price",
                "stop_price",
                "target_price",
                "closed_price",
                "status",
                "pnl_per_share",
                "pnl_total",
                "r_multiple",
                "strategy",
                "model",
            ])

        writer.writerow([
            trade.get("trade_id"),
            trade.get("opened_at"),
            trade.get("closed_at"),
            trade.get("symbol"),
            trade.get("side"),
            trade.get("qty"),
            trade.get("entry_price"),
            trade.get("stop_price"),
            trade.get("target_price"),
            trade.get("closed_price"),
            trade.get("status"),
            pnl_per_share,
            pnl_total,
            r_multiple,
            trade.get("strategy"),
            trade.get("model"),
        ])


def find_open_trade_for_symbol(symbol, strategy=None):
    """
    Find an existing open Strategy 5 trade for this symbol.
    Used to block duplicate buy entries.
    """
    symbol = str(symbol).upper().strip()
    trades = load_open_trades()

    for trade in trades:
        if str(trade.get("symbol", "")).upper().strip() != symbol:
            continue

        if trade.get("status") != "OPEN":
            continue

        if strategy and trade.get("strategy") != strategy:
            continue

        return trade

    return None


def close_open_trade_for_symbol(symbol, exit_price, strategy=None, close_reason="EXIT_SIGNAL"):
    """
    Close the most recent open Strategy 5 trade for this symbol.
    Used when TradingView sends a sell/exit alert.
    """
    symbol = str(symbol).upper().strip()
    exit_price = float(exit_price)
    trades = load_open_trades()

    closed_trade = None

    for trade in reversed(trades):
        if str(trade.get("symbol", "")).upper().strip() != symbol:
            continue

        if trade.get("status") != "OPEN":
            continue

        if strategy and trade.get("strategy") != strategy:
            continue

        trade["status"] = close_reason
        trade["closed_price"] = round(exit_price, 2)
        trade["closed_at"] = now_et()
        log_closed_trade(trade)
        closed_trade = trade
        break

    save_open_trades(trades)
    return closed_trade


def create_sim_trade(
    symbol,
    side,
    entry_price,
    qty=1,
    strategy="strategy_5_orb_vwap",
    model="strategy5_tradingview_simulator",
    stop_dollars=None,
    target_dollars=None,
):
    entry_price = float(entry_price)
    qty = float(qty or 1)
    side = str(side).lower().strip()

    if stop_dollars is not None and target_dollars is not None:
        stop_dollars = float(stop_dollars)
        target_dollars = float(target_dollars)

        if side == "buy":
            stop_price = round(entry_price - stop_dollars, 2)
            target_price = round(entry_price + target_dollars, 2)
        else:
            stop_price = round(entry_price + stop_dollars, 2)
            target_price = round(entry_price - target_dollars, 2)
    else:
        if side == "buy":
            stop_price = round(entry_price * 0.992, 2)
            target_price = round(entry_price * 1.013, 2)
        else:
            stop_price = round(entry_price * 1.008, 2)
            target_price = round(entry_price * 0.987, 2)

    trade = {
        "trade_id": str(uuid.uuid4()),
        "symbol": str(symbol).upper().strip(),
        "side": side,
        "qty": qty,
        "entry_price": round(entry_price, 2),
        "stop_price": stop_price,
        "target_price": target_price,
        "status": "OPEN",
        "opened_at": now_et(),
        "strategy": strategy,
        "model": model,
    }

    trades = load_open_trades()
    trades.append(trade)
    save_open_trades(trades)

    return trade


def update_trade_prices(symbol, current_price):
    trades = load_open_trades()
    current_price = float(current_price)
    symbol = str(symbol).upper().strip()

    updated = False
    updated_trades = []

    for trade in trades:
        if str(trade.get("symbol", "")).upper() != symbol:
            continue

        if trade.get("status") != "OPEN":
            continue

        side = str(trade.get("side", "buy")).lower()
        target_price = float(trade.get("target_price"))
        stop_price = float(trade.get("stop_price"))

        if side == "buy":
            target_hit = current_price >= target_price
            stop_hit = current_price <= stop_price
        else:
            target_hit = current_price <= target_price
            stop_hit = current_price >= stop_price

        if target_hit:
            trade["status"] = "TARGET_HIT"
            trade["closed_price"] = round(current_price, 2)
            trade["closed_at"] = now_et()
            log_closed_trade(trade)
            updated = True
            updated_trades.append(trade)

        elif stop_hit:
            trade["status"] = "STOP_HIT"
            trade["closed_price"] = round(current_price, 2)
            trade["closed_at"] = now_et()
            log_closed_trade(trade)
            updated = True
            updated_trades.append(trade)

    save_open_trades(trades)

    return trades, updated, updated_trades

def get_daily_pnl_summary(date_prefix=None):
    """
    Return today's realized Strategy 5 P/L from closed trades.
    date_prefix format: YYYY-MM-DD. Defaults to today's ET date.
    """
    if date_prefix is None:
        date_prefix = now_et()[:10]

    open_trades = [
        trade for trade in load_open_trades()
        if trade.get("status") == "OPEN"
    ]

    summary = {
        "date": date_prefix,
        "realized_pnl": 0.0,
        "closed_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "breakeven_trades": 0,
        "open_trades": len(open_trades),
        "open_symbols": sorted(list({
            trade.get("symbol")
            for trade in open_trades
            if trade.get("symbol")
        })),
    }

    if not os.path.isfile(CLOSED_TRADES_FILE):
        return summary

    with open(CLOSED_TRADES_FILE, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            closed_at = str(row.get("closed_at", ""))

            if not closed_at.startswith(date_prefix):
                continue

            pnl_total = float(row.get("pnl_total") or 0)

            summary["realized_pnl"] += pnl_total
            summary["closed_trades"] += 1

            if pnl_total > 0:
                summary["winning_trades"] += 1
            elif pnl_total < 0:
                summary["losing_trades"] += 1
            else:
                summary["breakeven_trades"] += 1

    summary["realized_pnl"] = round(summary["realized_pnl"], 2)

    if summary["closed_trades"] > 0:
        summary["win_rate"] = round(
            summary["winning_trades"] / summary["closed_trades"] * 100,
            2,
        )
    else:
        summary["win_rate"] = 0.0

    return summary