import csv
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

from sim_trade_manager import (
    create_sim_trade,
    update_trade_prices,
    find_open_trade_for_symbol,
    close_open_trade_for_symbol,
)

load_dotenv()

app = Flask(__name__)

WEBHOOK_SECRET = (
    os.getenv("SIM_WEBHOOK_SECRET")
    or os.getenv("WEBHOOK_SECRET")
    or "change_me"
)

LOG_FILE = "strategy5_sim_log.csv"

SIMULATION_ONLY = True
PLACE_ALPACA_ORDERS = False

STRATEGY_NAME = "strategy_5_orb_vwap"
MODEL_NAME = "strategy5_tradingview_simulator"

DEFAULT_QTY = float(os.getenv("STRATEGY5_DEFAULT_QTY", "1"))
DEFAULT_STOP_DOLLARS = float(os.getenv("STRATEGY5_STOP_DOLLARS", "1.50"))
DEFAULT_TARGET_DOLLARS = float(os.getenv("STRATEGY5_TARGET_DOLLARS", "3.00"))

ALPACA_API_KEY = (
    os.getenv("ALPACA_API_KEY")
    or os.getenv("APCA_API_KEY_ID")
)

ALPACA_SECRET_KEY = (
    os.getenv("ALPACA_SECRET_KEY")
    or os.getenv("APCA_API_SECRET_KEY")
)

MONITOR_ENABLED = bool(ALPACA_API_KEY and ALPACA_SECRET_KEY)

MARKET_DATA_CLIENT = None

if MONITOR_ENABLED:
    try:
        MARKET_DATA_CLIENT = StockHistoricalDataClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
        )
        print("Strategy 5 monitor market data client initialized", flush=True)
    except Exception as exc:
        print(f"Strategy 5 market data init failed: {exc}", flush=True)
        MARKET_DATA_CLIENT = None
        MONITOR_ENABLED = False


def now_et():
    return datetime.now(ZoneInfo("America/New_York"))


def now_et_iso():
    return now_et().isoformat()


def clean_json(value):
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def get_database_url():
    database_url = os.getenv("DATABASE_URL", "").strip()

    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def get_engine():
    database_url = get_database_url()

    if not database_url:
        return None

    return create_engine(database_url, pool_pre_ping=True)


def init_db():
    engine = get_engine()

    if engine is None:
        return False, "DATABASE_URL not configured"

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trade_events (
                    id SERIAL PRIMARY KEY,
                    timestamp_et TEXT,
                    strategy TEXT,
                    bot_name TEXT,
                    symbol TEXT,
                    side TEXT,
                    qty DOUBLE PRECISION,
                    entry_price DOUBLE PRECISION,
                    exit_price DOUBLE PRECISION,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    status TEXT,
                    reason TEXT,
                    order_id TEXT,
                    source TEXT,
                    simulation_only BOOLEAN,
                    raw_payload TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_events_strategy
                ON trade_events(strategy)
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_events_source
                ON trade_events(source)
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trade_events_symbol
                ON trade_events(symbol)
            """))

        return True, "trade_events table ready"

    except Exception as exc:
        return False, str(exc)


def upsert_trade_event(event):
    engine = get_engine()

    if engine is None:
        return False, "DATABASE_URL not configured"

    ok, init_msg = init_db()

    if not ok:
        return False, init_msg

    now_value = now_et_iso()

    order_id = str(event.get("order_id", "") or "")
    source = event.get("source", "strategy_5")

    try:
        with engine.begin() as conn:
            existing = conn.execute(text("""
                SELECT id
                FROM trade_events
                WHERE source = :source
                  AND order_id = :order_id
                LIMIT 1
            """), {
                "source": source,
                "order_id": order_id,
            }).mappings().first()

            params = {
                "timestamp_et": event.get("timestamp_et"),
                "strategy": event.get("strategy"),
                "bot_name": event.get("bot_name"),
                "symbol": event.get("symbol"),
                "side": event.get("side"),
                "qty": event.get("qty"),
                "entry_price": event.get("entry_price"),
                "exit_price": event.get("exit_price"),
                "stop_loss": event.get("stop_loss"),
                "take_profit": event.get("take_profit"),
                "status": event.get("status"),
                "reason": event.get("reason"),
                "order_id": order_id,
                "source": source,
                "simulation_only": bool(event.get("simulation_only", True)),
                "raw_payload": event.get("raw_payload"),
                "created_at": event.get("created_at") or now_value,
                "updated_at": now_value,
            }

            if existing:
                conn.execute(text("""
                    UPDATE trade_events
                    SET
                        timestamp_et = :timestamp_et,
                        strategy = :strategy,
                        bot_name = :bot_name,
                        symbol = :symbol,
                        side = :side,
                        qty = :qty,
                        entry_price = :entry_price,
                        exit_price = :exit_price,
                        stop_loss = :stop_loss,
                        take_profit = :take_profit,
                        status = :status,
                        reason = :reason,
                        simulation_only = :simulation_only,
                        raw_payload = :raw_payload,
                        updated_at = :updated_at
                    WHERE source = :source
                      AND order_id = :order_id
                """), params)
            else:
                conn.execute(text("""
                    INSERT INTO trade_events (
                        timestamp_et,
                        strategy,
                        bot_name,
                        symbol,
                        side,
                        qty,
                        entry_price,
                        exit_price,
                        stop_loss,
                        take_profit,
                        status,
                        reason,
                        order_id,
                        source,
                        simulation_only,
                        raw_payload,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :timestamp_et,
                        :strategy,
                        :bot_name,
                        :symbol,
                        :side,
                        :qty,
                        :entry_price,
                        :exit_price,
                        :stop_loss,
                        :take_profit,
                        :status,
                        :reason,
                        :order_id,
                        :source,
                        :simulation_only,
                        :raw_payload,
                        :created_at,
                        :updated_at
                    )
                """), params)

        return True, "trade_events row saved"

    except Exception as exc:
        return False, str(exc)


def log_event(payload, status, message):
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, mode="a", newline="", encoding="utf-8") as file:
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
                "raw_payload",
            ])

        writer.writerow([
            now_et_iso(),
            status,
            message,
            payload.get("symbol"),
            payload.get("side"),
            payload.get("price"),
            payload.get("strategy"),
            payload.get("model"),
            clean_json(payload),
        ])


def trade_to_event(trade, payload, status=None, reason=None):
    current_status = status or trade.get("status") or "SIMULATED"

    return {
        "timestamp_et": trade.get("closed_at") or trade.get("opened_at") or now_et_iso(),
        "strategy": trade.get("strategy") or payload.get("strategy") or STRATEGY_NAME,
        "bot_name": "strategy_5_simulator",
        "symbol": trade.get("symbol"),
        "side": trade.get("side"),
        "qty": safe_float(trade.get("qty"), DEFAULT_QTY),
        "entry_price": safe_float(trade.get("entry_price")),
        "exit_price": safe_float(trade.get("closed_price")),
        "stop_loss": safe_float(trade.get("stop_price")),
        "take_profit": safe_float(trade.get("target_price")),
        "status": current_status,
        "reason": reason or "Strategy 5 simulated trade event",
        "order_id": trade.get("trade_id"),
        "source": "strategy_5",
        "simulation_only": True,
        "raw_payload": clean_json({
            "payload": payload,
            "trade": trade,
        }),
        "created_at": trade.get("opened_at") or now_et_iso(),
    }

def market_monitor_window_open():
    now_time = now_et().time()

    market_start = datetime.strptime("09:30", "%H:%M").time()
    market_end = datetime.strptime("15:55", "%H:%M").time()

    return market_start <= now_time <= market_end


def get_open_strategy5_trades(date_prefix=None):
    """
    Return open Strategy 5 trades from Railway Postgres.
    Only today's simulated trades are monitored so older test rows
    are not accidentally closed using today's market price.
    """
    engine = get_engine()

    if engine is None:
        return []

    date_prefix = date_prefix or now_et().date().isoformat()

    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT
                    id,
                    symbol,
                    qty,
                    entry_price,
                    stop_loss,
                    take_profit,
                    status,
                    order_id,
                    strategy,
                    timestamp_et,
                    created_at
                FROM trade_events
                WHERE source = 'strategy_5'
                  AND symbol IS NOT NULL
                  AND created_at LIKE :date_like
                  AND exit_price IS NULL
                  AND status IN ('SIMULATED', 'OPEN')
                ORDER BY created_at DESC, id DESC
            """), {
                "date_like": f"{date_prefix}%"
            }).mappings().all()

            return [dict(row) for row in rows]

    except Exception as exc:
        print(f"S5 monitor open-trade query failed: {exc}", flush=True)
        return []

def find_open_postgres_trade_for_symbol(symbol, strategy=None):
    """
    Find today's open Strategy 5 trade for a symbol using Railway Postgres.
    """
    symbol = str(symbol).upper().strip()
    date_prefix = now_et().date().isoformat()
    engine = get_engine()

    if engine is None:
        return None

    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT
                    id,
                    symbol,
                    qty,
                    entry_price,
                    stop_loss,
                    take_profit,
                    status,
                    order_id,
                    strategy,
                    timestamp_et,
                    created_at
                FROM trade_events
                WHERE source = 'strategy_5'
                  AND symbol = :symbol
                  AND created_at LIKE :date_like
                  AND exit_price IS NULL
                  AND status IN ('SIMULATED', 'OPEN')
                  AND (:strategy IS NULL OR strategy = :strategy)
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """), {
                "symbol": symbol,
                "strategy": strategy,
                "date_like": f"{date_prefix}%"
            }).mappings().first()

            return dict(row) if row else None

    except Exception as exc:
        print(f"S5 Postgres open-trade lookup failed: {exc}", flush=True)
        return None

def close_postgres_trade_for_symbol(symbol, exit_price, strategy=None, status="EXIT_SIGNAL"):
    """
    Close today's open Strategy 5 trade for a symbol in Railway Postgres.
    """
    open_trade = find_open_postgres_trade_for_symbol(symbol, strategy)

    if not open_trade:
        return None

    updated = close_trade_in_postgres(
        open_trade,
        exit_price,
        status,
    )

    if not updated:
        return None

    open_trade["exit_price"] = round(float(exit_price), 2)
    open_trade["closed_price"] = round(float(exit_price), 2)
    open_trade["closed_at"] = now_et_iso()
    open_trade["status"] = status
    open_trade["trade_id"] = open_trade.get("order_id")
    open_trade["stop_price"] = open_trade.get("stop_loss")
    open_trade["target_price"] = open_trade.get("take_profit")

    return open_trade


def get_latest_prices(symbols):
    if not symbols:
        return {}

    if MARKET_DATA_CLIENT is None:
        return {}

    try:
        request = StockLatestTradeRequest(
            symbol_or_symbols=symbols
        )

        latest = MARKET_DATA_CLIENT.get_stock_latest_trade(request)

        prices = {}

        for symbol, trade in latest.items():
            prices[symbol] = float(trade.price)

        return prices

    except Exception as exc:
        print(f"S5 monitor latest-price request failed: {exc}", flush=True)
        return {}


def close_trade_in_postgres(trade, exit_price, status):
    engine = get_engine()

    if engine is None:
        return False

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE trade_events
                SET
                    exit_price = :exit_price,
                    status = :status,
                    updated_at = :updated_at,
                    timestamp_et = :timestamp_et
                WHERE source = 'strategy_5'
                  AND order_id = :order_id
            """), {
                "exit_price": round(float(exit_price), 2),
                "status": status,
                "updated_at": now_et_iso(),
                "timestamp_et": now_et_iso(),
                "order_id": trade["order_id"],
            })

        return True

    except Exception as exc:
        print(f"S5 monitor close update failed: {exc}", flush=True)
        return False


def run_strategy5_monitor_cycle():
    if not MONITOR_ENABLED:
        return

    current_time = now_et().time()

    market_start = datetime.strptime("09:30", "%H:%M").time()
    eod_close_start = datetime.strptime("15:55", "%H:%M").time()
    market_end = datetime.strptime("15:59", "%H:%M").time()

    if current_time < market_start or current_time > market_end:
        return

    open_trades = get_open_strategy5_trades()

    if not open_trades:
        return

    symbols = sorted(list({
        trade["symbol"]
        for trade in open_trades
        if trade.get("symbol")
    }))

    print(f"S5 MONITOR: checking {len(symbols)} symbols: {symbols}", flush=True)

    latest_prices = get_latest_prices(symbols)

    if not latest_prices:
        return

    eod_close_window = eod_close_start <= current_time <= market_end

    for trade in open_trades:
        symbol = trade["symbol"]

        if symbol not in latest_prices:
            continue

        current_price = float(latest_prices[symbol])
        stop_price = safe_float(trade.get("stop_loss"))
        target_price = safe_float(trade.get("take_profit"))

        if eod_close_window:
            updated = close_trade_in_postgres(
                trade,
                current_price,
                "EOD_CLOSE",
            )

            if updated:
                print(
                    f"S5 MONITOR: {symbol} EOD_CLOSE at ${current_price:.2f}",
                    flush=True,
                )

            continue

        if target_price is not None and current_price >= target_price:
            updated = close_trade_in_postgres(
                trade,
                target_price,
                "TARGET_HIT",
            )

            if updated:
                print(
                    f"S5 MONITOR: {symbol} TARGET_HIT at ${current_price:.2f}",
                    flush=True,
                )

        elif stop_price is not None and current_price <= stop_price:
            updated = close_trade_in_postgres(
                trade,
                stop_price,
                "STOP_HIT",
            )

            if updated:
                print(
                    f"S5 MONITOR: {symbol} STOP_HIT at ${current_price:.2f}",
                    flush=True,
                )


def start_monitor_loop():
    import threading
    import time

    def loop():
        print("Strategy 5 monitor loop started", flush=True)

        while True:
            try:
                run_strategy5_monitor_cycle()
            except Exception as exc:
                print(f"S5 MONITOR LOOP ERROR: {exc}", flush=True)

            time.sleep(60)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()


@app.route("/", methods=["GET"])
def home():
    db_ok, db_msg = init_db()

    return jsonify({
        "ok": True,
        "service": "Strategy 5 Simulator",
        "simulation_only": SIMULATION_ONLY,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS,
        "database_url_loaded": bool(get_database_url()),
        "database_ok": db_ok,
        "database_message": db_msg,
    })


@app.route("/health", methods=["GET"])
def health():
    db_ok, db_msg = init_db()

    return jsonify({
        "ok": True,
        "service": "strategy_5",
        "time_et": now_et_iso(),
        "database_url_loaded": bool(get_database_url()),
        "database_ok": db_ok,
        "database_message": db_msg,
    })


@app.route("/debug-env", methods=["GET"])
def debug_env():
    return jsonify({
        "ok": True,
        "sim_webhook_secret_loaded": bool(os.getenv("SIM_WEBHOOK_SECRET")),
        "webhook_secret_loaded": bool(os.getenv("WEBHOOK_SECRET")),
        "database_url_loaded": bool(get_database_url()),
        "default_qty": DEFAULT_QTY,
        "default_stop_dollars": DEFAULT_STOP_DOLLARS,
        "default_target_dollars": DEFAULT_TARGET_DOLLARS,
    })

@app.route("/daily-pnl", methods=["GET"])
def daily_pnl():
    """
    Strategy 5 daily P/L from Railway Postgres trade_events table.
    This matches the dashboard source of truth.
    """
    date_prefix = request.args.get("date") or now_et().date().isoformat()

    engine = get_engine()

    if engine is None:
        return jsonify({
            "ok": False,
            "service": "strategy_5",
            "error": "DATABASE_URL not configured",
            "summary": {
                "date": date_prefix,
                "realized_pnl": 0.0,
                "closed_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "breakeven_trades": 0,
                "open_trades": 0,
                "open_symbols": [],
                "win_rate": 0.0,
            },
        }), 500

    try:
        with engine.begin() as conn:
            closed_rows = conn.execute(text("""
                SELECT
                    symbol,
                    side,
                    qty,
                    entry_price,
                    exit_price,
                    status,
                    timestamp_et,
                    strategy
                FROM trade_events
                WHERE source = 'strategy_5'
                  AND timestamp_et LIKE :date_like
                  AND entry_price IS NOT NULL
                  AND exit_price IS NOT NULL
            """), {
                "date_like": f"{date_prefix}%"
            }).mappings().all()

            open_rows = conn.execute(text("""
                WITH latest_symbol_event AS (
                    SELECT DISTINCT ON (symbol)
                        id,
                        symbol,
                        status,
                        exit_price,
                        timestamp_et
                    FROM trade_events
                    WHERE source = 'strategy_5'
                      AND timestamp_et LIKE :date_like
                      AND symbol IS NOT NULL
                    ORDER BY symbol, timestamp_et DESC, id DESC
                )
                SELECT symbol
                FROM latest_symbol_event
                WHERE exit_price IS NULL
                  AND status IN ('SIMULATED', 'OPEN')
            """), {
                "date_like": f"{date_prefix}%"
            }).mappings().all()

        realized_pnl = 0.0
        winning_trades = 0
        losing_trades = 0
        breakeven_trades = 0

        for row in closed_rows:
            side = str(row.get("side") or "buy").lower()
            qty = safe_float(row.get("qty"), 1) or 1
            entry_price = safe_float(row.get("entry_price"), 0) or 0
            exit_price = safe_float(row.get("exit_price"), 0) or 0

            if side == "sell":
                pnl = (entry_price - exit_price) * qty
            else:
                pnl = (exit_price - entry_price) * qty

            pnl = round(pnl, 2)
            realized_pnl += pnl

            if pnl > 0:
                winning_trades += 1
            elif pnl < 0:
                losing_trades += 1
            else:
                breakeven_trades += 1

        closed_trades = len(closed_rows)
        realized_pnl = round(realized_pnl, 2)
        open_symbols = sorted([
            row.get("symbol")
            for row in open_rows
            if row.get("symbol")
        ])

        win_rate = round((winning_trades / closed_trades) * 100, 2) if closed_trades else 0.0

        return jsonify({
            "ok": True,
            "service": "strategy_5",
            "simulation_only": SIMULATION_ONLY,
            "source": "railway_postgres_trade_events",
            "summary": {
                "date": date_prefix,
                "realized_pnl": realized_pnl,
                "closed_trades": closed_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "breakeven_trades": breakeven_trades,
                "open_trades": len(open_symbols),
                "open_symbols": open_symbols,
                "win_rate": win_rate,
            },
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "service": "strategy_5",
            "error": str(exc),
            "summary": {
                "date": date_prefix,
                "realized_pnl": 0.0,
                "closed_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "breakeven_trades": 0,
                "open_trades": 0,
                "open_symbols": [],
                "win_rate": 0.0,
            },
        }), 500

@app.route("/trades", methods=["GET"])
def trades():

    engine = get_engine()

    if engine is None:
        return jsonify({
            "ok": False,
            "error": "DATABASE_URL not configured"
        }), 500

    try:

        with engine.begin() as conn:

            rows = conn.execute(text("""
                SELECT
                    timestamp_et,
                    symbol,
                    side,
                    qty,
                    entry_price,
                    exit_price,
                    stop_loss,
                    take_profit,
                    status,
                    order_id
                FROM trade_events
                WHERE source = 'strategy_5'
                ORDER BY id DESC
                LIMIT 100
            """)).mappings().all()

        return jsonify({
            "ok": True,
            "count": len(rows),
            "trades": [dict(r) for r in rows]
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc)
        }), 500


@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True)

    if not payload:
        return jsonify({"ok": False, "error": "Invalid or missing JSON"}), 400

    incoming_secret = str(payload.get("secret", "")).strip()

    if incoming_secret != WEBHOOK_SECRET:
        log_event(payload, "REJECTED", "Invalid secret")
        return jsonify({"ok": False, "error": "Invalid secret"}), 403

    symbol = str(payload.get("symbol", "")).upper().strip()
    side = str(payload.get("side", "")).lower().strip()
    price = payload.get("price") or payload.get("close") or payload.get("entry")

    qty = safe_float(payload.get("qty"), DEFAULT_QTY)

    strategy = payload.get("strategy") or STRATEGY_NAME
    model = payload.get("model") or MODEL_NAME

    if not symbol:
        return jsonify({"ok": False, "error": "Missing symbol"}), 400

    if side not in ["buy", "sell"]:
        return jsonify({"ok": False, "error": "Invalid side"}), 400

    if price is None:
        return jsonify({"ok": False, "error": "Missing price"}), 400

    price_float = safe_float(price)

    if price_float is None:
        return jsonify({"ok": False, "error": "Invalid price"}), 400

    # Sell alerts are exit alerts. They close an existing open trade.
    # They should not create a new simulated sell/short trade.
        if side == "sell":
            closed_trade = close_postgres_trade_for_symbol(
                symbol=symbol,
                exit_price=price_float,
                strategy=strategy,
                status="EXIT_SIGNAL",
            )

        if not closed_trade:
            log_event(payload, "SIM_EXIT_IGNORED", f"No open Strategy 5 trade found for {symbol}")

            return jsonify({
                "ok": False,
                "blocked": True,
                "simulation_only": True,
                "message": "No open Strategy 5 trade found to close.",
                "symbol": symbol,
                "side": side,
                "price": price_float,
            }), 200

        event = trade_to_event(
            closed_trade,
            payload,
            status=closed_trade.get("status"),
            reason="Strategy 5 simulated trade closed from TradingView sell/exit alert.",
        )

        pg_ok, pg_msg = upsert_trade_event(event)

        log_event(payload, "SIM_TRADE_CLOSED", f"Simulated trade closed for {symbol}")

        return jsonify({
            "ok": True,
            "closed": True,
            "simulation_only": True,
            "message": "Strategy 5 simulated trade closed.",
            "trade": closed_trade,
            "postgres": {
                "ok": pg_ok,
                "message": pg_msg,
            },
        }), 200

    # Update open trades first so stop/target hits still close before any new entry logic.
    trades, updated, updated_trades = update_trade_prices(symbol, price_float)

    postgres_results = []

    if updated:
        for updated_trade in updated_trades:
            event = trade_to_event(
                updated_trade,
                payload,
                status=updated_trade.get("status"),
                reason=f"Strategy 5 simulated trade closed: {updated_trade.get('status')}",
            )
            pg_ok, pg_msg = upsert_trade_event(event)
            postgres_results.append({
                "trade_id": updated_trade.get("trade_id"),
                "ok": pg_ok,
                "message": pg_msg,
            })

        log_event(payload, "SIM_TRADE_UPDATED", f"Trade updated for {symbol}")

        return jsonify({
            "ok": True,
            "message": "Existing Strategy 5 trade updated.",
            "updated": True,
            "simulation_only": True,
            "trades": trades,
            "postgres": postgres_results,
        })

    # Buy alerts should not create duplicate open trades for the same symbol.
    existing_open_trade = find_open_postgres_trade_for_symbol(
        symbol=symbol,
        strategy=strategy,
    )

    if existing_open_trade:
        log_event(payload, "SIM_ENTRY_BLOCKED", f"Duplicate open Strategy 5 trade blocked for {symbol}")

        return jsonify({
            "ok": False,
            "blocked": True,
            "simulation_only": True,
            "message": "Already in open Strategy 5 trade for this symbol.",
            "symbol": symbol,
            "strategy": strategy,
            "existing_trade": existing_open_trade,
        }), 200

    trade = create_sim_trade(
        symbol=symbol,
        side=side,
        entry_price=price_float,
        qty=qty,
        strategy=strategy,
        model=model,
        stop_dollars=DEFAULT_STOP_DOLLARS,
        target_dollars=DEFAULT_TARGET_DOLLARS,
    )

    event = trade_to_event(
        trade,
        payload,
        status="SIMULATED",
        reason="Strategy 5 simulated trade created from TradingView alert.",
    )

    pg_ok, pg_msg = upsert_trade_event(event)

    log_event(payload, "SIM_TRADE_CREATED", f"Simulated trade created for {symbol}")

    return jsonify({
        "ok": True,
        "simulation_only": True,
        "message": "Strategy 5 simulated trade created.",
        "trade": trade,
        "postgres": {
            "ok": pg_ok,
            "message": pg_msg,
        },
    })


if MONITOR_ENABLED:
    start_monitor_loop()
else:
    print("Strategy 5 monitor disabled - Alpaca credentials not loaded", flush=True)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
