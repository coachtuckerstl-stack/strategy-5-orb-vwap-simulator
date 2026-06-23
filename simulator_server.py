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
PLACE_ALPACA_ORDERS = str(
    os.getenv("STRAT5_ALPACA_ENABLED", "false")
).strip().lower() in {"1", "true", "yes", "on"}

STRATEGY_NAME = "strategy_5_orb_vwap"
MODEL_NAME = "strategy5_tradingview_simulator"

DEFAULT_QTY = float(os.getenv("STRATEGY5_DEFAULT_QTY", "1"))
DEFAULT_STOP_DOLLARS = float(os.getenv("STRATEGY5_STOP_DOLLARS", "1.50"))
DEFAULT_TARGET_DOLLARS = float(os.getenv("STRATEGY5_TARGET_DOLLARS", "3.00"))

# ===== COACH_T_STRAT5_500_SIM_RULES_START =====
def coach_t_env_bool(name, default="false"):
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def coach_t_env_float(name, default):
    value = os.getenv(name)
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


STRAT5_STARTING_EQUITY = coach_t_env_float("STRAT5_STARTING_EQUITY", 500)
STRAT5_MAX_POSITION_DOLLARS = coach_t_env_float(
    "STRAT5_MAX_POSITION_DOLLARS",
    os.getenv("MAX_DOLLARS_PER_TRADE", "500"),
)
STRAT5_ONE_OPEN_TRADE = coach_t_env_bool("STRAT5_ONE_OPEN_TRADE", "true")
STRAT5_ACCOUNT_NAME = os.getenv("STRAT5_ACCOUNT_NAME", "Strat 5 - 500 Paper").strip()
STRAT5_MODE = os.getenv("STRAT5_MODE", "paper").strip().lower()


def calculate_strategy5_qty(entry_price):
    """
    Strategy 5 simulated account sizing.
    Uses max position dollars divided by alert entry price.
    This does not place Alpaca orders.
    """
    try:
        entry_price = float(entry_price)
        max_dollars = float(STRAT5_MAX_POSITION_DOLLARS)
        if entry_price <= 0 or max_dollars <= 0:
            return 0
        return round(max_dollars / entry_price, 6)
    except Exception:
        return 0
# ===== COACH_T_STRAT5_500_SIM_RULES_END =====


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
                  AND created_at::text LIKE :date_like
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
                  AND created_at::text LIKE :date_like
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

        alpaca_exit_order = submit_strat5_alpaca_paper_order(
            symbol=trade.get("symbol"),
            side="sell",
            qty=safe_float(trade.get("qty"), 0) or 0,
            reason=f"Strategy 5 simulated exit: {status}",
        )

        if alpaca_exit_order.get("enabled"):
            print(f"S5 ALPACA EXIT ORDER RESULT: {alpaca_exit_order}", flush=True)

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
        "strat5_account_name": STRAT5_ACCOUNT_NAME,
        "strat5_mode": STRAT5_MODE,
        "strat5_starting_equity": STRAT5_STARTING_EQUITY,
        "strat5_max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
        "strat5_one_open_trade": STRAT5_ONE_OPEN_TRADE,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS,
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

    # ===== COACH_T_STRAT5_ONE_OPEN_TRADE_RULE =====
    if STRAT5_ONE_OPEN_TRADE:
        open_trades = get_open_strategy5_trades()

        if open_trades:
            existing_symbols = sorted([
                str(t.get("symbol", "")).upper()
                for t in open_trades
                if t.get("symbol")
            ])

            log_event(
                payload,
                "SIM_ENTRY_BLOCKED",
                f"One-open-trade rule blocked new {symbol} entry; open trades: {existing_symbols}",
            )

            return jsonify({
                "ok": False,
                "blocked": True,
                "simulation_only": True,
                "message": "Strategy 5 one-open-trade rule blocked this entry.",
                "symbol": symbol,
                "strategy": strategy,
                "open_symbols": existing_symbols,
                "open_trade_count": len(open_trades),
                "account_name": STRAT5_ACCOUNT_NAME,
                "max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
            }), 200

    qty = calculate_strategy5_qty(price_float)

    if qty <= 0:
        log_event(payload, "SIM_ENTRY_BLOCKED", f"Invalid calculated Strategy 5 qty for {symbol}")

        return jsonify({
            "ok": False,
            "blocked": True,
            "simulation_only": True,
            "message": "Invalid Strategy 5 calculated quantity.",
            "symbol": symbol,
            "price": price_float,
            "max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
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

    alpaca_entry_order = submit_strat5_alpaca_paper_order(
        symbol=symbol,
        side="buy",
        qty=trade.get("qty"),
        reason="Strategy 5 simulated entry",
    )

    if alpaca_entry_order.get("enabled"):
        print(f"S5 ALPACA ENTRY ORDER RESULT: {alpaca_entry_order}", flush=True)

    return jsonify({
        "ok": True,
        "simulation_only": True,
        "message": "Strategy 5 simulated trade created under $500 paper-account rules.",
        "account_name": STRAT5_ACCOUNT_NAME,
        "mode": STRAT5_MODE,
        "max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
        "one_open_trade": STRAT5_ONE_OPEN_TRADE,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS,
        "trade": trade,
        "alpaca_entry_order": alpaca_entry_order,
        "postgres": {
            "ok": pg_ok,
            "message": pg_msg,
        },
    })



# ===== COACH_T_STRAT5_ALPACA_STATUS_START =====
def strat5_env_bool(name, default="false"):
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def strat5_float(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def mask_secret(value):
    value = str(value or "")
    if len(value) <= 8:
        return "loaded" if value else ""
    return value[:4] + "..." + value[-4:]


def alpaca_position_to_dict(position):
    return {
        "symbol": getattr(position, "symbol", None),
        "side": getattr(position, "side", None),
        "qty": strat5_float(getattr(position, "qty", 0)),
        "avg_entry_price": strat5_float(getattr(position, "avg_entry_price", 0)),
        "current_price": strat5_float(getattr(position, "current_price", 0)),
        "market_value": strat5_float(getattr(position, "market_value", 0)),
        "unrealized_pl": strat5_float(getattr(position, "unrealized_pl", 0)),
        "unrealized_plpc": strat5_float(getattr(position, "unrealized_plpc", 0)),
    }


@app.route("/alpaca/status", methods=["GET"])
@app.route("/api/strat5/alpaca-status", methods=["GET"])
def strat5_alpaca_status():
    """
    Read-only Alpaca status check for Strategy 5 $500 paper account.
    This endpoint does not place, cancel, or modify orders.
    """
    api_key = (
        os.getenv("ALPACA_API_KEY")
        or os.getenv("APCA_API_KEY_ID")
        or ""
    ).strip()

    secret_key = (
        os.getenv("ALPACA_SECRET_KEY")
        or os.getenv("APCA_API_SECRET_KEY")
        or ""
    ).strip()

    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
    mode = os.getenv("STRAT5_MODE", "paper").strip().lower()
    account_name = os.getenv("STRAT5_ACCOUNT_NAME", "Strat 5 - 500 Paper").strip()
    starting_equity = strat5_float(os.getenv("STRAT5_STARTING_EQUITY", "500"), 500.0)
    max_position_dollars = strat5_float(os.getenv("STRAT5_MAX_POSITION_DOLLARS", "500"), 500.0)
    alpaca_orders_enabled = strat5_env_bool("STRAT5_ALPACA_ENABLED", "false")
    one_open_trade = strat5_env_bool("STRAT5_ONE_OPEN_TRADE", "true")

    paper = mode == "paper" or "paper" in base_url.lower()

    base_response = {
        "ok": False,
        "service": "strategy_5",
        "account_name": account_name,
        "mode": mode,
        "paper": paper,
        "base_url": base_url,
        "read_only_check": True,
        "alpaca_orders_enabled": alpaca_orders_enabled,
        "keys_loaded": bool(api_key and secret_key),
        "api_key": mask_secret(api_key),
        "starting_equity": starting_equity,
        "max_position_dollars": max_position_dollars,
        "one_open_trade": one_open_trade,
        "connected": False,
        "trading_blocked": True,
        "trading_block_reason": "",
        "positions": [],
    }

    if not api_key or not secret_key:
        base_response["trading_block_reason"] = "Missing Alpaca API key or secret."
        return jsonify(base_response), 200

    try:
        from alpaca.trading.client import TradingClient

        client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=paper,
        )

        account = client.get_account()
        positions = client.get_all_positions()

        equity = strat5_float(getattr(account, "equity", 0))
        last_equity = strat5_float(getattr(account, "last_equity", 0))
        cash = strat5_float(getattr(account, "cash", 0))
        buying_power = strat5_float(getattr(account, "buying_power", 0))

        account_blocked = bool(getattr(account, "account_blocked", False))
        trading_blocked = bool(getattr(account, "trading_blocked", False))
        transfers_blocked = bool(getattr(account, "transfers_blocked", False))

        block_reasons = []
        if not paper:
            block_reasons.append("Not paper mode.")
        if account_blocked:
            block_reasons.append("Account blocked.")
        if trading_blocked:
            block_reasons.append("Trading blocked by Alpaca.")
        if transfers_blocked:
            block_reasons.append("Transfers blocked by Alpaca.")
        if not alpaca_orders_enabled:
            block_reasons.append("STRAT5_ALPACA_ENABLED=false; orders disabled.")

        return jsonify({
            **base_response,
            "ok": True,
            "connected": True,
            "account_status": str(getattr(account, "status", "")),
            "currency": str(getattr(account, "currency", "")),
            "equity": equity,
            "cash": cash,
            "buying_power": buying_power,
            "last_equity": last_equity,
            "daily_pnl": round(equity - last_equity, 2) if last_equity else 0.0,
            "paper_account_pnl_from_start": round(equity - starting_equity, 2),
            "open_position_count": len(positions),
            "positions": [alpaca_position_to_dict(p) for p in positions],
            "account_blocked": account_blocked,
            "trading_blocked": trading_blocked,
            "transfers_blocked": transfers_blocked,
            "trading_allowed_if_enabled": paper and not account_blocked and not trading_blocked,
            "trading_block_reason": " | ".join(block_reasons) if block_reasons else "None",
        }), 200

    except Exception as exc:
        return jsonify({
            **base_response,
            "ok": False,
            "connected": False,
            "trading_block_reason": f"Alpaca status check failed: {exc}",
        }), 200
# ===== COACH_T_STRAT5_ALPACA_STATUS_END =====




# ===== COACH_T_STRAT5_ALPACA_PAPER_ORDER_HELPER_START =====
def submit_strat5_alpaca_paper_order(symbol, side, qty, reason=""):
    """
    Submit a Strategy 5 Alpaca PAPER market order only when STRAT5_ALPACA_ENABLED=true.
    Default is disabled.
    """
    if not PLACE_ALPACA_ORDERS:
        return {
            "enabled": False,
            "submitted": False,
            "reason": "STRAT5_ALPACA_ENABLED=false; no Alpaca paper order placed.",
        }

    api_key = (os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID") or "").strip()
    secret_key = (os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or "").strip()
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
    mode = os.getenv("STRAT5_MODE", "paper").strip().lower()
    paper = mode == "paper" or "paper" in base_url.lower()

    if not paper:
        return {
            "enabled": True,
            "submitted": False,
            "error": "Blocked: Strategy 5 paper orders require paper mode.",
            "base_url": base_url,
            "mode": mode,
        }

    if not api_key or not secret_key:
        return {
            "enabled": True,
            "submitted": False,
            "error": "Missing Alpaca API key or secret.",
        }

    try:
        qty = round(float(qty), 6)
    except Exception:
        qty = 0

    if qty <= 0:
        return {
            "enabled": True,
            "submitted": False,
            "error": "Invalid order quantity.",
            "symbol": str(symbol).upper().strip(),
            "side": side,
            "qty": qty,
        }

    side = str(side or "").lower().strip()
    if side not in {"buy", "sell"}:
        return {
            "enabled": True,
            "submitted": False,
            "error": f"Unsupported side: {side}",
            "symbol": str(symbol).upper().strip(),
            "qty": qty,
        }

    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=True,
        )

        order_request = MarketOrderRequest(
            symbol=str(symbol).upper().strip(),
            qty=qty,
            side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )

        order = client.submit_order(order_request)

        return {
            "enabled": True,
            "submitted": True,
            "paper": True,
            "symbol": str(symbol).upper().strip(),
            "side": side,
            "qty": qty,
            "reason": reason,
            "order_id": str(getattr(order, "id", "")),
            "client_order_id": str(getattr(order, "client_order_id", "")),
            "status": str(getattr(order, "status", "")),
            "submitted_at": str(getattr(order, "submitted_at", "")),
        }

    except Exception as exc:
        return {
            "enabled": True,
            "submitted": False,
            "error": str(exc),
            "symbol": str(symbol).upper().strip(),
            "side": side,
            "qty": qty,
            "reason": reason,
        }
# ===== COACH_T_STRAT5_ALPACA_PAPER_ORDER_HELPER_END =====

# ===== COACH_T_STRAT5_SIZING_CHECK_START =====
@app.route("/sizing-check", methods=["GET"])
@app.route("/api/strat5/sizing-check", methods=["GET"])
def strat5_sizing_check():
    """
    Dry-run Strategy 5 sizing check.
    This endpoint does not create trades and does not place Alpaca orders.
    """
    raw_price = (
        request.args.get("price")
        or request.args.get("entry_price")
        or ""
    )

    try:
        entry_price = float(raw_price)
    except Exception:
        return jsonify({
            "ok": False,
            "error": "Missing or invalid price. Use ?price=5.00",
            "example": "/api/strat5/sizing-check?price=5.00",
        }), 400

    qty = calculate_strategy5_qty(entry_price)
    estimated_notional = round(qty * entry_price, 2)

    return jsonify({
        "ok": True,
        "service": "strategy_5",
        "account_name": STRAT5_ACCOUNT_NAME,
        "mode": STRAT5_MODE,
        "simulation_only": SIMULATION_ONLY,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS,
        "read_only_check": True,
        "entry_price": round(entry_price, 4),
        "max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
        "calculated_qty": qty,
        "estimated_notional": estimated_notional,
        "starting_equity": STRAT5_STARTING_EQUITY,
        "estimated_cash_after_entry": round(STRAT5_STARTING_EQUITY - estimated_notional, 2),
        "one_open_trade": STRAT5_ONE_OPEN_TRADE,
        "message": "Dry-run only. No trade created. No Alpaca order placed.",
    }), 200
# ===== COACH_T_STRAT5_SIZING_CHECK_END =====


# ===== COACH_T_STRAT5_WEBHOOK_DRY_RUN_START =====
@app.route("/webhook-dry-run", methods=["POST"])
@app.route("/api/strat5/webhook-dry-run", methods=["POST"])
def strat5_webhook_dry_run():
    """
    Dry-run Strategy 5 webhook validation.
    This endpoint validates the same type of TradingView payload,
    calculates $500 sizing, checks one-open-trade rules,
    but does not create trades and does not place Alpaca orders.
    """
    payload = request.get_json(silent=True)

    if not payload:
        return jsonify({
            "ok": False,
            "dry_run": True,
            "would_accept": False,
            "blocked": True,
            "reason": "Invalid or missing JSON.",
        }), 400

    incoming_secret = str(payload.get("secret", "")).strip()

    if incoming_secret != WEBHOOK_SECRET:
        return jsonify({
            "ok": False,
            "dry_run": True,
            "would_accept": False,
            "blocked": True,
            "reason": "Invalid secret.",
        }), 200

    symbol = str(payload.get("symbol", "")).upper().strip()
    side = str(payload.get("side", "buy")).lower().strip()
    strategy = str(payload.get("strategy", STRATEGY_NAME)).strip()
    model = str(payload.get("model", MODEL_NAME)).strip()

    try:
        price_float = float(payload.get("price"))
    except Exception:
        return jsonify({
            "ok": False,
            "dry_run": True,
            "would_accept": False,
            "blocked": True,
            "reason": "Invalid price.",
            "symbol": symbol,
            "side": side,
        }), 200

    if not symbol:
        return jsonify({
            "ok": False,
            "dry_run": True,
            "would_accept": False,
            "blocked": True,
            "reason": "Missing symbol.",
            "side": side,
            "price": price_float,
        }), 200

    qty = calculate_strategy5_qty(price_float)

    open_trades = get_open_strategy5_trades()
    open_symbols = sorted([
        str(t.get("symbol", "")).upper()
        for t in open_trades
        if t.get("symbol")
    ])

    same_symbol_open = symbol in open_symbols
    one_open_block = STRAT5_ONE_OPEN_TRADE and len(open_trades) > 0

    block_reasons = []

    if side not in {"buy", "sell"}:
        block_reasons.append(f"Unsupported side: {side}")

    if qty <= 0 and side == "buy":
        block_reasons.append("Invalid calculated quantity.")

    if side == "buy" and same_symbol_open:
        block_reasons.append("Duplicate open Strategy 5 trade for this symbol.")

    if side == "buy" and one_open_block:
        block_reasons.append("One-open-trade rule would block this entry.")

    if side == "sell" and not same_symbol_open:
        block_reasons.append("Sell/exit alert has no matching open trade.")

    would_accept = len(block_reasons) == 0

    return jsonify({
        "ok": True,
        "dry_run": True,
        "would_accept": would_accept,
        "blocked": not would_accept,
        "reason": "PASS - webhook would be accepted." if would_accept else " | ".join(block_reasons),
        "simulation_only": SIMULATION_ONLY,
        "alpaca_orders_enabled": PLACE_ALPACA_ORDERS,
        "account_name": STRAT5_ACCOUNT_NAME,
        "mode": STRAT5_MODE,
        "symbol": symbol,
        "side": side,
        "strategy": strategy,
        "model": model,
        "entry_price": round(price_float, 4),
        "max_position_dollars": STRAT5_MAX_POSITION_DOLLARS,
        "calculated_qty": qty,
        "estimated_notional": round(qty * price_float, 2),
        "one_open_trade": STRAT5_ONE_OPEN_TRADE,
        "open_trade_count": len(open_trades),
        "open_symbols": open_symbols,
        "message": "Dry-run only. No trade created. No Alpaca order placed.",
    }), 200
# ===== COACH_T_STRAT5_WEBHOOK_DRY_RUN_END =====

if MONITOR_ENABLED:
    start_monitor_loop()
else:
    print("Strategy 5 monitor disabled - Alpaca credentials not loaded", flush=True)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

