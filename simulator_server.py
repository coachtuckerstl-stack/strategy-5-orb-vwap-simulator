import csv
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

from sim_trade_manager import create_sim_trade, update_trade_prices

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


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)