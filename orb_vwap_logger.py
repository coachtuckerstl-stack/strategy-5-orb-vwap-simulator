import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from orb_vwap_settings import LOG_FILE, STRATEGY_NAME, STRATEGY_NUMBER


def log_sim_trade(event_type, symbol, status, message="", entry=None, stop=None,
                  target=None, exit_price=None, r_multiple=None, raw=None):
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, mode="a", newline="") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "timestamp_et",
                "strategy_number",
                "strategy_name",
                "event_type",
                "symbol",
                "status",
                "message",
                "entry",
                "stop",
                "target",
                "exit_price",
                "r_multiple",
                "raw"
            ])

        writer.writerow([
            datetime.now(ZoneInfo("America/New_York")).isoformat(),
            STRATEGY_NUMBER,
            STRATEGY_NAME,
            event_type,
            symbol,
            status,
            message,
            entry,
            stop,
            target,
            exit_price,
            r_multiple,
            raw
        ])