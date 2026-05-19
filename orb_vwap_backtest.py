from orb_vwap_logger import log_sim_trade
from orb_vwap_settings import SYMBOLS, STRATEGY_NAME


def run_backtest():
    print(f"Starting backtest for {STRATEGY_NAME}")

    for symbol in SYMBOLS:
        print(f"Backtesting {symbol}...")
        log_sim_trade(
            event_type="BACKTEST_PLACEHOLDER",
            symbol=symbol,
            status="READY",
            message="Backtest structure created"
        )

    print("Backtest shell complete.")


if __name__ == "__main__":
    run_backtest()