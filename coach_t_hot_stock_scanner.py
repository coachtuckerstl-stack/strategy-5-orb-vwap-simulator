"""
Coach T Hot Stock Scanner

Purpose:
Creates a daily hot list of low-priced momentum stocks that meet these filters:
- Relative volume >= 5x the 50-day average
- Up at least 10% intraday vs previous close
- Has a recent news catalyst
- Price between $2 and $20

Data source:
- Alpaca Trading API for active US equities
- Alpaca Market Data API snapshots/bars/news

Important:
This scanner does NOT place trades. It only creates a watchlist/hot list.

Environment variables needed:
    APCA_API_KEY_ID=your_key
    APCA_API_SECRET_KEY=your_secret

Optional environment variables:
    ALPACA_DATA_FEED=iex        # use "sip" if your Alpaca plan supports it
    MIN_PRICE=2
    MAX_PRICE=20
    MIN_GAIN_PCT=10
    MIN_REL_VOLUME=5
    NEWS_LOOKBACK_HOURS=24
    MAX_RESULTS=50

Run:
    python coach_t_hot_stock_scanner.py

Outputs:
    hot_stock_list.json
    hot_stock_list.csv
"""

from __future__ import annotations

import csv
import json
import os
import smtplib
import time
from dataclasses import asdict, dataclass
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # Keeps the script usable even if python-dotenv is not installed yet.
    load_dotenv = None

if load_dotenv:
    load_dotenv()


TRADING_API_BASE = "https://paper-api.alpaca.markets"
DATA_API_BASE = "https://data.alpaca.markets"

API_KEY = os.getenv("APCA_API_KEY_ID", "").strip()
API_SECRET = os.getenv("APCA_API_SECRET_KEY", "").strip()

DATA_FEED = os.getenv("ALPACA_DATA_FEED", "iex").strip().lower()
MIN_PRICE = float(os.getenv("MIN_PRICE", "2"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "20"))
MIN_GAIN_PCT = float(os.getenv("MIN_GAIN_PCT", "10"))
MIN_REL_VOLUME = float(os.getenv("MIN_REL_VOLUME", "5"))
NEWS_LOOKBACK_HOURS = int(os.getenv("NEWS_LOOKBACK_HOURS", "24"))
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "50"))

# Optional email/text alert settings.
# For Gmail, use an App Password, not your normal Gmail password.
ALERT_EMAIL_ENABLED = os.getenv("ALERT_EMAIL_ENABLED", "false").strip().lower() == "true"
ALERT_FROM_EMAIL = os.getenv("ALERT_FROM_EMAIL", "").strip()
ALERT_TO_EMAIL = os.getenv("ALERT_TO_EMAIL", "").strip()
ALERT_EMAIL_PASSWORD = os.getenv("ALERT_EMAIL_PASSWORD", "").strip()
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Prevents repeated alerts for the same symbol all morning.
ALERT_STATE_FILE = os.getenv("ALERT_STATE_FILE", "hot_stock_alerted_symbols.json")

REQUEST_TIMEOUT = 20
BATCH_SIZE = 200
SLEEP_BETWEEN_CALLS = 0.25

CATALYST_KEYWORDS = {
    "earnings": ["earnings", "revenue", "eps", "guidance", "quarterly", "results"],
    "fda_biotech": ["fda", "phase", "trial", "clinical", "approval", "clearance", "drug", "therapy", "biotech"],
    "merger_acquisition": ["merger", "acquisition", "acquire", "buyout", "takeover", "strategic alternatives"],
    "offering_financing": ["offering", "registered direct", "private placement", "atm", "financing", "warrant"],
    "contract_partnership": ["contract", "agreement", "partnership", "collaboration", "award", "deal"],
    "analyst_pr": ["price target", "upgrade", "initiates", "rating", "coverage", "press release"],
    "crypto_ai_ev_theme": ["bitcoin", "crypto", "blockchain", "ai", "artificial intelligence", "ev", "electric vehicle"],
}


@dataclass
class HotStock:
    symbol: str
    price: float
    pct_change: float
    today_volume: int
    avg_50d_volume: int
    relative_volume: float
    news_headline: str
    news_source: str
    news_time: str
    news_url: str
    catalyst_type: str
    score: float


def require_env() -> None:
    missing = []
    if not API_KEY:
        missing.append("APCA_API_KEY_ID")
    if not API_SECRET:
        missing.append("APCA_API_SECRET_KEY")
    if missing:
        setup_message = f"""
Missing Alpaca environment variables: {', '.join(missing)}

Fix option 1 - PowerShell temporary setup:
  $env:APCA_API_KEY_ID="your_alpaca_key"
  $env:APCA_API_SECRET_KEY="your_alpaca_secret"

Fix option 2 - create a .env file in this same folder:
  APCA_API_KEY_ID=your_alpaca_key
  APCA_API_SECRET_KEY=your_alpaca_secret
  ALPACA_DATA_FEED=iex

If using the .env option, install this once:
  py -m pip install python-dotenv
""".strip()
        raise RuntimeError(setup_message)


def headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": API_KEY,
        "APCA-API-SECRET-KEY": API_SECRET,
        "Accept": "application/json",
    }


def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    response = requests.get(url, headers=headers(), params=params, timeout=REQUEST_TIMEOUT)
    if response.status_code >= 400:
        raise RuntimeError(
            f"API error {response.status_code} from {url}: {response.text[:500]}"
        )
    return response.json()


def chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def load_active_us_equities() -> List[str]:
    """Get active, tradable US equities from Alpaca."""
    url = f"{TRADING_API_BASE}/v2/assets"
    params = {"status": "active", "asset_class": "us_equity"}
    assets = get_json(url, params)

    symbols = []
    for asset in assets:
        symbol = asset.get("symbol")
        if not symbol:
            continue
        if not asset.get("tradable", False):
            continue
        # Skip warrants, rights, units, and symbols that often cause data issues.
        if any(x in symbol for x in ["/", ".", "-", "^"]):
            continue
        symbols.append(symbol)

    return sorted(set(symbols))


def fetch_snapshots(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch latest snapshots in batches."""
    all_snapshots: Dict[str, Dict[str, Any]] = {}
    url = f"{DATA_API_BASE}/v2/stocks/snapshots"

    for batch in chunks(symbols, BATCH_SIZE):
        params = {
            "symbols": ",".join(batch),
            "feed": DATA_FEED,
        }
        data = get_json(url, params)
        snapshots = data.get("snapshots", data)
        if isinstance(snapshots, dict):
            all_snapshots.update(snapshots)
        time.sleep(SLEEP_BETWEEN_CALLS)

    return all_snapshots


def snapshot_price_and_change(snapshot: Dict[str, Any]) -> Optional[Tuple[float, float, int]]:
    """
    Return current price, percent change vs prior close, and today's volume.
    Uses latest trade price first, then daily bar close.
    """
    latest_trade = snapshot.get("latestTrade") or {}
    daily_bar = snapshot.get("dailyBar") or {}
    prev_daily_bar = snapshot.get("prevDailyBar") or {}

    price = latest_trade.get("p") or daily_bar.get("c")
    previous_close = prev_daily_bar.get("c")
    today_volume = daily_bar.get("v") or 0

    if not price or not previous_close or previous_close <= 0:
        return None

    pct_change = ((float(price) - float(previous_close)) / float(previous_close)) * 100.0
    return float(price), pct_change, int(today_volume or 0)


def first_pass_candidates(snapshots: Dict[str, Dict[str, Any]]) -> Dict[str, Tuple[float, float, int]]:
    """Filter by price and intraday gain before making heavier volume/news calls."""
    candidates = {}
    for symbol, snapshot in snapshots.items():
        parsed = snapshot_price_and_change(snapshot)
        if not parsed:
            continue
        price, pct_change, today_volume = parsed
        if MIN_PRICE <= price <= MAX_PRICE and pct_change >= MIN_GAIN_PCT and today_volume > 0:
            candidates[symbol] = (price, pct_change, today_volume)
    return candidates


def fetch_50d_average_volume(symbol: str) -> Optional[int]:
    """Fetch about 70 calendar days to capture at least 50 trading bars."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=90)
    url = f"{DATA_API_BASE}/v2/stocks/bars"
    params = {
        "symbols": symbol,
        "timeframe": "1Day",
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "limit": 60,
        "feed": DATA_FEED,
        "adjustment": "raw",
    }
    data = get_json(url, params)
    bars_by_symbol = data.get("bars", {})
    bars = bars_by_symbol.get(symbol, []) if isinstance(bars_by_symbol, dict) else []

    # Exclude the latest partial/current day if included. Use the previous 50 completed daily bars.
    completed_bars = bars[:-1] if len(bars) > 50 else bars
    last_50 = completed_bars[-50:]
    volumes = [int(bar.get("v", 0)) for bar in last_50 if int(bar.get("v", 0)) > 0]
    if len(volumes) < 20:
        return None
    return int(sum(volumes) / len(volumes))


def classify_catalyst(headline: str, summary: str = "") -> str:
    text = f"{headline} {summary}".lower()
    for catalyst_type, words in CATALYST_KEYWORDS.items():
        if any(word in text for word in words):
            return catalyst_type
    return "recent_news"


def fetch_recent_news(symbol: str) -> Optional[Dict[str, str]]:
    """Return the most recent news article for a symbol within the lookback window."""
    start = datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)
    url = f"{DATA_API_BASE}/v1beta1/news"
    params = {
        "symbols": symbol,
        "start": start.isoformat().replace("+00:00", "Z"),
        "limit": 10,
        "sort": "desc",
    }
    data = get_json(url, params)
    articles = data.get("news", [])
    if not articles:
        return None

    article = articles[0]
    headline = article.get("headline", "").strip()
    summary = article.get("summary", "").strip()
    return {
        "headline": headline,
        "summary": summary,
        "source": article.get("source", ""),
        "created_at": article.get("created_at", ""),
        "url": article.get("url", ""),
        "catalyst_type": classify_catalyst(headline, summary),
    }


def score_stock(pct_change: float, relative_volume: float, catalyst_type: str) -> float:
    catalyst_bonus = 0.0 if catalyst_type == "recent_news" else 2.0
    return round((relative_volume * 2.0) + (pct_change / 5.0) + catalyst_bonus, 2)


def build_hot_list() -> List[HotStock]:
    require_env()

    print("Loading active US equities...")
    symbols = load_active_us_equities()
    print(f"Loaded {len(symbols)} symbols")

    print("Fetching market snapshots...")
    snapshots = fetch_snapshots(symbols)
    first_pass = first_pass_candidates(snapshots)
    print(f"First-pass candidates: {len(first_pass)}")

    hot_list: List[HotStock] = []

    for idx, (symbol, (price, pct_change, today_volume)) in enumerate(first_pass.items(), start=1):
        try:
            print(f"[{idx}/{len(first_pass)}] Checking {symbol}...")
            avg_50d_volume = fetch_50d_average_volume(symbol)
            time.sleep(SLEEP_BETWEEN_CALLS)
            if not avg_50d_volume:
                continue

            relative_volume = today_volume / avg_50d_volume if avg_50d_volume else 0
            if relative_volume < MIN_REL_VOLUME:
                continue

            news = fetch_recent_news(symbol)
            time.sleep(SLEEP_BETWEEN_CALLS)
            if not news:
                continue

            hot_list.append(
                HotStock(
                    symbol=symbol,
                    price=round(price, 4),
                    pct_change=round(pct_change, 2),
                    today_volume=int(today_volume),
                    avg_50d_volume=int(avg_50d_volume),
                    relative_volume=round(relative_volume, 2),
                    news_headline=news["headline"],
                    news_source=news["source"],
                    news_time=news["created_at"],
                    news_url=news["url"],
                    catalyst_type=news["catalyst_type"],
                    score=score_stock(pct_change, relative_volume, news["catalyst_type"]),
                )
            )
        except Exception as exc:
            print(f"Skipping {symbol}: {exc}")
            continue

    hot_list.sort(key=lambda x: x.score, reverse=True)
    return hot_list[:MAX_RESULTS]


def save_outputs(hot_list: List[HotStock]) -> None:
    rows = [asdict(item) for item in hot_list]

    with open("hot_stock_list.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "filters": {
                    "min_price": MIN_PRICE,
                    "max_price": MAX_PRICE,
                    "min_gain_pct": MIN_GAIN_PCT,
                    "min_relative_volume": MIN_REL_VOLUME,
                    "news_lookback_hours": NEWS_LOOKBACK_HOURS,
                    "data_feed": DATA_FEED,
                },
                "count": len(rows),
                "hot_list": rows,
            },
            f,
            indent=2,
        )

    with open("hot_stock_list.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "symbol",
            "price",
            "pct_change",
            "today_volume",
            "avg_50d_volume",
            "relative_volume",
            "catalyst_type",
            "score",
            "news_headline",
            "news_source",
            "news_time",
            "news_url",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)



def today_alert_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def load_alert_state() -> Dict[str, List[str]]:
    if not os.path.exists(ALERT_STATE_FILE):
        return {}
    try:
        with open(ALERT_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_alert_state(state: Dict[str, List[str]]) -> None:
    with open(ALERT_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def filter_new_alerts(hot_list: List[HotStock]) -> List[HotStock]:
    """Only alert once per symbol per day."""
    state = load_alert_state()
    key = today_alert_key()
    already_alerted = set(state.get(key, []))

    new_items = [item for item in hot_list if item.symbol not in already_alerted]

    if new_items:
        state[key] = sorted(already_alerted.union({item.symbol for item in new_items}))
        save_alert_state({key: state[key]})

    return new_items


def format_alert_body(items: List[HotStock]) -> str:
    lines = []
    lines.append("Coach T Hot Stock Scanner Alert")
    lines.append("")
    lines.append("New stocks matched your hot-list criteria:")
    lines.append("- Relative volume >= 5x 50-day average")
    lines.append("- Up at least 10% intraday")
    lines.append("- Price between $2 and $20")
    lines.append("- Has recent news catalyst")
    lines.append("")

    for item in items:
        lines.append(
            f"{item.symbol} | ${item.price:.2f} | +{item.pct_change:.2f}% | "
            f"RVOL {item.relative_volume:.2f}x | {item.catalyst_type} | Score {item.score}"
        )
        lines.append(f"News: {item.news_headline}")
        if item.news_url:
            lines.append(f"Link: {item.news_url}")
        lines.append("")

    return "\n".join(lines).strip()


def send_email_alert(items: List[HotStock]) -> None:
    if not items:
        return
    if not ALERT_EMAIL_ENABLED:
        print("Alert email is disabled. Set ALERT_EMAIL_ENABLED=true to enable it.")
        return

    missing = []
    if not ALERT_FROM_EMAIL:
        missing.append("ALERT_FROM_EMAIL")
    if not ALERT_TO_EMAIL:
        missing.append("ALERT_TO_EMAIL")
    if not ALERT_EMAIL_PASSWORD:
        missing.append("ALERT_EMAIL_PASSWORD")
    if missing:
        print("Alert skipped. Missing email settings: " + ", ".join(missing))
        return

    subject = f"Coach T Hot List Alert: {', '.join(item.symbol for item in items[:6])}"
    if len(items) > 6:
        subject += f" +{len(items) - 6} more"

    msg = EmailMessage()
    msg["From"] = ALERT_FROM_EMAIL
    msg["To"] = ALERT_TO_EMAIL
    msg["Subject"] = subject
    msg.set_content(format_alert_body(items))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(ALERT_FROM_EMAIL, ALERT_EMAIL_PASSWORD)
        server.send_message(msg)

    print(f"Sent alert for {len(items)} new hot-list stock(s).")


def print_hot_list(hot_list: List[HotStock]) -> None:
    print("\nCoach T Hot Stock List")
    print("=" * 120)
    if not hot_list:
        print("No stocks matched all filters.")
        return

    for item in hot_list:
        print(
            f"{item.symbol:6} | "
            f"${item.price:>7.2f} | "
            f"+{item.pct_change:>6.2f}% | "
            f"RVOL {item.relative_volume:>5.2f}x | "
            f"Vol {item.today_volume:,} | "
            f"{item.catalyst_type} | "
            f"Score {item.score}"
        )
        print(f"       News: {item.news_headline}")
        if item.news_url:
            print(f"       Link: {item.news_url}")
        print("-" * 120)


if __name__ == "__main__":
    hot_list = build_hot_list()
    save_outputs(hot_list)
    print_hot_list(hot_list)

    new_alerts = filter_new_alerts(hot_list)
    if new_alerts:
        send_email_alert(new_alerts)
    else:
        print("No new alert symbols since the last scan today.")

    print("\nSaved: hot_stock_list.json and hot_stock_list.csv")
