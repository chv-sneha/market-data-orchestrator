"""
scripts/stock_pipeline.py
=========================
Core data pipeline logic for the Stock Market Data Pipeline.

Responsibilities
----------------
- Fetch daily OHLCV data from the Alpha Vantage API.
- Validate and parse the JSON response.
- Transform records into a normalised format.
- Upsert records into the PostgreSQL ``stock_prices`` table.

This module contains *only* business / data logic.
Airflow orchestration lives in ``dags/stock_pipeline_dag.py``.

Design principles
-----------------
- All configuration comes from environment variables — no hard-coded values.
- Parameterised SQL queries prevent SQL injection.
- Transactions ensure atomic writes; failures trigger a full rollback.
- Missing or malformed records are skipped with a warning, not a crash.
- Secrets are never logged.
"""

from __future__ import annotations

import logging
import os
import time
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2
import psycopg2.extras
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock_pipeline")

# ---------------------------------------------------------------------------
# Constants / configuration
# ---------------------------------------------------------------------------
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
API_TIMEOUT_SECONDS = 30          # HTTP request timeout
MAX_RECORDS_PER_SYMBOL = 100      # Limit how many days we ingest per run

# Alpha Vantage TIME_SERIES_DAILY returns keys like "1. open", "2. high", …
_AV_KEY_MAP: dict[str, str] = {
    "1. open":   "open",
    "2. high":   "high",
    "3. low":    "low",
    "4. close":  "close",
    "5. volume": "volume",
}

# ---------------------------------------------------------------------------
# Helper: safe decimal conversion
# ---------------------------------------------------------------------------

def _to_decimal(value: Any, field_name: str) -> Decimal:
    """Convert *value* to a :class:`Decimal`, raising ``ValueError`` on failure.

    Args:
        value:      The raw value from the API response.
        field_name: Human-readable field name used in error messages.

    Returns:
        A ``Decimal`` representation of *value*.

    Raises:
        ValueError: If the conversion fails.
    """
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot convert field '{field_name}' value {value!r} to Decimal: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Step 1 — Fetch
# ---------------------------------------------------------------------------

def fetch_stock_data(symbol: str) -> dict[str, Any]:
    """Fetch daily OHLCV data for *symbol* from the Alpha Vantage API.

    Args:
        symbol: Stock ticker symbol (e.g. ``"AAPL"``).

    Returns:
        The raw JSON response as a Python dictionary.

    Raises:
        EnvironmentError:      If the API key environment variable is not set.
        requests.Timeout:      If the HTTP request times out.
        requests.HTTPError:    If the server returns a 4xx/5xx status code.
        requests.ConnectionError: If the network is unavailable.
        requests.RequestException: For any other ``requests`` error.
        ValueError:            If the response body cannot be decoded as JSON.
        RuntimeError:          If the API returns an application-level error
                               (invalid key, rate limit exceeded, etc.).
    """
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        raise EnvironmentError(
            "ALPHA_VANTAGE_API_KEY is not set or is still the placeholder value. "
            "Copy .env.example to .env and add your key."
        )

    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     symbol,
        "outputsize": "compact",   # last 100 trading days; use "full" for history
        "apikey":     api_key,     # never logged below
    }

    logger.info("Fetching data for symbol: %s", symbol)

    try:
        response = requests.get(
            ALPHA_VANTAGE_BASE_URL,
            params=params,
            timeout=API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.Timeout:
        logger.error("Request timed out after %ds for symbol %s.", API_TIMEOUT_SECONDS, symbol)
        raise
    except requests.ConnectionError as exc:
        logger.error("Network connection error for symbol %s: %s", symbol, exc)
        raise
    except requests.HTTPError as exc:
        logger.error(
            "HTTP error %s for symbol %s: %s",
            response.status_code, symbol, exc,
        )
        raise
    except requests.RequestException as exc:
        logger.error("Unexpected requests error for symbol %s: %s", symbol, exc)
        raise

    # Decode JSON — the API always returns 200 even for errors, so we check
    # the body content after status.
    try:
        data: dict[str, Any] = response.json()
    except ValueError as exc:
        logger.error("Failed to decode JSON response for symbol %s: %s", symbol, exc)
        raise ValueError(f"Invalid JSON response for {symbol}: {exc}") from exc

    # Detect application-level API errors embedded in the response body.
    _check_api_errors(data, symbol)

    logger.info("API request successful for symbol %s.", symbol)
    return data


def _check_api_errors(data: dict[str, Any], symbol: str) -> None:
    """Inspect the Alpha Vantage JSON body for embedded error messages.

    Alpha Vantage returns HTTP 200 even for errors and rate limits; the actual
    error is in the response body.

    Args:
        data:   Parsed JSON response.
        symbol: Symbol being fetched (used in error messages).

    Raises:
        RuntimeError: If the response signals an error or rate limit.
    """
    # Invalid API key or general API error
    if "Error Message" in data:
        raise RuntimeError(
            f"Alpha Vantage API error for {symbol}: {data['Error Message']}"
        )

    # Rate limit (free tier: 25 requests/day, 5 requests/minute)
    if "Note" in data:
        raise RuntimeError(
            f"Alpha Vantage rate limit reached for {symbol}: {data['Note']}"
        )

    # Information messages often indicate premium-only endpoints or bad symbols
    if "Information" in data:
        raise RuntimeError(
            f"Alpha Vantage information message for {symbol}: {data['Information']}"
        )


# ---------------------------------------------------------------------------
# Step 2 — Validate
# ---------------------------------------------------------------------------

def validate_stock_data(data: dict[str, Any], symbol: str) -> dict[str, Any]:
    """Validate that the API response contains the expected time-series field.

    Args:
        data:   Parsed JSON response from ``fetch_stock_data``.
        symbol: Symbol (used in log messages).

    Returns:
        The ``"Time Series (Daily)"`` sub-dictionary.

    Raises:
        ValueError: If the time-series field is absent or empty.
    """
    time_series_key = "Time Series (Daily)"

    if time_series_key not in data:
        raise ValueError(
            f"Response for {symbol} is missing the '{time_series_key}' field. "
            f"Keys present: {list(data.keys())}"
        )

    time_series: dict[str, Any] = data[time_series_key]

    if not time_series:
        raise ValueError(f"Time series data for {symbol} is empty.")

    record_count = len(time_series)
    logger.info("Symbol %s: received %d daily records from API.", symbol, record_count)

    return time_series


# ---------------------------------------------------------------------------
# Step 3 — Parse & Transform
# ---------------------------------------------------------------------------

def parse_stock_data(
    time_series: dict[str, Any],
    symbol: str,
) -> list[dict[str, Any]]:
    """Parse and transform the raw time-series dictionary into clean records.

    Each entry in the returned list is a flat dictionary ready to be
    inserted into ``stock_prices``.

    Missing or malformed individual records are skipped with a warning;
    the remaining valid records are still returned so the pipeline can
    make partial progress.

    Args:
        time_series: The ``"Time Series (Daily)"`` dictionary from the API.
        symbol:      Stock ticker symbol.

    Returns:
        A list of clean record dictionaries. May be empty if all records
        were invalid.
    """
    records: list[dict[str, Any]] = []
    skipped = 0

    # Iterate newest-first (Alpha Vantage returns dates in descending order).
    for date_str, raw_values in list(time_series.items())[:MAX_RECORDS_PER_SYMBOL]:
        try:
            record = _parse_single_record(symbol, date_str, raw_values)
        except (ValueError, KeyError) as exc:
            logger.warning(
                "Skipping record for %s on %s — %s", symbol, date_str, exc
            )
            skipped += 1
            continue

        records.append(record)

    logger.info(
        "Symbol %s: parsed %d valid records, skipped %d invalid records.",
        symbol, len(records), skipped,
    )
    return records


def _parse_single_record(
    symbol: str,
    date_str: str,
    raw_values: dict[str, Any],
) -> dict[str, Any]:
    """Parse and validate a single daily OHLCV record.

    Args:
        symbol:     Ticker symbol.
        date_str:   Date string in ``"YYYY-MM-DD"`` format.
        raw_values: Raw OHLCV dictionary from the API.

    Returns:
        A clean record dictionary.

    Raises:
        ValueError: If any required field is missing or invalid.
        KeyError:   If an expected key is absent.
    """
    if not date_str:
        raise ValueError("Empty date string.")

    # Validate date format early to surface bad data quickly.
    import datetime  # local import keeps top-level imports clean
    try:
        timestamp = datetime.date.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(f"Invalid date format '{date_str}': {exc}") from exc

    # Extract OHLCV fields using the canonical key map.
    extracted: dict[str, Any] = {}
    for av_key, col_name in _AV_KEY_MAP.items():
        if av_key not in raw_values:
            raise KeyError(f"Missing field '{av_key}' in record for {date_str}.")
        extracted[col_name] = raw_values[av_key]

    # Convert to appropriate Python types.
    open_price  = _to_decimal(extracted["open"],   "open")
    high_price  = _to_decimal(extracted["high"],   "high")
    low_price   = _to_decimal(extracted["low"],    "low")
    close_price = _to_decimal(extracted["close"],  "close")

    try:
        volume = int(extracted["volume"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot convert volume {extracted['volume']!r} to int: {exc}"
        ) from exc

    # Basic sanity checks on numeric values.
    if any(v <= 0 for v in (open_price, high_price, low_price, close_price)):
        raise ValueError(
            f"OHLC prices must be positive for {symbol} on {date_str}."
        )
    if volume < 0:
        raise ValueError(f"Volume cannot be negative for {symbol} on {date_str}.")
    if high_price < low_price:
        raise ValueError(
            f"High ({high_price}) < Low ({low_price}) for {symbol} on {date_str}."
        )

    return {
        "symbol":    symbol,
        "timestamp": timestamp,
        "open":      open_price,
        "high":      high_price,
        "low":       low_price,
        "close":     close_price,
        "volume":    volume,
    }


# ---------------------------------------------------------------------------
# Step 4 — Database helpers
# ---------------------------------------------------------------------------

def get_db_connection() -> "psycopg2.extensions.connection":
    """Create and return a psycopg2 database connection.

    All connection parameters are read from environment variables.

    Returns:
        An open psycopg2 connection with autocommit disabled.

    Raises:
        psycopg2.OperationalError: If the connection cannot be established.
        EnvironmentError:          If required environment variables are missing.
    """
    required = ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(
            f"Missing required database environment variables: {', '.join(missing)}"
        )

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )
    conn.autocommit = False  # we manage transactions explicitly
    logger.info(
        "Database connection established: host=%s db=%s user=%s",
        os.environ["POSTGRES_HOST"],
        os.environ["POSTGRES_DB"],
        os.environ["POSTGRES_USER"],
    )
    return conn


# ---------------------------------------------------------------------------
# Step 5 — Upsert
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume)
VALUES (%(symbol)s, %(timestamp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
ON CONFLICT (symbol, timestamp)
DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    volume     = EXCLUDED.volume,
    created_at = NOW();
"""


def upsert_stock_data(
    conn: "psycopg2.extensions.connection",
    records: list[dict[str, Any]],
    symbol: str,
) -> int:
    """Upsert a batch of OHLCV records inside a single transaction.

    Uses ``ON CONFLICT … DO UPDATE`` so re-running the pipeline never
    creates duplicate rows — it simply refreshes existing ones.

    Args:
        conn:    Open psycopg2 connection (autocommit must be False).
        records: List of clean record dicts from ``parse_stock_data``.
        symbol:  Symbol name (used in log messages only).

    Returns:
        The number of rows upserted.

    Raises:
        psycopg2.DatabaseError: On any database error; the transaction is
                                rolled back before re-raising.
    """
    if not records:
        logger.warning("No records to upsert for symbol %s.", symbol)
        return 0

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, _UPSERT_SQL, records, page_size=100)
        conn.commit()
        logger.info(
            "Database upsert successful for symbol %s: %d records committed.",
            symbol, len(records),
        )
        return len(records)
    except psycopg2.DatabaseError as exc:
        conn.rollback()
        logger.error(
            "Database error during upsert for symbol %s — transaction rolled back: %s",
            symbol, exc,
        )
        raise


# ---------------------------------------------------------------------------
# Orchestration entry point
# ---------------------------------------------------------------------------

def run_pipeline(symbols: list[str] | None = None) -> None:
    """Run the full ETL pipeline for each configured stock symbol.

    This function is called by the Airflow DAG task.  It reads the list of
    symbols from the ``STOCK_SYMBOLS`` environment variable if *symbols* is
    not provided.

    Args:
        symbols: Optional explicit list of ticker symbols.  If ``None``, the
                 list is read from the ``STOCK_SYMBOLS`` env var.

    Raises:
        RuntimeError: If no symbols are configured, or if all symbols fail.
    """
    if symbols is None:
        raw = os.environ.get("STOCK_SYMBOLS", "").strip()
        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]

    if not symbols:
        raise RuntimeError(
            "No stock symbols configured. "
            "Set STOCK_SYMBOLS in your .env file (e.g. STOCK_SYMBOLS=AAPL,MSFT,GOOGL)."
        )

    logger.info("Pipeline started. Symbols to process: %s", symbols)

    # Open a single DB connection for the entire run to avoid per-symbol
    # connection overhead.  Each symbol's upsert is committed independently
    # so a failure on one symbol does not roll back others.
    conn = get_db_connection()

    succeeded: list[str] = []
    failed: list[str] = []

    try:
        for symbol in symbols:
            try:
                _process_symbol(conn, symbol)
                succeeded.append(symbol)
            except Exception as exc:  # noqa: BLE001
                # Log and continue — one bad symbol should not abort others.
                logger.error("Failed to process symbol %s: %s", symbol, exc)
                failed.append(symbol)

            # Alpha Vantage free tier: 5 requests/minute.
            # Sleep briefly between symbols to avoid triggering the rate limit
            # when processing more than 5 symbols in a single run.
            if len(symbols) > 1:
                time.sleep(13)  # ≈ 4.6 req/min — comfortably under the limit

    finally:
        conn.close()
        logger.info("Database connection closed.")

    logger.info(
        "Pipeline complete. Succeeded: %s. Failed: %s.",
        succeeded or "none",
        failed or "none",
    )

    if failed and not succeeded:
        raise RuntimeError(
            f"Pipeline failed for all symbols: {failed}. "
            "Check logs for details."
        )

    if failed:
        logger.warning(
            "Pipeline completed with partial failures. "
            "Failed symbols: %s. Succeeded: %s.",
            failed, succeeded,
        )


def _process_symbol(
    conn: "psycopg2.extensions.connection",
    symbol: str,
) -> None:
    """Run the full ETL pipeline for a single stock symbol.

    Args:
        conn:   Open database connection.
        symbol: Stock ticker symbol.
    """
    logger.info("--- Processing symbol: %s ---", symbol)

    # 1. Fetch
    raw_data = fetch_stock_data(symbol)

    # 2. Validate
    time_series = validate_stock_data(raw_data, symbol)

    # 3. Parse & Transform
    records = parse_stock_data(time_series, symbol)

    if not records:
        logger.warning(
            "No valid records produced for %s after parsing. Skipping upsert.",
            symbol,
        )
        return

    # 4. Upsert
    upsert_stock_data(conn, records, symbol)


# ---------------------------------------------------------------------------
# CLI entry point (useful for local testing outside Airflow)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    cli_symbols = sys.argv[1:] if len(sys.argv) > 1 else None
    run_pipeline(cli_symbols)
