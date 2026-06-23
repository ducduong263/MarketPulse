"""
utils/dnse_helpers.py — Business logic for calling DNSE REST API and DAG utilities.

Provides:
- get_normalized_working_dates()            : fetch trading dates from DNSE API
- fetch_dnse_instruments(symbols?)          : fetch instruments (bulk or targeted)
- build_instrument_rows(instruments)        : build rows for instrument_master upsert
- run_sync_secdef(script_path, timeout)     : run sync_secdef.py subprocess
- trigger_dag(dag_id)                       : trigger another DAG
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date

logger = logging.getLogger(__name__)

# SDK path inside Airflow container (mounted from host via docker-compose volume)
_SDK_PATH = "/opt/airflow/sdk/openapi-sdk/python"

DNSE_BASE_URL = "https://openapi.dnse.com.vn"


def _get_dnse_client():
    """Instantiate DNSEClient from the SDK (HMAC-SHA256 auth)."""
    if _SDK_PATH not in sys.path:
        sys.path.insert(0, _SDK_PATH)

    from dnse import DNSEClient

    return DNSEClient(
        api_key=os.environ["DNSE_API_KEY"],
        api_secret=os.environ["DNSE_API_SECRET"],
        base_url=DNSE_BASE_URL,
    )


def _parse_dates(raw: str) -> list[str]:
    """
    Parse DNSE API response body into a list of 'YYYY-MM-DD' strings.

    Handles multiple possible response shapes:
      - list of strings: ["2026-01-02", ...]
      - list of dicts:   [{"date": "2026-01-02"}, ...]
      - dict wrapper:    {"data": [...], "workingDates": [...], ...}
    """
    data = json.loads(raw)

    # Unwrap dict wrapper if needed
    if isinstance(data, dict):
        data = (
            data.get("data")
            or data.get("workingDates")
            or data.get("tradingDates")
            or data.get("dates")
            or []
        )

    if not isinstance(data, list):
        raise ValueError(
            f"Cannot parse DNSE working-dates response: expected list, "
            f"got {type(data).__name__} — preview: {str(data)[:200]}"
        )

    normalized: list[str] = []
    for item in data:
        if isinstance(item, str):
            d_str = item[:10]
        elif isinstance(item, dict):
            raw_val = (
                item.get("date")
                or item.get("tradingDate")
                or item.get("workingDate")
                or ""
            )
            d_str = str(raw_val)[:10]
        else:
            logger.warning("Skipping unrecognized item: %r", item)
            continue

        try:
            date.fromisoformat(d_str)   # validate
            normalized.append(d_str)
        except ValueError:
            logger.warning("Skipping invalid date string: %r", d_str)

    return normalized


def get_normalized_working_dates() -> list[str]:
    """
    Fetch all trading dates from DNSE REST API and return as normalized list.

    API:     GET https://openapi.dnse.com.vn/market/working-dates
    Auth:    HMAC-SHA256 (via DNSEClient SDK)
    Returns: List of 'YYYY-MM-DD' strings (trading days returned by DNSE).
    Raises:  ValueError if API call fails or response is unparseable.
    """
    client = _get_dnse_client()

    logger.info("Calling DNSE API: GET /market/working-dates")
    status, body = client.get_working_dates()

    if status != 200:
        raise ValueError(f"DNSE API error {status}: {body}")

    dates = _parse_dates(body)

    if not dates:
        raise ValueError(
            "DNSE API returned 0 valid dates — response may have changed format. "
            f"Raw preview: {str(body)[:300]}"
        )

    logger.info("DNSE API returned %d trading dates (%s → %s)", len(dates), dates[0], dates[-1])
    return dates


# ── Instruments ───────────────────────────────────────────────────────────────

def _parse_date(v) -> "date | None":
    """Parse a date value from DNSE API (string or None) → date object."""
    from datetime import date
    if v is None:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError):
        return None


def _parse_index_name(v) -> str | None:
    """Normalize indexName: API may return list or string or None."""
    if v is None:
        return None
    if isinstance(v, list):
        return ",".join(v) if v else None
    return str(v)


def _instrument_dict_to_row(inst: dict, final_trade_date=None) -> tuple | None:
    """
    Convert a raw DNSE instrument dict to a tuple for instrument_master upsert.
    Returns None if symbol or market_id is missing.

    Args:
        inst:             Raw instrument dict from DNSE API.
        final_trade_date: Optional date — final trade date from security_definition.
    """
    symbol    = inst.get("symbol") or inst.get("ticker")
    market_id = inst.get("marketId") or inst.get("market_id")
    if not symbol or not market_id:
        return None

    return (
        symbol,
        market_id,
        inst.get("securityGroupId") or inst.get("security_group_id"),
        inst.get("symbolType")      or inst.get("symbol_type"),
        _parse_date(inst.get("listedDate") or inst.get("listed_date")),
        final_trade_date,
        inst.get("shortName")  or inst.get("short_name"),
        inst.get("name")       or inst.get("fullName"),
        _parse_index_name(inst.get("indexName") or inst.get("index_name")),
        True,  # is_active
    )


def fetch_dnse_instruments(symbols: list[str] | None = None) -> list[dict]:
    """
    Fetch instruments from DNSE REST API via DNSEClient SDK.

    Two modes:
    - symbols=None or [] : paginated fetch of ALL instruments (~3000)
    - symbols=["ACB", "VIC", ...] : single request for specific symbols
      using comma-separated format, limit=len(symbols)

    API:     GET https://openapi.dnse.com.vn/instruments
    Auth:    HMAC-SHA256 (via DNSEClient SDK)
    Returns: List of instrument dicts.
    Raises:  ValueError if API call fails or response is unparseable.
    """
    client = _get_dnse_client()

    # ── Targeted fetch: specific symbols in one request ───────────────────────
    if symbols:
        symbol_str = ",".join(symbols)
        logger.info("Fetching %d specific instruments: %s", len(symbols), symbol_str)
        status, body = client.get_instruments(
            symbol=symbol_str, market_id="", security_group_id="", index_name="",
            limit=len(symbols), page=1, dry_run=False,
        )
        if status != 200:
            raise ValueError(f"DNSE instruments API error {status}: {body[:200]}")
        data = json.loads(body)
        items = (
            data.get("data") or data.get("instruments") or data.get("items")
            if isinstance(data, dict) else data
        ) or []
        logger.info("Fetched %d instruments for %d requested symbols", len(items), len(symbols))
        return items if isinstance(items, list) else []

    # ── Bulk fetch: paginated, all instruments ────────────────────────────
    PAGE_LIMIT = 100
    MAX_PAGES  = 35

    all_instruments: list[dict] = []
    page = 1

    while page <= MAX_PAGES:
        logger.info("Fetching instruments page %d ...", page)
        status, body = client.get_instruments(
            symbol="", market_id="", security_group_id="", index_name="",
            limit=PAGE_LIMIT, page=page, dry_run=False,
        )

        if status != 200:
            raise ValueError(f"DNSE instruments API error {status} on page {page}: {body}")

        data = json.loads(body)

        # Unwrap {"total": N, "data": [...]} or bare list
        total = None
        if isinstance(data, dict):
            total = data.get("total")
            items = (
                data.get("data")
                or data.get("instruments")
                or data.get("items")
                or []
            )
        else:
            items = data

        if not isinstance(items, list):
            raise ValueError(
                f"Cannot parse page {page}: expected list, "
                f"got {type(items).__name__} — preview: {str(items)[:200]}"
            )

        if not items:
            break  # no more data

        all_instruments.extend(items)
        logger.info("Page %d: got %d items (total so far: %d)", page, len(items), len(all_instruments))

        # Stop if we've fetched everything
        if total is not None and len(all_instruments) >= total:
            break
        if len(items) < PAGE_LIMIT:
            break  # last page (partial)

        page += 1

    logger.info("DNSE API: fetched %d instruments across %d page(s)", len(all_instruments), page)
    return all_instruments


def build_instrument_rows(instruments: list[dict]) -> list[tuple]:
    """
    Convert DNSE instruments list to rows ready for instrument_master upsert.

    Args:
        instruments: Raw list from fetch_dnse_instruments().

    Returns:
        List of tuples matching instrument_master column order:
        (symbol, market_id, security_group_id, symbol_type,
         listed_date, final_trade_date, short_name, full_name, index_name, is_active)

    Note:
        final_trade_date is always None here — the DNSE instruments endpoint does not
        return expiry dates. It will be filled in via ON CONFLICT COALESCE from secdef
        during upsert, or populated by the backfill migration.
    """
    rows = []

    for inst in instruments:
        row = _instrument_dict_to_row(inst)
        if row is not None:
            rows.append(row)

    # Deduplicate by (symbol, market_id) — DNSE API may return duplicates across pages.
    # PostgreSQL ON CONFLICT DO UPDATE cannot update the same row twice in one batch.
    seen: dict[tuple, tuple] = {}
    for row in rows:
        key = (row[0], row[1])  # (symbol, market_id)
        seen[key] = row         # last occurrence wins

    deduped = list(seen.values())
    if len(deduped) < len(rows):
        logger.warning("Removed %d duplicate (symbol, market_id) rows before upsert", len(rows) - len(deduped))

    logger.info("Built %d instrument rows for upsert", len(deduped))
    return deduped


# ── DAG orchestration helpers ─────────────────────────────────────────────────

def run_sync_secdef(script_path: str, timeout: int) -> int:
    """
    Run sync_secdef.py as a subprocess and return number of upserted rows.

    Args:
        script_path: Absolute path to sync_secdef.py inside the container.
        timeout:     Max seconds to wait before killing the process.

    Returns:
        Number of rows upserted to DB (parsed from stdout).

    Raises:
        RuntimeError: If process exits with non-zero return code.
    """
    import subprocess
    import sys as _sys

    cmd = [_sys.executable, script_path, "--timeout", str(timeout)]
    logger.info("Running: %s", " ".join(cmd))

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout + 60
    )

    for line in result.stdout.splitlines():
        logger.info("[sync_secdef] %s", line)
    for line in result.stderr.splitlines():
        logger.warning("[sync_secdef stderr] %s", line)

    if result.returncode != 0:
        raise RuntimeError(
            f"sync_secdef.py failed with exit code {result.returncode}"
        )

    upserted = 0
    for line in result.stdout.splitlines():
        if "Upserted to DB:" in line:
            try:
                upserted = int(line.split("Upserted to DB:")[-1].strip())
            except ValueError:
                pass

    logger.info("[sync_secdef] Upserted %d rows", upserted)
    return upserted


def trigger_dag(dag_id: str) -> None:
    """
    Trigger another DAG via TriggerDagRunOperator.

    Args:
        dag_id: The dag_id to trigger.
    """
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    logger.info("[TRIGGER] Triggering %s ...", dag_id)
    TriggerDagRunOperator(
        task_id=f"trigger_{dag_id}",
        trigger_dag_id=dag_id,
        wait_for_completion=False,
        reset_dag_run=True,
    ).execute(context={})
