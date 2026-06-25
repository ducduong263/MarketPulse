"""
dag_backfill_trade_quote — Backfill historical trade and quote data from DNSE REST API.

Trigger: Manual only (schedule=None)

Usage:
  1. Go to Airflow UI -> DAGs -> dag_backfill_trade_quote
  2. Click "Trigger DAG w/ config" (Play icon)
  3. Enter parameters in the form (symbol, type, from_ts, to_ts, target)
  4. Click "Trigger"

Params:
  symbol  : Ticker symbol (e.g. VCB, FPT, VN30F1M)
  type    : Data type: trade | quote | all
  from_ts : Start time ICT (e.g. "2026-06-23 10:00:00")
  to_ts   : End time ICT (e.g. "2026-06-23 10:15:00")
  target  : Target storage: db | minio | both (currently only db supported)

Retention Notes:
  - market_trade:  only writes to DB if from_ts >= today - 30 days
  - order_book_l2: only writes to DB if from_ts >= today - 7 days
  The script will automatically report if the time range is outside retention limits.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from airflow.sdk import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

# ── SDK path setup ────────────────────────────────────────────────
_SDK_CANDIDATES = [
    Path(__file__).resolve().parents[3] / "sdk" / "openapi-sdk" / "python",
    Path("/opt/airflow/sdk/openapi-sdk/python"),
]
for _sdk_path in _SDK_CANDIDATES:
    if _sdk_path.exists() and str(_sdk_path) not in sys.path:
        sys.path.insert(0, str(_sdk_path))
        break


@dag(
    dag_id="dag_backfill_trade_quote",
    schedule=None,          # Manual trigger only
    start_date=None,
    catchup=False,
    tags=["marketpulse", "backfill", "manual"],
    doc_md=__doc__,
    params={
        "symbol": Param(
            default="VCB",
            type="string",
            title="Symbol",
            description="Ticker symbol to backfill (e.g. VCB, FPT, VN30F1M, VN30F2M). Ignored if Index Name is selected.",
        ),
        "index_name": Param(
            default="",
            enum=["", "VN30", "VN100", "HNX30"],
            title="Index Name",
            description="Select an index to backfill all its constituent symbols (e.g., VN30). If selected, this overrides the Symbol field.",
        ),
        "type": Param(
            default="trade",
            enum=["trade", "quote", "all"],
            title="Data Type",
            description="trade = match trades | quote = order book L2 | all = both",
        ),
        "from_ts": Param(
            default="2026-06-23 09:15:00",
            type="string",
            title="Start Time (ICT)",
            description="Format: YYYY-MM-DD HH:MM:SS (ICT = UTC+7)",
        ),
        "to_ts": Param(
            default="2026-06-23 15:00:00",
            type="string",
            title="End Time (ICT)",
            description="Format: YYYY-MM-DD HH:MM:SS (ICT = UTC+7)",
        ),
        "target": Param(
            default="db",
            enum=["db", "minio", "both"],
            title="Target Storage",
            description="db = TimescaleDB | minio = Delta Lake | both = both",
        ),
        "overwrite": Param(
            default=False,
            type="boolean",
            title="Overwrite",
            description="Set to true to clear existing duplicates in the time range",
        ),
    },
)
def dag_backfill_trade_quote():

    @task()
    def validate_params(**context) -> dict:
        """Validate input parameters before running the backfill."""
        params = context["params"]
        symbol     = params.get("symbol", "").strip().upper()
        index_name = params.get("index_name", "").strip().upper()
        dtype      = params.get("type", "trade")
        from_ts    = params.get("from_ts", "").strip()
        to_ts      = params.get("to_ts", "").strip()
        target     = params.get("target", "db")
        overwrite  = params.get("overwrite", False)

        # If index_name is selected, it overrides symbol
        resolved_symbol = index_name if index_name else symbol

        if not resolved_symbol:
            raise ValueError("Either 'symbol' or 'index_name' must be provided")
        if not from_ts or not to_ts:
            raise ValueError("Parameters 'from_ts' and 'to_ts' are required")
        if dtype not in ("trade", "quote", "all"):
            raise ValueError(f"type must be: trade | quote | all, received: {dtype!r}")
        if target not in ("db", "minio", "both"):
            raise ValueError(f"target must be: db | minio | both, received: {target!r}")

        logger.info(
            "[VALIDATE] OK: symbol=%s type=%s from=%s to=%s target=%s overwrite=%s",
            resolved_symbol, dtype, from_ts, to_ts, target, overwrite,
        )
        return {
            "symbol": resolved_symbol,
            "type": dtype,
            "from_ts": from_ts,
            "to_ts": to_ts,
            "target": target,
            "overwrite": overwrite,
        }

    @task()
    def run_backfill_task(validated: dict) -> dict:
        """
        Call run_backfill() from backfill_trade_quote module.
        """
        possible_roots = [
            Path(__file__).resolve().parents[2],  # Host path
            Path("/opt/airflow"),                  # Container path
        ]
        for root in possible_roots:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from ingestion.handlers.backfill_trade_quote import run_backfill  # noqa: E402

        logger.info(
            "[BACKFILL START] symbol=%s type=%s from=%s to=%s target=%s overwrite=%s",
            validated["symbol"],
            validated["type"],
            validated["from_ts"],
            validated["to_ts"],
            validated["target"],
            validated["overwrite"],
        )

        results = run_backfill(
            symbol=validated["symbol"],
            data_type=validated["type"],
            from_ts_str=validated["from_ts"],
            to_ts_str=validated["to_ts"],
            target=validated["target"],
            overwrite=validated["overwrite"],
        )

        for dtype, stats in results.items():
            logger.info(
                "[BACKFILL DONE] %s: fetched=%d pages=%d inserted=%d dedup_skip=%d",
                dtype.upper(),
                stats["fetched"],
                stats["pages"],
                stats["inserted"],
                stats["skipped_dup"],
            )

        return results

    @task()
    def log_summary(results: dict) -> None:
        """Print summary logs when completed."""
        if not results:
            logger.warning("[SUMMARY] No results were returned.")
            return

        logger.info("[BACKFILL SUMMARY]")
        for dtype, stats in results.items():
            logger.info(
                "  %s: fetched=%d | pages=%d | inserted=%d | dedup_skip=%d",
                dtype.upper(),
                stats.get("fetched", 0),
                stats.get("pages", 0),
                stats.get("inserted", 0),
                stats.get("skipped_dup", 0),
            )

    # ── DAG wiring ────────────────────────────────────────────────
    validated = validate_params()
    results   = run_backfill_task(validated)
    log_summary(results)


dag_backfill_trade_quote()
