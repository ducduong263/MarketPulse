"""
dag_backfill_ohlc — Backfill historical OHLC data from DNSE REST API into Delta Lake.

Trigger: Manual only (schedule=None)

Usage:
  1. Go to Airflow UI -> DAGs -> dag_backfill_ohlc
  2. Click "Trigger DAG w/ config" (Play icon)
  3. Enter parameters in the form (symbol, resolution, from_ts, to_ts, overwrite)
  4. Click "Trigger"

Params:
  symbol     : Ticker symbol or index (e.g. VCB, VN30, VN30F1M)
  resolution : Candle resolution: 1 | 3 | 5 | 15 | 30 | 1H | 1D | 1W
  from_ts    : Start time ICT (e.g. "2026-06-23 09:00:00")
  to_ts      : End time ICT (e.g. "2026-06-23 15:00:00")
  overwrite  : Set to true to delete old duplicate data in the time range before loading new data
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
    dag_id="dag_backfill_ohlc",
    schedule=None,          # Manual trigger only
    start_date=None,
    catchup=False,
    tags=["marketpulse", "backfill", "manual", "ohlc"],
    doc_md=__doc__,
    params={
        "symbol": Param(
            default="VCB",
            type="string",
            title="Symbol / Index",
            description="Stock ticker (e.g. VCB), rolling derivative (e.g. VN30F1M), or index (e.g. VN30, VNINDEX)",
        ),
        "resolution": Param(
            default="1min",
            enum=["1min", "3min", "5min", "15min", "30min", "1H", "1D", "1W"],
            title="Candle Resolution",
            description="Resolution of the candles",
        ),
        "from_ts": Param(
            default="2026-06-23 09:00:00",
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
        "overwrite": Param(
            default=False,
            type="boolean",
            title="Overwrite",
            description="Set to true to clear existing duplicates in the time range",
        ),
    },
)
def dag_backfill_ohlc():

    @task()
    def validate_params(**context) -> dict:
        """Validate input parameters before running the backfill."""
        params = context["params"]
        symbol     = params.get("symbol", "").strip().upper()
        resolution = params.get("resolution", "1").strip()
        from_ts    = params.get("from_ts", "").strip()
        to_ts      = params.get("to_ts", "").strip()
        overwrite  = params.get("overwrite", False)

        if not symbol:
            raise ValueError("Parameter 'symbol' cannot be empty")
        if not from_ts or not to_ts:
            raise ValueError("Parameters 'from_ts' and 'to_ts' are required")
        
        logger.info(
            "[VALIDATE] OK: symbol=%s resolution=%s from=%s to=%s overwrite=%s",
            symbol, resolution, from_ts, to_ts, overwrite,
        )
        return {
            "symbol":     symbol,
            "resolution": resolution,
            "from_ts":    from_ts,
            "to_ts":      to_ts,
            "overwrite":  overwrite,
        }

    @task()
    def run_backfill_task(validated: dict) -> dict:
        """
        Call run_backfill_ohlc() from backfill_ohlc module.
        """
        possible_roots = [
            Path(__file__).resolve().parents[2],  # Host path
            Path("/opt/airflow"),                  # Container path
        ]
        for root in possible_roots:
            if root.exists() and str(root) not in sys.path:
                sys.path.insert(0, str(root))

        from ingestion.handlers.backfill_ohlc import run_backfill_ohlc  # noqa: E402

        logger.info(
            "[BACKFILL START] OHLC: symbol=%s resolution=%s from=%s to=%s overwrite=%s",
            validated["symbol"],
            validated["resolution"],
            validated["from_ts"],
            validated["to_ts"],
            validated["overwrite"],
        )

        stats = run_backfill_ohlc(
            symbol=validated["symbol"],
            resolution=validated["resolution"],
            from_ts_str=validated["from_ts"],
            to_ts_str=validated["to_ts"],
            overwrite=validated["overwrite"],
        )

        logger.info(
            "[BACKFILL DONE] OHLC: fetched=%d inserted=%d skip_dup=%d",
            stats["fetched"],
            stats["inserted"],
            stats["skipped_dup"],
        )

        return stats

    # ── DAG wiring ────────────────────────────────────────────────
    validated = validate_params()
    run_backfill_task(validated)


dag_backfill_ohlc()
