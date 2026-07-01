"""
MarketPulse Alert Monitor — main polling loop.

Checks every POLL_INTERVAL_SECONDS (default: 60) during trading hours.
Sends Telegram alerts when:
  - Data freshness gap detected (market_trade > 3min, order_book_l2 > 5min)
  - Docker container is not running

Alert lifecycle:
  - New alert   → send alert message, mark key active
  - Resolved    → send recovery message (with 5-min cooldown per key)
  - Persistent  → no repeat spam
"""

import logging
import os
import time

from ingestion.monitor import telegram
from ingestion.monitor.checks import data_freshness, service_health, trading_hours, data_quality
from ingestion.monitor.state import AlertState
from docker.errors import DockerException

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("alert_monitor")

# ── Config ────────────────────────────────────────────────────────────────────
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
RECOVERY_COOLDOWN_MINUTES = int(os.getenv("RECOVERY_COOLDOWN_MINUTES", "5"))

# Alert key prefixes
_KEY_DATA   = "data:"       # e.g. "data:market_trade"
_KEY_SVC    = "svc:"        # e.g. "svc:p-trade"
_KEY_DB_ERR = "error:db"    # DB connection failure itself
_KEY_DC_ERR = "error:docker"  # Docker daemon unreachable


def run_checks(state: AlertState) -> None:
    """Run all checks once and handle alert state transitions."""

    # ── 1. DB Connection Test & Infrastructure Health ──
    # DB connection is tested first. If it fails, we alert immediately.
    db_ok = True
    try:
        # We perform a test query to check TimescaleDB connection health
        from ingestion.monitor.checks.data_freshness import psycopg2, _DB_CONFIG
        conn = psycopg2.connect(**_DB_CONFIG)
        conn.close()

        # Clear any prior DB error alert
        if state.is_active(_KEY_DB_ERR) and state.can_send_recovery(_KEY_DB_ERR, RECOVERY_COOLDOWN_MINUTES):
            telegram.send(telegram.build_alert_message(
                "✅ [MarketPulse] DB RECONNECTED",
                ["TimescaleDB connection restored."]
            ))
            state.mark_resolved(_KEY_DB_ERR)
    except Exception as exc:
        db_ok = False
        logger.error("DB connection check failed: %s", exc)
        if state.is_new_alert(_KEY_DB_ERR):
            telegram.send(telegram.build_alert_message(
                "🔴 [MarketPulse] DB CONNECTION ERROR",
                [f"TimescaleDB connection test failed: `{exc}`"]
            ))
            state.mark_active(_KEY_DB_ERR)

    # ── 2. Service health (Docker containers) ──
    # Checked 24/7 during broad monitoring hours.
    try:
        health_results = service_health.check()
        # Clear any prior Docker error alert
        if state.is_active(_KEY_DC_ERR) and state.can_send_recovery(_KEY_DC_ERR, RECOVERY_COOLDOWN_MINUTES):
            telegram.send(telegram.build_alert_message(
                "✅ [MarketPulse] DOCKER DAEMON RECONNECTED",
                ["Docker API connection restored."]
            ))
            state.mark_resolved(_KEY_DC_ERR)

        down    = [r for r in health_results if not r.is_healthy]
        running = [r for r in health_results if r.is_healthy]

        if down:
            new_down = [r for r in down if state.is_new_alert(_KEY_SVC + r.name)]
            if new_down:
                logger.warning("SERVICE DOWN: %s", [r.name for r in new_down])
                telegram.send(telegram.build_alert_message(
                    "❌ [MarketPulse] SERVICE DOWN",
                    [service_health.format_result(r) for r in down]
                    + ["", f"▶️  `docker start {' '.join(r.name for r in down)}`"]
                ))
                for r in new_down:
                    state.mark_active(_KEY_SVC + r.name)

        if running:
            for r in running:
                key = _KEY_SVC + r.name
                if state.is_resolved(key) and state.can_send_recovery(key, RECOVERY_COOLDOWN_MINUTES):
                    logger.info("SERVICE RECOVERED: %s", r.name)
                    recovered_items = [r2 for r2 in health_results if state.is_active(_KEY_SVC + r2.name) and r2.is_healthy]
                    if recovered_items:
                        telegram.send(telegram.build_alert_message(
                            "✅ [MarketPulse] SERVICE RECOVERED",
                            [service_health.format_result(r2) for r2 in recovered_items]
                        ))
                    for r2 in recovered_items:
                        state.mark_resolved(_KEY_SVC + r2.name)

    except DockerException as exc:
        logger.error("Docker check failed: %s", exc)
        if state.is_new_alert(_KEY_DC_ERR):
            telegram.send(telegram.build_alert_message(
                "🔴 [MarketPulse] DOCKER DAEMON UNREACHABLE",
                [
                    f"Cannot connect to Docker daemon.",
                    f"`{exc}`",
                    "",
                    "Ensure DOCKER\\_HOST is set and Docker Desktop TCP is enabled.",
                ]
            ))
            state.mark_active(_KEY_DC_ERR)

    # ── 3. Data Freshness & Data Quality (Active Session Hours Only) ──
    if trading_hours.should_monitor():
        if db_ok:
            logger.debug("Active session hours — running data checks")
            run_data_checks(state)
        else:
            logger.warning("DB is down — skipping data checks")
    else:
        logger.debug("Outside active trading hours — skipping data freshness/quality checks")


def run_data_checks(state: AlertState) -> None:
    """Run data gap and data quality checks (when DB is up and session is active)."""
    # ── Data Freshness (Gap) ──
    try:
        freshness_results = data_freshness.check()
        stale = [r for r in freshness_results if r.is_stale]
        ok    = [r for r in freshness_results if not r.is_stale]

        for r in stale:
            key = _KEY_DATA + r.table
            if state.is_new_alert(key):
                logger.warning("DATA GAP: %s (%.1f min)", r.table, r.gap_minutes or -1)
                telegram.send(telegram.build_alert_message(
                    "🚨 [MarketPulse] DATA GAP DETECTED",
                    [data_freshness.format_result(r) for r in freshness_results]
                ))
                state.mark_active(key)

        for r in ok:
            key = _KEY_DATA + r.table
            if state.is_resolved(key) and state.can_send_recovery(key, RECOVERY_COOLDOWN_MINUTES):
                logger.info("DATA RECOVERED: %s", r.table)
                telegram.send(telegram.build_alert_message(
                    "✅ [MarketPulse] DATA RECOVERED",
                    [data_freshness.format_result(r) for r in freshness_results]
                ))
                state.mark_resolved(key)
    except Exception as exc:
        logger.error("Freshness check error: %s", exc)

    # ── Data Quality (Anomalies) ──
    run_data_quality_checks(state)


def run_data_quality_checks(state: AlertState) -> None:
    """Check for new Price and Spread anomalies and alert if found."""
    # ── Price Anomalies ──
    try:
        last_price_ts = state.last_price_anomaly_ts
        price_anomalies = data_quality.check_price_anomalies(interval_seconds=600)  # Check last 10m to handle lag
        new_price = [a for a in price_anomalies if a.exchange_ts > last_price_ts]

        if new_price:
            logger.warning("Found %d new price anomalies!", len(new_price))
            alert_items = []
            max_ts = last_price_ts
            for a in new_price[:5]:
                direction = "Vượt Trần" if a.price > a.ceiling else "Dưới Sàn"
                alert_items.append(
                    f"⚠️ `{a.symbol}`: Giá GD `{a.price:,.1f}` — {direction} (Trần: `{a.ceiling:,.1f}` | Sàn: `{a.floor:,.1f}`)"
                )
                if a.exchange_ts > max_ts:
                    max_ts = a.exchange_ts
            
            if len(new_price) > 5:
                alert_items.append(f"... và {len(new_price) - 5} lỗi giá khác.")

            telegram.send(telegram.build_alert_message(
                "🚨 [MarketPulse] PRICE LIMIT ANOMALY DETECTED",
                alert_items
            ))
            state.last_price_anomaly_ts = max_ts
    except Exception as exc:
        logger.error("Price quality check failed: %s", exc)

    # ── Spread Anomalies ──
    try:
        last_spread_ts = state.last_spread_anomaly_ts
        spread_anomalies = data_quality.check_spread_anomalies(interval_seconds=600)
        new_spread = [a for a in spread_anomalies if a.exchange_ts > last_spread_ts]

        if new_spread:
            logger.warning("Found %d new spread anomalies!", len(new_spread))
            alert_items = []
            max_ts = last_spread_ts
            for a in new_spread[:5]:
                alert_items.append(
                    f"⚠️ `{a.symbol}`: Ask `{a.ask:,.1f}` < Bid `{a.bid:,.1f}` (Spread: `{a.spread:,.1f}`)"
                )
                if a.exchange_ts > max_ts:
                    max_ts = a.exchange_ts

            if len(new_spread) > 5:
                alert_items.append(f"... và {len(new_spread) - 5} lỗi spread khác.")

            telegram.send(telegram.build_alert_message(
                "📉 [MarketPulse] SPREAD ANOMALY DETECTED (Ask < Bid)",
                alert_items
            ))
            state.last_spread_anomaly_ts = max_ts
    except Exception as exc:
        logger.error("Spread quality check failed: %s", exc)


def main() -> None:
    logger.info("Alert monitor started (poll_interval=%ds)", POLL_INTERVAL)
    state = AlertState()

    while True:
        try:
            if trading_hours.should_monitor_infra():
                logger.debug("In monitoring hours — running checks")
                run_checks(state)
            else:
                now = trading_hours.now_ict()
                logger.debug("Outside monitoring hours (%s) — skipping", now.strftime("%H:%M ICT"))
        except Exception as exc:
            # Catch-all so the loop never dies
            logger.exception("Unexpected error in main loop: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
