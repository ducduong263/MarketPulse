"""
producers/dnse_market_index_producer.py

Publishes DNSE market index events to Kafka topic: market.index
"""
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingestion import DnseKafkaProducer

from dnse.websocket.models import MarketIndex

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_TOPIC    = "market.index"
MARKET_INDICES = ["VNINDEX", "VN30", "HNX", "HNX30", "UPCOM"]

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "market_index.avsc"


# ── Helpers ───────────────────────────────────────────────────────
def _to_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _f(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── to_dict ───────────────────────────────────────────────────────
def _mi_to_dict(mi: MarketIndex, _ctx) -> dict:
    exchange_ms = int(mi.transactTime * 1000)          if mi.transactTime          else None
    dnse_ms     = int(mi.multicastReceiveTime * 1000)  if mi.multicastReceiveTime  else None
    producer_ms = int(mi.receivedAt * 1000)            if mi.receivedAt            else None

    if exchange_ms is None:
        exchange_ms = dnse_ms or int(datetime.now(timezone.utc).timestamp() * 1000)


    return {
        "index_name":         mi.indexName,
        "market_id":          mi.marketId,
        "value":              _f(mi.valueIndexes),
        "prior_value":        _f(mi.priorValueIndexes),
        "highest_value":      _f(mi.highestValueIndexes),
        "lowest_value":       _f(mi.lowestValueIndexes),
        "changed_value":      _f(mi.changedValue),
        "changed_ratio":      _f(mi.changedRatio),
        "up_count":           _to_int(mi.fluctuationUpIssueCount),
        "down_count":         _to_int(mi.fluctuationDownIssueCount),
        "steady_count":       _to_int(mi.fluctuationSteadinessIssueCount),
        "upper_limit_count":  _to_int(mi.fluctuationUpperLimitIssueCount),
        "lower_limit_count":  _to_int(mi.fluctuationLowerLimitIssueCount),
        "up_volume":          _to_int(mi.fluctuationUpIssueVolume),
        "down_volume":        _to_int(mi.fluctuationDownIssueVolume),
        "steady_volume":      _to_int(mi.fluctuationSteadinessIssueVolume),
        "total_volume":       _to_int(mi.totalVolumeTraded),
        "total_value":        _f(mi.grossTradeAmount),
        "match_volume":       _to_int(mi.contauctAccTrdVol),
        "match_value":        _f(mi.contauctAccTrdVal),
        "deal_volume":        _to_int(mi.blkTrdAccTrdVol),
        "deal_value":         _f(mi.blkTrdAccTrdVal),
        "trading_session_id": str(mi.tradingSessionId) if mi.tradingSessionId is not None else None,
        "exchange_ts":        exchange_ms,
        "dnse_ts":            dnse_ms,
        "producer_ts":        producer_ms,
    }


# ── Main ──────────────────────────────────────────────────────────
async def main():
    producer = DnseKafkaProducer(
        topic=KAFKA_TOPIC,
        schema_path=SCHEMA_PATH,
        to_dict_fn=_mi_to_dict,
        producer_config={"linger.ms": 200},
        service_name="p-index",
    )

    print(f"[CONFIG] Indices: {MARKET_INDICES}")

    async def subscribe_fn(client):
        for idx in MARKET_INDICES:
            await client.subscribe_market_index(
                market_index=idx,
                on_market_index=lambda mi: producer.produce(mi.indexName, mi),
                encoding="msgpack",
            )
        print(f"[SUBSCRIBED] market_index for {len(MARKET_INDICES)} indices")

    await producer.run(subscribe_fn)


if __name__ == "__main__":
    asyncio.run(main())
