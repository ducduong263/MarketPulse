create materialized view if not exists ohlcv_1min
with (timescaledb.continuous) as
select
    symbol,
    time_bucket('1 minute', event_ts)              as bucket,
    first(price, event_ts)                         as open,
    max(price)                                     as high,
    min(price)                                     as low,
    last(price, event_ts)                          as close,
    sum(quantity)                                  as volume,
    sum(quantity * price)                          as turnover,
    sum(case when side = 1 then quantity else 0 end) as buy_vol,
    sum(case when side = 2 then quantity else 0 end) as sell_vol,
    count(*)                                       as tick_count,
    avg(extract(epoch from (received_ts - event_ts)) * 1000) as avg_latency_ms
from market_trade
group by symbol, time_bucket('1 minute', event_ts)
with no data;

select add_continuous_aggregate_policy('ohlcv_1min',
    start_offset      => interval '10 minutes',
    end_offset        => interval '1 minute',
    schedule_interval => interval '1 minute',
    if_not_exists     => true);
