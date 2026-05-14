create materialized view if not exists ohlcv_1min
with (timescaledb.continuous) as
select
    symbol,
    time_bucket('1 minute', exchange_ts)              as bucket,
    first(price, exchange_ts)                         as open,
    max(price)                                     as high,
    min(price)                                     as low,
    last(price, exchange_ts)                          as close,
    sum(quantity)                                  as volume,
    sum(quantity * price)                          as turnover,
    sum(case when side = 1 then quantity else 0 end) as buy_vol,
    sum(case when side = 2 then quantity else 0 end) as sell_vol,
    count(*)                                       as tick_count,
    avg(extract(epoch from (dnse_ts - exchange_ts)) * 1000) as avg_latency_ms
from market_trade
group by symbol, time_bucket('1 minute', exchange_ts)
with no data;

select add_continuous_aggregate_policy('ohlcv_1min',
    start_offset      => interval '10 minutes',
    end_offset        => interval '1 minute',
    schedule_interval => interval '1 minute',
    if_not_exists     => true);

-- ============================================================
-- Continuous Aggregate: market_index_1min
-- Tính nến 1 phút cho chỉ số thị trường từ snapshots 5 giây
-- Lưu ý: highest_value/lowest_value trong raw là lũy kế cả phiên,
--        nên phải dùng max/min của cột `value` cho H/L nến
-- ============================================================
create materialized view if not exists market_index_1min
with (timescaledb.continuous) as
select
    index_name,
    time_bucket('1 minute', exchange_ts)          as bucket,
    first(value, exchange_ts)                     as open,
    max(value)                                 as high,
    min(value)                                 as low,
    last(value, exchange_ts)                      as close,
    -- Volume delta trong bucket (total_volume là lũy kế cả phiên)
    (max(total_volume) - min(total_volume))    as volume,
    (max(total_value)  - min(total_value))     as turnover,
    -- Market breadth tại thời điểm cuối bucket
    last(up_count,          exchange_ts)          as up_count,
    last(down_count,        exchange_ts)          as down_count,
    last(steady_count,      exchange_ts)          as steady_count,
    last(upper_limit_count, exchange_ts)          as upper_limit_count,
    last(lower_limit_count, exchange_ts)          as lower_limit_count,
    avg(extract(epoch from (dnse_ts - exchange_ts)) * 1000) as avg_latency_ms
from market_index
group by index_name, time_bucket('1 minute', exchange_ts)
with no data;

select add_continuous_aggregate_policy('market_index_1min',
    start_offset      => interval '10 minutes',
    end_offset        => interval '1 minute',
    schedule_interval => interval '1 minute',
    if_not_exists     => true);
