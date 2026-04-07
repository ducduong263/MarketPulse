create extension if not exists timescaledb;

-- ============================================================
-- Bảng market trade (trade extra)
-- ============================================================
create table if not exists market_trade (
    symbol          text            not null,
    market_id       smallint        not null, 

    price           double precision not null,   -- đơn vị nghìn VND
    quantity        integer          not null,   
    side            smallint         not null,   -- 1=buy 2=sell
    -- session = cả ngày giao dịch 
    session_vol     bigint,                      -- total_volume_traded tích lũy
    session_high    double precision,            -- highest_price phiên
    session_low     double precision,            -- lowest_price phiên
    session_open    double precision,            -- open_price phiên
    session_vwap    double precision,            -- avg_price phiên (VWAP từ sàn)

    event_ts        timestamptz     not null,   -- sending_time: nguồn gốc từ sàn
    received_ts     timestamptz,                -- multicast_receive_time: đo latency
    ingested_ts     timestamptz     default now()
);

select create_hypertable('market_trade', 'event_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_trade_symbol_ts
    on market_trade (symbol, event_ts desc);

create index if not exists idx_trade_symbol_side_ts
    on market_trade (symbol, side, event_ts desc);

-- ============================================================
-- Bảng order_book_l2 (quote)
-- ============================================================
create table if not exists order_book_l2 (
    symbol          text            not null,
    market_id       smallint        not null,

    bid_price1      double precision, bid_qty1  integer,
    bid_price2      double precision, bid_qty2  integer,
    bid_price3      double precision, bid_qty3  integer,

    ask_price1      double precision, ask_qty1  integer,
    ask_price2      double precision, ask_qty2  integer,
    ask_price3      double precision, ask_qty3  integer,

    spread          double precision
        generated always as (ask_price1 - bid_price1) stored,

    event_ts        timestamptz     not null,
    received_ts     timestamptz,
    ingested_ts     timestamptz     default now()
);

select create_hypertable('order_book_l2', 'event_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_ob_symbol_ts
    on order_book_l2 (symbol, event_ts desc);

-- ============================================================
-- OHLCV 1 min 
-- ============================================================

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

-- ============================================================
-- Bảng news sentiment
-- ============================================================
create table if not exists news_sentiment (
    id              serial,
    symbol          text not null,
    headline        text,
    source          text,
    positive_score  double precision,
    negative_score  double precision,
    neutral_score   double precision,
    sentiment_score double precision,  
    published_ts    timestamptz not null default now()
);

select create_hypertable(
    'news_sentiment', 'published_ts',
    chunk_time_interval => interval '7 days',
    if_not_exists => true
);

create index if not exists idx_sentiment_symbol_ts
    on news_sentiment (symbol, published_ts desc);