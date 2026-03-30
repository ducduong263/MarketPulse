create extension if not exists timescaledb;

create table if not exists market_tick (
    symbol  text    not null,
    price   double precision not null,
    volume  double precision not null,
    event_ts    timestamptz not null,
    ingested_ts   timestamptz default now(),
    schema_ver smallint not null default 1
);

select create_hypertable(
    'market_tick', 'event_ts',  
    chunk_time_interval => interval '1 day',
    if_not_exists => true
);

create index if not exists idx_market_tick_symbol_ts on market_tick (symbol, event_ts desc);

create materialized view if not exists ohlcv_1min
with(timescaledb.continuous) as
select
    symbol,
    time_bucket('1 minute', event_ts) as bucket,
    first(price, event_ts) as open,
    max(price) as high,
    min(price) as low,
    last(price, event_ts) as close,
    sum(volume) as volume,
    count(*) as tick_count
from market_tick
group by symbol, bucket
with no data;

select add_continuous_aggregate_policy(
    'ohlcv_1min',
    start_offset => interval '10 minutes',
    end_offset => interval '1 minute',
    schedule_interval => interval '1 minute',
    if_not_exists => true
);

create table if not exists news_sentiment (
    id              serial,
    symbol          text             not null,
    headline        text,
    source          text,
    positive        double precision,
    negative        double precision,
    neutral         double precision,
    sentiment_score double precision,  
    scored_ts       timestamptz      not null default now()
);

select create_hypertable(
    'news_sentiment', 'scored_ts',
    chunk_time_interval => interval '7 days',
    if_not_exists => true
);

create index if not exists idx_sentiment_symbol_ts
    on news_sentiment (symbol, scored_ts desc);