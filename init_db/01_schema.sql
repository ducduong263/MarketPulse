create extension if not exists timescaledb;

-- ============================================================
-- Bảng market trade (trade extra)
-- ============================================================
create table if not exists market_trade (
    symbol          text            not null,
    market_id       text            not null, 
    board_id        text            not null,  
    price           double precision not null,   -- đơn vị nghìn VND
    quantity        integer          not null,   
    side            smallint         not null,   -- 1=buy 2=sell
    -- session = cả ngày giao dịch 
    session_vol     bigint,                      -- total_volume_traded tích lũy
    session_high    double precision,            -- highest_price phiên
    session_low     double precision,            -- lowest_price phiên
    session_open    double precision,            -- open_price phiên
    session_vwap    double precision,            -- avg_price phiên (VWAP từ sàn)

    exchange_ts      timestamptz     not null,   -- sending_time: origin from exchange
    dnse_ts         timestamptz,                -- multicast_receive_time: latency measurement
    producer_ts     timestamptz,                -- _receivedAt: timestamp when Python SDK decoded the message
    ingested_ts     timestamptz     default clock_timestamp()
    is_backfill     boolean         not null default false
);

select create_hypertable('market_trade', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_trade_symbol_ts
    on market_trade (symbol, exchange_ts desc);

create index if not exists idx_trade_symbol_side_ts
    on market_trade (symbol, side, exchange_ts desc);

-- ============================================================
-- order_book_l2 (quote)
-- ============================================================
create table if not exists order_book_l2 (
    symbol          text            not null,
    market_id       text            not null,

    bid_price1      double precision, bid_qty1  integer,
    bid_price2      double precision, bid_qty2  integer,
    bid_price3      double precision, bid_qty3  integer,

    ask_price1      double precision, ask_qty1  integer,
    ask_price2      double precision, ask_qty2  integer,
    ask_price3      double precision, ask_qty3  integer,

    spread          double precision
        generated always as (ask_price1 - bid_price1) stored,

    exchange_ts     timestamptz     not null,
    dnse_ts         timestamptz,
    producer_ts     timestamptz,
    ingested_ts     timestamptz     default clock_timestamp(),
    is_backfill     boolean         not null default false
);

select create_hypertable('order_book_l2', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_ob_symbol_ts
    on order_book_l2 (symbol, exchange_ts desc);

-- ============================================================
-- news_sentiment table
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
    published_ts    timestamptz not null default clock_timestamp()
);

select create_hypertable(
    'news_sentiment', 'published_ts',
    chunk_time_interval => interval '7 days',
    if_not_exists => true
);

create index if not exists idx_sentiment_symbol_ts
    on news_sentiment (symbol, published_ts desc);

-- ============================================================
-- security_definition table (ceiling/floor/reference prices)
-- ============================================================
create table if not exists security_definition (
    symbol                      text            not null,
    market_id                   text            not null,
    board_id                    text            not null,
    isin                        text,
    product_grp_id              text,
    security_group_id           text,
    
    basic_price                 double precision,
    ceiling_price               double precision,
    floor_price                 double precision,
    
    open_interest_qty           bigint,
    
    security_status             text,
    admin_status                text,
    trading_method_status       text,
    trading_sanction_status     text,
    
    listing_date                date,
    final_trade_date            date,
    
    trading_date                date            not null default current_date,
    ingested_ts                 timestamptz     default clock_timestamp(),
    
    primary key (symbol, market_id, board_id, trading_date)
);



-- ============================================================
-- expected_price (ATO/ATC expected match price)
-- Periodic data, only appears during ATO/ATC session
-- ============================================================
create table if not exists expected_price (
    symbol          text            not null,
    market_id       text,
    board_id        text,
    isin            text,
    close_price     double precision,   -- Reference price (previous session)
    expected_price  double precision,   -- Expected match price
    expected_qty    bigint,             -- Expected match quantity
    producer_ts     timestamptz     not null,
    ingested_ts     timestamptz     default clock_timestamp()
);

select create_hypertable('expected_price', 'producer_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_exprice_symbol_ts
    on expected_price (symbol, producer_ts desc);

-- ============================================================
-- market_index (Market index: VNINDEX, VN30, HNX...)
-- Periodic data, updated every 5 seconds during trading session
-- ============================================================
create table if not exists market_index (
    index_name          text            not null,   -- e.g. VNINDEX, VN30, HNX
    market_id           text,                       -- STO (HOSE), STX (HNX), UPX (UPCOM)

    -- Index values
    value               double precision,           -- Current value
    prior_value         double precision,           -- Reference value
    highest_value       double precision,           -- Session high
    lowest_value        double precision,           -- Session low
    changed_value       double precision,           -- Change compared to reference
    changed_ratio       double precision,           -- % change

    -- Market Breadth
    up_count            integer,                    -- Number of advancing stocks
    down_count          integer,                    -- Number of declining stocks
    steady_count        integer,                    -- Number of unchanged stocks
    upper_limit_count   integer,                    -- Number of ceiling stocks
    lower_limit_count   integer,                    -- Number of floor stocks

    -- Volume by direction
    up_volume           bigint,
    down_volume         bigint,
    steady_volume       bigint,

    -- Aggregate liquidity
    total_volume        bigint,                     -- Total volume of session
    total_value         double precision,           -- Total value of session (billion VND)
    match_volume        bigint,                     -- Match volume
    match_value         double precision,           -- Match value
    deal_volume         bigint,                     -- Deal volume
    deal_value          double precision,           -- Deal value

    trading_session_id  text,

    exchange_ts         timestamptz     not null,   -- transactTime from exchange
    dnse_ts             timestamptz,                -- multicastReceiveTime from DNSE server
    producer_ts         timestamptz,                -- _receivedAt: timestamp when Python SDK decoded the message
    ingested_ts         timestamptz     default clock_timestamp()
);

select create_hypertable('market_index', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);


create index if not exists idx_market_index_name_ts
    on market_index (index_name, exchange_ts desc);



-- ============================================================
-- foreign_investor (Foreign investor transactions)
-- Updated continuously during the session for each foreign transaction
-- ============================================================
create table if not exists foreign_investor (
    symbol              text            not null,
    market_id           text,
    board_id            text,
    trading_session_id  text,

    sell_volume         bigint,         -- Sell volume in this update
    sell_value          bigint,         -- Sell value (VND)
    buy_volume          bigint,         -- Buy volume
    buy_value           bigint,         -- Buy value (VND)

    -- Accumulated values for the entire session
    total_sell_volume   bigint,
    total_sell_value    bigint,
    total_buy_volume    bigint,
    total_buy_value     bigint,

    -- Foreign limits information
    order_limit_qty     bigint,         -- Foreigner order limit quantity
    buy_possible_qty    bigint,         -- Foreigner buy possible quantity

    exchange_ts         timestamptz,    -- transactTime (nullable, format undefined)
    producer_ts         timestamptz not null,   -- _receivedAt (always present)
    ingested_ts         timestamptz     default clock_timestamp()
);

select create_hypertable('foreign_investor', 'producer_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_fi_symbol_ts
    on foreign_investor (symbol, producer_ts desc);


-- ============================================================
-- trading_calendar (Trading calendar — fetched from DNSE REST API)
-- Dates in this table = trading days. Synchronized by dag_sync_calendar.
-- ============================================================
CREATE TABLE IF NOT EXISTS trading_calendar (
    trading_date  date  PRIMARY KEY
);

-- ============================================================
-- instrument_master (Security master list)
-- ============================================================
CREATE TABLE IF NOT EXISTS instrument_master (
    symbol              text        NOT NULL,
    market_id           text        NOT NULL,
    security_group_id   text,
    symbol_type         text,
    listed_date         date,
    final_trade_date    date,
    short_name          text,
    full_name           text,
    index_name          text,

    -- Metadata
    is_active           boolean     NOT NULL DEFAULT true,
    last_synced_ts      timestamptz DEFAULT clock_timestamp(),

    PRIMARY KEY (symbol, market_id)
);

-- ============================================================
-- Retention Policies
-- ============================================================
SELECT add_retention_policy('market_trade',     drop_after => interval '30 days', if_not_exists => true);
SELECT add_retention_policy('order_book_l2',    drop_after => interval  '7 days', if_not_exists => true);
SELECT add_retention_policy('expected_price',   drop_after => interval  '7 days', if_not_exists => true);
SELECT add_retention_policy('market_index',     drop_after => interval '30 days', if_not_exists => true);
SELECT add_retention_policy('foreign_investor', drop_after => interval '30 days', if_not_exists => true);


-- ============================================================
-- Compression Policies
-- ============================================================

ALTER TABLE order_book_l2 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'exchange_ts DESC'
);
SELECT add_compression_policy('order_book_l2',  compress_after => interval '2 days', if_not_exists => true);

ALTER TABLE expected_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'producer_ts DESC'
);
SELECT add_compression_policy('expected_price', compress_after => interval '2 days', if_not_exists => true);

ALTER TABLE market_trade SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'exchange_ts DESC'
);
SELECT add_compression_policy('market_trade',   compress_after => interval '7 days', if_not_exists => true);

ALTER TABLE market_index SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'index_name',
    timescaledb.compress_orderby   = 'exchange_ts DESC'
);
SELECT add_compression_policy('market_index',   compress_after => interval '7 days', if_not_exists => true);

ALTER TABLE foreign_investor SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'producer_ts DESC'
);
SELECT add_compression_policy('foreign_investor', compress_after => interval '7 days', if_not_exists => true);