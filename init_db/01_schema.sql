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

    exchange_ts      timestamptz     not null,   -- sending_time: nguồn gốc từ sàn
    dnse_ts         timestamptz,                -- multicast_receive_time: đo latency
    producer_ts     timestamptz,                -- _receivedAt: thời điểm Python SDK decode được message
    ingested_ts     timestamptz     default clock_timestamp()
);

select create_hypertable('market_trade', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_trade_symbol_ts
    on market_trade (symbol, exchange_ts desc);

create index if not exists idx_trade_symbol_side_ts
    on market_trade (symbol, side, exchange_ts desc);

-- ============================================================
-- Bảng order_book_l2 (quote)
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
    ingested_ts     timestamptz     default clock_timestamp()
);

select create_hypertable('order_book_l2', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_ob_symbol_ts
    on order_book_l2 (symbol, exchange_ts desc);

-- ============================================================
-- Bảng news_sentiment
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
-- Bảng security_definition (Giá trần sàn, tham chiếu)
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
-- Bảng expected_price (Giá dự kiến khớp ATO/ATC)
-- Dữ liệu Periodic, chỉ xuất hiện trong phiên ATO/ATC
-- ============================================================
create table if not exists expected_price (
    symbol          text            not null,
    market_id       text,
    board_id        text,
    isin            text,
    close_price     double precision,   -- Giá tham chiếu (phiên trước)
    expected_price  double precision,   -- Giá dự kiến khớp
    expected_qty    bigint,             -- KL dự kiến khớp
    producer_ts     timestamptz     not null,
    ingested_ts     timestamptz     default clock_timestamp()
);

select create_hypertable('expected_price', 'producer_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_exprice_symbol_ts
    on expected_price (symbol, producer_ts desc);

-- ============================================================
-- Bảng market_index (Chỉ số thị trường: VNINDEX, VN30, HNX...)
-- Dữ liệu Periodic, cập nhật mỗi 5 giây trong phiên giao dịch
-- ============================================================
create table if not exists market_index (
    index_name          text            not null,   -- VD: VNINDEX, VN30, HNX
    market_id           text,                       -- STO (HOSE), STX (HNX), UPX (UPCOM)

    -- Giá trị chỉ số
    value               double precision,           -- Giá trị hiện tại
    prior_value         double precision,           -- Giá trị tham chiếu
    highest_value       double precision,           -- Cao nhất phiên
    lowest_value        double precision,           -- Thấp nhất phiên
    changed_value       double precision,           -- Thay đổi so với tham chiếu
    changed_ratio       double precision,           -- % thay đổi

    -- Độ rộng thị trường (Market Breadth)
    up_count            integer,                    -- Số mã tăng
    down_count          integer,                    -- Số mã giảm
    steady_count        integer,                    -- Số mã không đổi
    upper_limit_count   integer,                    -- Số mã tăng trần
    lower_limit_count   integer,                    -- Số mã giảm sàn

    -- Khối lượng theo chiều
    up_volume           bigint,
    down_volume         bigint,
    steady_volume       bigint,

    -- Thanh khoản tổng hợp
    total_volume        bigint,                     -- Tổng KL toàn phiên
    total_value         double precision,           -- Tổng GT toàn phiên (tỷ đồng)
    match_volume        bigint,                     -- KL khớp lệnh
    match_value         double precision,           -- GT khớp lệnh
    deal_volume         bigint,                     -- KL thỏa thuận
    deal_value          double precision,           -- GT thỏa thuận

    trading_session_id  text,

    exchange_ts         timestamptz     not null,   -- transactTime từ sàn
    dnse_ts             timestamptz,                -- multicastReceiveTime từ DNSE server
    producer_ts         timestamptz,                -- _receivedAt: thời điểm Python SDK decode được message
    ingested_ts         timestamptz     default clock_timestamp()
);

select create_hypertable('market_index', 'exchange_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);


create index if not exists idx_market_index_name_ts
    on market_index (index_name, exchange_ts desc);



-- ============================================================
-- Bảng foreign_investor (Giao dịch nhà đầu tư nước ngoài)
-- Cập nhật liên tục trong phiên, mỗi lần có GD nước ngoài
-- ============================================================
create table if not exists foreign_investor (
    symbol              text            not null,
    market_id           text,
    board_id            text,
    trading_session_id  text,

    sell_volume         bigint,         -- KL bán trong lần cập nhật này
    sell_value          bigint,         -- GT bán (VND)
    buy_volume          bigint,         -- KL mua
    buy_value           bigint,         -- GT mua (VND)

    -- Dữ liệu lũy kế cả phiên
    total_sell_volume   bigint,
    total_sell_value    bigint,
    total_buy_volume    bigint,
    total_buy_value     bigint,

    -- Thông tin room nước ngoài
    order_limit_qty     bigint,         -- Room tổng (foreignerOrderLimitQuantity)
    buy_possible_qty    bigint,         -- Room còn lại (foreignerBuyPossibleQuantity)

    exchange_ts         timestamptz,    -- transactTime (nullable, format chưa xác định)
    producer_ts         timestamptz not null,   -- _receivedAt (luôn có)
    ingested_ts         timestamptz     default clock_timestamp()
);

select create_hypertable('foreign_investor', 'producer_ts',
    chunk_time_interval => interval '1 day', if_not_exists => true);

create index if not exists idx_fi_symbol_ts
    on foreign_investor (symbol, producer_ts desc);


-- ============================================================
-- Bảng trading_calendar (Lịch ngày giao dịch — lấy từ DNSE REST API)
-- Ngày nằm trong bảng = ngày giao dịch. Sync bởi dag_sync_calendar.
-- ============================================================
CREATE TABLE IF NOT EXISTS trading_calendar (
    trading_date  date  PRIMARY KEY
);

-- ============================================================
-- Bảng instrument_master (Danh mục mã chứng khoán)
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