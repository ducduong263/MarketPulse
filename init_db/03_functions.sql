create or replace function ohlcv_live(
    p_symbol    text,
    p_interval  text        default '1m',
    p_from      timestamptz default now() - interval '3 hours',
    p_to        timestamptz default now()
)
returns table (
    "time"          timestamptz,
    open            double precision,
    high            double precision,
    low             double precision,
    close           double precision,
    volume          bigint
)
language plpgsql volatile 
as $$
declare
    v_interval      interval;
    v_last_mat      timestamptz;
begin
    -- Chuyển đổi Grafana interval → PostgreSQL interval
    v_interval := case p_interval
        when '1m'  then interval '1 minute'
        when '3m'  then interval '3 minutes'
        when '5m'  then interval '5 minutes'
        when '15m' then interval '15 minutes'
        when '30m' then interval '30 minutes'
        when '1h'  then interval '1 hour'
        else interval '1 minute'
    end;

    -- Mốc cuối cùng đã materialized (= max bucket + 1 phút)
    select coalesce(max(o.bucket) + interval '1 minute',
                    '-infinity'::timestamptz)
      into v_last_mat
      from ohlcv_1min o
     where o.symbol = p_symbol;

    -- Căn chỉnh p_from về đúng boundary của interval
    -- VD: p_from=10:07, interval=15m → p_from=10:00
    p_from := time_bucket(v_interval, p_from);

    return query
    with
    ----------------------------------------------------------------
    -- 1) Gộp nến 1-phút: cagg (đã đóng) + live (chưa materialized)
    ----------------------------------------------------------------
    all_1min as (
        select o.bucket, o.open, o.high, o.low, o.close, o.volume
          from ohlcv_1min o
         where o.symbol = p_symbol
           and o.bucket >= p_from
           and o.bucket <  v_last_mat

        union all

        select
            time_bucket('1 minute', t.event_ts) as bucket,
            first(t.price, t.event_ts)          as open,
            max(t.price)                        as high,
            min(t.price)                        as low,
            last(t.price, t.event_ts)           as close,
            sum(t.quantity)::bigint              as volume
          from market_trade t
         where t.symbol   = p_symbol
           and t.event_ts >= v_last_mat
           and t.event_ts <= p_to
         group by time_bucket('1 minute', t.event_ts)
    ),
    ----------------------------------------------------------------
    -- 2) Re-aggregate lên interval mong muốn
    ----------------------------------------------------------------
    resampled as (
        select
            time_bucket(v_interval, a.bucket) as bucket,
            first(a.open, a.bucket)           as open,
            max(a.high)                       as high,
            min(a.low)                        as low,
            last(a.close, a.bucket)           as close,
            sum(a.volume)::bigint             as volume
          from all_1min a
         group by time_bucket(v_interval, a.bucket)
    )
    ----------------------------------------------------------------
    -- 3) Output
    ----------------------------------------------------------------
    select
        r.bucket,
        r.open,
        r.high,
        r.low,
        r.close,
        r.volume
      from resampled r
     where r.bucket >= p_from
       and r.bucket <= p_to
     order by r.bucket asc;
end;
$$;
