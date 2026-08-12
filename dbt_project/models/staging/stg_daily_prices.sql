with source as (

    select
        symbol,
        ingested_at,
        raw_json
    from {{ source('bronze', 'time_series_daily') }}

),

parsed as (

    select
        symbol,
        ingested_at,
        coalesce(
            safe.parse_timestamp('%Y%m%dT%H%M%SZ', ingested_at),
            safe_cast(ingested_at as timestamp)
        ) as ingested_at_ts,
        parse_json(raw_json) as json_data
    from source

),

date_keys as (

    select
        symbol,
        ingested_at_ts,
        json_data,
        date_key
    from parsed,
        unnest(
            json_keys(json_data['Time Series (Daily)'], 1)
        ) as date_key

),

extracted as (`

    select
        symbol,
        ingested_at_ts,
        date_key as price_date,
        json_value(json_data['Time Series (Daily)'][date_key]['1. open']) as open_price,
        json_value(json_data['Time Series (Daily)'][date_key]['2. high']) as high_price,
        json_value(json_data['Time Series (Daily)'][date_key]['3. low']) as low_price,
        json_value(json_data['Time Series (Daily)'][date_key]['4. close']) as close_price,
        json_value(json_data['Time Series (Daily)'][date_key]['5. volume']) as volume
    from date_keys

),

typed as (

    select
        symbol,
        cast(price_date as date) as price_date,
        ingested_at_ts,
        cast(open_price as numeric) as open_price,
        cast(high_price as numeric) as high_price,
        cast(low_price as numeric) as low_price,
        cast(close_price as numeric) as close_price,
        cast(volume as int64) as volume
    from extracted

),

deduped as (

    select
        symbol,
        price_date,
        ingested_at_ts,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        row_number() over (
            partition by symbol, price_date
            order by ingested_at_ts desc
        ) as rn
    from typed

)

select * except (rn)
from deduped
where rn = 1