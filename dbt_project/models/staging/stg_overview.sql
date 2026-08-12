with source as (

    select * from {{ source('bronze', 'overview') }}

),

parsed as (

    select
        symbol,
        ingested_at,
        coalesce(
            safe.parse_timestamp('%Y%m%dT%H%M%SZ', ingested_at),
            safe_cast(ingested_at as timestamp)
        ) as ingested_at_ts,

        json_value(raw_json, '$.Symbol')       as ticker,
        json_value(raw_json, '$.Name')         as company_name,
        json_value(raw_json, '$.Sector')       as sector,
        json_value(raw_json, '$.Industry')     as industry,
        json_value(raw_json, '$.Exchange')     as exchange,
        json_value(raw_json, '$.Country')      as country,
        json_value(raw_json, '$.Currency')     as currency,
        cast(json_value(raw_json, '$.MarketCapitalization') as int64) as market_cap

    from source
    where json_value(raw_json, '$.Symbol') is not null

),

deduped as (

    select *,
        row_number() over (
            partition by ticker
            order by ingested_at_ts desc
        ) as rn
    from parsed

)

select * except(rn)
from deduped
where rn = 1








































