with source as (

    select
        json_value(raw_json, '$.Symbol') as ticker,
        json_value(raw_json, '$.Name') as company_name,
        json_value(raw_json, '$.Sector') as sector,
        json_value(raw_json, '$.Industry') as industry,
        json_value(raw_json, '$.Exchange') as exchange,
        json_value(raw_json, '$.Country') as country,
        json_value(raw_json, '$.Currency') as currency,
        cast(
            json_value(raw_json, '$.MarketCapitalization') as int64
        ) as market_cap,
        coalesce(
            safe.parse_timestamp('%Y%m%dT%H%M%SZ', ingested_at),
            safe_cast(ingested_at as timestamp)
        ) as pulled_at
    from {{ source('bronze', 'overview') }}
    where json_value(raw_json, '$.Symbol') is not null

),

-- Collapse consecutive identical pulls so only genuine CHANGES create a new version
with_change_flag as (

    select
        ticker,
        company_name,
        sector,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        pulled_at,
        case
            when
                lag(sector) over (
                    partition by ticker order by pulled_at
                ) is distinct from sector
                or lag(exchange) over (
                    partition by ticker order by pulled_at
                ) is distinct from exchange
                or lag(market_cap) over (
                    partition by ticker order by pulled_at
                ) is distinct from market_cap
                or lag(company_name) over (
                    partition by ticker order by pulled_at
                ) is distinct from company_name
                or lag(industry) over (
                    partition by ticker order by pulled_at
                ) is distinct from industry
                or lag(country) over (
                    partition by ticker order by pulled_at
                ) is distinct from country
                or lag(currency) over (
                    partition by ticker order by pulled_at
                ) is distinct from currency
                then 1
            else 0
        end as is_new_version
    from source

),

versioned as (

    select
        ticker,
        company_name,
        sector,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        pulled_at,
        is_new_version,
        sum(is_new_version) over (
            partition by ticker order by pulled_at
        ) as version_id
    from with_change_flag

),

collapsed as (

    select
        ticker,
        company_name,
        sector,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        min(pulled_at) as dbt_valid_from
    from versioned
    group by
        ticker,
        company_name,
        sector,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        version_id

),

final as (

    select
        ticker,
        company_name,
        sector,
        industry,
        exchange,
        country,
        currency,
        market_cap,
        dbt_valid_from,
        lead(dbt_valid_from) over (
            partition by ticker order by dbt_valid_from
        ) as dbt_valid_to
    from collapsed

)

select
    ticker,
    company_name,
    sector,
    industry,
    exchange,
    country,
    currency,
    market_cap,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null) as is_current
from final