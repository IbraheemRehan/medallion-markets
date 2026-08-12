with prices as (

    select
        symbol,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume
    from {{ ref('stg_daily_prices') }}

),

company_history as (

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
        is_current,
        case
            when
                lag(dbt_valid_from) over (
                    partition by ticker order by dbt_valid_from
                ) is null
                then cast('1970-01-01' as timestamp)
            else dbt_valid_from
        end as effective_valid_from
    from {{ ref('dim_company_overview_scd2') }}

),

joined as (

    select
        prices.symbol,
        prices.price_date,
        prices.open_price,
        prices.high_price,
        prices.low_price,
        prices.close_price,
        prices.volume,
        company_history.company_name,
        company_history.sector,
        company_history.industry,
        company_history.exchange,
        company_history.country,
        company_history.market_cap,
        company_history.dbt_valid_from as company_info_valid_from,
        company_history.dbt_valid_to as company_info_valid_to
    from prices
    left join company_history
        on prices.symbol = company_history.ticker
        and prices.price_date >= date(company_history.effective_valid_from)
        and (
            company_history.dbt_valid_to is null
            or prices.price_date < date(company_history.dbt_valid_to)
        )

)

select
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    company_name,
    sector,
    industry,
    exchange,
    country,
    market_cap,
    company_info_valid_from,
    company_info_valid_to
from joined