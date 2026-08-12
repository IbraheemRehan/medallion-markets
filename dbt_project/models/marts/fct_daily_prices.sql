with prices as (

    select *
    from {{ ref('stg_daily_prices') }}

),

company_history as (

    select
        *,
        case
            when lag(dbt_valid_from) over (partition by ticker order by dbt_valid_from) is null
            then cast('1970-01-01' as timestamp)
            else dbt_valid_from
        end as effective_valid_from
    from {{ ref('dim_company_overview_scd2') }}

),

joined as (

    select
        p.symbol,
        p.price_date,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,
        p.volume,

        c.company_name,
        c.sector,
        c.industry,
        c.exchange,
        c.country,
        c.market_cap,
        c.dbt_valid_from as company_info_valid_from,
        c.dbt_valid_to as company_info_valid_to

    from prices p
    left join company_history c
        on p.symbol = c.ticker
        and p.price_date >= date(c.effective_valid_from)
        and (
            c.dbt_valid_to is null
            or p.price_date < date(c.dbt_valid_to)
        )

)

select * from joined