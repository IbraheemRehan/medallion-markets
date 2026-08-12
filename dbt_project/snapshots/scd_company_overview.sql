{% snapshot scd_company_overview %}

{{
    config(
        target_schema='snapshots',
        unique_key='ticker',
        strategy='check',
        check_cols=['company_name', 'sector', 'industry', 'exchange', 'country', 'currency', 'market_cap'],
        invalidate_hard_deletes=True
    )
}}

select
    ticker,
    company_name,
    sector,
    industry,
    exchange,
    country,
    currency,
    market_cap
from {{ ref('stg_overview') }}

{% endsnapshot %}