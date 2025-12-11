-- models/staging/stg_raw_prices.sql

select
    asset_id,
    asset_symbol,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    price_ts_utc
from {{ source('crypto', 'raw_prices') }}
