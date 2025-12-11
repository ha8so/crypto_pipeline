select
    asset_id,
    asset_symbol,
    price_ts_utc,
    price_usd,
    market_cap_usd,
    volume_24h_usd
from {{ ref('stg_raw_prices') }}