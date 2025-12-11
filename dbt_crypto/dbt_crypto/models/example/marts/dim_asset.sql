select
    asset_id,
    max(asset_symbol) as asset_symbol
from {{ ref('stg_raw_prices') }}
group by asset_id