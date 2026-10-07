-- Singular test: bollinger_lower must never exceed bollinger_upper.
-- dbt_utils.accepted_range only validates one column against a constant
-- range, not a cross-column relationship, so this needs its own query.
-- Returns offending rows; dbt fails the test if any are returned.

select *
from {{ ref('int_stock_daily_metrics') }}
where bollinger_lower > bollinger_upper
