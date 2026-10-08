{#
    fct_orders (public preview)

    Full version: order-grain fact table, incremental insert_overwrite,
    partitioned by order_date, clustered by status and country, rolling
    late-arriving-data lookback window, PII-free customer join.

    Get it in the complete blueprint: https://payhip.com/b/YIq4E
#}
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    date(ordered_at) as order_date,
    order_status
from {{ ref('stg_thelook__orders') }}
