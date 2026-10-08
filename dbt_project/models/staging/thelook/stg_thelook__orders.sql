with

source as (

    select * from {{ source('thelook', 'orders') }}

),

renamed as (

    select
        -- ids
        cast(order_id as int64) as order_id,
        cast(user_id as int64) as customer_id,

        -- attributes
        lower(status) as order_status,
        cast(num_of_item as int64) as item_count,

        -- timestamps
        created_at as ordered_at,
        shipped_at,
        delivered_at,
        returned_at

    from source

)

select * from renamed
