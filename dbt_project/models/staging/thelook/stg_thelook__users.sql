-- PII (names, emails, street addresses) is intentionally not selected.
with

source as (

    select * from {{ source('thelook', 'users') }}

),

renamed as (

    select
        -- ids
        cast(id as int64) as customer_id,

        -- attributes
        country as customer_country,
        state as customer_state,
        lower(traffic_source) as acquisition_channel,

        -- timestamps
        created_at as signed_up_at

    from source

)

select * from renamed
