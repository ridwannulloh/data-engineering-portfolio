-- staging/stg_orders.sql
-- Cleans and standardises raw order data from the ODS layer.
-- Materialised as a VIEW — cheap, always fresh, no storage cost.

with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        cast(order_id as varchar)                       as order_id,
        cast(customer_id as varchar)                    as customer_id,
        cast(product_id as varchar)                     as product_id,
        cast(order_status as varchar)                   as order_status,

        -- Normalise timestamp to Asia/Jakarta (UTC+7)
        {{ to_wib('created_at') }}                      as created_at_wib,

        cast(total_amount as decimal(18,2))             as total_amount,
        cast(currency_code as varchar)                  as currency_code,

        -- Audit columns
        _dms_timestamp                                  as _source_loaded_at,
        current_timestamp                               as _dbt_loaded_at

    from source

    -- Exclude hard-deleted sentinel rows injected by the CDC feed
    where order_id is not null

)

select * from renamed
