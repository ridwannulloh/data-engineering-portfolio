-- staging/stg_products.sql

with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        cast(product_id as varchar)                     as product_id,
        trim(product_name)                              as product_name,
        upper(trim(category))                           as category,
        upper(trim(subcategory))                        as subcategory,
        cast(unit_price as decimal(18,4))               as unit_price,
        cast(currency_code as varchar)                  as currency_code,
        cast(is_active as boolean)                      as is_active,

        _dms_timestamp                                  as _source_loaded_at,
        current_timestamp                               as _dbt_loaded_at

    from source

    where product_id is not null

)

select * from renamed
