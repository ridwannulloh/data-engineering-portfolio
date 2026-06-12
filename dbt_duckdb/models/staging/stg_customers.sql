-- staging/stg_customers.sql

with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        cast(customer_id as varchar)                    as customer_id,
        lower(trim(email))                              as email,
        upper(trim(country_code))                       as country_code,
        first_name,
        last_name,
        first_name || ' ' || last_name                  as full_name,

        -- Bucket customer tier from raw field
        case
            when customer_tier = 1 then 'bronze'
            when customer_tier = 2 then 'silver'
            when customer_tier = 3 then 'gold'
            else 'unknown'
        end                                             as customer_tier,

        {{ to_wib('registered_at') }}                   as registered_at_wib,

        _dms_timestamp                                  as _source_loaded_at,
        current_timestamp                               as _dbt_loaded_at

    from source

    where customer_id is not null
      and email is not null

)

select * from renamed
