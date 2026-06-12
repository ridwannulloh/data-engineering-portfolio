-- marts/mart_orders.sql
-- Wide denormalised table joining orders, customers, products and lookups.
-- Materialised as TABLE for fast BI tool queries (Metabase / Superset).

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'finance']
  )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

countries as (

    select * from {{ ref('country_codes') }}

),

status_labels as (

    select * from {{ ref('order_status_labels') }}

),

final as (

    select
        -- Surrogate key for the order grain
        {{ surrogate_key(['o.order_id']) }}             as order_key,

        -- Order grain
        o.order_id,
        o.order_status,
        sl.status_label,
        o.total_amount,
        o.currency_code,
        o.created_at_wib                                as order_datetime_wib,
        cast(date_trunc('day', o.created_at_wib) as date)   as order_date_wib,
        cast(date_trunc('month', o.created_at_wib) as date) as order_month_wib,
        extract(year from o.created_at_wib)::int        as order_year,

        -- Customer dimensions
        c.customer_id,
        c.full_name                                     as customer_name,
        c.email                                         as customer_email,
        c.country_code,
        ct.country_name,
        ct.region,
        c.customer_tier,

        -- Product dimensions
        p.product_id,
        p.product_name,
        p.category                                      as product_category,
        p.subcategory                                   as product_subcategory,

        -- Derived metrics: revenue counts only for statuses flagged revenue-bearing
        case
            when sl.is_revenue = 'true' then o.total_amount
            else 0
        end                                             as completed_revenue,

        case
            when o.order_status = 'cancelled' then 1
            else 0
        end                                             as is_cancelled,

        -- Audit
        o._source_loaded_at,
        o._dbt_loaded_at

    from orders o
    left join customers c       on o.customer_id  = c.customer_id
    left join products p        on o.product_id   = p.product_id
    left join countries ct      on c.country_code = ct.country_code
    left join status_labels sl  on o.order_status = sl.status_key

)

select * from final
