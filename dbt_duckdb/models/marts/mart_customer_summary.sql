-- marts/mart_customer_summary.sql
-- Customer-level revenue and order aggregation.
-- One row per customer. Suitable for cohort / RFM analysis.

{{
  config(
    materialized = 'table',
    tags         = ['marts', 'crm']
  )
}}

with orders as (

    select * from {{ ref('mart_orders') }}

),

agg as (

    select
        customer_id,
        customer_name,
        customer_email,
        country_code,
        region,
        customer_tier,

        count(order_id)                                 as total_orders,
        count(case when is_cancelled = 0 then 1 end)    as completed_orders,
        count(case when is_cancelled = 1 then 1 end)    as cancelled_orders,

        sum(completed_revenue)                          as lifetime_revenue,
        avg(case
              when is_cancelled = 0 then total_amount
            end)                                        as avg_order_value,

        min(order_date_wib)                             as first_order_date_wib,
        max(order_date_wib)                             as last_order_date_wib,

        date_diff('day', max(order_date_wib), {{ jakarta_today() }})
                                                        as days_since_last_order,

        -- RFM-style recency bucket
        case
            when date_diff('day', max(order_date_wib), {{ jakarta_today() }}) <= 30  then 'active'
            when date_diff('day', max(order_date_wib), {{ jakarta_today() }}) <= 90  then 'at_risk'
            when date_diff('day', max(order_date_wib), {{ jakarta_today() }}) <= 180 then 'lapsing'
            else 'churned'
        end                                             as recency_segment,

        current_timestamp                               as _dbt_loaded_at

    from orders
    group by 1, 2, 3, 4, 5, 6

)

select * from agg
