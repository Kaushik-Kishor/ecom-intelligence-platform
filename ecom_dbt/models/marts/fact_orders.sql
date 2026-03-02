with orders as (
    select * from {{ ref('stg_orders') }}
),

fact as (
    select
        -- Keys
        order_id,
        user_id,
        product_id,
        order_date,

        -- Measures
        quantity,
        unit_price,
        total_price,
        revenue_tier,
        is_bulk_order,

        -- Context
        category,
        status,
        payment_method,
        city,
        country,
        order_hour,
        day_of_week,
        event_time

    from orders
)

select * from fact