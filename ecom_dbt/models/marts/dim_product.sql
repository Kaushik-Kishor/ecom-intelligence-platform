with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select
        product_id,
        max(product_name)                           as product_name,
        max(category)                               as category,
        count(order_id)                             as total_orders,
        sum(quantity)                               as total_units_sold,
        round(avg(unit_price)::numeric, 2)          as avg_unit_price,
        min(unit_price)                             as min_price,
        max(unit_price)                             as max_price,
        round(sum(total_price)::numeric, 2)         as total_revenue_generated

    from orders
    group by product_id
)

select * from products