with orders as (
    select * from {{ ref('stg_orders') }}
),

users as (
    select
        user_id,
        max(country)                                        as country,
        count(order_id)                                     as total_orders,
        round(sum(total_price)::numeric, 2)                 as total_spent,
        round(avg(total_price)::numeric, 2)                 as avg_order_value,
        min(event_time)                                     as first_order_time,
        max(event_time)                                     as last_order_time,
        count(distinct payment_method)                      as payment_methods_used,

        case
            when sum(total_price) > 5000  then 'VIP'
            when sum(total_price) > 1000  then 'regular'
            else 'occasional'
        end as customer_segment

    from orders
    group by user_id
)

select * from users