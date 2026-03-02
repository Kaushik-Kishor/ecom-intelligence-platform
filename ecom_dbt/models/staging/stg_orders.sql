with source as (
    select * from {{ source('ecom_warehouse', 'silver_orders') }}
),

cleaned as (
    select
        order_id,
        user_id,
        product_id,
        product_name,
        category,
        quantity,
        unit_price,
        total_price,
        status,
        payment_method,
        city,
        country,
        event_time,

        -- Derived fields
        date(event_time)                    as order_date,
        extract(hour from event_time)       as order_hour,
        extract(dow from event_time)        as day_of_week,

        -- Flag large orders as potential anomalies
        case when quantity >= 10 then true else false end as is_bulk_order,

        -- Revenue tier
        case
            when total_price < 50   then 'low'
            when total_price < 500  then 'medium'
            else 'high'
        end as revenue_tier

    from source
    where order_id is not null
      and total_price > 0
)

select * from cleaned

where order_id is not null
      and total_price > 0
      and category is not null    -- ← add this line