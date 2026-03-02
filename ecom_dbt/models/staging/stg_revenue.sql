with source as (
    select * from {{ source('ecom_warehouse', 'gold_revenue_by_category') }}
),

cleaned as (
    select
        window_start,
        window_end,
        category,
        total_revenue,
        order_count,
        round(cast(avg_order_value as numeric), 2)  as avg_order_value,
        total_units_sold,

        -- Revenue per unit
        round(
            cast(total_revenue as numeric) / nullif(total_units_sold, 0),
        2) as revenue_per_unit,

        -- Window duration in minutes
        extract(epoch from (window_end - window_start)) / 60 as window_minutes

    from source
)

select * from cleaned