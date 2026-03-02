with orders as (
    select * from {{ ref('stg_orders') }}
),

dates as (
    select distinct
        order_date                                          as date,
        extract(year  from order_date)                      as year,
        extract(month from order_date)                      as month,
        extract(day   from order_date)                      as day,
        extract(dow   from order_date)                      as day_of_week,
        to_char(order_date, 'Day')                          as day_name,
        to_char(order_date, 'Month')                        as month_name,
        case when extract(dow from order_date) in (0,6)
             then true else false end                       as is_weekend

    from orders
)

select * from dates