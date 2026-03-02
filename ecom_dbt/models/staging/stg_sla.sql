with source as (
    select * from {{ source('ecom_warehouse', 'gold_sla_by_carrier') }}
),

cleaned as (
    select
        window_start,
        window_end,
        carrier,
        total_shipments,
        breached_count,
        round(cast(avg_delay_days as numeric), 2) as avg_delay_days,

        -- SLA breach rate as percentage
        round(
            cast(breached_count as numeric) * 100.0 / nullif(total_shipments, 0),
        2) as breach_rate_pct,

        -- Compliance rate
        round(
            (1 - cast(breached_count as numeric) / nullif(total_shipments, 0)) * 100,
        2) as compliance_rate_pct

    from source
)

select * from cleaned