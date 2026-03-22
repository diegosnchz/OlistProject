with sellers as (

    select * from {{ ref('stg_olist__sellers') }}

),

order_items as (

    select * from {{ ref('stg_olist__order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

seller_metrics as (

    select
        oi.seller_id,
        count(distinct oi.order_id)                            as total_orders,
        sum(o.total_amount)                                    as total_revenue,
        avg(o.delivery_days)                                   as avg_delivery_days,
        avg(o.review_score)                                    as avg_review_score,
        avg(case when o.is_late_delivery = false then 1.0
                 else 0.0
            end)                                               as on_time_delivery_rate

    from order_items oi
    left join orders o on oi.order_id = o.order_id
    where o.is_delivered = true
    group by oi.seller_id

),

final as (

    select
        s.seller_id,
        s.city,
        s.state,
        coalesce(sm.total_revenue, 0)       as total_revenue,
        coalesce(sm.total_orders, 0)        as total_orders,
        sm.avg_delivery_days,
        sm.on_time_delivery_rate,
        sm.avg_review_score

    from sellers s
    left join seller_metrics sm on s.seller_id = sm.seller_id

)

select * from final
