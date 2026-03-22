with orders as (

    select * from {{ ref('fct_orders') }}

),

delivered as (

    select * from orders
    where is_delivered = true

),

final as (

    select
        cast(purchased_at as date) as order_date,
        sum(total_amount)          as total_revenue,
        count(order_id)            as total_orders,
        avg(total_amount)          as avg_order_value,
        sum(total_freight)         as total_freight

    from delivered
    group by cast(purchased_at as date)

)

select * from final
