with customers as (

    select * from {{ ref('stg_olist__customers') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

customer_orders as (

    select
        c.customer_unique_id,
        max(c.city)                as city,
        max(c.state)               as state,
        count(o.order_id)          as total_orders,
        sum(o.total_amount)        as total_spend,
        min(o.purchased_at)        as first_order_at,
        max(o.purchased_at)        as last_order_at,
        avg(o.review_score)        as avg_review_score

    from customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_unique_id

),

final as (

    select
        customer_unique_id,
        city,
        state,
        total_orders,
        total_spend,
        first_order_at,
        last_order_at,
        avg_review_score,
        datediff('day', first_order_at, last_order_at) as customer_lifetime_days

    from customer_orders

)

select * from final
