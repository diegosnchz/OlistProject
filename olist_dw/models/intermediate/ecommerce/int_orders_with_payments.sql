with orders as (

    select * from {{ ref('stg_olist__orders') }}

),

payments as (

    select * from {{ ref('stg_olist__order_payments') }}

),

order_items as (

    select * from {{ ref('stg_olist__order_items') }}

),

payments_agg as (

    select
        order_id,
        sum(payment_value)   as total_amount,
        sum(installments)    as total_installments,
        mode(payment_type)   as payment_type

    from payments
    group by order_id

),

freight_agg as (

    select
        order_id,
        sum(freight_value) as total_freight

    from order_items
    group by order_id

),

joined as (

    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_to_carrier_at,
        o.delivered_to_customer_at,
        o.estimated_delivery_at,
        p.total_amount,
        p.payment_type,
        p.total_installments,
        f.total_freight

    from orders o
    left join payments_agg p on o.order_id = p.order_id
    left join freight_agg f on o.order_id = f.order_id

)

select * from joined
