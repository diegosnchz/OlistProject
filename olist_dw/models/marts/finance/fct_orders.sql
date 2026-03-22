with orders_payments as (

    select * from {{ ref('int_orders_with_payments') }}

),

orders_reviews as (

    select * from {{ ref('int_orders_with_reviews') }}

),

final as (

    select
        -- keys
        op.order_id,
        op.customer_id,

        -- status
        op.order_status,

        -- timestamps
        op.purchased_at,
        op.approved_at,
        op.delivered_to_customer_at,
        op.estimated_delivery_at,

        -- payment metrics
        op.total_amount,
        op.total_freight,
        op.payment_type,
        op.total_installments as installments,

        -- review metrics
        rv.review_score,
        rv.has_review,

        -- delivery metrics
        datediff(
            'day',
            op.purchased_at,
            op.delivered_to_customer_at
        ) as delivery_days,

        case
            when op.delivered_to_customer_at > op.estimated_delivery_at then true
            else false
        end as is_late_delivery,

        op.order_status = 'delivered' as is_delivered

    from orders_payments op
    left join orders_reviews rv on op.order_id = rv.order_id

)

select * from final
