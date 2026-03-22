with orders as (

    select * from {{ ref('stg_olist__orders') }}

),

reviews as (

    select * from {{ ref('stg_olist__order_reviews') }}

),

reviews_agg as (

    select
        order_id,
        avg(review_score)       as review_score,
        min(review_created_at)  as review_created_at,
        true                    as has_review

    from reviews
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
        r.review_score,
        coalesce(r.has_review, false) as has_review,
        r.review_created_at

    from orders o
    left join reviews_agg r on o.order_id = r.order_id

)

select * from joined
