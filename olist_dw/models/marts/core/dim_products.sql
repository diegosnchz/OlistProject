with products as (

    select * from {{ ref('stg_olist__products') }}

),

translations as (

    select * from {{ ref('product_category_name_translation') }}

),

order_items as (

    select * from {{ ref('stg_olist__order_items') }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

product_metrics as (

    select
        oi.product_id,
        count(distinct oi.order_id) as total_orders,
        avg(o.review_score)         as avg_review_score

    from order_items oi
    left join orders o on oi.order_id = o.order_id
    group by oi.product_id

),

final as (

    select
        p.product_id,
        p.category_name_pt,
        t.product_category_name_english as category_name_en,
        p.name_length,
        p.description_length,
        p.photos_qty,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        coalesce(pm.total_orders, 0) as total_orders,
        pm.avg_review_score

    from products p
    left join translations t
        on p.category_name_pt = t.product_category_name
    left join product_metrics pm
        on p.product_id = pm.product_id

)

select * from final
