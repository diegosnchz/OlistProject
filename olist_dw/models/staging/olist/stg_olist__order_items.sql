with source as (

    select * from {{ ref('olist_order_items_dataset') }}

),

renamed as (

    select
        -- ids
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- timestamps
        cast(shipping_limit_date as timestamp) as shipping_limit_at,

        -- amounts
        price,
        freight_value

    from source

)

select * from renamed
