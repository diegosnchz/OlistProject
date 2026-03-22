with source as (

    select * from {{ ref('olist_products_dataset') }}

),

renamed as (

    select
        -- ids
        product_id,

        -- category
        product_category_name as category_name_pt,

        -- attributes (fixing typos from source: lenght -> length)
        product_name_lenght        as name_length,
        product_description_lenght as description_length,
        product_photos_qty         as photos_qty,

        -- dimensions
        product_weight_g  as weight_g,
        product_length_cm as length_cm,
        product_height_cm as height_cm,
        product_width_cm  as width_cm

    from source

)

select * from renamed
