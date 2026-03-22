with source as (

    select * from {{ ref('olist_sellers_dataset') }}

),

renamed as (

    select
        -- ids
        seller_id,

        -- location
        seller_city         as city,
        seller_state        as state,
        seller_zip_code_prefix as zip_code_prefix

    from source

)

select * from renamed
