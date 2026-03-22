with source as (

    select * from {{ ref('olist_customers_dataset') }}

),

renamed as (

    select
        -- ids
        customer_id,
        customer_unique_id,

        -- location
        customer_city       as city,
        customer_state      as state,
        customer_zip_code_prefix as zip_code_prefix

    from source

)

select * from renamed
