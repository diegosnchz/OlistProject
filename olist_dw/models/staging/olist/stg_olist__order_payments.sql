with source as (

    select * from {{ ref('olist_order_payments_dataset') }}

),

renamed as (

    select
        -- ids
        order_id,
        payment_sequential,

        -- payment info
        payment_type,
        payment_installments as installments,
        payment_value

    from source

)

select * from renamed
