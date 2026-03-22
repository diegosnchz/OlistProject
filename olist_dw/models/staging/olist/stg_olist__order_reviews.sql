with source as (

    select * from {{ ref('olist_order_reviews_dataset') }}

),

renamed as (

    select
        -- ids
        review_id,
        order_id,

        -- review content
        review_score,
        review_comment_title,
        review_comment_message,

        -- timestamps
        cast(review_creation_date as timestamp)   as review_created_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at

    from source

)

select * from renamed
