with source as (
    select * from {{ source('raw', 'referee_assignments') }}
),

renamed as (
    select
        game_id,
        referee_1,
        referee_2,
        referee_3,
        loaded_at
    from source
)

select * from renamed