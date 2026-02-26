with source as (
    select * from {{ source('raw', 'broadcast_schedule') }}
),

renamed as (
    select
        game_date,
        game_id,
        is_national_broadcast,
        loaded_at
    from source
)

select * from renamed
