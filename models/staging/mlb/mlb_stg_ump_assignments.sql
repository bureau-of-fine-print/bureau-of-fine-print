with source as (
    select * from {{ source('mlb_raw', 'ump_assignments') }}
),

renamed as (
    select
        game_id,
        game_date,
        ump_id,
        ump_name,
        position,
        is_home_plate,
        inserted_at
    from source
)

select * from renamed