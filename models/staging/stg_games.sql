with source as (
    select * from {{ source('raw', 'games') }}
),

renamed as (
    select
        game_id,
        game_date,
        CAST(SPLIT(season, '-')[OFFSET(0)] AS INT64) + 1 as season,
        home_team_id,
        away_team_id,
        arena_id,
        tipoff_time,
        loaded_at
    from source
)

select * from renamed