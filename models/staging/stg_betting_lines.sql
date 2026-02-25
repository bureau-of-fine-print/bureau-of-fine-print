with source as (
    select * from {{ source('raw', 'betting_lines') }}
),

renamed as (
    select
        game_date,
        season,
        home_team,
        away_team,
        over_under,
        home_line,
        loaded_at
    from source
)

select * from renamed