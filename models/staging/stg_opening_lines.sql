with source as (
    select * from {{ source('raw', 'opening_lines') }}
),

renamed as (
    select
        game_date,
        season,
        home_team,
        away_team,
        over_under,
        home_line,
        home_moneyline,
        away_moneyline,
        odds_api_game_id,
        captured_at
    from source
)

select * from renamed