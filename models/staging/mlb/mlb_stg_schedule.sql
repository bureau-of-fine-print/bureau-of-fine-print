with source as (
    select * from {{ source('mlb_raw', 'schedule') }}
),

renamed as (
    select
        game_id,
        game_date,
        first_pitch_utc,
        first_pitch_et,
        home_team_id,
        home_team_abbr,
        away_team_id,
        away_team_abbr,
        venue_id,
        venue_name,
        game_status,
        is_doubleheader,
        doubleheader_game_num,
        odds_opening_scraped,
        odds_closing_scraped,
        preview_posted,
        inserted_at,
        updated_at
    from source
)

select * from renamed