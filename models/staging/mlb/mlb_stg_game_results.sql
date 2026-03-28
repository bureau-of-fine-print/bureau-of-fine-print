with source as (
    select * from {{ source('mlb_raw', 'game_results') }}
),

renamed as (
    select
        game_id,
        game_date,
        home_team_id,
        away_team_id,
        home_score,
        away_score,
        innings_played,
        game_duration_minutes,
        had_rain_delay,
        rain_delay_minutes,
        weather_temp_f,
        weather_condition,
        -- derived
        case when home_score > away_score then home_team_id
             else away_team_id
        end as winning_team_id,
        case when home_score > away_score then away_team_id
             else home_team_id
        end as losing_team_id,
        home_score - away_score as run_differential,
        home_score + away_score as total_runs,
        case when innings_played > 9 then true else false end as extra_innings,
        case when innings_played < 9 then true else false end as shortened_game,
        inserted_at
    from source
)

select * from renamed