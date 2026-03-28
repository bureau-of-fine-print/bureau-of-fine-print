with source as (
    select * from {{ source('mlb_raw', 'odds') }}
),

renamed as (
    select
        game_id,
        game_date,
        home_team_abbr,
        away_team_abbr,
        snapshot_type,
        scraped_at,
        -- run line
        home_runline,
        home_runline_price,
        away_runline,
        away_runline_price,
        -- moneyline
        home_ml,
        away_ml,
        -- total
        total_line,
        over_price,
        under_price,
        bookmaker,
        -- derived
        case when home_ml is not null and home_ml < 0
             then round(100.0 / (100.0 - home_ml) * -1 + 1, 3)
             when home_ml is not null and home_ml > 0
             then round(home_ml / 100.0 + 1, 3)
             else null
        end as home_ml_implied_prob,
        case when away_ml is not null and away_ml < 0
             then round(100.0 / (100.0 - away_ml) * -1 + 1, 3)
             when away_ml is not null and away_ml > 0
             then round(away_ml / 100.0 + 1, 3)
             else null
        end as away_ml_implied_prob,
        inserted_at
    from source
)

select * from renamed