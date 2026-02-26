with game_scores as (
    select * from {{ ref('int_game_scores') }}
),

game_results as (
    select * from {{ ref('stg_game_results') }}
),

combined as (
    select
        g.game_id,
        g.game_date,
        g.home_team_id,
        g.away_team_id,
        g.home_points,
        g.away_points,
        g.home_fgm, g.home_fga,
        g.home_ftm, g.home_fta,
        g.home_oreb, g.home_tov,
        g.away_fgm, g.away_fga,
        g.away_ftm, g.away_fta,
        g.away_oreb, g.away_tov,
        coalesce(r.overtime_periods, 0) as overtime_periods,
        48 + (coalesce(r.overtime_periods, 0) * 5) as total_minutes
    from game_scores g
    left join game_results r on g.game_id = r.game_id
),

possessions as (
    select
        game_id,
        game_date,
        home_team_id,
        away_team_id,
        total_minutes,
        overtime_periods,
        -- Home possessions
        home_fga - home_oreb + home_tov + (0.44 * home_fta) as home_possessions,
        -- Away possessions
        away_fga - away_oreb + away_tov + (0.44 * away_fta) as away_possessions
    from combined
)

select
    game_id,
    game_date,
    home_team_id,
    away_team_id,
    overtime_periods,
    total_minutes,
    round(home_possessions, 1) as home_possessions,
    round(away_possessions, 1) as away_possessions,
    round((home_possessions + away_possessions) / 2, 1) as avg_possessions,
    -- Pace = possessions per 48 minutes
    round((home_possessions / total_minutes) * 48, 1) as home_pace,
    round((away_possessions / total_minutes) * 48, 1) as away_pace,
    round(((home_possessions + away_possessions) / 2 / total_minutes) * 48, 1) as game_pace
from possessions