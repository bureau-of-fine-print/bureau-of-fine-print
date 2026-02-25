with team_record as (
    select * from {{ ref('int_team_record') }}
),

team_rolling as (
    select * from {{ ref('int_team_rolling_averages') }}
),

team_streaks as (
    select * from {{ ref('int_team_streaks') }}
),

game_scores as (
    select * from {{ ref('int_game_scores') }}
),

-- Most recent record per team
current_record as (
    select
        team_id,
        wins,
        losses,
        games_played,
        win_pct
    from team_record
    qualify row_number() over (partition by team_id order by game_date desc) = 1
),

-- Most recent rolling averages per team
current_rolling as (
    select
        team_id,
        pts_last5,
        pts_last10,
        opp_pts_last5,
        opp_pts_last10,
        win_pct_last5,
        win_pct_last10,
        pace_last5,
        pace_last10
    from team_rolling
    qualify row_number() over (partition by team_id order by game_date desc) = 1
),

-- Current streak per team
current_streak as (
    select
        team_id,
        streak_label,
        streak_length
    from team_streaks
    qualify row_number() over (partition by team_id order by game_date desc) = 1
),

-- Season averages from game scores
home_season_avgs as (
    select
        home_team_id as team_id,
        round(avg(home_points), 1) as avg_points_scored_home,
        round(avg(away_points), 1) as avg_points_allowed_home,
        round(avg(home_points - away_points), 1) as avg_margin_home,
        count(*) as home_games
    from game_scores
    group by home_team_id
),

away_season_avgs as (
    select
        away_team_id as team_id,
        round(avg(away_points), 1) as avg_points_scored_away,
        round(avg(home_points), 1) as avg_points_allowed_away,
        round(avg(away_points - home_points), 1) as avg_margin_away,
        count(*) as away_games
    from game_scores
    group by away_team_id
),
team_quarter_avgs as (
    select
        team_id,
        round(avg(case when quarter = 'Q1' then points end), 1) as avg_pts_q1,
        round(avg(case when quarter = 'Q2' then points end), 1) as avg_pts_q2,
        round(avg(case when quarter = 'Q3' then points end), 1) as avg_pts_q3,
        round(avg(case when quarter = 'Q4' then points end), 1) as avg_pts_q4,
        round(avg(case when quarter = 'OT1' then points end), 1) as avg_pts_ot1,
        round(avg(case when quarter = 'Q1' then turnovers end), 1) as avg_tov_q1,
        round(avg(case when quarter = 'Q2' then turnovers end), 1) as avg_tov_q2,
        round(avg(case when quarter = 'Q3' then turnovers end), 1) as avg_tov_q3,
        round(avg(case when quarter = 'Q4' then turnovers end), 1) as avg_tov_q4,
        round(avg(case when quarter = 'Q1' then field_goals_attempted end), 1) as avg_fga_q1,
        round(avg(case when quarter = 'Q2' then field_goals_attempted end), 1) as avg_fga_q2,
        round(avg(case when quarter = 'Q3' then field_goals_attempted end), 1) as avg_fga_q3,
        round(avg(case when quarter = 'Q4' then field_goals_attempted end), 1) as avg_fga_q4
    from {{ ref('stg_team_quarter_stats') }}
    group by team_id
)

select
    cr.team_id,
    cr.wins,
    cr.losses,
    cr.games_played,
    cr.win_pct,

    -- Streak
    cs.streak_label as current_streak,
    cs.streak_length,

    -- Rolling form
    rol.pts_last5,
    rol.pts_last10,
    rol.opp_pts_last5,
    rol.opp_pts_last10,
    rol.win_pct_last5,
    rol.win_pct_last10,
    rol.pace_last5,
    rol.pace_last10,

    -- Home splits
    ha.avg_points_scored_home,
    ha.avg_points_allowed_home,
    ha.avg_margin_home,
    ha.home_games,

    -- Away splits
    aa.avg_points_scored_away,
    aa.avg_points_allowed_away,
    aa.avg_margin_away,
    aa.away_games,

    -- Quarter breakdowns
    tqa.avg_pts_q1,
    tqa.avg_pts_q2,
    tqa.avg_pts_q3,
    tqa.avg_pts_q4,
    tqa.avg_pts_ot1,
    tqa.avg_tov_q1,
    tqa.avg_tov_q2,
    tqa.avg_tov_q3,
    tqa.avg_tov_q4,
    tqa.avg_fga_q1,
    tqa.avg_fga_q2,
    tqa.avg_fga_q3,
    tqa.avg_fga_q4
from current_record cr
left join current_streak cs on cr.team_id = cs.team_id
left join current_rolling rol on cr.team_id = rol.team_id
left join home_season_avgs ha on cr.team_id = ha.team_id
left join away_season_avgs aa on cr.team_id = aa.team_id
left join team_quarter_avgs tqa on cr.team_id = tqa.team_id