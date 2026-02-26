with player_rolling as (
    select * from {{ ref('int_player_rolling_averages') }}
),

player_streaks as (
    select * from {{ ref('int_player_streaks') }}
),

player_quarter_avgs as (
    select
        player_name,
        team_id,
        round(avg(case when quarter = 'Q1' then points end), 1) as avg_pts_q1,
        round(avg(case when quarter = 'Q2' then points end), 1) as avg_pts_q2,
        round(avg(case when quarter = 'Q3' then points end), 1) as avg_pts_q3,
        round(avg(case when quarter = 'Q4' then points end), 1) as avg_pts_q4,
        round(avg(case when quarter = 'Q1' then assists end), 1) as avg_ast_q1,
        round(avg(case when quarter = 'Q2' then assists end), 1) as avg_ast_q2,
        round(avg(case when quarter = 'Q3' then assists end), 1) as avg_ast_q3,
        round(avg(case when quarter = 'Q4' then assists end), 1) as avg_ast_q4,
        round(avg(case when quarter = 'Q1' then plus_minus end), 1) as avg_pm_q1,
        round(avg(case when quarter = 'Q2' then plus_minus end), 1) as avg_pm_q2,
        round(avg(case when quarter = 'Q3' then plus_minus end), 1) as avg_pm_q3,
        round(avg(case when quarter = 'Q4' then plus_minus end), 1) as avg_pm_q4
    from {{ ref('stg_player_quarter_stats') }}
    group by player_name, team_id
),

-- Most recent stats per player
current_form as (
    select
        player_name,
        team_id,
        points,
        rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        plus_minus,
        pts_season_avg,
        reb_season_avg,
        ast_season_avg,
        stl_season_avg,
        blk_season_avg,
        tov_season_avg,
        plus_minus_season_avg,
        games_played,
        pts_last3,
        pts_last5,
        reb_last3,
        reb_last5,
        ast_last3,
        ast_last5,
        pts_above_avg_last3,
        pts_above_avg_last5
    from player_rolling
    qualify row_number() over (partition by player_name, team_id order by game_date desc) = 1
),

-- Current streak per player
current_streak as (
    select
        player_name,
        team_id,
        is_pts_hot_last3,
        is_pts_hot_last5,
        is_pts_cold_last3,
        is_pts_cold_last5,
        hot_game_streak,
        below_avg_streak
    from player_streaks
    qualify row_number() over (partition by player_name, team_id order by game_date desc) = 1
)

select
    cf.player_name,
    cf.team_id,
    cf.games_played,

    -- Season averages
    cf.pts_season_avg,
    cf.reb_season_avg,
    cf.ast_season_avg,
    cf.stl_season_avg,
    cf.blk_season_avg,
    cf.tov_season_avg,
    cf.plus_minus_season_avg,

    -- Last game
    cf.points as last_game_points,
    cf.rebounds as last_game_rebounds,
    cf.assists as last_game_assists,

    -- Rolling averages
    cf.pts_last3,
    cf.pts_last5,
    cf.reb_last3,
    cf.reb_last5,
    cf.ast_last3,
    cf.ast_last5,
    cf.pts_above_avg_last3,
    cf.pts_above_avg_last5,

    -- Hot/cold flags
    cs.is_pts_hot_last3,
    cs.is_pts_hot_last5,
    cs.is_pts_cold_last3,
    cs.is_pts_cold_last5,
    cs.hot_game_streak,
    cs.below_avg_streak,

    -- Quarter breakdowns
    pqa.avg_pts_q1,
    pqa.avg_pts_q2,
    pqa.avg_pts_q3,
    pqa.avg_pts_q4,
    pqa.avg_ast_q1,
    pqa.avg_ast_q2,
    pqa.avg_ast_q3,
    pqa.avg_ast_q4,
    pqa.avg_pm_q1,
    pqa.avg_pm_q2,
    pqa.avg_pm_q3,
    pqa.avg_pm_q4

from current_form cf
left join current_streak cs
    on cf.player_name = cs.player_name
    and cf.team_id = cs.team_id
left join player_quarter_avgs pqa
    on cf.player_name = pqa.player_name
    and cf.team_id = pqa.team_id