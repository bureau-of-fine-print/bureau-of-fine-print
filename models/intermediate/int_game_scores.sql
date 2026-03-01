with games as (
    select * from {{ ref('stg_games') }}
),

team_stats as (
    select * from {{ ref('stg_team_game_stats') }}
),

quarter_stats as (
    select * from {{ ref('stg_team_quarter_stats') }}
),

home as (
    select
        game_id,
        team_id as home_team_id,
        points as home_points,
        field_goals_made as home_fgm,
        field_goals_attempted as home_fga,
        three_pointers_made as home_3pm,
        three_pointers_attempted as home_3pa,
        free_throws_made as home_ftm,
        free_throws_attempted as home_fta,
        offensive_rebounds as home_oreb,
        defensive_rebounds as home_dreb,
        assists as home_ast,
        steals as home_stl,
        blocks as home_blk,
        turnovers as home_tov,
        personal_fouls as home_pf
    from team_stats
    where home_away = 'home'
),

away as (
    select
        game_id,
        team_id as away_team_id,
        points as away_points,
        field_goals_made as away_fgm,
        field_goals_attempted as away_fga,
        three_pointers_made as away_3pm,
        three_pointers_attempted as away_3pa,
        free_throws_made as away_ftm,
        free_throws_attempted as away_fta,
        offensive_rebounds as away_oreb,
        defensive_rebounds as away_dreb,
        assists as away_ast,
        steals as away_stl,
        blocks as away_blk,
        turnovers as away_tov,
        personal_fouls as away_pf
    from team_stats
    where home_away = 'away'
),

home_quarters as (
    select
        game_id,
        max(case when quarter = 'Q1'  then points end) as home_pts_q1,
        max(case when quarter = 'Q2'  then points end) as home_pts_q2,
        max(case when quarter = 'Q3'  then points end) as home_pts_q3,
        max(case when quarter = 'Q4'  then points end) as home_pts_q4,
        max(case when quarter = 'OT1' then points end) as home_pts_ot1,
        max(case when quarter = 'OT2' then points end) as home_pts_ot2,
        countif(quarter like 'OT%') as home_ot_periods
    from quarter_stats
    where home_away = 'home'
    group by game_id
),

away_quarters as (
    select
        game_id,
        max(case when quarter = 'Q1'  then points end) as away_pts_q1,
        max(case when quarter = 'Q2'  then points end) as away_pts_q2,
        max(case when quarter = 'Q3'  then points end) as away_pts_q3,
        max(case when quarter = 'Q4'  then points end) as away_pts_q4,
        max(case when quarter = 'OT1' then points end) as away_pts_ot1,
        max(case when quarter = 'OT2' then points end) as away_pts_ot2
    from quarter_stats
    where home_away = 'away'
    group by game_id
)

select
    g.game_id,
    g.game_date,
    g.season,
    g.arena_id,
    g.tipoff_time,
    h.home_team_id,
    a.away_team_id,
    h.home_points,
    a.away_points,
    h.home_points - a.away_points                    as point_differential,
    case
        when h.home_points > a.away_points then h.home_team_id
        else a.away_team_id
    end                                               as winner_team_id,
    case
        when h.home_points > a.away_points then 'home'
        else 'away'
    end                                               as winner_home_away,
    h.home_points + a.away_points                    as total_points,

    -- Home team stats
    h.home_fgm, h.home_fga,
    h.home_3pm, h.home_3pa,
    h.home_ftm, h.home_fta,
    h.home_oreb, h.home_dreb,
    h.home_ast, h.home_stl, h.home_blk, h.home_tov, h.home_pf,

    -- Away team stats
    a.away_fgm, a.away_fga,
    a.away_3pm, a.away_3pa,
    a.away_ftm, a.away_fta,
    a.away_oreb, a.away_dreb,
    a.away_ast, a.away_stl, a.away_blk, a.away_tov, a.away_pf,

    -- Quarter scores
    hq.home_pts_q1,
    hq.home_pts_q2,
    hq.home_pts_q3,
    hq.home_pts_q4,
    hq.home_pts_ot1,
    hq.home_pts_ot2,
    aq.away_pts_q1,
    aq.away_pts_q2,
    aq.away_pts_q3,
    aq.away_pts_q4,
    aq.away_pts_ot1,
    aq.away_pts_ot2,

    -- Overtime flag (driven from home side, same game so either works)
    coalesce(hq.home_ot_periods, 0)                  as overtime_periods

from games g
inner join home h        on g.game_id = h.game_id
inner join away a        on g.game_id = a.game_id
left join  home_quarters hq on g.game_id = hq.game_id
left join  away_quarters aq on g.game_id = aq.game_id