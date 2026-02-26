with team_record as (
    select * from {{ ref('int_team_record') }}
),

game_pace as (
    select * from {{ ref('int_game_pace') }}
)

select
    tr.team_id,
    tr.game_id,
    tr.game_date,
    tr.home_away,
    tr.win,

    -- Rolling last 5 games
    round(avg(tr.team_points) over (
        partition by tr.team_id order by tr.game_date
        rows between 4 preceding and current row
    ), 1) as pts_last5,

    round(avg(tr.opp_points) over (
        partition by tr.team_id order by tr.game_date
        rows between 4 preceding and current row
    ), 1) as opp_pts_last5,

    round(avg(tr.win) over (
        partition by tr.team_id order by tr.game_date
        rows between 4 preceding and current row
    ), 3) as win_pct_last5,

    -- Rolling last 10 games
    round(avg(tr.team_points) over (
        partition by tr.team_id order by tr.game_date
        rows between 9 preceding and current row
    ), 1) as pts_last10,

    round(avg(tr.opp_points) over (
        partition by tr.team_id order by tr.game_date
        rows between 9 preceding and current row
    ), 1) as opp_pts_last10,

    round(avg(tr.win) over (
        partition by tr.team_id order by tr.game_date
        rows between 9 preceding and current row
    ), 3) as win_pct_last10,

    -- Rolling pace last 5
    round(avg(case
        when tr.home_away = 'home' then gp.home_pace
        else gp.away_pace
    end) over (
        partition by tr.team_id order by tr.game_date
        rows between 4 preceding and current row
    ), 1) as pace_last5,

    -- Rolling pace last 10
    round(avg(case
        when tr.home_away = 'home' then gp.home_pace
        else gp.away_pace
    end) over (
        partition by tr.team_id order by tr.game_date
        rows between 9 preceding and current row
    ), 1) as pace_last10

from team_record tr
left join game_pace gp on tr.game_id = gp.game_id