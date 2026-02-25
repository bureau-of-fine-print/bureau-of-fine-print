with team_record as (
    select * from {{ ref('int_team_record') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

ref_games as (
    select game_id, referee_1 as referee_id from referee_assignments where referee_1 is not null
    union all
    select game_id, referee_2 as referee_id from referee_assignments where referee_2 is not null
    union all
    select game_id, referee_3 as referee_id from referee_assignments where referee_3 is not null
),

team_ref_games as (
    select
        tr.team_id,
        tr.game_id,
        tr.game_date,
        tr.home_away,
        tr.win,
        tr.team_points,
        tr.opp_points,
        rg.referee_id
    from team_record tr
    inner join ref_games rg on tr.game_id = rg.game_id
)

select
    team_id,
    referee_id,
    count(*) as games_with_referee,
    sum(win) as wins,
    sum(1 - win) as losses,
    round(sum(win) / count(*), 3) as win_pct,
    round(avg(team_points), 1) as avg_points_scored,
    round(avg(opp_points), 1) as avg_points_allowed,
    round(avg(team_points - opp_points), 1) as avg_point_differential,
    sum(case when home_away = 'home' then win else 0 end) as home_wins,
    sum(case when home_away = 'home' then 1 - win else 0 end) as home_losses,
    sum(case when home_away = 'away' then win else 0 end) as away_wins,
    sum(case when home_away = 'away' then 1 - win else 0 end) as away_losses
from team_ref_games
group by team_id, referee_id