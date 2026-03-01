with team_record as (
    select * from {{ ref('int_team_record') }}
),

rolling as (
    select * from {{ ref('int_team_rolling_averages') }}
),

games as (
    select game_id, season from {{ ref('stg_games') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

current_season as (
    select max(season) as season from {{ ref('stg_games') }}
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
        rg.referee_id,
        g.season,
        cs.season as current_season,
        rol.win_pct_last10 as expected_win_pct,
        cast(tr.win as float64) - coalesce(rol.win_pct_last10, 0.5) as ref_effect_this_game,
        case
            when g.season = cs.season       then 1.0
            when g.season = cs.season - 1   then 0.4
            when g.season = cs.season - 2   then 0.15
            else null
        end as season_weight
    from team_record tr
    inner join ref_games rg   on tr.game_id  = rg.game_id
    inner join games g        on tr.game_id  = g.game_id
    inner join rolling rol    on tr.game_id  = rol.game_id
                             and tr.team_id  = rol.team_id
    cross join current_season cs
    where g.season >= cs.season - 2
),

current_season_stats as (
    select
        team_id,
        referee_id,
        count(*)                                        as current_season_games,
        round(sum(win) / nullif(count(*), 0), 3)       as current_season_win_pct
    from team_ref_games
    where season = current_season
    group by team_id, referee_id
),

weighted_stats as (
    select
        team_id,
        referee_id,
        sum(season_weight)                                              as total_weight,
        sum(win * season_weight)                                        as weighted_wins,
        sum(ref_effect_this_game * season_weight)                       as weighted_ref_effect_sum,
        stddev(ref_effect_this_game)                                    as ref_effect_stddev,
        count(*)                                                        as games_with_referee,
        sum(win)                                                        as total_wins,
        sum(team_points * season_weight)                                as weighted_pts,
        sum(opp_points * season_weight)                                 as weighted_opp_pts,
        sum(expected_win_pct * season_weight)                           as weighted_expected_sum,
        sum(case when home_away = 'home' then 1 else 0 end)            as home_games,
        sum(case when home_away = 'home' then win else 0 end)          as home_wins
    from team_ref_games
    where season_weight is not null
    group by team_id, referee_id
)

select
    ws.team_id,
    ws.referee_id,
    ws.games_with_referee,
    coalesce(cs.current_season_games, 0)                                as current_season_games,
    round(ws.weighted_ref_effect_sum / nullif(ws.total_weight, 0), 3)  as weighted_ref_effect,
    cs.current_season_win_pct                                           as current_season_ref_effect,
    round(ws.ref_effect_stddev, 3)                                      as ref_effect_stddev,
    round(ws.weighted_wins / nullif(ws.total_weight, 0), 3)            as weighted_win_pct,
    cs.current_season_win_pct,
    round(ws.weighted_expected_sum / nullif(ws.total_weight, 0), 3)    as avg_expected_win_pct,
    round(ws.weighted_pts / nullif(ws.total_weight, 0), 1)             as avg_points_scored,
    round(ws.weighted_opp_pts / nullif(ws.total_weight, 0), 1)         as avg_points_allowed,
    round(
        (ws.weighted_pts - ws.weighted_opp_pts) / nullif(ws.total_weight, 0)
    , 1)                                                                as avg_point_differential,
    ws.home_games,
    ws.home_wins,
    round(ws.home_wins / nullif(ws.home_games, 0), 3)                  as home_win_pct
from weighted_stats ws
left join current_season_stats cs
    on ws.team_id    = cs.team_id
    and ws.referee_id = cs.referee_id
