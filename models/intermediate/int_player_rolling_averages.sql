with player_stats as (
    select * from {{ ref('stg_player_game_stats') }}
),

game_scores as (
    select game_id, game_date from {{ ref('int_game_scores') }}
),

games as (
    select game_id, season from {{ ref('stg_games') }}
),

with_date as (
    select
        p.player_name,
        p.team_id,
        p.game_id,
        g.game_date,
        gs.season,
        p.points,
        p.rebounds,
        p.assists,
        p.steals,
        p.blocks,
        p.turnovers,
        p.field_goals_made,
        p.field_goals_attempted,
        p.three_pointers_made,
        p.three_pointers_attempted,
        p.free_throws_made,
        p.free_throws_attempted,
        p.plus_minus,
        p.starter
    from player_stats p
    inner join game_scores g on p.game_id = g.game_id
    inner join games gs on p.game_id = gs.game_id
    where p.minutes_played > 0 and p.minutes_played is not null
),

season_avgs as (
    select
        player_name,
        count(*) as games_played,
        round(avg(points), 1) as pts_season_avg,
        round(avg(rebounds), 1) as reb_season_avg,
        round(avg(assists), 1) as ast_season_avg,
        round(avg(steals), 1) as stl_season_avg,
        round(avg(blocks), 1) as blk_season_avg,
        round(avg(turnovers), 1) as tov_season_avg,
        round(avg(plus_minus), 1) as plus_minus_season_avg
    from with_date
    where season = 2026
    group by player_name
)

select
    w.player_name,
    w.team_id,
    w.game_id,
    w.game_date,
    w.points,
    w.rebounds,
    w.assists,
    w.steals,
    w.blocks,
    w.turnovers,
    w.plus_minus,
    w.starter,

    -- Season averages (across all teams)
    s.games_played,
    s.pts_season_avg,
    s.reb_season_avg,
    s.ast_season_avg,
    s.stl_season_avg,
    s.blk_season_avg,
    s.tov_season_avg,
    s.plus_minus_season_avg,

    -- Rolling last 3 (across all teams)
    round(avg(w.points) over (
        partition by w.player_name order by w.game_date
        rows between 2 preceding and current row
    ), 1) as pts_last3,
    round(avg(w.rebounds) over (
        partition by w.player_name order by w.game_date
        rows between 2 preceding and current row
    ), 1) as reb_last3,
    round(avg(w.assists) over (
        partition by w.player_name order by w.game_date
        rows between 2 preceding and current row
    ), 1) as ast_last3,

    -- Rolling last 5 (across all teams)
    round(avg(w.points) over (
        partition by w.player_name order by w.game_date
        rows between 4 preceding and current row
    ), 1) as pts_last5,
    round(avg(w.rebounds) over (
        partition by w.player_name order by w.game_date
        rows between 4 preceding and current row
    ), 1) as reb_last5,
    round(avg(w.assists) over (
        partition by w.player_name order by w.game_date
        rows between 4 preceding and current row
    ), 1) as ast_last5,

    -- Points above season average last 3 and last 5
    round(avg(w.points) over (
        partition by w.player_name order by w.game_date
        rows between 2 preceding and current row
    ) - s.pts_season_avg, 1) as pts_above_avg_last3,

    round(avg(w.points) over (
        partition by w.player_name order by w.game_date
        rows between 4 preceding and current row
    ) - s.pts_season_avg, 1) as pts_above_avg_last5

from with_date w
inner join season_avgs s
    on w.player_name = s.player_name