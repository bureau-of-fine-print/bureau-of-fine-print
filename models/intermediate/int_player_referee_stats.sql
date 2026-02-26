with player_stats as (
    select * from {{ ref('stg_player_game_stats') }}
),

game_scores as (
    select game_id, game_date from {{ ref('int_game_scores') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

-- Unpivot refs into one row per referee per game
ref_games as (
    select game_id, referee_1 as referee_id from referee_assignments where referee_1 is not null
    union all
    select game_id, referee_2 as referee_id from referee_assignments where referee_2 is not null
    union all
    select game_id, referee_3 as referee_id from referee_assignments where referee_3 is not null
),

player_ref_games as (
    select
        p.player_name,
        p.team_id,
        r.referee_id,
        p.points,
        p.rebounds,
        p.assists,
        p.steals,
        p.blocks,
        p.turnovers,
        p.plus_minus,
        p.minutes_played
    from player_stats p
    inner join game_scores g on p.game_id = g.game_id
    inner join ref_games r on p.game_id = r.game_id
    where p.minutes_played > 0 and p.minutes_played is not null
)

select
    player_name,
    team_id,
    referee_id,
    count(*) as games_with_referee,
    round(avg(points), 1) as avg_points,
    round(avg(rebounds), 1) as avg_rebounds,
    round(avg(assists), 1) as avg_assists,
    round(avg(steals), 1) as avg_steals,
    round(avg(blocks), 1) as avg_blocks,
    round(avg(turnovers), 1) as avg_turnovers,
    round(avg(plus_minus), 1) as avg_plus_minus
from player_ref_games
group by player_name, team_id, referee_id