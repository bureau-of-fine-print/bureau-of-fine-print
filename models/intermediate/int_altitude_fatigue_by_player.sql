with player_quarter_stats as (
    select * from {{ ref('stg_player_quarter_stats') }}
),

game_scores as (
    select game_id, game_date, home_team_id, away_team_id from {{ ref('int_game_scores') }}
),

altitude_fatigue as (
    select * from {{ ref('int_altitude_fatigue') }}
)

select
    pqs.game_id,
    pqs.player_name,
    pqs.team_id,
    pqs.quarter,
    g.game_date,
    pqs.points,
    pqs.assists,
    pqs.offensive_rebounds,
    pqs.defensive_rebounds,
    pqs.steals,
    pqs.blocks,
    pqs.turnovers,
    pqs.personal_fouls,
    pqs.field_goals_made,
    pqs.field_goals_attempted,
    pqs.three_pointers_made,
    pqs.three_pointers_attempted,
    pqs.free_throws_made,
    pqs.free_throws_attempted,
    pqs.plus_minus,
    pqs.minutes,

    -- Altitude context
    case
        when pqs.team_id = g.home_team_id then af.home_arena_altitude
        else af.away_arena_altitude
    end as game_altitude,

    case
        when pqs.team_id = g.home_team_id then af.home_altitude_change
        else af.away_altitude_change
    end as altitude_change,

    case
        when pqs.team_id = g.home_team_id then af.home_prev_altitude
        else af.away_prev_altitude
    end as prev_altitude

from player_quarter_stats pqs
inner join game_scores g on pqs.game_id = g.game_id
inner join altitude_fatigue af on pqs.game_id = af.game_id