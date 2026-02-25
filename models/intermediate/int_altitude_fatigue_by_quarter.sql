with team_quarter_stats as (
    select * from {{ ref('stg_team_quarter_stats') }}
),

altitude_fatigue as (
    select * from {{ ref('int_altitude_fatigue') }}
)

select
    tqs.game_id,
    tqs.team_id,
    tqs.home_away,
    tqs.quarter,
    tqs.points,
    tqs.field_goals_made,
    tqs.field_goals_attempted,
    tqs.three_pointers_made,
    tqs.three_pointers_attempted,
    tqs.free_throws_made,
    tqs.free_throws_attempted,
    tqs.offensive_rebounds,
    tqs.defensive_rebounds,
    tqs.assists,
    tqs.steals,
    tqs.blocks,
    tqs.turnovers,
    tqs.personal_fouls,

    -- Altitude context
    case
        when tqs.home_away = 'home' then af.home_arena_altitude
        else af.away_arena_altitude
    end as game_altitude,

    case
        when tqs.home_away = 'home' then af.home_altitude_change
        else af.away_altitude_change
    end as altitude_change,

    case
        when tqs.home_away = 'home' then af.home_prev_altitude
        else af.away_prev_altitude
    end as prev_altitude

from team_quarter_stats tqs
inner join altitude_fatigue af on tqs.game_id = af.game_id