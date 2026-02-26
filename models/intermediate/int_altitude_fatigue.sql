with games as (
    select * from {{ ref('stg_games') }}
),

arenas as (
    select * from `project-71e6f4ed-bf24-4c0f-bb0.seeds.dim_arenas`
),

team_games as (
    select
        game_id,
        game_date,
        home_team_id as team_id,
        'home' as home_away,
        arena_id
    from games

    union all

    select
        game_id,
        game_date,
        away_team_id as team_id,
        'away' as home_away,
        arena_id
    from games
),

with_altitude as (
    select
        tg.team_id,
        tg.game_id,
        tg.game_date,
        tg.home_away,
        a.altitude_feet as game_altitude,
        a.city as game_city,
        lag(a.altitude_feet) over (partition by tg.team_id order by tg.game_date) as prev_altitude,
        lag(a.city) over (partition by tg.team_id order by tg.game_date) as prev_city
    from team_games tg
    left join arenas a on tg.arena_id = a.arena_id
)

select
    g.game_id,
    g.game_date,
    g.home_team_id,
    g.away_team_id,
    home_alt.game_altitude as home_arena_altitude,
    home_alt.prev_altitude as home_prev_altitude,
    home_alt.game_altitude - coalesce(home_alt.prev_altitude, home_alt.game_altitude) as home_altitude_change,
    away_alt.game_altitude as away_arena_altitude,
    away_alt.prev_altitude as away_prev_altitude,
    away_alt.game_altitude - coalesce(away_alt.prev_altitude, away_alt.game_altitude) as away_altitude_change,
    (away_alt.game_altitude - coalesce(away_alt.prev_altitude, away_alt.game_altitude)) -
    (home_alt.game_altitude - coalesce(home_alt.prev_altitude, home_alt.game_altitude)) as altitude_fatigue_advantage
from games g
left join with_altitude home_alt
    on g.game_id = home_alt.game_id
    and g.home_team_id = home_alt.team_id
left join with_altitude away_alt
    on g.game_id = away_alt.game_id
    and g.away_team_id = away_alt.team_id