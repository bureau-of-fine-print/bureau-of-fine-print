with games as (
    select * from {{ ref('int_game_scores') }}
),

arenas as (
    select * from `project-71e6f4ed-bf24-4c0f-bb0.seeds.dim_arenas`
),

-- Get each team's previous game arena
home_with_prev as (
    select
        game_id,
        game_date,
        home_team_id as team_id,
        arena_id,
        lag(arena_id) over (partition by home_team_id order by game_date) as prev_arena_id
    from games
),

away_with_prev as (
    select
        game_id,
        game_date,
        away_team_id as team_id,
        arena_id,
        lag(arena_id) over (partition by away_team_id order by game_date) as prev_arena_id
    from games
),

-- Calculate distance using Haversine formula
home_travel as (
    select
        h.game_id,
        h.team_id,
        h.arena_id,
        h.prev_arena_id,
        case
            when h.prev_arena_id is null then 0
            when h.prev_arena_id = h.arena_id then 0
            else round(
                3958.8 * acos(
                    least(1.0, 
                        sin(a1.latitude * acos(-1) / 180) * sin(a2.latitude * acos(-1) / 180) +
                        cos(a1.latitude * acos(-1) / 180) * cos(a2.latitude * acos(-1) / 180) *
                        cos((a2.longitude - a1.longitude) * acos(-1) / 180)
                    )
                ), 0
            )
        end as travel_miles
    from home_with_prev h
    left join arenas a1 on h.prev_arena_id = a1.arena_id
    left join arenas a2 on h.arena_id = a2.arena_id
),

away_travel as (
    select
        a.game_id,
        a.team_id,
        a.arena_id,
        a.prev_arena_id,
        case
            when a.prev_arena_id is null then 0
            when a.prev_arena_id = a.arena_id then 0
            else round(
                3958.8 * acos(
                    least(1.0,
                        sin(ar1.latitude * acos(-1) / 180) * sin(ar2.latitude * acos(-1) / 180) +
                        cos(ar1.latitude * acos(-1) / 180) * cos(ar2.latitude * acos(-1) / 180) *
                        cos((ar2.longitude - ar1.longitude) * acos(-1) / 180)
                    )
                ), 0
            )
        end as travel_miles
    from away_with_prev a
    left join arenas ar1 on a.prev_arena_id = ar1.arena_id
    left join arenas ar2 on a.arena_id = ar2.arena_id
)

select
    g.game_id,
    g.game_date,
    g.home_team_id,
    g.away_team_id,
    ht.travel_miles as home_travel_miles,
    atravel.travel_miles as away_travel_miles,
    atravel.travel_miles - ht.travel_miles as travel_advantage
from games g
left join home_travel ht
    on g.game_id = ht.game_id
    and g.home_team_id = ht.team_id
left join away_travel atravel
    on g.game_id = atravel.game_id
    and g.away_team_id = atravel.team_id