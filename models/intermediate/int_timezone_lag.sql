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

with_timezones as (
    select
        tg.team_id,
        tg.game_id,
        tg.game_date,
        tg.home_away,
        a.timezone as game_timezone,
        lag(a.timezone) over (partition by tg.team_id order by tg.game_date) as prev_timezone,
        lag(tg.game_date) over (partition by tg.team_id order by tg.game_date) as prev_game_date
    from team_games tg
    left join arenas a on tg.arena_id = a.arena_id
),

with_tz_offset as (
    select
        team_id,
        game_id,
        game_date,
        home_away,
        game_timezone,
        prev_timezone,
        prev_game_date,
        case
            when prev_timezone is null then 0
            when game_timezone = prev_timezone then 0
            when prev_timezone = 'America/New_York' and game_timezone = 'America/Chicago' then -1
            when prev_timezone = 'America/New_York' and game_timezone = 'America/Denver' then -2
            when prev_timezone = 'America/New_York' and game_timezone = 'America/Los_Angeles' then -3
            when prev_timezone = 'America/Chicago' and game_timezone = 'America/New_York' then 1
            when prev_timezone = 'America/Chicago' and game_timezone = 'America/Denver' then -1
            when prev_timezone = 'America/Chicago' and game_timezone = 'America/Los_Angeles' then -2
            when prev_timezone = 'America/Denver' and game_timezone = 'America/New_York' then 2
            when prev_timezone = 'America/Denver' and game_timezone = 'America/Chicago' then 1
            when prev_timezone = 'America/Denver' and game_timezone = 'America/Los_Angeles' then -1
            when prev_timezone = 'America/Los_Angeles' and game_timezone = 'America/New_York' then 3
            when prev_timezone = 'America/Los_Angeles' and game_timezone = 'America/Chicago' then 2
            when prev_timezone = 'America/Los_Angeles' and game_timezone = 'America/Denver' then 1
            else 0
        end as timezone_shift
    from with_timezones
)

select
    g.game_id,
    g.game_date,
    g.home_team_id,
    g.away_team_id,
    home_tz.timezone_shift as home_timezone_shift,
    away_tz.timezone_shift as away_timezone_shift,
    away_tz.timezone_shift as timezone_lag_advantage
from games g
left join with_tz_offset home_tz
    on g.game_id = home_tz.game_id
    and g.home_team_id = home_tz.team_id
left join with_tz_offset away_tz
    on g.game_id = away_tz.game_id
    and g.away_team_id = away_tz.team_id