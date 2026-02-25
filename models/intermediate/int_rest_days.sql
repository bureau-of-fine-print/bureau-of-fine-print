with games as (
    select * from {{ ref('int_game_scores') }}
),

home_rest as (
    select
        game_id,
        game_date,
        home_team_id as team_id,
        date_diff(
            game_date,
            lag(game_date) over (partition by home_team_id order by game_date),
            day
        ) - 1 as days_rest
    from games
),

away_rest as (
    select
        game_id,
        game_date,
        away_team_id as team_id,
        date_diff(
            game_date,
            lag(game_date) over (partition by away_team_id order by game_date),
            day
        ) - 1 as days_rest
    from games
),

combined as (
    select * from home_rest
    union all
    select * from away_rest
)

select
    g.game_id,
    g.game_date,
    g.home_team_id,
    g.away_team_id,
    home_r.days_rest as home_days_rest,
    away_r.days_rest as away_days_rest,
    away_r.days_rest - home_r.days_rest as rest_advantage
from games g
left join combined home_r
    on g.game_id = home_r.game_id
    and g.home_team_id = home_r.team_id
left join combined away_r
    on g.game_id = away_r.game_id
    and g.away_team_id = away_r.team_id