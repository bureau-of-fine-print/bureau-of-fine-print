with player_rolling as (
    select * from {{ ref('int_player_rolling_averages') }}
),

with_hot_cold as (
    select
        player_name,
        team_id,
        game_id,
        game_date,
        points,
        rebounds,
        assists,
        pts_season_avg,
        reb_season_avg,
        ast_season_avg,
        pts_last3,
        reb_last3,
        ast_last3,
        pts_above_avg_last3,
        pts_above_avg_last5,

        case when pts_above_avg_last3 >= 5 then true else false end as is_pts_hot_last3,
        case when pts_above_avg_last5 >= 5 then true else false end as is_pts_hot_last5,
        case when pts_above_avg_last3 <= -5 then true else false end as is_pts_cold_last3,
        case when pts_above_avg_last5 <= -5 then true else false end as is_pts_cold_last5,

        case when points >= pts_season_avg + 5 then 1 else 0 end as hot_game,
        case when points < pts_season_avg then 1 else 0 end as cold_game

    from player_rolling
    where games_played >= 10
),

with_prev as (
    select
        *,
        lag(hot_game)  over (partition by player_name order by game_date) as prev_hot_game,
        lag(cold_game) over (partition by player_name order by game_date) as prev_cold_game
    from with_hot_cold
),

with_streak_groups as (
    select
        *,
        sum(case when hot_game  != prev_hot_game  then 1 else 0 end)
            over (partition by player_name order by game_date) as hot_streak_group,
        sum(case when cold_game != prev_cold_game then 1 else 0 end)
            over (partition by player_name order by game_date) as cold_streak_group
    from with_prev
),

with_streak_lengths as (
    select
        *,
        row_number() over (
            partition by player_name, hot_streak_group
            order by game_date
        ) as hot_streak_length,
        row_number() over (
            partition by player_name, cold_streak_group
            order by game_date
        ) as cold_streak_length
    from with_streak_groups
)

select
    player_name,
    team_id,
    game_id,
    game_date,
    points,
    rebounds,
    assists,
    pts_season_avg,
    pts_last3,
    pts_above_avg_last3,
    pts_above_avg_last5,
    is_pts_hot_last3,
    is_pts_hot_last5,
    is_pts_cold_last3,
    is_pts_cold_last5,
    case when hot_game  = 1 then hot_streak_length  else 0 end as hot_game_streak,
    case when cold_game = 1 then cold_streak_length else 0 end as below_avg_streak
from with_streak_lengths