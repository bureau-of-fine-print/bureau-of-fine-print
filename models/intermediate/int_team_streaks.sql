with team_record as (
    select * from {{ ref('int_team_record') }}
),

with_prev_win as (
    select
        team_id,
        game_id,
        game_date,
        win,
        lag(win) over (partition by team_id order by game_date) as prev_win
    from team_record
),

with_streak_group as (
    select
        team_id,
        game_id,
        game_date,
        win,
        sum(case
            when win != prev_win then 1 else 0
        end) over (partition by team_id order by game_date) as streak_group
    from with_prev_win
),

with_streak_length as (
    select
        team_id,
        game_id,
        game_date,
        win,
        streak_group,
        row_number() over (
            partition by team_id, streak_group
            order by game_date
        ) as streak_length
    from with_streak_group
)

select
    team_id,
    game_id,
    game_date,
    win,
    streak_length,
    case when win = 1 then streak_length else 0 end as win_streak,
    case when win = 0 then streak_length else 0 end as loss_streak,
    case
        when win = 1 then concat('W', cast(streak_length as string))
        else concat('L', cast(streak_length as string))
    end as streak_label
from with_streak_length