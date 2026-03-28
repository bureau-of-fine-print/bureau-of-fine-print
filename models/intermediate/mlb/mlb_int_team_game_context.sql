-- mlb_int_team_game_context.sql
-- Per-team per-game context signals entering each game.
-- Covers rest days, travel, home/away streak, recent form.
-- One row per team per game.

with game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

schedule as (
    select
        game_id,
        game_date,
        home_team_id,
        home_team_abbr,
        away_team_id,
        away_team_abbr,
        venue_id,
        is_doubleheader,
        doubleheader_game_num
    from {{ ref('mlb_stg_schedule') }}
),

-- Unpivot to one row per team per game
team_games as (
    select
        s.game_id,
        s.game_date,
        s.home_team_id      as team_id,
        s.home_team_abbr    as team_abbr,
        s.away_team_id      as opponent_id,
        s.away_team_abbr    as opponent_abbr,
        s.venue_id,
        true                as is_home,
        s.is_doubleheader,
        s.doubleheader_game_num,
        gr.home_score       as runs_scored,
        gr.away_score       as runs_allowed,
        case when gr.home_score > gr.away_score then 1 else 0 end as win,
        extract(year from s.game_date) as season
    from schedule s
    left join game_results gr on s.game_id = gr.game_id

    union all

    select
        s.game_id,
        s.game_date,
        s.away_team_id      as team_id,
        s.away_team_abbr    as team_abbr,
        s.home_team_id      as opponent_id,
        s.home_team_abbr    as opponent_abbr,
        s.venue_id,
        false               as is_home,
        s.is_doubleheader,
        s.doubleheader_game_num,
        gr.away_score       as runs_scored,
        gr.home_score       as runs_allowed,
        case when gr.away_score > gr.home_score then 1 else 0 end as win,
        extract(year from s.game_date) as season
    from schedule s
    left join game_results gr on s.game_id = gr.game_id
),

with_context as (
    select
        game_id,
        game_date,
        team_id,
        team_abbr,
        opponent_id,
        opponent_abbr,
        venue_id,
        is_home,
        is_doubleheader,
        doubleheader_game_num,
        runs_scored,
        runs_allowed,
        win,
        season,

        -- days since last game
        date_diff(
            game_date,
            lag(game_date) over (
                partition by team_id
                order by game_date, game_id
            ),
            day
        )                                               as days_rest,

        -- previous game home/away
        lag(is_home) over (
            partition by team_id
            order by game_date, game_id
        )                                               as prev_game_home,

        -- win streak (positive = win streak, negative = loss streak)
        sum(case when win = 1 then 1 else -1 end) over (
            partition by team_id, season
            order by game_date, game_id
            rows between 9 preceding and 1 preceding
        )                                               as win_streak_signal,

        -- last 3 game results for narrative
        lag(win, 1) over (
            partition by team_id order by game_date, game_id
        )                                               as result_1g_ago,
        lag(win, 2) over (
            partition by team_id order by game_date, game_id
        )                                               as result_2g_ago,
        lag(win, 3) over (
            partition by team_id order by game_date, game_id
        )                                               as result_3g_ago,

        -- runs scored last 3 games
        lag(runs_scored, 1) over (
            partition by team_id order by game_date, game_id
        )                                               as runs_1g_ago,
        lag(runs_scored, 2) over (
            partition by team_id order by game_date, game_id
        )                                               as runs_2g_ago,
        lag(runs_scored, 3) over (
            partition by team_id order by game_date, game_id
        )                                               as runs_3g_ago

    from team_games
),

final as (
    select
        game_id,
        game_date,
        season,
        team_id,
        team_abbr,
        opponent_id,
        opponent_abbr,
        venue_id,
        is_home,
        is_doubleheader,
        doubleheader_game_num,
        days_rest,

        -- rest classification
        case
            when days_rest is null      then 'season_opener'
            when days_rest = 0          then 'doubleheader'
            when days_rest = 1          then 'back_to_back'
            when days_rest = 2          then 'standard_rest'
            when days_rest >= 3         then 'well_rested'
        end                                             as rest_classification,

        -- travel flag (was away last game, home this game or vice versa)
        case
            when prev_game_home is null                 then false
            when is_home = true and prev_game_home = false then true
            when is_home = false and prev_game_home = true then true
            else false
        end                                             as travel_day,

        -- back to back flag
        case when days_rest = 1 then true else false end as is_back_to_back,

        -- win streak
        win_streak_signal,
        case
            when win_streak_signal >= 5     then 'hot_streak'
            when win_streak_signal >= 2     then 'winning_streak'
            when win_streak_signal <= -5    then 'cold_streak'
            when win_streak_signal <= -2    then 'losing_streak'
            else 'even'
        end                                             as streak_classification,

        -- last 3 results summary
        result_1g_ago,
        result_2g_ago,
        result_3g_ago,
        coalesce(result_1g_ago, 0)
            + coalesce(result_2g_ago, 0)
            + coalesce(result_3g_ago, 0)               as wins_last_3,

        runs_1g_ago,
        runs_2g_ago,
        runs_3g_ago,

        -- doubleheader game 2 fatigue flag
        case
            when is_doubleheader = true
                 and doubleheader_game_num = 2          then true
            else false
        end                                             as is_dh_game_2

    from with_context
)

select * from final