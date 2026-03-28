-- mlb_int_team_rolling.sql
-- Rolling team offensive and pitching stats over last 10 games.
-- One row per team per game representing trailing stats
-- entering that game (excludes current game).

with game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

-- Unpivot to get one row per team per game
team_games as (
    select
        game_id,
        game_date,
        home_team_id    as team_id,
        home_score      as runs_scored,
        away_score      as runs_allowed,
        case when home_score > away_score then 1 else 0 end as win,
        innings_played,
        extra_innings,
        had_rain_delay,
        extract(year from game_date) as season
    from game_results

    union all

    select
        game_id,
        game_date,
        away_team_id    as team_id,
        away_score      as runs_scored,
        home_score      as runs_allowed,
        case when away_score > home_score then 1 else 0 end as win,
        innings_played,
        extra_innings,
        had_rain_delay,
        extract(year from game_date) as season
    from game_results
),

-- Join batter logs for team OPS
team_batting as (
    select
        game_id,
        team_id,
        sum(ab)                                             as team_ab,
        sum(h)                                             as team_h,
        sum(bb)                                            as team_bb,
        sum(hbp)                                           as team_hbp,
        sum(sac)                                           as team_sac,
        sum(hr)                                            as team_hr,
        sum(so)                                            as team_so,
        sum(doubles)                                       as team_doubles,
        sum(triples)                                       as team_triples,
        sum(plate_appearances)                             as team_pa
    from {{ ref('mlb_stg_game_batter_logs') }}
    where is_starter = true
    group by 1, 2
),

combined as (
    select
        tg.game_id,
        tg.game_date,
        tg.team_id,
        tg.runs_scored,
        tg.runs_allowed,
        tg.win,
        tg.innings_played,
        tg.season,
        coalesce(tb.team_ab, 0)       as team_ab,
        coalesce(tb.team_h, 0)        as team_h,
        coalesce(tb.team_bb, 0)       as team_bb,
        coalesce(tb.team_hbp, 0)      as team_hbp,
        coalesce(tb.team_sac, 0)      as team_sac,
        coalesce(tb.team_hr, 0)       as team_hr,
        coalesce(tb.team_so, 0)       as team_so,
        coalesce(tb.team_doubles, 0)  as team_doubles,
        coalesce(tb.team_triples, 0)  as team_triples,
        coalesce(tb.team_pa, 0)       as team_pa
    from team_games tg
    left join team_batting tb
        on tg.game_id = tb.game_id
        and tg.team_id = tb.team_id
),

rolling as (
    select
        game_id,
        game_date,
        team_id,
        season,

        -- last 10 games offense
        avg(runs_scored) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_runs_scored_avg,

        avg(runs_allowed) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_runs_allowed_avg,

        sum(win) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_wins,

        count(*) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_games,

        sum(team_hr) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_hr,

        sum(team_so) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_so,

        sum(team_bb) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_bb,

        sum(team_ab) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_ab,

        sum(team_h) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_h,

        sum(team_hbp) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_hbp,

        sum(team_sac) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_sac,

        sum(team_doubles) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_doubles,

        sum(team_triples) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_triples,

        sum(team_pa) over (
            partition by team_id
            order by game_date
            rows between 10 preceding and 1 preceding
        )                                                   as last10_pa,

        -- season totals
        sum(runs_scored) over (
            partition by team_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        )                                                   as season_runs_scored,

        sum(runs_allowed) over (
            partition by team_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        )                                                   as season_runs_allowed,

        sum(win) over (
            partition by team_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        )                                                   as season_wins,

        count(*) over (
            partition by team_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        )                                                   as season_games

    from combined
),

final as (
    select
        game_id,
        game_date,
        team_id,
        season,
        last10_games,
        last10_wins,
        last10_runs_scored_avg,
        last10_runs_allowed_avg,
        last10_hr,
        last10_so,
        last10_bb,

        -- last 10 team OPS
        case when last10_ab > 0 and last10_pa > 0
             then round(
                (last10_h + last10_bb + last10_hbp) / last10_pa
                + (last10_h - last10_doubles - last10_triples - last10_hr
                   + 2*last10_doubles + 3*last10_triples + 4*last10_hr) / last10_ab,
                3) else null
        end                                                 as last10_ops,

        -- last 10 team AVG
        case when last10_ab > 0
             then round(last10_h / last10_ab, 3) else null
        end                                                 as last10_avg,

        -- season stats
        season_games,
        season_wins,
        case when season_games > 0
             then round(season_wins / season_games, 3) else null
        end                                                 as season_win_pct,
        case when season_games > 0
             then round(season_runs_scored / season_games, 2) else null
        end                                                 as season_runs_scored_avg,
        case when season_games > 0
             then round(season_runs_allowed / season_games, 2) else null
        end                                                 as season_runs_allowed_avg,

        -- hot/cold streak
        case
            when last10_games >= 7 and last10_wins >= 7 then 'hot'
            when last10_games >= 7 and last10_wins <= 3 then 'cold'
            else 'neutral'
        end                                                 as team_streak_flag,

        case when last10_games >= 7 then true else false end as has_rolling_sample

    from rolling
)

select * from final