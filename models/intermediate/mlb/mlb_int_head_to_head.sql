-- mlb_int_head_to_head.sql
-- Team vs team historical results.
-- Current season + last 2 seasons for context.
-- One row per team pair per season.

with game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

-- Create both perspectives (home team and away team)
matchups as (
    select
        home_team_id                as team_id,
        away_team_id                as opponent_id,
        game_id,
        game_date,
        home_score                  as team_runs,
        away_score                  as opp_runs,
        case when home_score > away_score then 1 else 0 end as win,
        total_runs,
        extra_innings,
        extract(year from game_date) as season
    from game_results

    union all

    select
        away_team_id                as team_id,
        home_team_id                as opponent_id,
        game_id,
        game_date,
        away_score                  as team_runs,
        home_score                  as opp_runs,
        case when away_score > home_score then 1 else 0 end as win,
        total_runs,
        extra_innings,
        extract(year from game_date) as season
    from game_results
),

aggregated as (
    select
        team_id,
        opponent_id,
        season,
        count(*)                                        as games,
        sum(win)                                        as wins,
        round(avg(team_runs), 2)                        as avg_runs_scored,
        round(avg(opp_runs), 2)                         as avg_runs_allowed,
        round(avg(total_runs), 2)                       as avg_total_runs,
        countif(extra_innings)                          as extra_inning_games,
        -- last 5 matchups this season
        sum(case when row_number() over (
                partition by team_id, opponent_id, season
                order by game_date desc) <= 5
            then win else 0 end)                        as last5_wins,
        count(case when row_number() over (
                partition by team_id, opponent_id, season
                order by game_date desc) <= 5
            then 1 end)                                 as last5_games
    from matchups
    group by 1, 2, 3
),

final as (
    select
        team_id,
        opponent_id,
        season,
        games,
        wins,
        games - wins                                    as losses,
        round(wins / nullif(games, 0), 3)               as win_pct,
        avg_runs_scored,
        avg_runs_allowed,
        avg_total_runs,
        extra_inning_games,
        last5_wins,
        last5_games,
        round(last5_wins / nullif(last5_games, 0), 3)   as last5_win_pct,

        -- dominance flag
        case
            when games >= 5
                 and wins / nullif(games, 0) >= 0.65   then 'dominant'
            when games >= 5
                 and wins / nullif(games, 0) <= 0.35   then 'dominated'
            else 'even'
        end                                             as series_dominance,

        -- run scoring tendency in this matchup
        case
            when games >= 5
                 and avg_total_runs >= 11.0             then 'high_scoring'
            when games >= 5
                 and avg_total_runs <= 7.5              then 'low_scoring'
            else 'average'
        end                                             as run_tendency,

        case when games >= 5 then true else false end   as has_sample

    from aggregated
)

select * from final