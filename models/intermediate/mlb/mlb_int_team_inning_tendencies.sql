-- mlb_int_team_inning_tendencies.sql
-- Team run-scoring patterns by inning.
-- First inning scoring rate and late inning (7-9) scoring rate.
-- One row per team per season.

with inning_scores as (
    select * from {{ ref('mlb_stg_inning_scores') }}
),

game_results as (
    select
        game_id,
        game_date,
        innings_played,
        extract(year from game_date) as season
    from {{ ref('mlb_stg_game_results') }}
),

-- Join inning scores to game results for game count
inning_with_games as (
    select
        ins.game_id,
        ins.game_date,
        ins.team_id,
        ins.team_abbr,
        ins.inning,
        ins.runs,
        ins.is_home,
        gr.innings_played,
        gr.season
    from inning_scores ins
    inner join game_results gr on ins.game_id = gr.game_id
),

aggregated as (
    select
        team_id,
        team_abbr,
        season,
        count(distinct game_id)                         as total_games,

        -- first inning
        countif(inning = 1 and runs > 0)               as games_scored_1st,
        sum(case when inning = 1 then runs else 0 end) as total_runs_1st,
        count(case when inning = 1 then 1 end)         as innings_1st,

        -- early innings (1-3)
        sum(case when inning <= 3 then runs else 0 end) as total_runs_early,

        -- middle innings (4-6)
        sum(case when inning between 4 and 6
                 then runs else 0 end)                  as total_runs_middle,

        -- late innings (7-9)
        sum(case when inning between 7 and 9
                 then runs else 0 end)                  as total_runs_late,
        countif(inning between 7 and 9 and runs > 0)   as late_innings_scored,
        count(case when inning between 7 and 9
                   then 1 end)                          as late_innings_played,

        -- extra innings
        sum(case when inning > 9 then runs else 0 end)  as total_runs_extra

    from inning_with_games
    group by 1, 2, 3
),

final as (
    select
        team_id,
        team_abbr,
        season,
        total_games,

        -- first inning scoring rate
        round(games_scored_1st / nullif(total_games, 0), 3) as first_inning_score_rate,
        round(total_runs_1st / nullif(innings_1st, 0), 3)   as avg_runs_1st_inning,

        -- runs per inning by period
        round(total_runs_early / nullif(total_games * 3, 0), 3) as avg_runs_per_inning_early,
        round(total_runs_middle / nullif(total_games * 3, 0), 3) as avg_runs_per_inning_middle,
        round(total_runs_late / nullif(total_games * 3, 0), 3)  as avg_runs_per_inning_late,

        -- late inning scoring rate
        round(late_innings_scored / nullif(late_innings_played, 0), 3) as late_inning_score_rate,

        -- classification
        case
            when total_games >= 20
                 and games_scored_1st / nullif(total_games, 0) >= 0.45
            then 'scores_early'
            else null
        end                                                  as first_inning_tendency,

        case
            when total_games >= 20
                 and late_innings_scored / nullif(late_innings_played, 0) >= 0.35
            then 'strong_late'
            when total_games >= 20
                 and late_innings_scored / nullif(late_innings_played, 0) <= 0.20
            then 'weak_late'
            else 'average_late'
        end                                                  as late_inning_tendency,

        case when total_games >= 20 then true else false end as has_sample

    from aggregated
)

select * from final