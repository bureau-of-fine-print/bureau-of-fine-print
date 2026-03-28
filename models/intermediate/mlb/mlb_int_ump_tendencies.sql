-- mlb_int_ump_tendencies.sql
-- Historical tendencies for home plate umpires.
-- Covers K rate, BB rate, runs per game, and over/under rate.
-- One row per ump — career/multi-season aggregate.
-- Excludes current game (trailing stats only).

with umps as (
    select * from {{ ref('mlb_stg_ump_assignments') }}
    where is_home_plate = true
),

game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
),

-- Join ump to game results
ump_games as (
    select
        u.ump_id,
        u.ump_name,
        u.game_id,
        u.game_date,
        gr.total_runs,
        gr.home_score,
        gr.away_score,
        gr.innings_played
    from umps u
    inner join game_results gr on u.game_id = gr.game_id
),

-- Join ump games to pitcher logs to get K and BB
ump_pitcher_stats as (
    select
        ug.ump_id,
        ug.ump_name,
        ug.game_id,
        ug.game_date,
        ug.total_runs,
        ug.innings_played,
        sum(pl.so)  as game_so,
        sum(pl.bb)  as game_bb,
        sum(pl.ip_outs) as game_ip_outs
    from ump_games ug
    left join pitcher_logs pl on ug.game_id = pl.game_id
    group by 1, 2, 3, 4, 5, 6
),

final as (
    select
        ump_id,
        ump_name,
        count(*)                                                    as games_umpired,

        -- runs per game
        round(avg(total_runs), 2)                                   as avg_runs_per_game,
        round(avg(total_runs / nullif(innings_played, 0) * 9), 2)  as avg_runs_per_9,

        -- K rate (per 9 innings)
        case when sum(game_ip_outs) > 0
             then round(sum(game_so) * 27.0 / sum(game_ip_outs), 1)
             else null
        end                                                         as k_per_9,

        -- BB rate (per 9 innings)
        case when sum(game_ip_outs) > 0
             then round(sum(game_bb) * 27.0 / sum(game_ip_outs), 1)
             else null
        end                                                         as bb_per_9,

        -- K/BB ratio
        case when sum(game_bb) > 0
             then round(sum(game_so) / sum(game_bb), 2)
             else null
        end                                                         as k_bb_ratio,

        -- pct of games going over 9 total runs (rough over/under proxy)
        round(countif(total_runs >= 9) / count(*), 3)              as pct_high_scoring,
        round(countif(total_runs <= 6) / count(*), 3)              as pct_low_scoring,

        -- strike zone classification
        case
            when count(*) >= 20
                 and sum(game_so) * 27.0 / nullif(sum(game_ip_outs), 0) >= 8.8
            then 'large_zone'
            when count(*) >= 20
                 and sum(game_so) * 27.0 / nullif(sum(game_ip_outs), 0) <= 8.4
            then 'small_zone'
            else 'average_zone'
        end                                                         as zone_classification,

        -- run environment classification
        case
            when count(*) >= 20 and avg(total_runs) >= 9.3 then 'high_run'
            when count(*) >= 20 and avg(total_runs) <= 8.5  then 'low_run'
            else 'average_run'
        end                                                         as run_environment,

        case when count(*) >= 20 then true else false end          as has_sufficient_sample,
        max(game_date)                                              as last_game_date

    from ump_pitcher_stats
    group by 1, 2
)

select * from final