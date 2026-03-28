-- mlb_int_pitcher_rolling.sql
-- Rolling stats for starting pitchers over last 5 starts
-- and relievers over last 7 days.
-- One row per pitcher per game representing their trailing stats
-- entering that game (excludes the current game).

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
),

starts_only as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_id,
        game_date,
        is_starter,
        ip_outs,
        h,
        r,
        er,
        bb,
        so,
        hr,
        pitches,
        throws,
        season
    from pitcher_logs
),

-- Rolling last 5 starts for starters
starter_rolling as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_id,
        game_date,
        throws,
        season,

        -- rolling window of last 5 starts (excluding current game)
        sum(ip_outs) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_ip_outs,

        sum(er) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_er,

        sum(h) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_h,

        sum(bb) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_bb,

        sum(so) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_so,

        sum(hr) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_hr,

        count(*) over (
            partition by player_id
            order by game_date
            rows between 5 preceding and 1 preceding
        ) as last5_games,

        -- season totals to date (excluding current game)
        sum(ip_outs) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_ip_outs,

        sum(er) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_er,

        sum(h) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_h,

        sum(bb) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_bb,

        sum(so) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_so,

        sum(hr) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_hr,

        count(*) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_games

    from starts_only
    where is_starter = true
),

final as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_id,
        game_date,
        throws,
        season,
        last5_games,
        last5_ip_outs,
        round(last5_ip_outs / 3.0, 1)                                          as last5_ip,

        -- last 5 starts ERA
        case when last5_ip_outs > 0
             then round(last5_er * 27.0 / last5_ip_outs, 2)
             else null
        end                                                                      as last5_era,

        -- last 5 starts WHIP
        case when last5_ip_outs > 0
             then round((last5_h + last5_bb) * 3.0 / last5_ip_outs, 2)
             else null
        end                                                                      as last5_whip,

        -- last 5 starts K/9
        case when last5_ip_outs > 0
             then round(last5_so * 27.0 / last5_ip_outs, 1)
             else null
        end                                                                      as last5_k_per_9,

        -- last 5 starts BB/9
        case when last5_ip_outs > 0
             then round(last5_bb * 27.0 / last5_ip_outs, 1)
             else null
        end                                                                      as last5_bb_per_9,

        -- last 5 starts HR/9
        case when last5_ip_outs > 0
             then round(last5_hr * 27.0 / last5_ip_outs, 1)
             else null
        end                                                                      as last5_hr_per_9,

        -- last 5 starts K/BB
        case when last5_bb > 0
             then round(last5_so / last5_bb, 2)
             else null
        end                                                                      as last5_k_bb_ratio,

        -- season ERA
        case when season_ip_outs > 0
             then round(season_er * 27.0 / season_ip_outs, 2)
             else null
        end                                                                      as season_era,

        -- season WHIP
        case when season_ip_outs > 0
             then round((season_h + season_bb) * 3.0 / season_ip_outs, 2)
             else null
        end                                                                      as season_whip,

        -- season K/9
        case when season_ip_outs > 0
             then round(season_so * 27.0 / season_ip_outs, 1)
             else null
        end                                                                      as season_k_per_9,

        -- season BB/9
        case when season_ip_outs > 0
             then round(season_bb * 27.0 / season_ip_outs, 1)
             else null
        end                                                                      as season_bb_per_9,

        -- season HR/9
        case when season_ip_outs > 0
             then round(season_hr * 27.0 / season_ip_outs, 1)
             else null
        end                                                                      as season_hr_per_9,

        -- season K/BB
        case when season_bb > 0
             then round(season_so / season_bb, 2)
             else null
        end                                                                      as season_k_bb_ratio,

        season_games,
        round(season_ip_outs / 3.0, 1)                                          as season_ip,

        -- trend flag: is last5 ERA better or worse than season ERA?
        case
            when season_ip_outs > 0 and last5_ip_outs > 0
                 and (last5_er * 27.0 / last5_ip_outs)
                     < (season_er * 27.0 / season_ip_outs) - 0.50
            then 'trending_better'
            when season_ip_outs > 0 and last5_ip_outs > 0
                 and (last5_er * 27.0 / last5_ip_outs)
                     > (season_er * 27.0 / season_ip_outs) + 0.50
            then 'trending_worse'
            else 'stable'
        end                                                                      as era_trend,

        -- sufficient sample flag
        case when last5_games >= 3 then true else false end                     as has_rolling_sample,
        case when season_games >= 5 then true else false end                    as has_season_sample

    from starter_rolling
)

select * from final