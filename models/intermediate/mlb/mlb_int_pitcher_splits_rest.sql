-- mlb_int_pitcher_splits_rest.sql
-- Starting pitcher performance by days of rest.
-- Short rest (< 4 days) is a meaningful negative signal.
-- Extra rest (> 5 days) can go either way.
-- One row per pitcher start with rest days calculated.

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = true
),

with_rest as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        game_id,
        game_date,
        season,
        ip_outs,
        er,
        h,
        bb,
        so,
        hr,

        -- days since last start (null for first start of season)
        date_diff(
            game_date,
            lag(game_date) over (
                partition by player_id, season
                order by game_date
            ),
            day
        )                                               as days_rest,

        -- previous start stats for context
        lag(ip_outs) over (
            partition by player_id, season
            order by game_date
        )                                               as prev_start_ip_outs,

        lag(er) over (
            partition by player_id, season
            order by game_date
        )                                               as prev_start_er

    from pitcher_logs
),

final as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        game_id,
        game_date,
        season,
        days_rest,
        round(ip_outs / 3.0, 1)                        as ip,
        ip_outs,
        er,
        so,
        bb,
        hr,

        -- rest classification
        case
            when days_rest is null          then 'first_start'
            when days_rest <= 3             then 'short_rest'
            when days_rest <= 5             then 'standard_rest'
            when days_rest <= 8             then 'extra_rest'
            else 'long_layoff'
        end                                             as rest_classification,

        -- flags
        case when days_rest is not null
                  and days_rest <= 3        then true else false
        end                                             as is_short_rest,

        case when days_rest is not null
                  and days_rest >= 6        then true else false
        end                                             as is_extra_rest,

        -- derived stats for this start
        case when ip_outs > 0
             then round(er * 27.0 / ip_outs, 2) else null
        end                                             as start_era,

        case when ip_outs > 0
             then round((h + bb) * 3.0 / ip_outs, 2) else null
        end                                             as start_whip,

        case when ip_outs > 0
             then round(so * 27.0 / ip_outs, 1) else null
        end                                             as start_k_per_9,

        prev_start_ip_outs,
        prev_start_er

    from with_rest
)

select * from final