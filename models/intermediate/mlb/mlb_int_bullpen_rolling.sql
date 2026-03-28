-- mlb_int_bullpen_rolling.sql
-- Bullpen fatigue and performance signals per team per game.
-- Tracks IP in last 1/2/3 days and ERA over last 7 days.
-- One row per team per game entering that game.

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = false  -- relievers only
),

game_dates as (
    select distinct
        team_id,
        game_date,
        game_id,
        extract(year from game_date) as season
    from {{ ref('mlb_stg_game_pitcher_logs') }}
),

-- Daily bullpen usage per team
daily_bullpen as (
    select
        team_id,
        game_date,
        sum(ip_outs)                                    as day_ip_outs,
        sum(er)                                         as day_er,
        sum(h)                                          as day_h,
        sum(bb)                                         as day_bb,
        sum(so)                                         as day_so,
        count(distinct player_id)                       as pitchers_used,
        -- flag if any reliever threw more than 1 inning (heavy usage)
        max(ip_outs)                                    as max_single_pitcher_outs,
        countif(ip_outs >= 6)                           as pitchers_over_2_innings
    from pitcher_logs
    group by 1, 2
),

-- Join back to game level and compute rolling windows
rolling as (
    select
        gd.game_id,
        gd.game_date,
        gd.team_id,
        gd.season,

        -- IP in last 1 day (yesterday)
        sum(case when db.game_date = date_sub(gd.game_date, interval 1 day)
                 then db.day_ip_outs else 0 end)        as ip_outs_1d,

        -- IP in last 2 days
        sum(case when db.game_date >= date_sub(gd.game_date, interval 2 day)
                 and db.game_date < gd.game_date
                 then db.day_ip_outs else 0 end)        as ip_outs_2d,

        -- IP in last 3 days
        sum(case when db.game_date >= date_sub(gd.game_date, interval 3 day)
                 and db.game_date < gd.game_date
                 then db.day_ip_outs else 0 end)        as ip_outs_3d,

        -- ER in last 7 days
        sum(case when db.game_date >= date_sub(gd.game_date, interval 7 day)
                 and db.game_date < gd.game_date
                 then db.day_er else 0 end)             as er_7d,

        -- IP in last 7 days
        sum(case when db.game_date >= date_sub(gd.game_date, interval 7 day)
                 and db.game_date < gd.game_date
                 then db.day_ip_outs else 0 end)        as ip_outs_7d,

        -- H + BB in last 7 days (for WHIP)
        sum(case when db.game_date >= date_sub(gd.game_date, interval 7 day)
                 and db.game_date < gd.game_date
                 then db.day_h + db.day_bb else 0 end)  as h_bb_7d,

        -- SO in last 7 days
        sum(case when db.game_date >= date_sub(gd.game_date, interval 7 day)
                 and db.game_date < gd.game_date
                 then db.day_so else 0 end)             as so_7d,

        -- heavy usage days in last 3 days
        countif(db.game_date >= date_sub(gd.game_date, interval 3 day)
                and db.game_date < gd.game_date
                and db.pitchers_over_2_innings > 0)     as heavy_usage_days_3d

    from game_dates gd
    left join daily_bullpen db
        on gd.team_id = db.team_id
        and db.game_date >= date_sub(gd.game_date, interval 7 day)
        and db.game_date < gd.game_date
    group by 1, 2, 3, 4
),

final as (
    select
        game_id,
        game_date,
        team_id,
        season,

        round(ip_outs_1d / 3.0, 1)                     as bp_ip_1d,
        round(ip_outs_2d / 3.0, 1)                     as bp_ip_2d,
        round(ip_outs_3d / 3.0, 1)                     as bp_ip_3d,
        round(ip_outs_7d / 3.0, 1)                     as bp_ip_7d,

        -- 7-day ERA
        case when ip_outs_7d > 0
             then round(er_7d * 27.0 / ip_outs_7d, 2)
             else null
        end                                             as bp_era_7d,

        -- 7-day WHIP
        case when ip_outs_7d > 0
             then round(h_bb_7d * 3.0 / ip_outs_7d, 2)
             else null
        end                                             as bp_whip_7d,

        -- 7-day K/9
        case when ip_outs_7d > 0
             then round(so_7d * 27.0 / ip_outs_7d, 1)
             else null
        end                                             as bp_k_per_9_7d,

        heavy_usage_days_3d,

        -- fatigue signal
        case
            when ip_outs_3d >= 18 then 'heavily_fatigued'  -- 6+ IP last 3 days
            when ip_outs_3d >= 12 then 'fatigued'          -- 4+ IP last 3 days
            when ip_outs_2d >= 9  then 'moderately_tired'  -- 3+ IP last 2 days
            when ip_outs_1d >= 6  then 'used_yesterday'    -- 2+ IP yesterday
            else 'fresh'
        end                                             as fatigue_signal,

        -- rested flag
        case when ip_outs_2d = 0 then true else false end as bullpen_rested

    from rolling
)

select * from final