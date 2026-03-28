-- mlb_int_pitcher_splits_park.sql
-- Starting pitcher performance at each ballpark.
-- Some pitchers have dramatic home/road splits or perform
-- very differently at specific parks (e.g. Coors, Oracle).
-- One row per pitcher per venue per season.

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = true
),

schedule as (
    select
        game_id,
        venue_id,
        venue_name
    from {{ ref('mlb_stg_schedule') }}
),

parks as (
    select
        venue_id,
        park_factor_runs,
        park_factor_hr,
        park_type,
        is_coors,
        is_high_altitude,
        valid_from_season,
        valid_to_season
    from {{ ref('mlb_int_park_factors') }}
),

pitcher_with_venue as (
    select
        pl.player_id,
        pl.player_name,
        pl.team_id,
        pl.team_abbr,
        pl.throws,
        pl.game_id,
        pl.game_date,
        pl.season,
        pl.ip_outs,
        pl.h,
        pl.er,
        pl.bb,
        pl.so,
        pl.hr,
        pl.pitches,
        s.venue_id,
        s.venue_name,
        -- is this the pitcher's home park?
        case when pl.is_home then true else false end    as is_home_start
    from pitcher_logs pl
    inner join schedule s on pl.game_id = s.game_id
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        venue_id,
        venue_name,
        season,
        is_home_start,
        count(distinct game_id)         as games_started,
        sum(ip_outs)                    as ip_outs,
        sum(h)                          as h,
        sum(er)                         as er,
        sum(bb)                         as bb,
        sum(so)                         as so,
        sum(hr)                         as hr
    from pitcher_with_venue
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

final as (
    select
        a.player_id,
        a.player_name,
        a.team_id,
        a.team_abbr,
        a.throws,
        a.venue_id,
        a.venue_name,
        a.season,
        a.is_home_start,
        a.games_started,
        round(a.ip_outs / 3.0, 1)                      as ip,
        a.so,
        a.bb,
        a.hr,

        case when a.ip_outs > 0
             then round(a.er * 27.0 / a.ip_outs, 2) else null
        end                                             as era,

        case when a.ip_outs > 0
             then round((a.h + a.bb) * 3.0 / a.ip_outs, 2) else null
        end                                             as whip,

        case when a.ip_outs > 0
             then round(a.so * 27.0 / a.ip_outs, 1) else null
        end                                             as k_per_9,

        case when a.ip_outs > 0
             then round(a.bb * 27.0 / a.ip_outs, 1) else null
        end                                             as bb_per_9,

        case when a.ip_outs > 0
             then round(a.hr * 27.0 / a.ip_outs, 1) else null
        end                                             as hr_per_9,

        case when a.bb > 0
             then round(a.so / a.bb, 2) else null
        end                                             as k_bb_ratio,

        -- park context
        p.park_factor_runs,
        p.park_factor_hr,
        p.park_type,
        p.is_coors,
        p.is_high_altitude,

        case when a.games_started >= 3 then true else false end as has_sample

    from aggregated a
    left join parks p
        on a.venue_id = p.venue_id
        and p.valid_from_season <= a.season
        and (p.valid_to_season is null or p.valid_to_season >= a.season)
)

select * from final