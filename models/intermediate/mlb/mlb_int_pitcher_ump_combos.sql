-- mlb_int_pitcher_ump_combos.sql
-- Historical performance of starting pitchers with specific HP umps.
-- Small sample but directionally useful for content gen flavor.
-- One row per pitcher per ump (career aggregate).

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = true
),

ump_assignments as (
    select * from {{ ref('mlb_stg_ump_assignments') }}
    where is_home_plate = true
),

ump_tendencies as (
    select
        ump_id,
        ump_name,
        k_per_9             as ump_career_k_per_9,
        bb_per_9            as ump_career_bb_per_9,
        avg_runs_per_game   as ump_career_runs_per_game,
        zone_classification,
        run_environment,
        has_sufficient_sample as ump_has_sample
    from {{ ref('mlb_int_ump_tendencies') }}
),

combined as (
    select
        pl.player_id,
        pl.player_name,
        pl.team_id,
        pl.team_abbr,
        pl.throws,
        pl.game_id,
        pl.game_date,
        pl.ip_outs,
        pl.er,
        pl.h,
        pl.bb,
        pl.so,
        pl.hr,
        ua.ump_id,
        ua.ump_name
    from pitcher_logs pl
    inner join ump_assignments ua on pl.game_id = ua.game_id
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        ump_id,
        ump_name,
        count(distinct game_id)     as games_together,
        sum(ip_outs)                as ip_outs,
        sum(er)                     as er,
        sum(h)                      as h,
        sum(bb)                     as bb,
        sum(so)                     as so,
        sum(hr)                     as hr
    from combined
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        a.player_id,
        a.player_name,
        a.team_id,
        a.team_abbr,
        a.throws,
        a.ump_id,
        a.ump_name,
        a.games_together,
        round(a.ip_outs / 3.0, 1)                      as ip,

        case when a.ip_outs > 0
             then round(a.er * 27.0 / a.ip_outs, 2) else null
        end                                             as era_with_ump,

        case when a.ip_outs > 0
             then round((a.h + a.bb) * 3.0 / a.ip_outs, 2) else null
        end                                             as whip_with_ump,

        case when a.ip_outs > 0
             then round(a.so * 27.0 / a.ip_outs, 1) else null
        end                                             as k_per_9_with_ump,

        case when a.ip_outs > 0
             then round(a.bb * 27.0 / a.ip_outs, 1) else null
        end                                             as bb_per_9_with_ump,

        -- ump context
        ut.ump_career_k_per_9,
        ut.ump_career_bb_per_9,
        ut.ump_career_runs_per_game,
        ut.zone_classification,
        ut.run_environment,
        ut.ump_has_sample,

        -- sample flag — need at least 3 starts together for signal
        case when a.games_together >= 3 then true else false end as has_combo_sample,

        -- narrative flag for content gen
        case
            when a.games_together >= 3
                 and a.ip_outs > 0
                 and ut.zone_classification = 'large_zone'
                 and a.so * 27.0 / a.ip_outs >= 9.0
            then 'strikeout_pitcher_large_zone'
            when a.games_together >= 3
                 and a.ip_outs > 0
                 and ut.zone_classification = 'small_zone'
                 and a.bb * 27.0 / a.ip_outs >= 4.0
            then 'control_pitcher_small_zone_concern'
            else null
        end                                             as combo_narrative_flag

    from aggregated a
    left join ump_tendencies ut on a.ump_id = ut.ump_id
)

select * from final