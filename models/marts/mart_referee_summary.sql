with ref_tendencies as (
    select * from {{ ref('int_referee_tendencies') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

dim_referees as (
    select * from `project-71e6f4ed-bf24-4c0f-bb0.seeds.dim_referees`
)

select
    rt.referee_id,
    dr.full_name as referee_name,
    rt.games_officiated,
    rt.avg_total_points,
    rt.avg_total_fouls,
    rt.avg_total_fta,
    rt.avg_total_3pa,
    rt.avg_total_tov,
    rt.home_win_pct,
    rt.first_game,
    rt.last_game,

    -- Over/under implication
    -- League average total points for context
    round(rt.avg_total_points - avg(rt.avg_total_points) over (), 1) as pts_vs_league_avg,
    round(rt.avg_total_fouls - avg(rt.avg_total_fouls) over (), 1) as fouls_vs_league_avg,

    -- Pace implication
    round(rt.avg_total_fta - avg(rt.avg_total_fta) over (), 1) as fta_vs_league_avg,

    -- Home court bias
    case
        when rt.home_win_pct >= 0.65 then 'strong_home_bias'
        when rt.home_win_pct >= 0.58 then 'moderate_home_bias'
        when rt.home_win_pct <= 0.35 then 'strong_away_bias'
        when rt.home_win_pct <= 0.42 then 'moderate_away_bias'
        else 'neutral'
    end as home_bias_label,

    -- Foul tendency
    case
        when rt.avg_total_fouls >= avg(rt.avg_total_fouls) over () + 4 then 'whistle_happy'
        when rt.avg_total_fouls <= avg(rt.avg_total_fouls) over () - 4 then 'let_them_play'
        else 'average'
    end as foul_tendency_label

from ref_tendencies rt
left join dim_referees dr on rt.referee_id = CAST(dr.referee_id as string)