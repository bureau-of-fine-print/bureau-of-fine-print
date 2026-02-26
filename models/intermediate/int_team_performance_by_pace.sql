with team_record as (
    select * from {{ ref('int_team_record') }}
),

game_pace as (
    select * from {{ ref('int_game_pace') }}
),

games as (
    select game_id, season from {{ ref('stg_games') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

ref_tendencies as (
    select * from {{ ref('int_referee_tendencies') }}
),

game_ref_pace as (
    select
        ra.game_id,
        round(avg(rt.avg_total_points), 1) as crew_avg_total_points,
        round(avg(rt.avg_total_fta), 1) as crew_avg_fta,
        round(avg(rt.avg_total_fouls), 1) as crew_avg_fouls
    from referee_assignments ra
    left join ref_tendencies rt
        on rt.referee_id in (ra.referee_1, ra.referee_2, ra.referee_3)
    group by ra.game_id
),

base as (
    select
        tr.team_id,
        tr.game_id,
        tr.game_date,
        tr.win,
        tr.home_away,
        tr.team_points,
        tr.opp_points,
        gp.game_pace,
        gp.overtime_periods,
        g.season,
        case
            when gp.game_pace >= 106 then 'fast'
            when gp.game_pace >= 100 then 'average'
            else 'slow'
        end as pace_bucket,
        grp.crew_avg_total_points,
        grp.crew_avg_fta,
        grp.crew_avg_fouls,
        row_number() over (partition by tr.team_id order by tr.game_date desc) as game_recency
    from team_record tr
    inner join game_pace gp on tr.game_id = gp.game_id
    inner join games g on tr.game_id = g.game_id
    left join game_ref_pace grp on tr.game_id = grp.game_id
)

select
    *,
    -- Flags for filtering in downstream queries
    case when season = (select max(season) from base) then true else false end as is_current_season,
    case when game_recency <= 10 then true else false end as is_last_10
from base