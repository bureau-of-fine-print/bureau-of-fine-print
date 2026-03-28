-- mlb_int_starter_length.sql
-- Average innings per start for each starting pitcher.
-- Used to estimate bullpen exposure in game context.
-- One row per pitcher per season.

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = true
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        season,
        count(distinct game_id)                         as starts,
        sum(ip_outs)                                    as total_ip_outs,
        avg(ip_outs)                                    as avg_ip_outs_per_start,
        min(ip_outs)                                    as min_ip_outs,
        max(ip_outs)                                    as max_ip_outs,
        -- short outings (less than 15 outs = less than 5 IP)
        countif(ip_outs < 15)                           as short_outings,
        -- quality starts (18+ outs = 6+ IP)
        countif(ip_outs >= 18)                          as quality_starts,
        -- deep into game (21+ outs = 7+ IP)
        countif(ip_outs >= 21)                          as deep_outings
    from pitcher_logs
    group by 1, 2, 3, 4, 5, 6
),

-- Last 5 starts for recent trend
recent_starts as (
    select
        player_id,
        season,
        avg(ip_outs) over (
            partition by player_id, season
            order by game_date
            rows between 5 preceding and 1 preceding
        )                                               as last5_avg_ip_outs
    from pitcher_logs
    qualify row_number() over (
        partition by player_id, season
        order by game_date desc
    ) = 1
),

final as (
    select
        a.player_id,
        a.player_name,
        a.team_id,
        a.team_abbr,
        a.throws,
        a.season,
        a.starts,
        round(a.total_ip_outs / 3.0, 1)                as total_ip,
        round(a.avg_ip_outs_per_start / 3.0, 1)        as avg_ip_per_start,
        round(r.last5_avg_ip_outs / 3.0, 1)            as last5_avg_ip_per_start,
        round(a.min_ip_outs / 3.0, 1)                  as min_ip,
        round(a.max_ip_outs / 3.0, 1)                  as max_ip,
        a.short_outings,
        a.quality_starts,
        a.deep_outings,
        round(a.quality_starts / nullif(a.starts, 0), 3) as quality_start_rate,
        round(a.short_outings / nullif(a.starts, 0), 3)  as short_outing_rate,

        -- bullpen exposure estimate (innings SP typically doesn't cover)
        round(9.0 - a.avg_ip_outs_per_start / 3.0, 1)  as avg_bullpen_innings_needed,

        -- starter durability classification
        case
            when a.starts >= 5
                 and a.avg_ip_outs_per_start >= 18      then 'workhorse'
            when a.starts >= 5
                 and a.avg_ip_outs_per_start >= 15      then 'solid_starter'
            when a.starts >= 5
                 and a.avg_ip_outs_per_start < 15       then 'short_starter'
            else 'insufficient_sample'
        end                                             as durability_classification,

        case when a.starts >= 5 then true else false end as has_sample

    from aggregated a
    left join recent_starts r
        on a.player_id = r.player_id
        and a.season = r.season
)

select * from final