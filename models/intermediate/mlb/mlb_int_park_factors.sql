-- mlb_int_park_factors.sql
-- Clean park factor view for use in game context models.
-- Joins to schedule by venue_id and season.
-- One row per venue per valid season.

with parks as (
    select * from {{ ref('mlb_stg_parks') }}
),

final as (
    select
        venue_id,
        venue_name,
        team_id,
        team_abbr,
        city,
        state,
        elevation_ft,
        roof_type,
        is_dome,
        has_retractable_roof,
        is_high_altitude,
        is_coors,
        park_type,
        hr_park_type,
        park_factor_runs,
        park_factor_hr,
        park_factor_season,
        valid_from_season,
        valid_to_season,
        orientation_degrees,
        -- dimensions
        lf_line,
        lf_center,
        center,
        rf_center,
        rf_line,
        lf_wall_height_ft,
        cf_wall_height_ft,
        rf_wall_height_ft,
        -- totals adjustment multiplier
        -- used in totals model: projected_total * park_factor_runs
        -- capped at 1.20 to avoid extreme outliers distorting model
        least(park_factor_runs, 1.20)                   as capped_park_factor_runs,
        least(park_factor_hr, 1.20)                     as capped_park_factor_hr,
        -- narrative tag for content gen
        case
            when is_coors                               then 'Coors Field — extreme hitter park at altitude'
            when is_high_altitude                       then 'High altitude park — ball carries significantly'
            when park_type = 'hitter_friendly'
                 and hr_park_type = 'hr_friendly'       then 'Hitter-friendly park with elevated HR rates'
            when park_type = 'hitter_friendly'          then 'Hitter-friendly park'
            when park_type = 'pitcher_friendly'
                 and hr_park_type = 'hr_suppressing'    then 'Pitcher-friendly park — suppresses both runs and HR'
            when park_type = 'pitcher_friendly'         then 'Pitcher-friendly park'
            when hr_park_type = 'hr_friendly'           then 'Neutral run environment but elevated HR rates'
            when hr_park_type = 'hr_suppressing'        then 'Neutral run environment but suppresses HR'
            else 'Neutral park environment'
        end                                             as park_narrative
    from parks
)

select * from final