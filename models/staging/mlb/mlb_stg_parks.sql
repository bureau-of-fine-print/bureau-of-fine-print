with source as (
    select * from {{ source('mlb_raw', 'parks') }}
),

renamed as (
    select
        venue_id,
        venue_name,
        team_id,
        team_abbr,
        city,
        state,
        latitude,
        longitude,
        elevation_ft,
        roof_type,
        lf_line,
        lf_center,
        center,
        rf_center,
        rf_line,
        lf_wall_height_ft,
        cf_wall_height_ft,
        rf_wall_height_ft,
        orientation_degrees,
        park_factor_runs,
        park_factor_hr,
        park_factor_season,
        is_coors,
        notes,
        valid_from_season,
        valid_to_season,
        -- derived
        case
            when roof_type = 'dome'         then true
            else false
        end as is_dome,
        case
            when roof_type = 'retractable'  then true
            else false
        end as has_retractable_roof,
        case
            when is_coors = true            then true
            when elevation_ft >= 3000       then true
            else false
        end as is_high_altitude,
        case
            when park_factor_runs >= 1.08   then 'hitter_friendly'
            when park_factor_runs <= 0.92   then 'pitcher_friendly'
            else 'neutral'
        end as park_type,
        case
            when park_factor_hr >= 1.10     then 'hr_friendly'
            when park_factor_hr <= 0.90     then 'hr_suppressing'
            else 'neutral'
        end as hr_park_type,
        inserted_at
    from source
)

select * from renamed