with source as (
    select * from {{ source('mlb_raw', 'weather') }}
),

renamed as (
    select
        game_id,
        game_date,
        venue_id,
        scraped_at,
        forecast_time_utc,
        temp_f,
        feels_like_f,
        wind_speed_mph,
        wind_direction_degrees,
        wind_direction_label,
        wind_relative_to_park,
        precip_probability,
        condition,
        is_dome,
        -- derived
        case
            when is_dome = true                         then 'dome'
            when condition = 'roof closed'              then 'dome'
            when precip_probability >= 0.5              then 'rain_likely'
            when precip_probability >= 0.25             then 'rain_possible'
            else 'clear'
        end as weather_category,
        case
            when is_dome = true                         then false
            when condition = 'roof closed'              then false
            else true
        end as is_outdoor,
        -- wind signal for totals model
        case
            when is_dome = true                         then 'neutral'
            when condition = 'roof closed'              then 'neutral'
            when wind_speed_mph >= 15
                 and wind_relative_to_park = 'out'      then 'strong_out'
            when wind_speed_mph >= 10
                 and wind_relative_to_park = 'out'      then 'moderate_out'
            when wind_speed_mph >= 15
                 and wind_relative_to_park = 'in'       then 'strong_in'
            when wind_speed_mph >= 10
                 and wind_relative_to_park = 'in'       then 'moderate_in'
            when wind_speed_mph >= 10                   then 'crosswind'
            else 'calm'
        end as wind_signal,
        -- temp signal
        case
            when is_dome = true                         then 'neutral'
            when condition = 'roof closed'              then 'neutral'
            when temp_f < 45                            then 'cold'
            when temp_f < 55                            then 'cool'
            when temp_f >= 85                           then 'hot'
            else 'neutral'
        end as temp_signal,
        inserted_at
    from source
)

select * from renamed