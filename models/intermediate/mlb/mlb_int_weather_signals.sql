-- mlb_int_weather_signals.sql
-- Pre-game weather signals for use in totals model and content gen.
-- One row per game — uses most recent weather scrape before first pitch.

with weather as (
    select * from {{ ref('mlb_stg_weather') }}
),

-- Take the most recent scrape per game
latest_weather as (
    select *
    from weather
    qualify row_number() over (
        partition by game_id
        order by scraped_at desc
    ) = 1
),

final as (
    select
        game_id,
        game_date,
        venue_id,
        scraped_at,
        temp_f,
        wind_speed_mph,
        wind_direction_label,
        wind_relative_to_park,
        precip_probability,
        condition,
        is_dome,
        is_outdoor,
        weather_category,
        wind_signal,
        temp_signal,

        -- totals impact score (-2 to +2)
        -- positive = favors over, negative = favors under
        case
            when not is_outdoor                         then 0
            when wind_signal = 'strong_out'             then 2
            when wind_signal = 'moderate_out'           then 1
            when wind_signal = 'strong_in'              then -2
            when wind_signal = 'moderate_in'            then -1
            when temp_signal = 'cold'                   then -1
            when temp_signal = 'hot'                    then 1
            else 0
        end                                             as wind_totals_impact,

        case
            when not is_outdoor                         then 0
            when temp_signal = 'cold'                   then -1
            when temp_signal = 'hot'                    then 1
            else 0
        end                                             as temp_totals_impact,

        case
            when not is_outdoor                         then 0
            when precip_probability >= 0.50             then -1
            else 0
        end                                             as rain_totals_impact,

        -- combined weather totals impact
        case
            when not is_outdoor                         then 0
            else (
                case wind_signal
                    when 'strong_out'   then 2
                    when 'moderate_out' then 1
                    when 'strong_in'    then -2
                    when 'moderate_in'  then -1
                    else 0
                end
                + case temp_signal
                    when 'cold' then -1
                    when 'hot'  then 1
                    else 0
                  end
                + case when precip_probability >= 0.50 then -1 else 0 end
            )
        end                                             as total_weather_impact,

        -- narrative for content gen
        case
            when not is_outdoor
                 then null
            when wind_signal = 'strong_out' and temp_signal = 'hot'
                 then concat(round(wind_speed_mph, 0), ' mph wind blowing out with hot temperatures — significant over lean')
            when wind_signal = 'strong_out'
                 then concat(round(wind_speed_mph, 0), ' mph wind blowing out to center — favors offense')
            when wind_signal = 'strong_in'
                 then concat(round(wind_speed_mph, 0), ' mph wind blowing in — favors pitchers')
            when wind_signal = 'moderate_out'
                 then concat(round(wind_speed_mph, 0), ' mph wind out — slight offensive lean')
            when wind_signal = 'moderate_in'
                 then concat(round(wind_speed_mph, 0), ' mph wind in — slight pitching lean')
            when temp_signal = 'cold'
                 then concat(temp_f, '°F at first pitch — cold weather suppresses offense')
            when precip_probability >= 0.50
                 then concat(round(precip_probability * 100, 0), '% chance of rain — rain delay risk')
            else null
        end                                             as weather_narrative

    from latest_weather
)

select * from final