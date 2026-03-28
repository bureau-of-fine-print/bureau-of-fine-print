with source as (
    select * from {{ source('mlb_raw', 'injuries') }}
),

renamed as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        il_type,
        injury_description,
        date_placed,
        expected_return_date,
        is_active,
        scraped_at,
        -- derived
        case
            when il_type = '60-day'   then 3
            when il_type = '15-day'   then 2
            when il_type = '10-day'   then 1
            when il_type = 'day-to-day' then 0
            else 1
        end as severity_rank,
        case
            when il_type = 'day-to-day' then true
            else false
        end as is_day_to_day,
        case
            when il_type in ('10-day', '15-day', '60-day') then true
            else false
        end as is_on_il,
        inserted_at
    from source
)

select * from renamed