with source as (
    select * from {{ source('mlb_raw', 'game_batter_logs') }}
),

renamed as (
    select
        game_id,
        game_date,
        team_id,
        team_abbr,
        is_home,
        inning,
        runs,
        hits,
        errors,
        -- derived
        case when inning <= 3  then 'early'
             when inning <= 6  then 'middle'
             when inning <= 9  then 'late'
             else 'extra'
        end as inning_group,
        case when inning >= 7 then true else false end as is_late_inning,
        case when inning > 9  then true else false end as is_extra_inning,
        inserted_at
    from source
)

select * from renamed