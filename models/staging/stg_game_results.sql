with source as (
    select * from {{ source('raw', 'game_results') }}
),

renamed as (
    select
        game_id,
        home_score,
        away_score,
        overtime_flag,
        overtime_periods,
        loaded_at
    from source
)

select * from renamed