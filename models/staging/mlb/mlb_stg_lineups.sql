with source as (
    select * from {{ source('mlb_raw', 'lineups') }}
),

renamed as (
    select
        game_id,
        game_date,
        team_id,
        team_abbr,
        is_home,
        player_id,
        player_name,
        batting_order,
        position,
        is_starter,
        throws,
        bats,
        confirmed_at,
        -- derived
        case when position = 'SP' then true else false end as is_starting_pitcher,
        inserted_at
    from source
)

select * from renamed