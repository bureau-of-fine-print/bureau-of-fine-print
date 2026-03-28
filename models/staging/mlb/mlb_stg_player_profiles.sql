with source as (
    select * from {{ source('mlb_raw', 'player_profiles') }}
),

renamed as (
    select
        player_id,
        player_name,
        bats,
        throws,
        primary_position,
        -- derived
        case
            when primary_position in ('SP','RP') then 'pitcher'
            when primary_position in ('C','1B','2B','3B','SS',
                                      'LF','CF','RF','OF','DH') then 'position_player'
            else 'other'
        end as player_type,
        case when throws = 'L' then true else false end as is_lefty_pitcher,
        case when bats   = 'L' then true else false end as is_lefty_batter,
        case when bats   = 'S' then true else false end as is_switch_hitter,
        inserted_at,
        updated_at
    from source
)

select * from renamed