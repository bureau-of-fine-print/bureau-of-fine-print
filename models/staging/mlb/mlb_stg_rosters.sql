with source as (
    select * from {{ source('mlb_raw', 'rosters') }}
),

renamed as (
    select
        snapshot_date,
        team_id,
        team_abbr,
        player_id,
        player_name,
        position,
        bats,
        throws,
        jersey_number,
        -- derived
        case
            when position = 'SP' then 'starter'
            when position = 'RP' then 'reliever'
            when position in ('C','1B','2B','3B','SS',
                              'LF','CF','RF','OF','DH') then 'position_player'
            else 'other'
        end as roster_role,
        case
            when position in ('SP','RP') then true else false
        end as is_pitcher,
        inserted_at
    from source
)

select * from renamed