with source as (
    select * from {{ source('mlb_raw', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        transaction_date,
        type_code,
        type_description,
        player_id,
        player_name,
        from_team_id,
        from_team_abbr,
        to_team_id,
        to_team_abbr,
        description,
        -- derived
        case
            when type_code in ('IL10', 'D10') then 'il_placement'
            when type_code in ('IL15', 'D15') then 'il_placement'
            when type_code in ('IL60', 'D60') then 'il_placement'
            when type_code = 'DTD'            then 'day_to_day'
            when type_code = 'RECALL'         then 'recall'
            when type_code = 'OPTION'         then 'option'
            when type_code = 'TRADE'          then 'trade'
            when type_code = 'RELEASE'        then 'release'
            when type_code = 'OUTRIGHTED'     then 'outright'
            when type_code = 'SELECTED'       then 'selected'
            else 'other'
        end as transaction_category,
        case
            when type_code in ('IL10','IL15','IL60','D10','D15','D60','DTD')
            then true else false
        end as is_injury_related,
        case
            when type_code = 'TRADE' then true else false
        end as is_trade,
        case
            when type_code in ('RECALL','SELECTED') then true else false
        end as is_callup,
        inserted_at
    from source
)

select * from renamed