with source as (
    select * from {{ source('mlb_raw', 'game_batter_logs') }}
),

renamed as (
    select
        game_id,
        game_date,
        player_id,
        player_name,
        team_id,
        team_abbr,
        is_home,
        batting_order,
        is_starter,
        sub_inning,
        ab,
        r,
        h,
        doubles,
        triples,
        hr,
        rbi,
        bb,
        so,
        sb,
        cs,
        hbp,
        sac,
        left_on_base,
        ab + bb + hbp + sac as plate_appearances,
        h - doubles - triples - hr as singles,
        case when ab > 0 then round(h / ab, 3) else null end as avg,
        case when (ab + bb + hbp + sac) > 0
             then round((h + bb + hbp) / (ab + bb + hbp + sac), 3)
             else null end as obp,
        case when ab > 0
             then round((h - doubles - triples - hr + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null end as slg,
        case when ab > 0 and (ab + bb + hbp + sac) > 0
             then round(
                (h + bb + hbp) / (ab + bb + hbp + sac)
                + (h - doubles - triples - hr + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null end as ops,
        extract(year from game_date) as season,
        inserted_at
    from source
)

select * from renamed