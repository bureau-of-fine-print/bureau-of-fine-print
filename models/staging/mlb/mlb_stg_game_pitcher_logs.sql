with source as (
    select * from {{ source('mlb_raw', 'game_pitcher_logs') }}
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
        is_starter,
        pitching_order,
        inning_start,
        inning_end,
        ip_outs,
        h,
        r,
        er,
        bb,
        so,
        hr,
        hbp,
        pitches,
        strikes,
        win,
        loss,
        save,
        hold,
        blown_save,
        throws,
        -- derived
        round(ip_outs / 3.0, 1)                                    as ip,
        case when ip_outs > 0
             then round(er * 27.0 / ip_outs, 2)
             else null
        end                                                         as era,
        case when ip_outs > 0
             then round((h + bb) * 3.0 / ip_outs, 2)
             else null
        end                                                         as whip,
        case when ip_outs > 0
             then round(so * 27.0 / ip_outs, 1)
             else null
        end                                                         as k_per_9,
        case when ip_outs > 0
             then round(bb * 27.0 / ip_outs, 1)
             else null
        end                                                         as bb_per_9,
        case when ip_outs > 0
             then round(hr * 27.0 / ip_outs, 1)
             else null
        end                                                         as hr_per_9,
        case when bb > 0
             then round(so / bb, 2)
             else null
        end                                                         as k_bb_ratio,
        case when pitches > 0
             then round(strikes / pitches, 3)
             else null
        end                                                         as strike_pct,
        inning_end - inning_start + 1                              as innings_spanned,
        extract(year from game_date)                               as season,
        inserted_at
    from source
)

select * from renamed