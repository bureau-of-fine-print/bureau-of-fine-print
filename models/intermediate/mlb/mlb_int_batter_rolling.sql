-- mlb_int_batter_rolling.sql
-- Rolling batting stats over last 7 and last 15 games.
-- One row per batter per game representing trailing stats
-- entering that game (excludes the current game).
-- Starters only — excludes pinch hit appearances for rolling window
-- but includes all PA for season totals.

with batter_logs as (
    select * from {{ ref('mlb_stg_game_batter_logs') }}
),

-- Use all appearances for season totals, starters for rolling
rolling as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_id,
        game_date,
        is_starter,
        season,
        ab,
        h,
        doubles,
        triples,
        hr,
        rbi,
        bb,
        so,
        hbp,
        sac,
        sb,
        cs,
        plate_appearances,
        singles,

        -- last 7 games (excluding current)
        sum(ab) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_ab,
        sum(h) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_h,
        sum(doubles) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_doubles,
        sum(triples) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_triples,
        sum(hr) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_hr,
        sum(bb) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_bb,
        sum(so) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_so,
        sum(hbp) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_hbp,
        sum(sac) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_sac,
        sum(plate_appearances) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_pa,
        count(*) over (
            partition by player_id
            order by game_date
            rows between 7 preceding and 1 preceding
        ) as last7_games,

        -- last 15 games (excluding current)
        sum(ab) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_ab,
        sum(h) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_h,
        sum(doubles) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_doubles,
        sum(triples) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_triples,
        sum(hr) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_hr,
        sum(bb) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_bb,
        sum(so) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_so,
        sum(hbp) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_hbp,
        sum(sac) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_sac,
        sum(plate_appearances) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_pa,
        count(*) over (
            partition by player_id
            order by game_date
            rows between 15 preceding and 1 preceding
        ) as last15_games,

        -- season totals (excluding current game)
        sum(ab) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_ab,
        sum(h) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_h,
        sum(doubles) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_doubles,
        sum(triples) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_triples,
        sum(hr) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_hr,
        sum(bb) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_bb,
        sum(so) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_so,
        sum(hbp) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_hbp,
        sum(sac) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_sac,
        sum(plate_appearances) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_pa,
        count(*) over (
            partition by player_id, season
            order by game_date
            rows between unbounded preceding and 1 preceding
        ) as season_games

    from batter_logs
),

final as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_id,
        game_date,
        is_starter,
        season,
        last7_games,
        last7_pa,
        last15_games,
        last15_pa,
        season_games,
        season_pa,

        -- last 7 rate stats
        case when last7_ab > 0
             then round(last7_h / last7_ab, 3) else null
        end                                                             as last7_avg,
        case when last7_pa > 0
             then round((last7_h + last7_bb + last7_hbp) / last7_pa, 3) else null
        end                                                             as last7_obp,
        case when last7_ab > 0
             then round(
                (last7_h - last7_doubles - last7_triples - last7_hr
                 + 2*last7_doubles + 3*last7_triples + 4*last7_hr)
                / last7_ab, 3) else null
        end                                                             as last7_slg,
        case when last7_ab > 0 and last7_pa > 0
             then round(
                (last7_h + last7_bb + last7_hbp) / last7_pa
                + (last7_h - last7_doubles - last7_triples - last7_hr
                   + 2*last7_doubles + 3*last7_triples + 4*last7_hr) / last7_ab,
                3) else null
        end                                                             as last7_ops,
        last7_hr,
        last7_so,
        last7_bb,

        -- last 15 rate stats
        case when last15_ab > 0
             then round(last15_h / last15_ab, 3) else null
        end                                                             as last15_avg,
        case when last15_pa > 0
             then round((last15_h + last15_bb + last15_hbp) / last15_pa, 3) else null
        end                                                             as last15_obp,
        case when last15_ab > 0
             then round(
                (last15_h - last15_doubles - last15_triples - last15_hr
                 + 2*last15_doubles + 3*last15_triples + 4*last15_hr)
                / last15_ab, 3) else null
        end                                                             as last15_slg,
        case when last15_ab > 0 and last15_pa > 0
             then round(
                (last15_h + last15_bb + last15_hbp) / last15_pa
                + (last15_h - last15_doubles - last15_triples - last15_hr
                   + 2*last15_doubles + 3*last15_triples + 4*last15_hr) / last15_ab,
                3) else null
        end                                                             as last15_ops,
        last15_hr,
        last15_so,
        last15_bb,

        -- season rate stats
        case when season_ab > 0
             then round(season_h / season_ab, 3) else null
        end                                                             as season_avg,
        case when season_pa > 0
             then round((season_h + season_bb + season_hbp) / season_pa, 3) else null
        end                                                             as season_obp,
        case when season_ab > 0
             then round(
                (season_h - season_doubles - season_triples - season_hr
                 + 2*season_doubles + 3*season_triples + 4*season_hr)
                / season_ab, 3) else null
        end                                                             as season_slg,
        case when season_ab > 0 and season_pa > 0
             then round(
                (season_h + season_bb + season_hbp) / season_pa
                + (season_h - season_doubles - season_triples - season_hr
                   + 2*season_doubles + 3*season_triples + 4*season_hr) / season_ab,
                3) else null
        end                                                             as season_ops,
        season_hr,
        season_so,
        season_bb,
        season_ab,
        season_h,

        -- hot/cold flag based on last7 vs season avg
        case
            when season_ab >= 50 and last7_ab >= 10
                 and (last7_h / last7_ab) >= (season_h / season_ab) + 0.050
            then 'hot'
            when season_ab >= 50 and last7_ab >= 10
                 and (last7_h / last7_ab) <= (season_h / season_ab) - 0.050
            then 'cold'
            else 'neutral'
        end                                                             as hot_cold_flag,

        -- sample flags
        case when last7_games  >= 5  then true else false end          as has_last7_sample,
        case when last15_games >= 10 then true else false end          as has_last15_sample,
        case when season_games >= 20 then true else false end          as has_season_sample

    from rolling
)

select * from final