-- mlb_int_batter_splits_handedness.sql
-- Batter performance vs LHP and vs RHP.
-- Built from batter logs joined to pitcher logs via inning ranges
-- to determine which pitcher a batter faced.
-- One row per batter per pitcher handedness per season.

with batter_logs as (
    select * from {{ ref('mlb_stg_game_batter_logs') }}
),

pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
),

player_profiles as (
    select * from {{ ref('mlb_stg_player_profiles') }}
),

-- For each game, identify which pitcher(s) a batter likely faced
-- We use a simplified approach: for starters, credit the opposing SP
-- For the full game line, we weight by innings (SP throws most innings)
-- This is an approximation -- exact matchup requires play-by-play

-- Get the starting pitcher for each team per game
starting_pitchers as (
    select
        game_id,
        game_date,
        team_id     as pitching_team_id,
        player_id   as sp_player_id,
        throws      as sp_throws,
        ip_outs     as sp_ip_outs,
        season
    from pitcher_logs
    where is_starter = true
),

-- Join batters to opposing starting pitcher
batter_vs_sp as (
    select
        bl.player_id,
        bl.player_name,
        bl.team_id,
        bl.team_abbr,
        bl.game_id,
        bl.game_date,
        bl.season,
        bl.ab,
        bl.h,
        bl.doubles,
        bl.triples,
        bl.hr,
        bl.bb,
        bl.hbp,
        bl.sac,
        bl.so,
        bl.plate_appearances,
        bl.singles,
        sp.sp_throws          as pitcher_throws,
        sp.sp_ip_outs
    from batter_logs bl
    inner join starting_pitchers sp
        on bl.game_id = sp.game_id
        -- batter faces opposing pitcher
        and bl.team_id != sp.pitching_team_id
    where bl.ab > 0
),

-- Aggregate by batter, pitcher handedness, season
aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        season,
        pitcher_throws,
        count(distinct game_id)                         as games,
        sum(ab)                                         as ab,
        sum(h)                                          as h,
        sum(doubles)                                    as doubles,
        sum(triples)                                    as triples,
        sum(hr)                                         as hr,
        sum(bb)                                         as bb,
        sum(hbp)                                        as hbp,
        sum(sac)                                        as sac,
        sum(so)                                         as so,
        sum(plate_appearances)                          as pa,
        sum(singles)                                    as singles
    from batter_vs_sp
    group by 1, 2, 3, 4, 5, 6
),

final as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        season,
        pitcher_throws,
        games,
        ab,
        pa,
        h,
        hr,
        bb,
        so,

        -- rate stats
        case when ab > 0
             then round(h / ab, 3) else null
        end                                             as avg,

        case when pa > 0
             then round((h + bb + hbp) / pa, 3) else null
        end                                             as obp,

        case when ab > 0
             then round(
                (singles + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null
        end                                             as slg,

        case when ab > 0 and pa > 0
             then round(
                (h + bb + hbp) / pa
                + (singles + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null
        end                                             as ops,

        case when ab > 0
             then round(hr / ab * 550, 1) else null
        end                                             as hr_per_550_ab,

        case when pa > 0
             then round(so / pa, 3) else null
        end                                             as k_rate,

        case when pa > 0
             then round(bb / pa, 3) else null
        end                                             as bb_rate,

        -- sample size flag
        case when ab >= 50 then true else false end     as has_sample

    from aggregated
    where pitcher_throws is not null
)

select * from final