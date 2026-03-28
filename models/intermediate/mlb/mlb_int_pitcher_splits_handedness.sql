-- mlb_int_pitcher_splits_handedness.sql
-- Pitcher performance vs LHH and vs RHH.
-- Uses same game-level approximation as batter splits —
-- credits opposing lineup handedness composition to the pitcher.
-- One row per pitcher per batter handedness per season.

with pitcher_logs as (
    select * from {{ ref('mlb_stg_game_pitcher_logs') }}
    where is_starter = true
),

batter_logs as (
    select * from {{ ref('mlb_stg_game_batter_logs') }}
    where ab > 0
),

player_profiles as (
    select
        player_id,
        bats,
        is_switch_hitter
    from {{ ref('mlb_stg_player_profiles') }}
),

-- Get batter handedness from profiles
batter_with_hand as (
    select
        bl.game_id,
        bl.game_date,
        bl.player_id,
        bl.team_id,
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
        coalesce(pp.bats, 'R') as bats  -- default to R if unknown
    from batter_logs bl
    left join player_profiles pp on bl.player_id = pp.player_id
),

-- Join pitchers to opposing batters
pitcher_vs_batters as (
    select
        pl.player_id,
        pl.player_name,
        pl.team_id,
        pl.team_abbr,
        pl.game_id,
        pl.game_date,
        pl.season,
        pl.throws,
        bwh.bats                as batter_bats,
        -- switch hitters bat opposite of pitcher hand
        case
            when bwh.bats = 'S' and pl.throws = 'L' then 'R'
            when bwh.bats = 'S' and pl.throws = 'R' then 'L'
            else bwh.bats
        end                     as effective_batter_side,
        bwh.ab,
        bwh.h,
        bwh.doubles,
        bwh.triples,
        bwh.hr,
        bwh.bb,
        bwh.hbp,
        bwh.sac,
        bwh.so,
        bwh.plate_appearances,
        bwh.singles
    from pitcher_logs pl
    inner join batter_with_hand bwh
        on pl.game_id = bwh.game_id
        -- pitcher faces opposing team batters
        and pl.team_id != bwh.team_id
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        effective_batter_side   as batter_side,
        season,
        count(distinct game_id) as games,
        sum(ab)                 as ab,
        sum(plate_appearances)  as pa,
        sum(h)                  as h,
        sum(doubles)            as doubles,
        sum(triples)            as triples,
        sum(hr)                 as hr,
        sum(bb)                 as bb,
        sum(hbp)                as hbp,
        sum(sac)                as sac,
        sum(so)                 as so,
        sum(singles)            as singles
    from pitcher_vs_batters
    where effective_batter_side in ('L', 'R')
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        throws,
        batter_side,
        season,
        games,
        ab,
        pa,
        h,
        hr,
        bb,
        so,

        -- opponent AVG
        case when ab > 0
             then round(h / ab, 3) else null
        end                                             as opp_avg,

        -- opponent OBP
        case when pa > 0
             then round((h + bb + hbp) / pa, 3) else null
        end                                             as opp_obp,

        -- opponent SLG
        case when ab > 0
             then round(
                (singles + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null
        end                                             as opp_slg,

        -- opponent OPS
        case when ab > 0 and pa > 0
             then round(
                (h + bb + hbp) / pa
                + (singles + 2*doubles + 3*triples + 4*hr) / ab, 3)
             else null
        end                                             as opp_ops,

        -- K rate vs this batter side
        case when pa > 0
             then round(so / pa, 3) else null
        end                                             as k_rate,

        -- BB rate vs this batter side
        case when pa > 0
             then round(bb / pa, 3) else null
        end                                             as bb_rate,

        -- HR rate vs this batter side
        case when ab > 0
             then round(hr / ab, 3) else null
        end                                             as hr_rate,

        -- handedness matchup advantage
        -- same-side matchups favor pitcher (LHP vs LHH, RHP vs RHH)
        case
            when throws = batter_side then 'platoon_advantage'
            else 'platoon_disadvantage'
        end                                             as platoon_matchup,

        case when ab >= 30 then true else false end     as has_sample

    from aggregated
)

select * from final