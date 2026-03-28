-- mlb_int_batter_splits_park.sql
-- Batter performance at each ballpark.
-- Useful for power hitters with extreme park splits
-- (e.g. LHH slugger at Yankee Stadium short porch vs Oracle Park).
-- One row per batter per venue per season.

with batter_logs as (
    select * from {{ ref('mlb_stg_game_batter_logs') }}
    where ab > 0
),

schedule as (
    select
        game_id,
        venue_id,
        venue_name
    from {{ ref('mlb_stg_schedule') }}
),

parks as (
    select
        venue_id,
        park_factor_runs,
        park_factor_hr,
        park_type,
        hr_park_type,
        valid_from_season,
        valid_to_season
    from {{ ref('mlb_int_park_factors') }}
),

batter_with_venue as (
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
        s.venue_id,
        s.venue_name
    from batter_logs bl
    inner join schedule s on bl.game_id = s.game_id
),

aggregated as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        venue_id,
        venue_name,
        season,
        count(distinct game_id)     as games,
        sum(ab)                     as ab,
        sum(plate_appearances)      as pa,
        sum(h)                      as h,
        sum(doubles)                as doubles,
        sum(triples)                as triples,
        sum(hr)                     as hr,
        sum(bb)                     as bb,
        sum(hbp)                    as hbp,
        sum(sac)                    as sac,
        sum(so)                     as so,
        sum(singles)                as singles
    from batter_with_venue
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        a.player_id,
        a.player_name,
        a.team_id,
        a.team_abbr,
        a.venue_id,
        a.venue_name,
        a.season,
        a.games,
        a.ab,
        a.pa,
        a.h,
        a.hr,
        a.bb,
        a.so,

        case when a.ab > 0
             then round(a.h / a.ab, 3) else null
        end                                             as avg,

        case when a.pa > 0
             then round((a.h + a.bb + a.hbp) / a.pa, 3) else null
        end                                             as obp,

        case when a.ab > 0
             then round(
                (a.singles + 2*a.doubles + 3*a.triples + 4*a.hr) / a.ab, 3)
             else null
        end                                             as slg,

        case when a.ab > 0 and a.pa > 0
             then round(
                (a.h + a.bb + a.hbp) / a.pa
                + (a.singles + 2*a.doubles + 3*a.triples + 4*a.hr) / a.ab, 3)
             else null
        end                                             as ops,

        case when a.ab > 0
             then round(a.hr / a.ab * 550, 1) else null
        end                                             as hr_per_550_ab,

        -- park context
        p.park_factor_runs,
        p.park_factor_hr,
        p.park_type,
        p.hr_park_type,

        case when a.ab >= 20 then true else false end   as has_sample

    from aggregated a
    left join parks p
        on a.venue_id = p.venue_id
        and p.valid_from_season <= a.season
        and (p.valid_to_season is null or p.valid_to_season >= a.season)
)

select * from final