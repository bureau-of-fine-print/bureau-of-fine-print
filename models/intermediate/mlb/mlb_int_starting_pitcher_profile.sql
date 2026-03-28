-- mlb_int_starting_pitcher_profile.sql
-- Aggregates all pitcher signals into one row per starter per game.
-- Used as the primary input for content gen and picks model.
-- Joins rolling stats, splits, rest, park, and ump context.

with schedule as (
    select
        game_id,
        game_date,
        home_team_id,
        home_team_abbr,
        away_team_id,
        away_team_abbr,
        venue_id
    from {{ ref('mlb_stg_schedule') }}
),

lineups as (
    select
        game_id,
        game_date,
        team_id,
        player_id,
        player_name,
        is_home,
        throws
    from {{ ref('mlb_stg_lineups') }}
    where is_starting_pitcher = true
),

pitcher_rolling as (
    select * from {{ ref('mlb_int_pitcher_rolling') }}
),

pitcher_rest as (
    select
        player_id,
        game_id,
        days_rest,
        rest_classification,
        is_short_rest,
        is_extra_rest
    from {{ ref('mlb_int_pitcher_splits_rest') }}
),

ump_assignments as (
    select
        game_id,
        ump_id,
        ump_name
    from {{ ref('mlb_stg_ump_assignments') }}
    where is_home_plate = true
),

ump_tendencies as (
    select * from {{ ref('mlb_int_ump_tendencies') }}
),

pitcher_ump as (
    select * from {{ ref('mlb_int_pitcher_ump_combos') }}
),

park_factors as (
    select
        venue_id,
        park_factor_runs,
        park_factor_hr,
        park_type,
        hr_park_type,
        is_coors,
        is_high_altitude,
        park_narrative,
        valid_from_season,
        valid_to_season
    from {{ ref('mlb_int_park_factors') }}
),

-- Home/away park splits for this pitcher at this venue
pitcher_park as (
    select
        player_id,
        venue_id,
        season,
        era             as park_era,
        whip            as park_whip,
        k_per_9         as park_k_per_9,
        games_started   as park_games_started,
        has_sample      as park_has_sample
    from {{ ref('mlb_int_pitcher_splits_park') }}
),

-- Combine lineup with schedule to get pitcher + game context
base as (
    select
        s.game_id,
        s.game_date,
        s.venue_id,
        l.player_id,
        l.player_name,
        l.team_id,
        l.is_home,
        l.throws,
        case when l.is_home then s.away_team_id else s.home_team_id end as opponent_team_id,
        extract(year from s.game_date) as season
    from schedule s
    inner join lineups l on s.game_id = l.game_id
),

final as (
    select
        b.game_id,
        b.game_date,
        b.season,
        b.player_id,
        b.player_name,
        b.team_id,
        b.opponent_team_id,
        b.is_home,
        b.throws,
        b.venue_id,

        -- rolling stats entering this game
        pr.last5_games,
        pr.last5_era,
        pr.last5_whip,
        pr.last5_k_per_9,
        pr.last5_bb_per_9,
        pr.last5_hr_per_9,
        pr.last5_k_bb_ratio,
        pr.last5_ip,
        pr.season_era,
        pr.season_whip,
        pr.season_k_per_9,
        pr.season_bb_per_9,
        pr.season_hr_per_9,
        pr.season_k_bb_ratio,
        pr.season_games,
        pr.season_ip,
        pr.era_trend,
        pr.has_rolling_sample,
        pr.has_season_sample,

        -- rest situation
        prest.days_rest,
        prest.rest_classification,
        prest.is_short_rest,
        prest.is_extra_rest,

        -- ump context
        ua.ump_id,
        ua.ump_name,
        ut.k_per_9              as ump_k_per_9,
        ut.bb_per_9             as ump_bb_per_9,
        ut.avg_runs_per_game    as ump_runs_per_game,
        ut.zone_classification,
        ut.run_environment,
        ut.has_sufficient_sample as ump_has_sample,

        -- pitcher + ump combo
        puc.games_together      as ump_combo_games,
        puc.era_with_ump,
        puc.k_per_9_with_ump,
        puc.has_combo_sample,
        puc.combo_narrative_flag,

        -- park context at this venue this season
        pf.park_factor_runs,
        pf.park_factor_hr,
        pf.park_type,
        pf.hr_park_type,
        pf.is_coors,
        pf.is_high_altitude,
        pf.park_narrative,

        -- pitcher at this park historically
        pp.park_era,
        pp.park_whip,
        pp.park_k_per_9,
        pp.park_games_started,
        pp.park_has_sample,

        -- composite short rest flag for content gen
        case
            when prest.is_short_rest = true
            then concat('SHORT REST: ', b.player_name,
                        ' on ', prest.days_rest, ' days rest')
            else null
        end                                         as short_rest_alert

    from base b

    -- rolling stats (most recent game before this one)
    left join pitcher_rolling pr
        on b.player_id = pr.player_id
        and b.game_id = pr.game_id

    -- rest
    left join pitcher_rest prest
        on b.player_id = prest.player_id
        and b.game_id = prest.game_id

    -- ump
    left join ump_assignments ua on b.game_id = ua.game_id
    left join ump_tendencies ut on ua.ump_id = ut.ump_id

    -- pitcher+ump combo
    left join pitcher_ump puc
        on b.player_id = puc.player_id
        and ua.ump_id = puc.ump_id

    -- park factors
    left join park_factors pf
        on b.venue_id = pf.venue_id
        and pf.valid_from_season <= b.season
        and (pf.valid_to_season is null or pf.valid_to_season >= b.season)

    -- pitcher park splits
    left join pitcher_park pp
        on b.player_id = pp.player_id
        and b.venue_id = pp.venue_id
        and b.season = pp.season
)

select * from final