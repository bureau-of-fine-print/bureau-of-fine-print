with game_scores as (
    select * from {{ ref('int_game_scores') }}
),

game_pace as (
    select * from {{ ref('int_game_pace') }}
),

rest_days as (
    select * from {{ ref('int_rest_days') }}
),

travel as (
    select * from {{ ref('int_travel') }}
),

timezone_lag as (
    select * from {{ ref('int_timezone_lag') }}
),

altitude_fatigue as (
    select * from {{ ref('int_altitude_fatigue') }}
),

national_broadcast as (
    select * from {{ ref('int_national_broadcast_flag') }}
),

home_streaks as (
    select
        ts.game_id,
        streak_label as home_streak_label,
        streak_length as home_streak_length
    from {{ ref('int_team_streaks') }} ts
    inner join {{ ref('int_game_scores') }} g
        on ts.game_id = g.game_id
        and ts.team_id = g.home_team_id
),

away_streaks as (
    select
        ts.game_id,
        streak_label as away_streak_label,
        streak_length as away_streak_length
    from {{ ref('int_team_streaks') }} ts
    inner join {{ ref('int_game_scores') }} g
        on ts.game_id = g.game_id
        and ts.team_id = g.away_team_id
),

home_rolling as (
    select
        tr.game_id,
        tr.pts_last5 as home_pts_last5,
        tr.pts_last10 as home_pts_last10,
        tr.opp_pts_last5 as home_opp_pts_last5,
        tr.opp_pts_last10 as home_opp_pts_last10,
        tr.win_pct_last5 as home_win_pct_last5,
        tr.win_pct_last10 as home_win_pct_last10,
        tr.pace_last5 as home_pace_last5,
        tr.pace_last10 as home_pace_last10
    from {{ ref('int_team_rolling_averages') }} tr
    inner join {{ ref('int_game_scores') }} g
        on tr.game_id = g.game_id
        and tr.team_id = g.home_team_id
),

away_rolling as (
    select
        tr.game_id,
        tr.pts_last5 as away_pts_last5,
        tr.pts_last10 as away_pts_last10,
        tr.opp_pts_last5 as away_opp_pts_last5,
        tr.opp_pts_last10 as away_opp_pts_last10,
        tr.win_pct_last5 as away_win_pct_last5,
        tr.win_pct_last10 as away_win_pct_last10,
        tr.pace_last5 as away_pace_last5,
        tr.pace_last10 as away_pace_last10
    from {{ ref('int_team_rolling_averages') }} tr
    inner join {{ ref('int_game_scores') }} g
        on tr.game_id = g.game_id
        and tr.team_id = g.away_team_id
),

head_to_head as (
    select * from {{ ref('int_head_to_head') }}
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

home_ref_record as (
    select
        trr.team_id,
        trr.referee_id,
        trr.games_with_referee,
        trr.win_pct,
        trr.avg_points_scored,
        trr.avg_points_allowed
    from {{ ref('int_team_referee_record') }} trr
),

away_ref_record as (
    select
        trr.team_id,
        trr.referee_id,
        trr.games_with_referee,
        trr.win_pct,
        trr.avg_points_scored,
        trr.avg_points_allowed
    from {{ ref('int_team_referee_record') }} trr
),

ref_tendencies as (
    select * from {{ ref('int_referee_tendencies') }}
)

select
    -- Game info
    g.game_id,
    g.game_date,
    g.season,
    g.arena_id,
    g.tipoff_time,
    g.home_team_id,
    g.away_team_id,
    nb.is_national_broadcast,

    -- Score and result
    g.home_points,
    g.away_points,
    g.point_differential,
    g.winner_team_id,
    g.winner_home_away,
    g.total_points,

    -- Pace
    gp.home_possessions,
    gp.away_possessions,
    gp.game_pace,
    gp.overtime_periods,

    -- Rest
    r.home_days_rest,
    r.away_days_rest,
    r.rest_advantage,

    -- Travel
    t.home_travel_miles,
    t.away_travel_miles,
    t.travel_advantage,

    -- Timezone
    tz.home_timezone_shift,
    tz.away_timezone_shift,
    tz.timezone_lag_advantage,

    -- Altitude
    al.home_arena_altitude,
    al.away_arena_altitude,
    al.home_altitude_change,
    al.away_altitude_change,
    al.altitude_fatigue_advantage,

    -- Home team streaks
    hs.home_streak_label,
    hs.home_streak_length,

    -- Away team streaks
    aws.away_streak_label,
    aws.away_streak_length,

    -- Home team rolling averages
    hr.home_pts_last5,
    hr.home_pts_last10,
    hr.home_opp_pts_last5,
    hr.home_opp_pts_last10,
    hr.home_win_pct_last5,
    hr.home_win_pct_last10,
    hr.home_pace_last5,
    hr.home_pace_last10,

    -- Away team rolling averages
    ar.away_pts_last5,
    ar.away_pts_last10,
    ar.away_opp_pts_last5,
    ar.away_opp_pts_last10,
    ar.away_win_pct_last5,
    ar.away_win_pct_last10,
    ar.away_pace_last5,
    ar.away_pace_last10,

    -- Head to head
    h2h.games_played as h2h_games_played,
    h2h.home_team_wins as h2h_home_wins,
    h2h.away_team_wins as h2h_away_wins,
    h2h.avg_total_points as h2h_avg_total_points,
    h2h.last_meeting as h2h_last_meeting,

    -- Referees
    ra.referee_1,
    ra.referee_2,
    ra.referee_3,

    -- Referee tendencies (crew average based on ref 1 as crew chief)
    rt.games_officiated as crew_chief_games,
    rt.avg_total_fouls as crew_chief_avg_fouls,
    rt.avg_total_fta as crew_chief_avg_fta,
    rt.avg_total_points as crew_chief_avg_total_points,
    rt.home_win_pct as crew_chief_home_win_pct,

    -- Home team record with crew chief
    htr.games_with_referee as home_games_with_crew_chief,
    htr.win_pct as home_win_pct_with_crew_chief,

    -- Away team record with crew chief
    atr.games_with_referee as away_games_with_crew_chief,
    atr.win_pct as away_win_pct_with_crew_chief

from game_scores g
left join game_pace gp on g.game_id = gp.game_id
left join rest_days r on g.game_id = r.game_id
left join travel t on g.game_id = t.game_id
left join timezone_lag tz on g.game_id = tz.game_id
left join altitude_fatigue al on g.game_id = al.game_id
left join national_broadcast nb on g.game_id = nb.game_id
left join home_streaks hs on g.game_id = hs.game_id
left join away_streaks aws on g.game_id = aws.game_id
left join home_rolling hr
    on g.game_id = hr.game_id
left join away_rolling ar
    on g.game_id = ar.game_id
left join head_to_head h2h
    on g.home_team_id = h2h.home_team_id
    and g.away_team_id = h2h.away_team_id
left join referee_assignments ra on g.game_id = ra.game_id
left join ref_tendencies rt on ra.referee_1 = rt.referee_id
left join home_ref_record htr
    on g.home_team_id = htr.team_id
    and ra.referee_1 = htr.referee_id
left join away_ref_record atr
    on g.away_team_id = atr.team_id
    and ra.referee_1 = atr.referee_id