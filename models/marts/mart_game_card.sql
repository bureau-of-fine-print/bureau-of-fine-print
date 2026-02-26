with games as (
    select * from {{ ref('stg_games') }}
),

game_scores as (
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

team_record_current as (
    select
        tr.team_id,
        tr.game_id,
        tr.game_date,
        tr.win,
        sum(tr.win) over (partition by tr.team_id order by tr.game_date rows between unbounded preceding and current row) as wins,
        sum(1 - tr.win) over (partition by tr.team_id order by tr.game_date rows between unbounded preceding and current row) as losses,
        round(sum(tr.win) over (partition by tr.team_id order by tr.game_date rows between unbounded preceding and current row) /
            (row_number() over (partition by tr.team_id order by tr.game_date)), 3) as win_pct
    from {{ ref('int_team_record') }} tr
    inner join {{ ref('stg_games') }} g on tr.game_id = g.game_id
    where g.season = (
        select max(season) from {{ ref('stg_games') }}
    )
),

home_record as (
    select
        team_id,
        wins,
        losses,
        win_pct
    from team_record_current
    qualify row_number() over (partition by team_id order by game_date desc) = 1
),

away_record as (
    select
        team_id,
        wins,
        losses,
        win_pct
    from team_record_current
    qualify row_number() over (partition by team_id order by game_date desc) = 1
),

home_streaks as (
    select
        ts.team_id,
        ts.streak_label as home_streak_label,
        ts.streak_length as home_streak_length
    from {{ ref('int_team_streaks') }} ts
    qualify row_number() over (partition by ts.team_id order by ts.game_date desc) = 1
),

away_streaks as (
    select
        ts.team_id,
        ts.streak_label as away_streak_label,
        ts.streak_length as away_streak_length
    from {{ ref('int_team_streaks') }} ts
    qualify row_number() over (partition by ts.team_id order by ts.game_date desc) = 1
),

home_rolling as (
    select
        tr.team_id,
        tr.pts_last5 as home_pts_last5,
        tr.pts_last10 as home_pts_last10,
        tr.opp_pts_last5 as home_opp_pts_last5,
        tr.opp_pts_last10 as home_opp_pts_last10,
        tr.win_pct_last5 as home_win_pct_last5,
        tr.win_pct_last10 as home_win_pct_last10,
        tr.pace_last5 as home_pace_last5,
        tr.pace_last10 as home_pace_last10
    from {{ ref('int_team_rolling_averages') }} tr
    qualify row_number() over (partition by tr.team_id order by tr.game_date desc) = 1
),

away_rolling as (
    select
        tr.team_id,
        tr.pts_last5 as away_pts_last5,
        tr.pts_last10 as away_pts_last10,
        tr.opp_pts_last5 as away_opp_pts_last5,
        tr.opp_pts_last10 as away_opp_pts_last10,
        tr.win_pct_last5 as away_win_pct_last5,
        tr.win_pct_last10 as away_win_pct_last10,
        tr.pace_last5 as away_pace_last5,
        tr.pace_last10 as away_pace_last10
    from {{ ref('int_team_rolling_averages') }} tr
    qualify row_number() over (partition by tr.team_id order by tr.game_date desc) = 1
),

head_to_head as (
    select
        g1.home_team_id,
        g1.away_team_id,
        count(*) as h2h_games,
        sum(case when g1.home_points > g1.away_points then 1 else 0 end) as h2h_home_wins,
        sum(case when g1.away_points > g1.home_points then 1 else 0 end) as h2h_away_wins,
        round(avg(g1.home_points + g1.away_points), 1) as h2h_avg_total_points,
        max(g1.game_date) as h2h_last_meeting
    from {{ ref('int_game_scores') }} g1
    inner join {{ ref('stg_games') }} sg on g1.game_id = sg.game_id
    where sg.season = (select max(season) from {{ ref('stg_games') }})
    group by g1.home_team_id, g1.away_team_id
),

referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

ref_tendencies as (
    select * from {{ ref('int_referee_tendencies') }}
),

home_ref_record as (
    select
        team_id,
        referee_id,
        games_with_referee,
        win_pct,
        avg_points_scored,
        avg_points_allowed
    from {{ ref('int_team_referee_record') }}
),

away_ref_record as (
    select
        team_id,
        referee_id,
        games_with_referee,
        win_pct,
        avg_points_allowed
    from {{ ref('int_team_referee_record') }}
),

home_pace_perf as (
    select
        team_id,
        pace_bucket,
        count(*) as games,
        sum(win) as wins,
        round(sum(win)/count(*), 3) as win_pct
    from {{ ref('int_team_performance_by_pace') }}
    where is_current_season = true
    group by team_id, pace_bucket
),

away_pace_perf as (
    select
        team_id,
        pace_bucket,
        count(*) as games,
        sum(win) as wins,
        round(sum(win)/count(*), 3) as win_pct
    from {{ ref('int_team_performance_by_pace') }}
    where is_current_season = true
    group by team_id, pace_bucket
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

    -- Scores (null until game is played)
    gs.home_points,
    gs.away_points,
    gs.point_differential,
    gs.winner_team_id,
    gs.winner_home_away,
    gs.total_points,

    -- Pace (null until game is played)
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

    -- Current season records
    hr.wins as home_wins,
    hr.losses as home_losses,
    hr.win_pct as home_win_pct,
    ar.wins as away_wins,
    ar.losses as away_losses,
    ar.win_pct as away_win_pct,

    -- Streaks
    hs.home_streak_label,
    hs.home_streak_length,
    aws.away_streak_label,
    aws.away_streak_length,

    -- Rolling averages
    hrol.home_pts_last5,
    hrol.home_pts_last10,
    hrol.home_opp_pts_last5,
    hrol.home_opp_pts_last10,
    hrol.home_win_pct_last5,
    hrol.home_win_pct_last10,
    hrol.home_pace_last5,
    hrol.home_pace_last10,
    arol.away_pts_last5,
    arol.away_pts_last10,
    arol.away_opp_pts_last5,
    arol.away_opp_pts_last10,
    arol.away_win_pct_last5,
    arol.away_win_pct_last10,
    arol.away_pace_last5,
    arol.away_pace_last10,

    -- Head to head current season
    h2h.h2h_games,
    h2h.h2h_home_wins,
    h2h.h2h_away_wins,
    h2h.h2h_avg_total_points,
    h2h.h2h_last_meeting,

    -- Referees
    ra.referee_1,
    ra.referee_2,
    ra.referee_3,

    -- Crew chief tendencies
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
    atr.win_pct as away_win_pct_with_crew_chief,

    -- Home team pace performance current season
    hpp_fast.games as home_fast_pace_games,
    hpp_fast.wins as home_fast_pace_wins,
    hpp_fast.win_pct as home_fast_pace_win_pct,
    hpp_avg.games as home_avg_pace_games,
    hpp_avg.wins as home_avg_pace_wins,
    hpp_avg.win_pct as home_avg_pace_win_pct,
    hpp_slow.games as home_slow_pace_games,
    hpp_slow.wins as home_slow_pace_wins,
    hpp_slow.win_pct as home_slow_pace_win_pct,

    -- Away team pace performance current season
    app_fast.games as away_fast_pace_games,
    app_fast.wins as away_fast_pace_wins,
    app_fast.win_pct as away_fast_pace_win_pct,
    app_avg.games as away_avg_pace_games,
    app_avg.wins as away_avg_pace_wins,
    app_avg.win_pct as away_avg_pace_win_pct,
    app_slow.games as away_slow_pace_games,
    app_slow.wins as away_slow_pace_wins,
    app_slow.win_pct as away_slow_pace_win_pct

from games g
left join game_scores gs on g.game_id = gs.game_id
left join game_pace gp on g.game_id = gp.game_id
left join rest_days r on g.game_id = r.game_id
left join travel t on g.game_id = t.game_id
left join timezone_lag tz on g.game_id = tz.game_id
left join altitude_fatigue al on g.game_id = al.game_id
left join national_broadcast nb on g.game_id = nb.game_id
left join home_record hr on g.home_team_id = hr.team_id
left join away_record ar on g.away_team_id = ar.team_id
left join home_streaks hs on g.home_team_id = hs.team_id
left join away_streaks aws on g.away_team_id = aws.team_id
left join home_rolling hrol on g.home_team_id = hrol.team_id
left join away_rolling arol on g.away_team_id = arol.team_id
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
left join home_pace_perf hpp_fast
    on g.home_team_id = hpp_fast.team_id
    and hpp_fast.pace_bucket = 'fast'
left join home_pace_perf hpp_avg
    on g.home_team_id = hpp_avg.team_id
    and hpp_avg.pace_bucket = 'average'
left join home_pace_perf hpp_slow
    on g.home_team_id = hpp_slow.team_id
    and hpp_slow.pace_bucket = 'slow'
left join away_pace_perf app_fast
    on g.away_team_id = app_fast.team_id
    and app_fast.pace_bucket = 'fast'
left join away_pace_perf app_avg
    on g.away_team_id = app_avg.team_id
    and app_avg.pace_bucket = 'average'
left join away_pace_perf app_slow
    on g.away_team_id = app_slow.team_id
    and app_slow.pace_bucket = 'slow'