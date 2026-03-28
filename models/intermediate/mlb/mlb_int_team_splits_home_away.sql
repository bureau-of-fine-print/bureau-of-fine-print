-- mlb_int_team_splits_home_away.sql
with game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

team_games as (
    select
        game_id, game_date,
        home_team_id as team_id,
        home_score as runs_scored,
        away_score as runs_allowed,
        case when home_score > away_score then 1 else 0 end as win,
        true as is_home,
        total_runs,
        innings_played,
        extract(year from game_date) as season
    from game_results
    union all
    select
        game_id, game_date,
        away_team_id as team_id,
        away_score as runs_scored,
        home_score as runs_allowed,
        case when away_score > home_score then 1 else 0 end as win,
        false as is_home,
        total_runs,
        innings_played,
        extract(year from game_date) as season
    from game_results
),

team_batting as (
    select
        game_id, team_id,
        sum(ab) as ab, sum(h) as h, sum(bb) as bb,
        sum(hbp) as hbp, sum(sac) as sac, sum(hr) as hr,
        sum(so) as so, sum(doubles) as doubles,
        sum(triples) as triples, sum(plate_appearances) as pa,
        sum(singles) as singles
    from {{ ref('mlb_stg_game_batter_logs') }}
    group by 1, 2
),

combined as (
    select
        tg.team_id, tg.season, tg.is_home,
        tg.runs_scored, tg.runs_allowed, tg.win, tg.total_runs,
        coalesce(tb.ab, 0) as ab, coalesce(tb.h, 0) as h,
        coalesce(tb.bb, 0) as bb, coalesce(tb.hbp, 0) as hbp,
        coalesce(tb.sac, 0) as sac, coalesce(tb.hr, 0) as hr,
        coalesce(tb.so, 0) as so, coalesce(tb.doubles, 0) as doubles,
        coalesce(tb.triples, 0) as triples, coalesce(tb.pa, 0) as pa,
        coalesce(tb.singles, 0) as singles
    from team_games tg
    left join team_batting tb on tg.game_id = tb.game_id and tg.team_id = tb.team_id
),

aggregated as (
    select
        team_id, season, is_home,
        count(*) as games,
        sum(win) as wins,
        round(avg(runs_scored), 2) as avg_runs_scored,
        round(avg(runs_allowed), 2) as avg_runs_allowed,
        round(avg(total_runs), 2) as avg_total_runs,
        sum(hr) as hr, sum(so) as so, sum(bb) as bb,
        sum(ab) as ab, sum(h) as h, sum(bb) as total_bb,
        sum(hbp) as hbp, sum(sac) as sac,
        sum(doubles) as doubles, sum(triples) as triples,
        sum(pa) as pa, sum(singles) as singles
    from combined
    group by 1, 2, 3
)

select
    team_id, season, is_home, games, wins,
    round(wins / nullif(games, 0), 3) as win_pct,
    avg_runs_scored, avg_runs_allowed, avg_total_runs, hr,
    case when ab > 0 then round(h / ab,