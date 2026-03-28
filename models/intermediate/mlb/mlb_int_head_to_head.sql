-- mlb_int_head_to_head.sql
with game_results as (
    select * from {{ ref('mlb_stg_game_results') }}
),

matchups as (
    select
        home_team_id as team_id, away_team_id as opponent_id,
        game_id, game_date,
        home_score as team_runs, away_score as opp_runs,
        case when home_score > away_score then 1 else 0 end as win,
        total_runs, extra_innings,
        extract(year from game_date) as season
    from game_results
    union all
    select
        away_team_id as team_id, home_team_id as opponent_id,
        game_id, game_date,
        away_score as team_runs, home_score as opp_runs,
        case when away_score > home_score then 1 else 0 end as win,
        total_runs, extra_innings,
        extract(year from game_date) as season
    from game_results
),

last5 as (
    select team_id, opponent_id, season,
        sum(win) as last5_wins,
        count(*) as last5_games
    from (
        select team_id, opponent_id, season, win,
            row_number() over (
                partition by team_id, opponent_id, season
                order by game_date desc
            ) as rn
        from matchups
    )
    where rn <= 5
    group by 1, 2, 3
),

aggregated as (
    select
        m.team_id, m.opponent_id, m.season,
        count(*)                        as games,
        sum(m.win)                      as wins,
        round(avg(m.team_runs), 2)      as avg_runs_scored,
        round(avg(m.opp_runs), 2)       as avg_runs_allowed,
        round(avg(m.total_runs), 2)     as avg_total_runs,
        countif(m.extra_innings)        as extra_inning_games,
        max(l5.last5_wins)              as last5_wins,
        max(l5.last5_games)             as last5_games
    from matchups m
    left join last5 l5
        on m.team_id = l5.team_id
        and m.opponent_id = l5.opponent_id
        and m.season = l5.season
    group by 1, 2, 3
),

final as (
    select
        team_id, opponent_id, season, games, wins,
        games - wins as losses,
        round(wins / nullif(games, 0), 3) as win_pct,
        avg_runs_scored, avg_runs_allowed, avg_total_runs,
        extra_inning_games, last5_wins, last5_games,
        round(last5_wins / nullif(last5_games, 0), 3) as last5_win_pct,
        case
            when games >= 5 and wins / nullif(games, 0) >= 0.65 then 'dominant'
            when games >= 5 and wins / nullif(games, 0) <= 0.35 then 'dominated'
            else 'even'
        end as series_dominance,
        case
            when games >= 5 and avg_total_runs >= 11.0 then 'high_scoring'
            when games >= 5 and avg_total_runs <= 7.5  then 'low_scoring'
            else 'average'
        end as run_tendency,
        case when games >= 5 then true else false end as has_sample
    from aggregated
)

select * from final