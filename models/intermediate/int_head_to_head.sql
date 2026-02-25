with game_scores as (
    select * from {{ ref('int_game_scores') }}
)

select
    home_team_id,
    away_team_id,
    count(*) as games_played,
    sum(case when home_points > away_points then 1 else 0 end) as home_team_wins,
    sum(case when away_points > home_points then 1 else 0 end) as away_team_wins,
    round(avg(home_points), 1) as avg_home_points,
    round(avg(away_points), 1) as avg_away_points,
    round(avg(home_points + away_points), 1) as avg_total_points,
    round(avg(home_points - away_points), 1) as avg_point_differential,
    max(game_date) as last_meeting,
    min(game_date) as first_meeting
from game_scores
group by home_team_id, away_team_id