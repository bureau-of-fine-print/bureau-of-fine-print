with game_scores as (
    select * from {{ ref('int_game_scores') }}
),

-- One row per team per game
team_games as (
    select
        game_id,
        game_date,
        home_team_id as team_id,
        away_team_id as opponent_id,
        'home' as home_away,
        home_points as team_points,
        away_points as opp_points,
        case when home_points > away_points then 1 else 0 end as win
    from game_scores

    union all

    select
        game_id,
        game_date,
        away_team_id as team_id,
        home_team_id as opponent_id,
        'away' as home_away,
        away_points as team_points,
        home_points as opp_points,
        case when away_points > home_points then 1 else 0 end as win
    from game_scores
),

with_running_record as (
    select
        team_id,
        game_id,
        game_date,
        opponent_id,
        home_away,
        team_points,
        opp_points,
        win,
        sum(win) over (partition by team_id order by game_date rows between unbounded preceding and current row) as cumulative_wins,
        sum(1 - win) over (partition by team_id order by game_date rows between unbounded preceding and current row) as cumulative_losses,
        row_number() over (partition by team_id order by game_date desc) as game_recency
    from team_games
)

select
    team_id,
    game_id,
    game_date,
    opponent_id,
    home_away,
    team_points,
    opp_points,
    win,
    cumulative_wins as wins,
    cumulative_losses as losses,
    cumulative_wins + cumulative_losses as games_played,
    round(cumulative_wins / (cumulative_wins + cumulative_losses), 3) as win_pct,
    game_recency
from with_running_record