with game_scores as (
    select * from {{ ref('int_game_scores') }}
),

-- Only completed games, direction-neutral pairing
all_matchups as (
    select
        least(home_team_id, away_team_id)    as team_a,
        greatest(home_team_id, away_team_id) as team_b,
        game_date,
        case
            when home_team_id = least(home_team_id, away_team_id)
            then case when home_points > away_points then 1 else 0 end
            else case when away_points > home_points then 1 else 0 end
        end as team_a_win
    from game_scores
    where home_points is not null
        and home_points > 0
)

select
    team_a,
    team_b,
    count(*)              as games_played,
    sum(team_a_win)       as team_a_wins,
    sum(1 - team_a_win)   as team_b_wins,
    round(avg(team_a_win), 3) as team_a_win_pct,
    max(game_date)        as last_meeting,
    min(game_date)        as first_meeting
from all_matchups
group by team_a, team_b