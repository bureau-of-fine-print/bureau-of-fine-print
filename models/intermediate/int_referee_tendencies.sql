with referee_assignments as (
    select * from {{ ref('stg_referee_assignments') }}
),

game_scores as (
    select * from {{ ref('int_game_scores') }}
),

-- Unpivot refs into one row per referee per game
ref_games as (
    select game_id, referee_1 as referee_id from referee_assignments where referee_1 is not null
    union all
    select game_id, referee_2 as referee_id from referee_assignments where referee_2 is not null
    union all
    select game_id, referee_3 as referee_id from referee_assignments where referee_3 is not null
),

ref_with_stats as (
    select
        rg.referee_id,
        g.game_id,
        g.game_date,
        g.home_points + g.away_points as total_points,
        g.home_pf + g.away_pf as total_fouls,
        g.home_fta + g.away_fta as total_fta,
        g.home_3pa + g.away_3pa as total_3pa,
        g.home_tov + g.away_tov as total_tov,
        case when g.home_points > g.away_points then 1 else 0 end as home_win
    from ref_games rg
    inner join game_scores g on rg.game_id = g.game_id
)

select
    referee_id,
    count(*) as games_officiated,
    round(avg(total_points), 1) as avg_total_points,
    round(avg(total_fouls), 1) as avg_total_fouls,
    round(avg(total_fta), 1) as avg_total_fta,
    round(avg(total_3pa), 1) as avg_total_3pa,
    round(avg(total_tov), 1) as avg_total_tov,
    round(avg(home_win), 3) as home_win_pct,
    min(game_date) as first_game,
    max(game_date) as last_game
from ref_with_stats
group by referee_id