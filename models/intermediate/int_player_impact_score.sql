WITH current_team_stats AS (
    SELECT
        ps.player_name,
        ps.team_id,
        COUNT(DISTINCT ps.game_id)        AS games_with_current_team,
        ROUND(AVG(ps.points), 1)          AS pts_season_avg,
        ROUND(AVG(ps.rebounds), 1)        AS reb_season_avg,
        ROUND(AVG(ps.assists), 1)         AS ast_season_avg,
        ROUND(AVG(ps.steals), 1)          AS stl_season_avg,
        ROUND(AVG(ps.blocks), 1)          AS blk_season_avg,
        ROUND(AVG(ps.turnovers), 1)       AS tov_season_avg,
        ROUND(AVG(ps.plus_minus), 1)      AS plus_minus_season_avg,
        ROUND(
            AVG(ps.points)
            + (AVG(ps.rebounds) * 1.2)
            + (AVG(ps.assists) * 1.5)
            + (AVG(ps.steals) * 3.0)
            + (AVG(ps.blocks) * 2.5)
        , 2) AS impact_score
    FROM {{ ref('stg_player_game_stats') }} ps
    JOIN {{ ref('stg_games') }} g ON ps.game_id = g.game_id
    WHERE g.season = (SELECT MAX(season) FROM {{ ref('stg_games') }})
    GROUP BY ps.player_name, ps.team_id
),

team_season_games AS (
    SELECT
        home_team_id AS team_id,
        COUNT(DISTINCT game_id) AS games_played_this_season
    FROM {{ ref('stg_games') }}
    WHERE season = (SELECT MAX(season) FROM {{ ref('stg_games') }})
    AND game_date < CURRENT_DATE('America/New_York')
    GROUP BY home_team_id
),

player_stats AS (
    SELECT
        cts.*,
        tsg.games_played_this_season,
        LEAST(5, CEIL(tsg.games_played_this_season * 0.10)) AS min_games_threshold,
        mps.pts_last5,
        mps.reb_last5,
        mps.ast_last5,
        mps.is_pts_hot_last5,
        mps.is_pts_cold_last5,
        mps.hot_game_streak,
        mps.below_avg_streak
    FROM current_team_stats cts
    JOIN team_season_games tsg ON cts.team_id = tsg.team_id
    LEFT JOIN {{ ref('mart_player_summary') }} mps
        ON cts.player_name = mps.player_name
        AND cts.team_id = mps.team_id
    WHERE cts.games_with_current_team >= LEAST(5, CEIL(tsg.games_played_this_season * 0.10))
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id
            ORDER BY impact_score DESC
        ) AS team_rank
    FROM player_stats
)

SELECT
    player_name,
    team_id,
    games_with_current_team,
    team_rank,
    impact_score,
    pts_season_avg,
    reb_season_avg,
    ast_season_avg,
    stl_season_avg,
    blk_season_avg,
    tov_season_avg,
    plus_minus_season_avg,
    pts_last5,
    reb_last5,
    ast_last5,
    is_pts_hot_last5,
    is_pts_cold_last5,
    hot_game_streak,
    below_avg_streak
FROM ranked
WHERE team_rank <= 7