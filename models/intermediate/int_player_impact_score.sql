WITH player_stats AS (
    SELECT
        player_name,
        team_id,
        games_played,
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
        below_avg_streak,
        ROUND(
            pts_season_avg
            + (reb_season_avg * 1.2)
            + (ast_season_avg * 1.5)
            + (stl_season_avg * 3.0)
            + (blk_season_avg * 2.5)
        , 2) AS impact_score
    FROM `project-71e6f4ed-bf24-4c0f-bb0.marts.mart_player_summary`
    WHERE games_played >= 10
),

team_games AS (
    SELECT MAX(games_played) AS max_games
    FROM player_stats
),

ranked AS (
    SELECT
        ps.*,
        ROW_NUMBER() OVER (
            PARTITION BY ps.team_id
            ORDER BY ps.impact_score DESC
        ) AS team_rank
    FROM player_stats ps
    CROSS JOIN team_games tg
    WHERE (
        ps.games_played >= tg.max_games * 0.25
        AND ps.pts_season_avg >= 20
    )
    OR ps.games_played >= tg.max_games * 0.50
)

SELECT
    player_name,
    team_id,
    games_played,
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