-- models/intermediate/int_returning_players.sql
WITH last_game AS (
    SELECT
        ps.player_name,
        ps.team_id,
        ps.pts_season_avg,
        ps.reb_season_avg,
        ps.ast_season_avg,
        MAX(g.game_date) AS last_game_date
    FROM `project-71e6f4ed-bf24-4c0f-bb0.intermediate.int_player_rolling_averages` ps
    INNER JOIN `project-71e6f4ed-bf24-4c0f-bb0.staging.stg_games` g
        ON ps.game_id = g.game_id
    GROUP BY ps.player_name, ps.team_id, ps.pts_season_avg, ps.reb_season_avg, ps.ast_season_avg
),

today_active AS (
    -- Players NOT in today's injury report = active tonight
    SELECT DISTINCT team_id, player_name
    FROM `project-71e6f4ed-bf24-4c0f-bb0.staging.stg_player_game_stats` pgs
    INNER JOIN `project-71e6f4ed-bf24-4c0f-bb0.staging.stg_games` g
        ON pgs.game_id = g.game_id
    WHERE g.season = (SELECT MAX(season) FROM `project-71e6f4ed-bf24-4c0f-bb0.staging.stg_games`)
    AND NOT EXISTS (
        SELECT 1 FROM `project-71e6f4ed-bf24-4c0f-bb0.staging.stg_injuries` i
        WHERE i.player_name = pgs.player_name
        AND i.team_id = pgs.team_id
        AND i.game_date = CURRENT_DATE()
        AND i.status IN ('Out', 'Doubtful')
    )
)

SELECT
    lg.player_name,
    lg.team_id,
    lg.last_game_date,
    DATE_DIFF(CURRENT_DATE(), lg.last_game_date, DAY) AS days_since_last_game,
    lg.pts_season_avg,
    lg.reb_season_avg,
    lg.ast_season_avg,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), lg.last_game_date, DAY) >= 30 THEN 'LONG_ABSENCE'
        WHEN DATE_DIFF(CURRENT_DATE(), lg.last_game_date, DAY) >= 15 THEN 'MEDIUM_ABSENCE'
        WHEN DATE_DIFF(CURRENT_DATE(), lg.last_game_date, DAY) >= 7  THEN 'SHORT_ABSENCE'
        ELSE NULL
    END AS absence_tier
FROM last_game lg
INNER JOIN today_active ta
    ON lg.player_name = ta.player_name
    AND lg.team_id = ta.team_id
WHERE DATE_DIFF(CURRENT_DATE(), lg.last_game_date, DAY) >= 7
AND lg.pts_season_avg >= 15
