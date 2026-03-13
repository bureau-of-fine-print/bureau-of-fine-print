WITH game_ats AS (
    SELECT * FROM {{ ref('int_game_ats') }}
    WHERE ats_result IS NOT NULL
),

ref_assignments AS (
    SELECT * FROM {{ ref('stg_referee_assignments') }}
),

home_games AS (
    SELECT
        g.home_team_id                                          AS team_id,
        ra.referee_1                                            AS referee_id,
        g.game_id,
        g.season,
        'home'                                                  AS home_away,
        CASE WHEN g.ats_result = 'home_covered' THEN 1
             WHEN g.ats_result = 'push' THEN NULL
             ELSE 0 END                                         AS covered,
        CASE WHEN g.ou_result = 'over' THEN 1
             WHEN g.ou_result = 'push' THEN NULL
             ELSE 0 END                                         AS went_over,
        g.ats_margin,
        g.total_vs_line
    FROM game_ats g
    INNER JOIN ref_assignments ra ON g.game_id = ra.game_id
    WHERE ra.referee_1 IS NOT NULL
),

away_games AS (
    SELECT
        g.away_team_id                                          AS team_id,
        ra.referee_1                                            AS referee_id,
        g.game_id,
        g.season,
        'away'                                                  AS home_away,
        CASE WHEN g.ats_result = 'away_covered' THEN 1
             WHEN g.ats_result = 'push' THEN NULL
             ELSE 0 END                                         AS covered,
        CASE WHEN g.ou_result = 'over' THEN 1
             WHEN g.ou_result = 'push' THEN NULL
             ELSE 0 END                                         AS went_over,
        g.ats_margin * -1                                       AS ats_margin,
        g.total_vs_line
    FROM game_ats g
    INNER JOIN ref_assignments ra ON g.game_id = ra.game_id
    WHERE ra.referee_1 IS NOT NULL
),

combined AS (
    SELECT * FROM home_games
    UNION ALL
    SELECT * FROM away_games
),

current_season AS (
    SELECT MAX(season) AS season FROM combined
)

SELECT
    c.team_id,
    c.referee_id,
    COUNT(*)                                                    AS games_with_ref,
    SUM(covered)                                               AS ats_wins,
    COUNT(covered) - SUM(covered)                              AS ats_losses,
    ROUND(AVG(covered), 3)                                     AS ats_win_pct,
    SUM(went_over)                                             AS overs,
    COUNT(went_over) - SUM(went_over)                          AS unders,
    ROUND(AVG(went_over), 3)                                   AS over_pct,
    ROUND(AVG(ats_margin), 2)                                  AS avg_ats_margin,
    ROUND(AVG(total_vs_line), 2)                               AS avg_total_vs_line
FROM combined c
CROSS JOIN current_season cs
WHERE c.season = cs.season
GROUP BY c.team_id, c.referee_id