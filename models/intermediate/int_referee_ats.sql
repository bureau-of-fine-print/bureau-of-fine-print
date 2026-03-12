WITH game_ats AS (
    SELECT * FROM {{ ref('int_game_ats') }}
    WHERE ats_result IS NOT NULL
),

ref_assignments AS (
    SELECT ra.*, g.season
    FROM {{ ref('stg_referee_assignments') }} ra
    INNER JOIN {{ ref('stg_games') }} g ON ra.game_id = g.game_id
),

current_season AS (
    SELECT MAX(season) AS season
    FROM {{ ref('stg_games') }}
)

SELECT
    ra.referee_1                                                AS referee_id,
    COUNT(*)                                                    AS games_officiated,

    -- Career ATS record
    COUNTIF(ga.ats_result = 'home_covered')                    AS home_ats_wins,
    COUNTIF(ga.ats_result = 'away_covered')                    AS away_ats_wins,
    COUNTIF(ga.ats_result = 'push')                            AS ats_pushes,
    ROUND(
        COUNTIF(ga.ats_result = 'home_covered') /
        NULLIF(COUNTIF(ga.ats_result != 'push'), 0)
    , 3)                                                        AS home_ats_pct,

    -- Career over/under record
    COUNTIF(ga.ou_result = 'over')                             AS overs,
    COUNTIF(ga.ou_result = 'under')                            AS unders,
    COUNTIF(ga.ou_result = 'push')                             AS ou_pushes,
    ROUND(
        COUNTIF(ga.ou_result = 'over') /
        NULLIF(COUNTIF(ga.ou_result != 'push'), 0)
    , 3)                                                        AS over_pct,

    -- Career averages
    ROUND(AVG(ga.ats_margin), 2)                               AS avg_ats_margin,
    ROUND(AVG(ga.total_vs_line), 2)                            AS avg_total_vs_line,
    ROUND(AVG(ga.actual_total), 1)                             AS avg_actual_total,
    ROUND(AVG(ga.over_under), 1)                               AS avg_line_total,

    -- Current season over/under record
    COUNTIF(ga.ou_result = 'over' AND ra.season = cs.season)   AS current_season_overs,
    COUNTIF(ga.ou_result = 'under' AND ra.season = cs.season)  AS current_season_unders,
    COUNTIF(ga.ou_result = 'push' AND ra.season = cs.season)   AS current_season_ou_pushes,
    COUNTIF(ra.season = cs.season AND ga.ou_result != 'push')  AS current_season_ou_games,
    ROUND(
        COUNTIF(ga.ou_result = 'over' AND ra.season = cs.season) /
        NULLIF(COUNTIF(ra.season = cs.season AND ga.ou_result != 'push'), 0)
    , 3)                                                        AS current_season_over_pct,
    ROUND(
        AVG(CASE WHEN ra.season = cs.season THEN ga.total_vs_line END)
    , 2)                                                        AS current_season_avg_total_vs_line

FROM ref_assignments ra
INNER JOIN game_ats ga ON ra.game_id = ga.game_id
CROSS JOIN current_season cs
WHERE ra.referee_1 IS NOT NULL
GROUP BY ra.referee_1, cs.season