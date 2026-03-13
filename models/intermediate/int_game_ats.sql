WITH normalized_lines AS (
    -- Historical bulk import — normalize BKN→BRK, CHA→CHO
    SELECT
        game_date, season,
        CASE home_team WHEN 'BKN' THEN 'BRK' WHEN 'CHA' THEN 'CHO' ELSE home_team END AS home_team,
        CASE away_team WHEN 'BKN' THEN 'BRK' WHEN 'CHA' THEN 'CHO' ELSE away_team END AS away_team,
        over_under, home_line,
        loaded_at AS line_timestamp,
        'betting_lines' AS source,
        1 AS source_priority  -- lowest priority, use only if nothing else
    FROM {{ ref('stg_betting_lines') }}

    UNION ALL

    -- Opening lines — use if no closing line available
    SELECT
        game_date, season, home_team, away_team,
        over_under, home_line,
        captured_at AS line_timestamp,
        'opening_lines' AS source,
        2 AS source_priority
    FROM {{ ref('stg_opening_lines') }}

    UNION ALL

    -- Closing lines — always prefer these
    SELECT
        game_date, season, home_team, away_team,
        over_under, home_line,
        captured_at AS line_timestamp,
        'closing_lines' AS source,
        3 AS source_priority  -- highest priority
    FROM {{ ref('stg_closing_lines') }}
),

deduped_lines AS (
    SELECT *
    FROM normalized_lines
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY game_date, home_team, away_team
        ORDER BY source_priority DESC, line_timestamp DESC
    ) = 1
),

game_ats AS (
    SELECT
        g.game_id,
        g.game_date,
        g.season,
        g.home_team_id,
        g.away_team_id,
        l.home_line,
        l.over_under,
        l.source AS line_source,  -- useful for debugging later
        gr.home_score,
        gr.away_score,
        gr.home_score - gr.away_score                            AS actual_margin,
        gr.home_score + gr.away_score                            AS actual_total,

        CASE
            WHEN gr.home_score IS NULL THEN NULL
            WHEN (gr.home_score - gr.away_score) + l.home_line > 0 THEN 'home_covered'
            WHEN (gr.home_score - gr.away_score) + l.home_line < 0 THEN 'away_covered'
            ELSE 'push'
        END                                                      AS ats_result,

        CASE
            WHEN gr.home_score IS NULL THEN NULL
            WHEN (gr.home_score + gr.away_score) > l.over_under THEN 'over'
            WHEN (gr.home_score + gr.away_score) < l.over_under THEN 'under'
            ELSE 'push'
        END                                                      AS ou_result,

        ROUND((gr.home_score - gr.away_score) + l.home_line, 1) AS ats_margin,
        ROUND((gr.home_score + gr.away_score) - l.over_under, 1) AS total_vs_line

    FROM {{ ref('stg_games') }} g
    INNER JOIN deduped_lines l
        ON g.game_date = l.game_date
        AND g.home_team_id = l.home_team
        AND g.away_team_id = l.away_team
    LEFT JOIN {{ ref('stg_game_results') }} gr
        ON g.game_id = gr.game_id
)

SELECT * FROM game_ats