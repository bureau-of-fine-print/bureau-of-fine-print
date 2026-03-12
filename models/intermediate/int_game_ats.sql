WITH best_lines AS (
    SELECT
        game_date, season, home_team, away_team,
        over_under, home_line,
        loaded_at AS line_timestamp
    FROM {{ ref('stg_betting_lines') }}
    
    UNION ALL
    
    SELECT
        game_date, season, home_team, away_team,
        over_under, home_line,
        captured_at AS line_timestamp
    FROM {{ ref('stg_opening_lines') }}
    
    UNION ALL
    
    SELECT
        game_date, season, home_team, away_team,
        over_under, home_line,
        captured_at AS line_timestamp
    FROM {{ ref('stg_closing_lines') }}
),

deduped_lines AS (
    SELECT *
    FROM best_lines
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY game_date, home_team, away_team
        ORDER BY line_timestamp DESC
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
        gr.home_score,
        gr.away_score,
        gr.home_score - gr.away_score                           AS actual_margin,
        gr.home_score + gr.away_score                           AS actual_total,

        -- ATS result from home team perspective
        -- home_line negative = home favored, so home needs to win by more than ABS(home_line)
        CASE
            WHEN gr.home_score IS NULL THEN NULL
            WHEN (gr.home_score - gr.away_score) + l.home_line > 0 THEN 'home_covered'
            WHEN (gr.home_score - gr.away_score) + l.home_line < 0 THEN 'away_covered'
            ELSE 'push'
        END                                                     AS ats_result,

        -- Over/under result
        CASE
            WHEN gr.home_score IS NULL THEN NULL
            WHEN (gr.home_score + gr.away_score) > l.over_under THEN 'over'
            WHEN (gr.home_score + gr.away_score) < l.over_under THEN 'under'
            ELSE 'push'
        END                                                     AS ou_result,

        -- Margin vs spread (positive = home covered by this much)
        ROUND((gr.home_score - gr.away_score) + l.home_line, 1) AS ats_margin,

        -- Total vs line (positive = went over by this much)
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
