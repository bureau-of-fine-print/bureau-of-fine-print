WITH deduped AS (
    SELECT
        game_date,
        game_time,
        matchup,
        team_id,
        player_name,
        status,
        reason,
        pdf_url,
        scraped_at,
        ROW_NUMBER() OVER (
            PARTITION BY game_date, team_id, player_name
            ORDER BY scraped_at DESC
        ) AS rn
    FROM `project-71e6f4ed-bf24-4c0f-bb0.raw.injuries`
)

SELECT
    game_date,
    game_time,
    matchup,
    team_id,
    player_name,
    status,
    reason,
    pdf_url,
    scraped_at
FROM deduped
WHERE rn = 1