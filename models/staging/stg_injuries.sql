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
    INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
    REGEXP_REPLACE(REGEXP_REPLACE(
        player_name,
        r'ņ', 'n'), r'ģ', 'g'),
        r'[čć]|Ä\x87|Ä\x8d', 'c'), r'[šŠ]|Å\xa1', 's'),
        r'[žŽ]|Å\xbe', 'z'), r'[đĐ]|Ä\x91', 'd'),
        r'[şŞ]', 's'), r'[üÜ]', 'u'),
        r'[öÖ]', 'o'), r'[çÇ]', 'c'),
        r'[ğĞ]', 'g'), r'[ıİ]', 'i'),
        r'[àáâãäåÀÁÂÃÄÅ]', 'a'), r'[èéêëÈÉÊË]', 'e'),
        r'Å', 'S'), r'Ã¼', 'u'),
        r'[^\x00-\x7F]', ''),
        r'\s+(III|II|IV|V|II)$', '')) AS player_name,
    status,
    reason,
    pdf_url,
    scraped_at
FROM deduped
WHERE rn = 1
