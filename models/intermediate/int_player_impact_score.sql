WITH current_team_games AS (
    SELECT
        ps.player_name,
        ps.team_id,
        COUNT(DISTINCT ps.game_id) AS games_with_current_team
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
        mps.player_name,
        mps.team_id,
        mps.games_played,
        ctg.games_with_current_team,
        mps.pts_season_avg,
        mps.reb_season_avg,
        mps.ast_season_avg,
        mps.stl_season_avg,
        mps.blk_season_avg,
        mps.tov_season_avg,
        mps.plus_minus_season_avg,
        mps.pts_last5,
        mps.reb_last5,
        mps.ast_last5,
        mps.is_pts_hot_last5,
        mps.is_pts_cold_last5,
        mps.hot_game_streak,
        mps.below_avg_streak,
        ROUND(
            mps.pts_season_avg
            + (mps.reb_season_avg * 1.2)
            + (mps.ast_season_avg * 1.5)
            + (mps.stl_season_avg * 3.0)
            + (mps.blk_season_avg * 2.5)
        , 2) AS impact_score
    FROM {{ ref('mart_player_summary') }} mps
    JOIN current_team_games ctg
        ON mps.player_name = ctg.player_name
        AND mps.team_id = ctg.team_id
),

ranked AS (
    SELECT
        ps.*,
        tsg.games_played_this_season,
        LEAST(5, CEIL(tsg.games_played_this_season * 0.10)) AS min_games_threshold,
        ROW_NUMBER() OVER (
            PARTITION BY ps.team_id
            ORDER BY ps.impact_score DESC
        ) AS team_rank
    FROM player_stats ps
    JOIN team_season_games tsg ON ps.team_id = tsg.team_id
    WHERE ps.games_with_current_team >= LEAST(5, CEIL(tsg.games_played_this_season * 0.10))
)

SELECT
    player_name,
    team_id,
    games_played,
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