-- mlb_int_player_current_team.sql
-- Resolves each player's current team using the most recent
-- signal from either game logs or transactions.
-- Handles mid-season trades (Anthony Davis problem) by checking
-- transactions even if the player hasn't played for new team yet.
-- One row per player.

with pitcher_logs as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_date,
        'pitcher_log'   as source
    from {{ ref('mlb_stg_game_pitcher_logs') }}
),

batter_logs as (
    select
        player_id,
        player_name,
        team_id,
        team_abbr,
        game_date,
        'batter_log'    as source
    from {{ ref('mlb_stg_game_batter_logs') }}
    where is_starter = true
),

transactions as (
    select
        player_id,
        player_name,
        to_team_id      as team_id,
        to_team_abbr    as team_abbr,
        transaction_date as game_date,
        'transaction'   as source
    from {{ ref('mlb_stg_transactions') }}
    where is_trade = true
       or is_callup = true
    and to_team_id is not null
),

-- Union all sources with effective date
all_sources as (
    select * from pitcher_logs
    union all
    select * from batter_logs
    union all
    select * from transactions
),

-- Take the most recent team signal per player
final as (
    select
        player_id,
        player_name,
        team_id         as current_team_id,
        team_abbr       as current_team_abbr,
        game_date       as effective_date,
        source          as team_source
    from all_sources
    where team_id is not null
    qualify row_number() over (
        partition by player_id
        order by game_date desc, source desc
        -- transactions rank higher than game logs on same date
    ) = 1
)

select * from final