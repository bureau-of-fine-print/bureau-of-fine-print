with source as (
    select * from {{ source('raw', 'player_game_stats') }}
),

renamed as (
    select
        game_id,
        player_name,
        team_id,
        starter,
        -- Convert minutes from "MM:SS" string to decimal minutes
        CASE 
            WHEN minutes IS NULL OR minutes = '' THEN 0.0
            WHEN STRPOS(minutes, ':') > 0 THEN
                CAST(SPLIT(minutes, ':')[OFFSET(0)] AS FLOAT64) + 
                CAST(SPLIT(minutes, ':')[OFFSET(1)] AS FLOAT64) / 60
            ELSE CAST(minutes AS FLOAT64)
        END as minutes_played,
        points,
        rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        personal_fouls,
        field_goals_made,
        field_goals_attempted,
        three_pointers_made,
        three_pointers_attempted,
        free_throws_made,
        free_throws_attempted,
        plus_minus,
        loaded_at
    from source
)

select * from renamed