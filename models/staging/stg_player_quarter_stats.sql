with source as (
    select * from {{ source('raw', 'player_quarter_stats') }}
),

renamed as (
    select
        game_id,
        player_name,
        team_id,
        quarter,
        minutes,
        points,
        offensive_rebounds,
        defensive_rebounds,
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