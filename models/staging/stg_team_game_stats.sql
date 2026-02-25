with source as (
    select * from {{ source('raw', 'team_game_stats') }}
),

renamed as (
    select
        game_id,
        team_id,
        home_away,
        field_goals_made,
        field_goals_attempted,
        three_pointers_made,
        three_pointers_attempted,
        free_throws_made,
        free_throws_attempted,
        offensive_rebounds,
        defensive_rebounds,
        offensive_rebounds + defensive_rebounds as total_rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        personal_fouls,
        points,
        loaded_at
    from source
)

select * from renamed