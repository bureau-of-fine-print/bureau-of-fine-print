with games as (
    select * from {{ ref('int_game_scores') }}
),

broadcasts as (
    select * from {{ ref('stg_broadcast_schedule') }}
)

select
    g.game_id,
    g.game_date,
    g.home_team_id,
    g.away_team_id,
    coalesce(b.is_national_broadcast, false) as is_national_broadcast
from games g
left join broadcasts b on g.game_id = b.game_id