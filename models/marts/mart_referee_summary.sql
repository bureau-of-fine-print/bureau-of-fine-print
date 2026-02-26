with ref_games as (
    select
        ra.referee_1 as referee_id,
        g.game_id,
        g.season
    from {{ ref('stg_referee_assignments') }} ra
    inner join {{ ref('stg_games') }} g on ra.game_id = g.game_id
    union all
    select
        ra.referee_2 as referee_id,
        g.game_id,
        g.season
    from {{ ref('stg_referee_assignments') }} ra
    inner join {{ ref('stg_games') }} g on ra.game_id = g.game_id
    where ra.referee_2 is not null
    union all
    select
        ra.referee_3 as referee_id,
        g.game_id,
        g.season
    from {{ ref('stg_referee_assignments') }} ra
    inner join {{ ref('stg_games') }} g on ra.game_id = g.game_id
    where ra.referee_3 is not null
),

game_stats as (
    select * from {{ ref('int_game_scores') }}
),

game_pace as (
    select * from {{ ref('int_game_pace') }}
),

ref_with_stats as (
    select
        rg.referee_id,
        rg.season,
        gs.game_id,
        gs.game_date,
        gs.home_points + gs.away_points as total_points,
        gs.home_pf + gs.away_pf as total_fouls,
        gs.home_fta + gs.away_fta as total_fta,
        gs.home_3pa + gs.away_3pa as total_3pa,
        gs.home_tov + gs.away_tov as total_tov,
        gp.game_pace,
        case when gs.home_points > gs.away_points then 1 else 0 end as home_win
    from ref_games rg
    inner join game_stats gs on rg.game_id = gs.game_id
    inner join game_pace gp on rg.game_id = gp.game_id
),

career_stats as (
    select
        referee_id,
        count(*) as career_games,
        round(avg(total_points), 1) as career_avg_total_points,
        round(avg(total_fouls), 1) as career_avg_total_fouls,
        round(avg(total_fta), 1) as career_avg_total_fta,
        round(avg(total_3pa), 1) as career_avg_total_3pa,
        round(avg(total_tov), 1) as career_avg_total_tov,
        round(avg(game_pace), 1) as career_avg_pace,
        round(avg(home_win), 3) as career_home_win_pct,
        min(game_date) as first_game,
        max(game_date) as last_game
    from ref_with_stats
    group by referee_id
),

season_stats as (
    select
        referee_id,
        season,
        count(*) as season_games,
        round(avg(total_points), 1) as season_avg_total_points,
        round(avg(total_fouls), 1) as season_avg_total_fouls,
        round(avg(total_fta), 1) as season_avg_total_fta,
        round(avg(total_3pa), 1) as season_avg_total_3pa,
        round(avg(total_tov), 1) as season_avg_total_tov,
        round(avg(game_pace), 1) as season_avg_pace,
        round(avg(home_win), 3) as season_home_win_pct
    from ref_with_stats
    group by referee_id, season
),

current_season as (
    select * from season_stats
    qualify row_number() over (partition by referee_id order by season desc) = 1
),

dim_referees as (
    select * from `project-71e6f4ed-bf24-4c0f-bb0.seeds.dim_referees`
)

select
    cs.referee_id,
    dr.full_name as referee_name,

    -- Career
    c.career_games,
    c.career_avg_total_points,
    c.career_avg_total_fouls,
    c.career_avg_total_fta,
    c.career_avg_total_3pa,
    c.career_avg_total_tov,
    c.career_avg_pace,
    c.career_home_win_pct,
    c.first_game,
    c.last_game,

    -- Current season
    cs.season as current_season,
    cs.season_games,
    cs.season_avg_total_points,
    cs.season_avg_total_fouls,
    cs.season_avg_total_fta,
    cs.season_avg_total_3pa,
    cs.season_avg_total_tov,
    cs.season_avg_pace,
    cs.season_home_win_pct,

    -- vs league average (career)
    round(c.career_avg_total_points - avg(c.career_avg_total_points) over (), 1) as pts_vs_league_avg,
    round(c.career_avg_total_fouls - avg(c.career_avg_total_fouls) over (), 1) as fouls_vs_league_avg,
    round(c.career_avg_pace - avg(c.career_avg_pace) over (), 1) as pace_vs_league_avg,

    -- vs league average (season)
    round(cs.season_avg_total_points - avg(cs.season_avg_total_points) over (), 1) as season_pts_vs_league_avg,
    round(cs.season_avg_total_fouls - avg(cs.season_avg_total_fouls) over (), 1) as season_fouls_vs_league_avg,
    round(cs.season_avg_pace - avg(cs.season_avg_pace) over (), 1) as season_pace_vs_league_avg,

    -- Labels based on season stats with tightened thresholds
    case
        when cs.season_avg_total_fouls >= avg(cs.season_avg_total_fouls) over () + 2 then 'whistle_happy'
        when cs.season_avg_total_fouls <= avg(cs.season_avg_total_fouls) over () - 2 then 'let_them_play'
        else 'average'
    end as foul_tendency_label,

    case
        when cs.season_home_win_pct >= 0.65 then 'strong_home_bias'
        when cs.season_home_win_pct >= 0.58 then 'moderate_home_bias'
        when cs.season_home_win_pct <= 0.35 then 'strong_away_bias'
        when cs.season_home_win_pct <= 0.42 then 'moderate_away_bias'
        else 'neutral'
    end as home_bias_label,

    case
        when cs.season_avg_pace >= avg(cs.season_avg_pace) over () + 2 then 'fast_pace'
        when cs.season_avg_pace <= avg(cs.season_avg_pace) over () - 2 then 'slow_pace'
        else 'average_pace'
    end as pace_tendency_label,

    -- Sample size flag
    case
        when cs.season_games >= 20 then 'sufficient'
        when cs.season_games >= 10 then 'limited'
        else 'insufficient'
    end as sample_size_label

from current_season cs
inner join career_stats c on cs.referee_id = c.referee_id
left join dim_referees dr on cs.referee_id = cast(dr.referee_id as string)