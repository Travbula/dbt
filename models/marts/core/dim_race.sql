with race_program as (
    select * from {{ ref('stg_race_program') }}
),

race_result as (
    select * from {{ ref('stg_race_result') }}
),

race_stats as (
    select * from {{ ref('race_stats') }}
),

final as (
    select
        distinct(race_program.race_id),
        race_program.racetrack_id,
        race_program.race_name,
        race_program.date,
        race_program.distance,
        race_program.race_form,
        race_program.start_method,
        race_result.track_state,
        race_stats.number_of_horses_to_start,
        race_stats.total_prize_pool,
        case
            when race_program.distance < 1500 then 'ultra sprint'
            when race_program.distance >= 1500 and race_program.distance < 2000 then 'sprint'
            when race_program.distance >= 2000 and race_program.distance < 2200 then 'regular'
            when race_program.distance >= 2200 and race_program.distance < 3000 then 'long'
            when race_program.distance >= 3000 then 'ultra long'
            else 'unknown'
        end as distance_segment

    from race_program
    inner join race_stats on race_program.race_id = race_stats.race_id -- left join instead? (this results in some null values because some complete_results are missing)
    left join race_result on race_program.race_id = race_result.race_id
)

select * from final

