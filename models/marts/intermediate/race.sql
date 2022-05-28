with race_program as (
    select * from {{ ref('stg_race_program') }}
),

race_result as (
    select * from {{ ref('stg_race_result') }}
),

horses_in_race_count as (
    select
        race_id,
        count(1) as number_of_horses_to_start,
        sum(prize) as total_prize_pool
    
    from {{ ref('stg_horse_result_in_race') }}
    where odds <> 0 -- er dette riktig? antar nå at hvis odds er 0 så starter ikke hesten i løpet
    --where kmtime is not null
    group by 1
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
        horses_in_race_count.number_of_horses_to_start,
        horses_in_race_count.total_prize_pool

    from race_program
    inner join horses_in_race_count on race_program.race_id = horses_in_race_count.race_id -- left join instead? (this results in some null values because some complete_results are missing)
    left join race_result on race_program.race_id = race_result.race_id
)

select * from final

