with race_program as (
    select * from {{ ref('stg_race_program') }}
),

horses_in_race_count as (
    select
        race_id,
        count(1) as number_of_horses_to_start
    
    from {{ ref('stg_horse_result_in_race') }}
   -- where kmtime is not null
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
        horses_in_race_count.number_of_horses_to_start,
        race_program.propositions -- todo: split this into stuff (prizes and class? and kaldblods/varmblods)

    from race_program
    inner join horses_in_race_count on race_program.race_id = horses_in_race_count.race_id -- left join instead? (this results in some null values because some complete_results are missing)
)

select * from final

