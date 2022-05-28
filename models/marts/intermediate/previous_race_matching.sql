with program as (
    select * from {{ ref('stg_race_program') }}
),

program_horse as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

first_race as (
    select
        program.race_id,
        program.date,
        program_horse.horse_id

    from program_horse
    inner join program on program_horse.race_id = program.race_id
),

second_race as (
    select * from first_race
),

race_matching as (
    select
        first_race.race_id as first_race,
        first_race.horse_id,
        min(second_race.date) as date_of_next_race

    from first_race
    left join second_race on first_race.horse_id = second_race.horse_id and first_race.date < second_race.date

    group by 1, 2
),

final as (
    select
        race_matching.first_race,
        second_race.race_id as second_race,
        race_matching.horse_id

    from race_matching
    left join second_race on race_matching.date_of_next_race = second_race.date and race_matching.horse_id = second_race.horse_id
)

select * from final

-- hvorfor er den første høyere?
--select count(1) from final -- 36174
--select count(1) from {{ ref('stg_horse_in_race_program') }} -- 35756
