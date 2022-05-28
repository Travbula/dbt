with program_horse as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

program as (
    select * from {{ ref('stg_race_program') }}
),

result_horse as (
    select * from {{ ref('stg_horse_result_in_race' )}}
),

race as (
    select * from {{ ref('race') }}
),

race_result as (
    select * from {{ ref('stg_race_result') }}
),

result_value as (
    select
        race.race_id,
        result_horse.horse_id,
        case when result_horse.finishing_position = 0 then 0 else (race.number_of_horses_to_start - result_horse.finishing_position) end as horses_beaten,
        case
            when result_horse.finishing_position = 0 then 0
            else 1 - (result_horse.finishing_position - 1)/race.number_of_horses_to_start
        end as result_score

    from result_horse
    left join race on result_horse.race_id = race.race_id
),

final as (
    select
        result_horse.race_id,
        program.racetrack_id,
        result_horse.horse_id,
        result_horse.driver_id,
        program_horse.owner_name,
        program_horse.trainer_name,
        program_horse.breeder_name,
        result_horse.start_number,
        program_horse.extra_distance,
        program_horse.shoes,
        program_horse.sulky,
        result_horse.finishing_position,
        result_horse.prize,
        result_horse.kmtime,
        result_horse.odds as win_odds,
        result_value.horses_beaten,
        result_value.result_score,
        result_horse.galloped,
        result_horse.paced,
        result_horse.timed,
        result_horse.distanced,
        result_horse.broke_race,
        result_horse.disqualified,
        case when result_horse.finishing_position = 1 then 1 else 0 end as first_place,
        case when result_horse.finishing_position = 2 then 1 else 0 end as second_place,
        case when result_horse.finishing_position = 3 then 1 else 0 end as third_place,
        case when result_horse.finishing_position = 1 then race_result.winning_distance else null end as winning_distance,
        case when result_horse.finishing_position = 1 then race_result.last_500_meters_time else null end as last_500m_time,
        case when race_result.first_500_meters_horse_name = program_horse.start_number || ' ' || result_horse.horse_name then 1 else null end as lead_after_500m,
        case when race_result.first_500_meters_horse_name = program_horse.start_number || ' ' || result_horse.horse_name then race_result.first_500_meters_time else null end as lead_after_500m_time,
        case when race_result.first_1000_meters_horse_name = program_horse.start_number || ' ' || result_horse.horse_name then 1 else null end as lead_after_1000m,
        case when race_result.first_1000_meters_horse_name = program_horse.start_number || ' ' || result_horse.horse_name then race_result.first_1000_meters_time else null end as lead_after_1000m_time
        

    from program_horse
    inner join result_horse on program_horse.race_id = result_horse.race_id and program_horse.horse_id = result_horse.horse_id
    left join program on program_horse.race_id = program.race_id
    left join result_value on program_horse.race_id = result_value.race_id and program_horse.horse_id = result_value.horse_id
    left join race_result on program_horse.race_id = race_result.race_id
)

select * from final