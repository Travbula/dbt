with program_horse as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

program as (
    select * from {{ ref('stg_race_program') }}
),

result_horse as (
    select * from {{ ref('stg_horse_result_in_race' )}}
),

final as (
    select
        result_horse.race_id,
        result_horse.horse_id,
        result_horse.driver_id,
        program_horse.owner_name,
        program_horse.trainer_name,
        result_horse.start_number,
        program_horse.extra_distance,
        program_horse.shoes,
        program_horse.sulky,
        result_horse.finishing_position,
        result_horse.prize,
        result_horse.kmtime,
        result_horse.odds as win_odds,
        case when result_horse.finishing_position = 1 then true else false end as first_place,
        case when result_horse.finishing_position = 2 then true else false end as second_place,
        case when result_horse.finishing_position = 3 then true else false end as third_place
        
    from program_horse
    left join program on program_horse.race_id = program.race_id
    inner join result_horse on program_horse.race_id = result_horse.race_id and program_horse.horse_id = result_horse.horse_id
)

select * from final