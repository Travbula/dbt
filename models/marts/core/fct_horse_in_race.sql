with program_horse as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

program as (
    select * from {{ ref('stg_race_program') }}
),

horse_result as (
    select * from {{ ref('horse_result') }}
),

final as (
    select
        horse_result.race_id,
        program.racetrack_id,
        horse_result.horse_id,
        horse_result.driver_id,
        program_horse.owner_name,
        program_horse.trainer_name,
        program_horse.breeder_name,
        horse_result.start_number,
        program_horse.extra_distance,
        program_horse.shoes,
        program_horse.sulky,
        horse_result.finishing_position,
        horse_result.prize,
        horse_result.kmtime,
        horse_result.win_odds,
        horse_result.horses_beaten,
        horse_result.result_score,
        horse_result.galloped,
        horse_result.paced,
        horse_result.timed,
        horse_result.distanced,
        horse_result.broke_race,
        horse_result.disqualified,
        horse_result.first_place,
        horse_result.second_place,
        horse_result.third_place,
        horse_result.winning_distance,
        horse_result.last_500m_time,
        horse_result.lead_after_500m,
        horse_result.lead_after_500m_time,
        horse_result.lead_after_1000m,
        horse_result.lead_after_1000m_time
        

    from horse_result
    left join program_horse on program_horse.race_id = horse_result.race_id and program_horse.horse_id = horse_result.horse_id
    left join program on program_horse.race_id = program.race_id
)

select * from final