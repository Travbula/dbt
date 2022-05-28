with program_meta as (
    select * from {{ source('rikstoto', 'rikstoto_program') }}
),

program as (
    {{ 
        dbt_utils.deduplicate(
            relation=source('rikstoto', 'rikstoto_program_result'),
            partition_by='_airbyte_result_hashid',
            order_by='_airbyte_normalized_at desc',
        )
    }}
),

horse as (
    {{ 
        dbt_utils.deduplicate(
            relation=source('rikstoto', 'rikstoto_program_result_starts'),
            partition_by='_airbyte_starts_hashid',
            order_by='_airbyte_normalized_at desc',
        )
    }}
),

final as (
    select
        program_meta.raceday_key || '_' || program.racenumber as race_id,
        horse.horseregistrationnumber as horse_id,
        horse.horsename as horse_name,
        horse.age,
        horse.sex,
        horse.color,
        horse.father as father_name,
        horse.mother as mother_name,
        horse.grandfather as grandfather_name,
        horse.breeder as breeder_name,
        horse.owner as owner_name,
        horse.trainer as trainer_name,
        horse.driver as driver_name,
        horse.driverid as driver_id,
        horse.shoes,
        horse.previousstartshoes as previous_start_shoes,
        horse.sulky,
        horse.previousstartsulky as previous_start_sulky,
        horse.recordauto as record_auto,
        horse.recordvolt as record_volt,
        horse.startnumber as start_number,
        horse.postposition as post_position,
        horse.extradistance as extra_distance,
        horse.totalearnings as total_earnings,
        horse.winpercentage as win_percentage,
        horse.triplepercentage as triple_percentage,
        horse.galloppercentage as gallop_percentage

    from horse
    left join program on horse._airbyte_result_hashid = program._airbyte_result_hashid
    left join program_meta on program._airbyte_rikstoto_program_hashid = program_meta._airbyte_rikstoto_program_hashid
)

select * from final
--where race_id = 'MO_NR_2021-12-01_6'

--{% if target.name == 'dev' %}
--where created_at >= dateadd('day', -3, current_date)
--{% endif %}