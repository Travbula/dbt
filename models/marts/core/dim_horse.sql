with horse_in_program as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

final as (
    select
        distinct(horse_id),
        horse_name,
        sex,
        color,
        father_name,
        mother_name,
        grandfather_name,
        breeder_name

    from horse_in_program
)

select * from final