{{ config(materialized='table') }}

with race as (
    select race_id as id from {{ ref('stg_race_program') }}
    where start_method = 'Auto'
),

result_horse as (
    select * from {{ ref('horse_result') }} as horse_result
    inner join race on race.id = horse_result.race_id
),

start_number_10_post_position as (
    select race_id, 10 as start_number, 8 + count(1) as post_position
    from result_horse
    where start_number <= 10 and start_number >= 9
    group by race_id
),

start_number_11_post_position as (
    select race_id, 11 as start_number, 8 + count(1) as post_position
    from result_horse
    where start_number <= 11 and start_number >= 9
    group by race_id
),

start_number_12_post_position as (
    select race_id, 12 as start_number, 8 + count(1) as post_position
    from result_horse
    where start_number <= 12 and start_number >= 9
    group by race_id
),

start_number_14_post_position as (
    select race_id, 14 as start_number, 12 + count(1) as post_position
    from result_horse
    where start_number <= 14 and start_number >= 13
    group by race_id
),

start_number_15_post_position as (
    select race_id, 15 as start_number, 12 + count(1) as post_position
    from result_horse
    where start_number <= 15 and start_number >= 13
    group by race_id
),

final as (
    select
        result_horse.race_id,
        result_horse.horse_id,
        result_horse.start_number,
        case 
            when result_horse.start_number = 9 then 9
            when result_horse.start_number = 10 then ten.post_position
            when result_horse.start_number = 11 then eleven.post_position
            when result_horse.start_number = 12 then twelve.post_position
            when result_horse.start_number = 13 then 13
            when result_horse.start_number = 14 then fourteen.post_position
            when result_horse.start_number = 15 then fifteen.post_position
            else null
        end as post_position

    from result_horse
    left join start_number_10_post_position as ten on ten.race_id = result_horse.race_id and ten.start_number = result_horse.start_number
    left join start_number_11_post_position as eleven on eleven.race_id = result_horse.race_id and eleven.start_number = result_horse.start_number
    left join start_number_12_post_position as twelve on twelve.race_id = result_horse.race_id and twelve.start_number = result_horse.start_number

    left join start_number_14_post_position as fourteen on fourteen.race_id = result_horse.race_id and fourteen.start_number = result_horse.start_number
    left join start_number_15_post_position as fifteen on fifteen.race_id = result_horse.race_id and fifteen.start_number = result_horse.start_number
)

select * from final
where final.post_position is not null
