{{ config(materialized='table') }}

with race as (
    select race_id as id from {{ ref('stg_race_program') }}
    where start_method = 'Auto'
),

result_horse as (
    select * from {{ ref('horse_result') }} as horse_result
    inner join race on race.id = horse_result.race_id
),

start_number_2_post_position as (
    select race_id, 2 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 2
    group by race_id
),

start_number_3_post_position as (
    select race_id, 3 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 3
    group by race_id
),

start_number_4_post_position as (
    select race_id, 4 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 4
    group by race_id
),

start_number_5_post_position as (
    select race_id, 5 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 5
    group by race_id
),

start_number_6_post_position as (
    select race_id, 6 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 6
    group by race_id
),

start_number_7_post_position as (
    select race_id, 7 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 7
    group by race_id
),

start_number_8_post_position as (
    select race_id, 8 as start_number, count(1) as post_position
    from result_horse
    where start_number <= 8
    group by race_id
),

final as (
    select
        result_horse.race_id,
        result_horse.horse_id,
        result_horse.start_number,
        case 
            when result_horse.start_number = 1 then 1
            when result_horse.start_number = 2 then two.post_position
            when result_horse.start_number = 3 then three.post_position
            when result_horse.start_number = 4 then four.post_position
            when result_horse.start_number = 5 then five.post_position
            when result_horse.start_number = 6 then six.post_position
            when result_horse.start_number = 7 then seven.post_position
            when result_horse.start_number = 8 then eight.post_position
            else null
        end as post_position

    from result_horse
    left join start_number_2_post_position as two on two.race_id = result_horse.race_id and two.start_number = result_horse.start_number
    left join start_number_3_post_position as three on three.race_id = result_horse.race_id and three.start_number = result_horse.start_number
    left join start_number_4_post_position as four on four.race_id = result_horse.race_id and four.start_number = result_horse.start_number
    left join start_number_5_post_position as five on five.race_id = result_horse.race_id and five.start_number = result_horse.start_number
    left join start_number_6_post_position as six on six.race_id = result_horse.race_id and six.start_number = result_horse.start_number
    left join start_number_7_post_position as seven on seven.race_id = result_horse.race_id and seven.start_number = result_horse.start_number
    left join start_number_8_post_position as eight on eight.race_id = result_horse.race_id and eight.start_number = result_horse.start_number
)

select * from final
where post_position is not null
