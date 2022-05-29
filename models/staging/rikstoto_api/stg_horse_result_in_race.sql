with race_meta as (
    {{ 
        dbt_utils.deduplicate(
            relation=source('rikstoto', 'complete_results'),
            partition_by='_airbyte_complete_results_hashid',
            order_by='_airbyte_normalized_at desc',
        )
    }}
),

race as (
    {{ 
        dbt_utils.deduplicate(
            relation=source('rikstoto', 'complete_results_result'),
            partition_by='_airbyte_result_hashid',
            order_by='_airbyte_normalized_at desc',
        )
    }}
),  

horse as (
    {{ 
        dbt_utils.deduplicate(
            relation=source('rikstoto', 'complete_results_result_results'),
            partition_by='_airbyte_results_hashid',
            order_by='_airbyte_normalized_at desc',
        )
    }}
),

full_race as (
    select
        {{ dbt_utils.split_part(string_text='race_meta.raceday_key', delimiter_text="'/'", part_number=1) }} || '_' || 
        {{ dbt_utils.split_part(string_text='race_meta.raceday_key', delimiter_text="'/'", part_number=2) }} as race_id,
        race_meta.date,
        race._airbyte_result_hashid

    from race_meta
    left join race on race._airbyte_complete_results_hashid = race_meta._airbyte_complete_results_hashid
),

final as (
    select
        full_race.race_id,
        full_race.date,
        horse.horseregistrationnumber as horse_id,
        horse.horsename as horse_name,
        horse.startnumber as start_number,
        horse.postposition as post_position,
        horse.order as finishing_position,
        horse.place,
        coalesce(horse.prize, 0) / 100 AS prize,
        horse.distance,
        horse.driverid as driver_id,
        horse.drivername as driver_name,
        horse.drivershortname as driver_short_name,
        horse.odds / 10 AS win_odds,
        --horse.kmtime,
        regexp_extract(horse.kmtime, '[0-9]+,[0-9]+') as kmtime,
        case when regexp_contains(horse.kmtime, 'g') then 1 else 0 end as galloped,
        case when regexp_contains(horse.kmtime, 'p') then 1 else 0 end as paced,
        case when regexp_contains(horse.kmtime, 'br') then 1 else 0 end as broke_race,
        case when regexp_contains(horse.kmtime, '(nc|temp)') then 1 else 0 end as timed,
        case when regexp_contains(horse.kmtime, 'dist') then 1 else 0 end as distanced,
        case when length(horse.kmtime) = 2 and regexp_contains(horse.kmtime, 'd') then 1 else 0 end as disqualified

    from horse
    left join full_race ON horse._airbyte_result_hashid = full_race._airbyte_result_hashid
)

select * from final
