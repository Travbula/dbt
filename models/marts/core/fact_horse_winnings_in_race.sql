select
    horse.raceday_key,
    horse.horseregistrationnumber,
    horse.horsename,
    horse.prize

from {{ ref('stg_horse_result_in_race') }} as horse