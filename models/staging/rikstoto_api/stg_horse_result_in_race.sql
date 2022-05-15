select
    horse.horseregistrationnumber,
    horse.horsename,
    race_meta.raceday_key,
    horse.startnumber,
    horse.postposition,
    horse."order" AS finishing_position,
    horse.place,
    horse.prize / 100 AS prize,
    horse.distance,
    horse.kmtime,
    horse.driverid,
    horse.drivername,
    horse.drivershortname,
    horse.odds / 10 AS odds

from {{ source('rikstoto', 'complete_results_result_results') }} horse
left join {{ source('rikstoto', 'complete_results_result') }} race ON horse._airbyte_result_hashid = race._airbyte_result_hashid
left join {{ source('rikstoto', 'complete_results') }} race_meta ON race._airbyte_complete_results_hashid = race_meta._airbyte_complete_results_hashid