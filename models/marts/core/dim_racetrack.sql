with race_program as (
    select * from {{ ref('stg_race_program') }}
),

racetrack as (
    select * from {{ ref('stg_racetrack') }}
),

final as (
    select distinct(race_program.racetrack_id),
    racetrack.distance,
    racetrack.open_stretch,
    racetrack.name,
    racetrack.opening_year,
    racetrack.municipality
    
    from race_program
    left join racetrack on race_program.racetrack_id = racetrack.racetrack_id
)

select * from final