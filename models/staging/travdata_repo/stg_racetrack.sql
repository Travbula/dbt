with racetrack as (
    select * from {{ source('travdata_repo', 'racetracks') }}
),

final as (
    select
        racetrack_id,
        name,
        distance,
        open_stretch,
        municipality,
        opening_year
    
    from racetrack
)

select * from final