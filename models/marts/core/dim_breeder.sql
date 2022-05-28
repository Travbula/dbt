with program_row as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

final as (
    select distinct(breeder_name) from program_row
)

select * from final