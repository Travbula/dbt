with race_program as (
    select * from {{ source('rikstoto', 'rikstoto_program_result') }}
),

race_program_meta as (
    select * from {{ source('rikstoto', 'rikstoto_program') }}
),

final as (
    select
        race_program_meta.raceday_key || '_' || race_program.racenumber as race_id,
        {{ dbt_utils.split_part(string_text='race_program_meta.raceday_key', delimiter_text="'_'", part_number=1) }} || '_' || 
        {{ dbt_utils.split_part(string_text='race_program_meta.raceday_key', delimiter_text="'_'", part_number=2) }} as racetrack_id,
        race_program_meta.date,
        race_program.racenumber as race_number,
        race_program.distance,
        race_program.raceform as race_form,
        race_program.racename as race_name,
        race_program.startmethod as start_method,
        race_program.ismonte as is_monte,
        race_program.propositions

    from race_program
    left join race_program_meta on race_program._airbyte_rikstoto_program_hashid = race_program_meta._airbyte_rikstoto_program_hashid
)

select * from final