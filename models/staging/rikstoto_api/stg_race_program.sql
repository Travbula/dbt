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
    race_program.propositions,
    race_program.ismonte as is_monte

from {{ source('rikstoto', 'rikstoto_program_result') }} AS race_program
left join {{ source('rikstoto', 'rikstoto_program') }} AS race_program_meta ON race_program._airbyte_rikstoto_program_hashid = race_program_meta._airbyte_rikstoto_program_hashid