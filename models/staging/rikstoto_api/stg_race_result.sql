select
    --request.raceday_key as raceday_id,
    {{ dbt_utils.split_part(string_text='request.raceday_key', delimiter_text="'/'", part_number=1) }} || '_' || 
        {{ dbt_utils.split_part(string_text='request.raceday_key', delimiter_text="'/'", part_number=2) }} as race_id,
    resultat.distance,
    resultat.trackstate as track_state,
    resultat.startmethod as start_method,
    resultat.winningdistance as winning_distance,
    resultat.last500meterstime as last_500_meters_time,
    resultat.first500meterstime as first_500_meters_time,
    resultat.first1000meterstime as first_1000_meters_time,
    resultat.first500metershorsename as first_500_meters_horse_name,
    resultat.first1000metershorsename as first_1000_meters_horse_name

from {{ source('rikstoto', 'complete_results_result') }} resultat

left join {{ source('rikstoto', 'complete_results') }} request ON resultat._airbyte_complete_results_hashid = request._airbyte_complete_results_hashid