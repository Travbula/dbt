select
    request.raceday_key,
    resultat.distance,
    resultat.trackstate,
    resultat.startmethod,
    resultat.winningdistance,
    resultat.last500meterstime,
    resultat.first500meterstime,
    resultat.first1000meterstime,
    resultat.first500metershorsename,
    resultat.first1000metershorsename

from {{ source('rikstoto', 'complete_results_result') }} resultat

left join {{ source('rikstoto', 'complete_results') }} request ON resultat._airbyte_complete_results_hashid = request._airbyte_complete_results_hashid