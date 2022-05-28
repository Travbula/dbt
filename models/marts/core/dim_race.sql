with race as (
    select * from {{ ref('race') }}
)

select * from race
