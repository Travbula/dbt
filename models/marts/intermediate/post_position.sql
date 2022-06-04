with post_position as (
    select * from {{ ref('post_position_first_row') }}
    union all
    select * from {{ ref('post_position_behind') }}
),

final as (
    select * from post_position
)

select * from final