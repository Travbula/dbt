with horse_in_program as (
    select * from {{ ref('stg_horse_in_race_program') }}
),

horse_dim_rows as (
    select
        horse_id,
        horse_name,
        sex,
        color,
        father_name,
        mother_name,
        grandfather_name,
        breeder_name

    from horse_in_program
),

final as (
    -- todo: look into better order by here
    {{ 
        dbt_utils.deduplicate(
            relation='horse_dim_rows',
            partition_by='horse_id',
            order_by='horse_id asc',
        )
    }}

)

select * from final