with horse_results as (
    select * from {{ ref('stg_horse_result_in_race') }}
),

race_stats as (
    select
        race_id,
        count(1) as number_of_horses_to_start,
        sum(prize) as total_prize_pool
    
    from horse_results
    where win_odds <> 0 and win_odds is not null -- er dette riktig? antar nå at hvis odds er 0 så starter ikke hesten i løpet
    --where kmtime is not null
    group by 1
)

select * from race_stats