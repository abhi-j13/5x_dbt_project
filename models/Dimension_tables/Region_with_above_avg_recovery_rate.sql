{{ config(materialized='table') }}

with total_recovery_data as(
    select DATE,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    max(total_recovered_location_level) total_recovered_location_level
    from {{ref('recovered_cases')}}
    group by DATE,location order by DATE,location desc
),
 avg_recovery_data as (
    select date,country,
    avg(total_recovered_location_level) avg_recovered_location_level
    from {{ref('recovered_cases')}}  group by date,country
)
,
 above_avg_recovery_data as(
    select daily.* 
 from total_recovery_data daily 
 left join avg_recovery_data agg ON 
 daily.date=agg.date and daily.country=agg.country 
 where total_recovered_location_level>avg_recovered_location_level
) 
select * from above_avg_recovery_data


