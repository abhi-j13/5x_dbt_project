{{ config(materialized='table') }}

with total_death_data as(
    select DATE,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    max(total_deaths_cases_location_level) total_deaths_cases_location_level
    from {{ref('Daily_Deaths')}}
    group by DATE,location order by DATE,location desc
),
 avg_death_data as (
    select date,country,
    avg(total_deaths_cases_location_level) avg_deaths_cases_location_level
    from {{ref('Daily_Deaths')}}  group by date,country
)
,
above_avg_death_data as(
    select daily.* 
 from total_death_data daily 
 left join avg_death_data agg ON 
 daily.date=agg.date and daily.country=agg.country 
 where total_deaths_cases_location_level>avg_deaths_cases_location_level
) 
select * from above_avg_death_data


