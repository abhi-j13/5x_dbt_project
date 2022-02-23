{{ config(materialized='table') }}

with Daily_data as(
    select DATE,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    concat(max(latitude),',',max(longitude)) as coordinates,
    max(LOCATION_ISO_CODE) LOCATION_ISO_CODE,
    sum(new_cases) as new_cases,
    sum(new_active_cases) as new_active_cases,
    sum(NEW_CASES_PER_MILLION) as NEW_CASES_PER_MILLION,
    sum(GROWTH_FACTOR_OF_NEW_CASES) as GROWTH_FACTOR_OF_NEW_CASES
    from {{ref('base_data')}} 
    group by DATE,location order by DATE,location desc
),
 daily_cases_per_location as (
    select date,country,
    location_level,
    sum(new_cases) total_new_cases_location_level,
    sum(new_active_cases) as total_new_active_cases_location_level,
    sum(NEW_CASES_PER_MILLION) as total_new_cases_per_million_location_level
    from Daily_data group by date,country,location_level
)
,
Daily_cases as(
    select daily.*,
agg.total_new_cases_location_level,
agg.total_new_active_cases_location_level,
agg.total_new_cases_per_million_location_level
 from Daily_data daily 
 left join daily_cases_per_location agg ON 
 daily.date=agg.date AND daily.location_level=agg.location_level and daily.country=agg.country
)

select * from daily_cases


