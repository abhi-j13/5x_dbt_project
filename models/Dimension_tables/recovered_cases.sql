{{ config(materialized='table') }}

with recovered_data as(
    select DATE,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    concat(max(latitude),',',max(longitude)) as coordinates,
    max(LOCATION_ISO_CODE) LOCATION_ISO_CODE,
    sum(total_recovered) as total_recovered,
    sum(new_recovered) as new_recovered,
    sum(CASE_RECOVERED_RATE) as CASE_RECOVERED_RATE
    from {{ref('base_data')}} 
    group by DATE,location order by DATE,location desc
),
 recovered_data_per_location as (
    select date,country,
    location_level,
    sum(total_recovered) total_recovered_location_level,
    sum(new_recovered) as total_new_recovered_cases_location_level,
    sum(CASE_RECOVERED_RATE) as total_CASE_RECOVERED_RATE_location_level
    from recovered_data group by date,country,location_level
)
,
recovered_cases as(
    select daily.*,
agg.total_recovered_location_level,
agg.total_new_recovered_cases_location_level,
agg.total_CASE_RECOVERED_RATE_location_level
 from recovered_data daily 
 left join recovered_data_per_location agg ON 
 daily.date=agg.date AND daily.location_level=agg.location_level and daily.country=agg.country
)

select * from recovered_cases


