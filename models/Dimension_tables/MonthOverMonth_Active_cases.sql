{{ config(materialized='table') }}

with MOM_active as(
    select monthname(to_date(date,'mm/dd/yyyy')) as month_name,
    *
    from {{ref('Daily_Cases')}} 
),

MOM_Active_cases as (
    
    select month_name,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    max(coordinates) coordinates,
    max(LOCATION_ISO_CODE) LOCATION_ISO_CODE,
    sum(new_cases) as new_cases,
    sum(new_active_cases) as new_active_cases,
    sum(NEW_CASES_PER_MILLION) as NEW_CASES_PER_MILLION,
    sum(GROWTH_FACTOR_OF_NEW_CASES) as GROWTH_FACTOR_OF_NEW_CASES,
    sum(total_new_cases_location_level) total_new_cases_location_level,
    sum(total_new_active_cases_location_level) total_new_active_cases_location_level,
    sum(total_new_cases_per_million_location_level) total_new_cases_per_million_location_level
    from MOM_active
    group by month_name,location order by month_name,location desc
)

select * from MOM_Active_cases