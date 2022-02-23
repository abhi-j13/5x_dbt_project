{{ config(materialized='table') }}

with MOM_death as(
    select monthname(to_date(date,'mm/dd/yyyy')) as month_name,
    *
    from {{ref('Daily_Deaths')}} 
),

MOM_death_cases as (
    select month_name,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    max(coordinates) coordinates,
    max(LOCATION_ISO_CODE) LOCATION_ISO_CODE,
    sum(new_deaths) as new_deaths,
    sum(total_deaths) as total_deaths,
    sum(total_cases) as total_cases,
    sum(new_deaths_per_million) as new_deaths_per_million,
    sum(case_fatality_rate) as case_fatality_rate,
    sum(GROWTH_FACTOR_OF_NEW_DEATHS) as GROWTH_FACTOR_OF_NEW_DEATHS,
    sum(total_new_deaths_location_level) total_new_deaths_location_level,
    sum(total_deaths_cases_location_level) total_deaths_cases_location_level,
    sum(total_cases_location_level) total_cases_location_level,
    sum(total_new_deaths_per_million_location_level) total_new_deaths_per_million_location_level

    from MOM_death
    group by month_name,location order by month_name,location desc
)

select * from MOM_death_cases