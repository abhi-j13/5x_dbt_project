{{ config(materialized='table') }}

with Daily_death_data as(
    select DATE,
    max(continent) continent,
    max(country)country,
    max(location_level) location_level,
    location,
    concat(max(latitude),',',max(longitude)) as coordinates,
    max(LOCATION_ISO_CODE) LOCATION_ISO_CODE,
    sum(new_deaths) as new_deaths,
    sum(total_deaths) as total_deaths,
    sum(total_cases) as total_cases,
    sum(new_deaths_per_million) as new_deaths_per_million,
    sum(case_fatality_rate) as case_fatality_rate,
    sum(GROWTH_FACTOR_OF_NEW_DEATHS) as GROWTH_FACTOR_OF_NEW_DEATHS
    from {{ref('base_data')}} 
    group by DATE,location order by DATE,location desc
),
 daily_cases_per_location as (
    select date,country,
    location_level,
    sum(new_deaths) total_new_deaths_location_level,
    sum(total_deaths) as total_deaths_cases_location_level,
    sum(total_cases) as total_cases_location_level,
    sum(new_deaths_per_million) as total_new_deaths_per_million_location_level
    from Daily_death_data group by date,country,location_level
)
,
Daily_deaths as(
    select daily.*,
agg.total_new_deaths_location_level,
agg.total_deaths_cases_location_level,
agg.total_cases_location_level,
agg.total_new_deaths_per_million_location_level
 from Daily_death_data daily 
 left join daily_cases_per_location agg ON 
 daily.date=agg.date AND daily.location_level=agg.location_level and daily.country=agg.country
) 
select * from Daily_deaths


