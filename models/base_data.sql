{{ config(materialized='table') }}

with source_data as (

    select * from {{ source('base_data', 'COVID_19_INDONESIA_ABHISHEK_JAIN') }}

)

select *
from source_data where location is not null