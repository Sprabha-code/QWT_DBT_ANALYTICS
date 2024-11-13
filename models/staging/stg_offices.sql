{{ config ( materialized = 'table') }}

 
select 
office as officeid,
officeaddress as address,
officepostalcode as postalcode,
officecity as city,
officestateprovince as stateprovince,
officephone as phone,
officefax as fax,
officecountry as country
from 
{{source('qwt_raw','offices')}}