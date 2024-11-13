{{ config ( materialized = 'table', schema='staging') }}
select * from 
{{source('qwt_raw','suppliers_xml')}}