{{ config ( materialized = 'table', schema= env_var('DBT_STAGESCHEMA','staging')) }}
select * from 
{{source('qwt_raw','suppliers_xml')}}