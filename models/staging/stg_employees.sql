{{ config ( materialized = 'table') }}

 
select  
empid ,
lastname ,
firstname ,
title ,
to_date(hire_date,'mm/dd/yy') as hire_date,
office ,
extension ,
Reports_to ,
Year_Salary  from 
{{source('qwt_raw','employees')}}
