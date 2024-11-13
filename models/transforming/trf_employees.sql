{{ config ( materialized='table', schema='transforming')}}

select 
e.empid,
e.firstname,
e.lastname,
e.title,
e.hire_date,
o.address,
o.city,
o.stateprovince,
o.phone,
iff(mngr.firstname is null,e.firstname,mngr.firstname) as mngr_name,
iff(mngr.title is null,e.title,mngr.title) as mngr_title,
iff(e.extension ='-','NA',e.extension) as extension, 
e.year_salary
from 
{{ref('stg_employees')}} as e inner join {{ref('stg_employees')}} as mngr
on mngr.empid=e.reports_to
inner join {{ref('stg_offices')}} as o
--on o.officeid=e.office
on e.office=o.officeid
