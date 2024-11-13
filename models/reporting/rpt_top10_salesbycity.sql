{{ config(materialized='view', schema='reporting')}}

select companyname,
contactname,
sum(ordr.linesalesamount) as linesalesamount ,
sum(ordr.quantity) as total_orders,
avg(ordr.margin) as margin
from {{ref('fct_orders')}} ordr inner join {{ref('dim_customers')}} cust
on ordr.customerid = cust.customerid
where cust.city = '{{ var('v_city',"'Berlin'")}}'
group by companyname, contactname
order by linesalesamount desc