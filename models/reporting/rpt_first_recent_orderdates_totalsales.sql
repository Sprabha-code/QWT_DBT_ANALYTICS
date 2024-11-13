{{ config(materialized='view', schema='reporting')}}

select companyname,
contactname,
city,
min(dt.DAY_OF_WEEK_NAME) as first_order_weekname,
max(dt.DAY_OF_WEEK_NAME) as recent_order_weekname,
sum(ordr.quantity) as total_orders,
sum(ordr.linesalesamount) as totalsales
from {{ref('fct_orders')}} ordr inner join {{ref('dim_customers')}} cust
on ordr.customerid = cust.customerid
inner join {{ref('dim_date')}} dt on 
ordr.orderdate=dt.date_day
group by companyname, contactname,city

