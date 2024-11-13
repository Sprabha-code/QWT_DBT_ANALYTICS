{{ config(materialized='view', schema='reporting')}}

WITH ordr as
(
    select customerid,orderdate,quantity,linesalesamount
    from {{ref('fct_orders')}}
),
CUST as 
(
    select customerid,companyname,contactname,city
    from {{ref('dim_customers')}}
),

cust_orders as
(
select cust.companyname,
cust.contactname,
cust.city,
min(ordr.orderdate) as first_order_weekname,
max(ordr.orderdate) as recent_order_weekname,
sum(ordr.quantity) as total_orders,
sum(ordr.linesalesamount) as totalsales
from ordr inner join cust
on ordr.customerid = cust.customerid
group by 1,2,3
)
select * from cust_orders
order by total_orders desc