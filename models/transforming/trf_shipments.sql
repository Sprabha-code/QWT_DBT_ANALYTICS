{{ config(materialized='table', schema='transforming')}}

select 
    shipments.OrderID,
    shipments.LineNo,
    shippers.companyname,
    shipments.ShipmentDate,
    shipments.Status,
    shipments.dbt_valid_from,
    shipments.dbt_valid_to
from 
{{ref("shipments_snapshot")}} as shipments
inner join 
{{ref("lkp_shippers")}} as shippers
on shipments.shipperid=shippers.shipperid

