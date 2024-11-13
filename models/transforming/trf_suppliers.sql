{{config(materialized='table',schema='transforming')}}
select 
GET(XMLGET(SUPPLIERINFO,'SupplierID'),'$') as SupplierID,
GET(XMLGET(SUPPLIERINFO,'CompanyName'),'$'):: varchar as CompanyName,
GET(XMLGET(SUPPLIERINFO,'ContactName'),'$'):: varchar as ContactName,
GET(XMLGET(SUPPLIERINFO,'Address'),'$'):: varchar as Address,
GET(XMLGET(SUPPLIERINFO,'City'),'$'):: varchar as City,
GET(XMLGET(SUPPLIERINFO,'PostalCode'),'$'):: varchar as PostalCode,
GET(XMLGET(SUPPLIERINFO,'Country'),'$'):: varchar as Country,
GET(XMLGET(SUPPLIERINFO,'Phone'),'$'):: varchar as Phone,
GET(XMLGET(SUPPLIERINFO,'Fax'),'$'):: varchar as Fax
from
{{ref('stg_suppliers')}}