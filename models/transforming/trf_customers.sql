{{ config ( materialized = 'table', schema = 'transforming', transient=false,
            pre_hook="use warehouse loading_wh;",
            post_hook="create or replace table transforming.trf_customers_copy clone {{this}};") }}

 
select 
CustomerID ,CompanyName ,ContactName ,City ,Country  ,
--DivisionID ,
d.DivisionName as DivisionName,
Address ,Fax ,Phone ,PostalCode ,
iff(StateProvince='','NA',StateProvince) as StateProvince
FROM
 
{{ref('stg_customers')}} as c inner join {{ref('lkp_divisions')}} as d
on c.DivisionID = d.DivisionID

