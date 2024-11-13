import snowflake.snowpark.functions as f
import holidays
import pandas as pd

def is_holiday(date_col):
    france_holidays = holidays.France() 
    is_holiday = (date_col in france_holidays)
    return is_holiday

def avg_order(X,Y):
    return X/Y
def model(dbt,session):
    dbt.config(materialized='table',schema='reporting',packages = ['holidays'])
    dim_customers_df=dbt.ref('dim_customers')
    fct_orders_df=dbt.ref('fct_orders')
    cust_orders_df=(fct_orders_df.group_by('customerid')
                    .agg(
                        f.min(f.col('orderdate')).alias('first_order_date'),
                        f.max(f.col('orderdate')).alias('last_order_date'),
                        f.sum(f.col('quantity')).alias('total_orders'),
                        f.sum(f.col('linesalesamount')).alias('total_sales')
                    ))
    final_df=(
        dim_customers_df
        .join(cust_orders_df,dim_customers_df.customerid==cust_orders_df.customerid,'left')
        .select(
            dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                dim_customers_df.city.alias('city'),
                cust_orders_df.first_order_date.alias('first_order_date'),
                cust_orders_df.last_order_date.alias('last_order_date'),
                cust_orders_df.total_orders.alias('total_orders'),
                cust_orders_df.total_sales.alias('total_sales')

        )
    )
    final_df=final_df.withColumn('avg_orders_value',avg_order(final_df['total_sales'],final_df['total_orders']))

    final_df=final_df.filter(f.col("first_order_date").isNotNull())

    final_df=final_df.to_pandas()

    final_df['IS_FIRST_ORDER_ON_HOLIDAY']=final_df['FIRST_ORDER_DATE'].apply(is_holiday)

    return final_df