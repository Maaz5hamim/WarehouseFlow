from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import os
from datetime import datetime

database = "mini_motors_warehouse"


turnoverRate_by_productCategory_query ="""
SELECT 
    p.prod_category,
    SUM(itf.quantity) / COUNT(DISTINCT itf.date_id) AS InventoryTurnoverRate
FROM 
    Product p
JOIN 
    Inventory_Transactions_Fact itf ON p.prod_id = itf.prod_id
GROUP BY 
    p.prod_category
"""
lateShipment_by_supplier_query ="""
SELECT 
    s.sup_name,
    SUM(CASE WHEN itf.Delay < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(itf.transaction_id) AS EarlyShipmentPercentage,
    SUM(CASE WHEN itf.Delay > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(itf.transaction_id) AS LateShipmentPercentage
FROM 
    Supplier s
JOIN 
    Inventory_Transactions_Fact itf ON s.sup_id = itf.sup_id
GROUP BY 
    s.sup_name;
"""
grossMargin_per_product_query ="""
SELECT 
    p.Prod_Name,
    (SUM(itf.Quantity * itf.Cost) - SUM(itf.Freight_Charges)) AS GrossMargin
FROM 
    Product p
JOIN 
    Inventory_Transactions_Fact itf ON p.prod_id = itf.prod_id
GROUP BY 
    p.prod_name;
"""


def generate_snapshot(mart, query):
    hook = MySqlHook(mysql_conn_id=database) 
    df = hook.get_pandas_df(query)
    df.to_csv(f'/opt/airflow/dimensional_queries/{mart}_snapshot.csv')

with DAG(dag_id="dimensional_query_dag", start_date= days_ago(1), schedule_interval=None) as dag:

    turnoverRate_by_productCategory = PythonOperator(
        task_id = 'TurnoverRate_By_ProductCategory',
        python_callable=generate_snapshot,
        op_args=['TurnoverRate_By_ProductCategory', turnoverRate_by_productCategory_query]
    )

    lateShipment_by_supplier = PythonOperator(
        task_id = 'LateShipment_by_Supplier',
        python_callable=generate_snapshot,
        op_args=['LateShipment_by_Supplier', lateShipment_by_supplier_query]
    )

    grossMargin_per_product = PythonOperator(
        task_id = 'GrossMargin_Per_Product',
        python_callable=generate_snapshot,
        op_args=['GrossMargin_Per_Product', grossMargin_per_product_query]
    )



