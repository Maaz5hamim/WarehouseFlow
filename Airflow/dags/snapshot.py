from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
from datetime import datetime

database = "mini_motors_warehouse"


procurement_mart_query ="""
SELECT date_id AS date, date_day, date_week, date_month, date_quarter, date_year, sup_id, sup_company, sup_name, sup_phone, sup_email, sup_city, sup_province, sup_country, sup_address, prod_id, prod_name, prod_description, prod_category, prod_category_description, prod_brand, wh_id, wh_address, wh_city, wh_province, wh_country, wh_capacity, quantity, cost, delay, freight_charges, inventory_transaction_id, total_cost    
FROM Inventory_Transactions_Fact JOIN Product using(prod_id) JOIN Supplier using(sup_id) JOIN Warehouse using(wh_id) JOIN Date using(date_id) JOIN Transaction_Type using(type_id)
WHERE type_description = 'Purchases'
"""
revenue_mart_query ="""
SELECT date_id AS date, date_day, date_week, date_month, date_quarter, date_year, cust_id, cust_name, cust_company, cust_phone, cust_email, cust_city, cust_province, cust_country, cust_address, prod_id, prod_name, prod_description, prod_category, prod_category_description, prod_brand, wh_id, wh_address, wh_city, wh_province, wh_country, wh_capacity, quantity, cost, delay, freight_charges, inventory_transaction_id, total_cost   
FROM Inventory_Transactions_Fact JOIN Product using(prod_id) JOIN Customer using(cust_id) JOIN Warehouse using(wh_id) JOIN Date using(date_id) JOIN Transaction_Type using(type_id)
WHERE type_description = 'Sales';
"""
inventory_mart_query ="""
SELECT type_description, date_id AS date, date_day, date_week, date_month, date_quarter, date_year, prod_id, prod_name, prod_description, prod_category, prod_category_description, prod_brand, wh_id, wh_address, wh_city, wh_province, wh_country, wh_capacity, quantity, cost, freight_charges, inventory_transaction_id, total_cost   
FROM Inventory_Transactions_Fact LEFT JOIN Product using(prod_id) JOIN Supplier using(sup_id) JOIN Customer using(cust_id) JOIN Warehouse using(wh_id) JOIN Date using(date_id) JOIN Transaction_Type using(type_id);
"""


def generate_snapshot(mart, query):
    hook = MySqlHook(mysql_conn_id=database) 
    df = hook.get_pandas_df(query)
    df.to_csv(f'/opt/airflow/snapshots/{mart}_snapshot.csv')

def calculate_inventory():
    df = pd.read_csv('/opt/airflow/snapshots/Inventory_snapshot.csv')
    
    purchases = df[df['type_description'] == 'Purchases']
    sales = df[df['type_description'] == 'Sales']

    total_purchase_quantity = purchases['quantity'].sum()
    total_purchase_value = purchases['total_cost'].sum()

    total_sales_quantity = sales['quantity'].sum()
    total_sales_value = sales['total_cost'].sum()
    
    net_quantity = total_purchase_quantity - total_sales_quantity
    net_value = total_purchase_value - total_sales_value

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    output_csv_path = '/opt/airflow/snapshots/Version_History.csv'
    
    if os.path.exists(output_csv_path):
        summary_df = pd.read_csv(output_csv_path)
        previous_row = summary_df.iloc[-1]
        previous_net_quantity = previous_row['net_quantity']
        previous_net_value = previous_row['net_value']
    else:
        previous_net_quantity = 0
        previous_net_value = 0

    change_in_quantity = net_quantity - previous_net_quantity
    change_in_value = net_value - previous_net_value

    result = pd.DataFrame([{
        'timestamp': timestamp,
        'total_purchase_quantity': total_purchase_quantity,
        'total_purchase_value': total_purchase_value,
        'total_sales_quantity': total_sales_quantity,
        'total_sales_value': total_sales_value,
        'net_quantity': net_quantity,
        'net_value': net_value,
        'change_in_quantity': change_in_quantity,
        'change_in_value': change_in_value
    }])
    
    if os.path.exists(output_csv_path):
        result.to_csv(output_csv_path, mode='a', header=False, index=False)
    else:
        result.to_csv(output_csv_path, mode='w', header=True, index=False)

with DAG(dag_id="snapshot_dag", start_date= days_ago(1), schedule_interval=None) as dag:

    procurement_mart_snapshot = PythonOperator(
        task_id = 'Procurement_Mart_Snapshot',
        python_callable=generate_snapshot,
        op_args=['Procurement', procurement_mart_query]
    )

    revenue_mart_snapshot = PythonOperator(
        task_id = 'Revenue_Mart_Snapshot',
        python_callable=generate_snapshot,
        op_args=['Revenue', revenue_mart_query]
    )

    inventory_mart_snapshot = PythonOperator(
        task_id = 'Inventory_Mart_Snapshot',
        python_callable=generate_snapshot,
        op_args=['Inventory', inventory_mart_query]
    )

    execute_dimensional_queries = TriggerDagRunOperator(
        task_id="Execute_Dimensional_Queries",
        trigger_dag_id="dimensional_query_dag"
    ) 

    update_version_history = PythonOperator(
        task_id='Update_Version_History',
        python_callable=calculate_inventory,
    )

    inventory_mart_snapshot >> update_version_history

