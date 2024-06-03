from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

database = "mini_motors_staging"

product_query = """
INSERT IGNORE INTO data_warehouse.Product (prod_id, prod_name, prod_description, prod_category, prod_category_description, prod_brand)
SELECT 
    product_id, 
    product_name, 
    product_description, 
    category_name, 
    category_description, 
    brand_name
FROM Product;
"""

supplier_query = """
INSERT IGNORE INTO data_warehouse.Supplier (sup_id, sup_company, sup_name, sup_phone, sup_email, sup_city, sup_province, sup_country, sup_address)
SELECT 
    supplier_id, 
    company_name, 
    contact_name, 
    phone, 
    email, 
    city, 
    province, 
    country, 
    address
FROM Supplier;
"""

customer_query = """
INSERT IGNORE INTO data_warehouse.Customer (cust_id, cust_name, cust_company, cust_phone, cust_email, cust_city, cust_province, cust_country, cust_address)
SELECT 
    customer_id, 
    customer_name, 
    company_name,  
    phone, 
    email, 
    city, 
    province, 
    country, 
    address
FROM Customer;
"""

warehouse_query = """
INSERT IGNORE INTO data_warehouse.Warehouse (wh_id, wh_address, wh_city, wh_province, wh_country, wh_capacity)
SELECT 
    warehouse_id, 
    address, 
    city, 
    province, 
    country, 
    capacity
FROM Warehouse;
"""

transaction_type_query = """
INSERT IGNORE INTO data_warehouse.Transaction_Type (type_id, type_description)
SELECT distinct *
FROM Transaction_Type;
"""

date_query = """
INSERT IGNORE INTO data_warehouse.Date (date_id, date_day, date_month, date_year, date_quarter, date_week)
SELECT * FROM Date;
"""

fact_table_query ="""
INSERT IGNORE INTO data_warehouse.Inventory_Transactions_Fact (date_id, type_id, sup_id, cust_id, prod_id, wh_id, quantity, cost, delay, freight_charges, inventory_transaction_id, total_cost)
SELECT 
    date AS date_id,
    transaction_type_id,
    supplier_id,
    customer_id,
    product_id,
    warehouse_id,
    quantity,
    cost,
    delay,
    freight_charges,
    db_id,
    total_cost
FROM Inventory_Transactions;
"""


with DAG(dag_id="load_dag", start_date= days_ago(1), schedule_interval=None) as dag:

    with TaskGroup(group_id="Load_Dimensions") as load_dimensions:

        load_product_task = MySqlOperator(
            task_id="Load_Product_Dimension",
            mysql_conn_id=database,
            sql=product_query,
        )

        load_supplier_task = MySqlOperator(
            task_id="Load_Supplier_Dimension",
            mysql_conn_id=database,
            sql=supplier_query,
        )

        load_customer_task = MySqlOperator(
            task_id="Load_Customer_Dimension",
            mysql_conn_id=database,
            sql=customer_query,
        )

        load_warehouse_task = MySqlOperator(
            task_id="Load_Warehouse_Dimension",
            mysql_conn_id=database,
            sql=warehouse_query,
        )

        load_date_task = MySqlOperator(
            task_id="Load_Date_Dimension",
            mysql_conn_id=database,
            sql=date_query,
        )

        load_transaction_type_task = MySqlOperator(
            task_id="load_TransactionType_Dimension",
            mysql_conn_id=database,
            sql=transaction_type_query,
        )


    load_fact_table_task = MySqlOperator(
        task_id = 'Load_Inventory_Transactions_Fact',
        mysql_conn_id = database,
        sql = fact_table_query,
    )

    generate_snapshots = TriggerDagRunOperator(
        task_id="Generate_Snapshots",
        trigger_dag_id="snapshot_dag"
    )

    load_dimensions >>  load_fact_table_task >> generate_snapshots
