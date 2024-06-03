from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.mysql_operator import MySqlOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
import pandas as pd
from ydata_profiling import ProfileReport

tables = ['Inventory_Transactions', 'Product', 'Warehouse', 'Supplier', 'Customer']
database = "mini_motors_staging"

def generate_profile():
    hook = MySqlHook(mysql_conn_id=database) 
    for table in tables:
        sql = f"SELECT * FROM {table}"
        df = hook.get_pandas_df(sql)
        profile = ProfileReport(df, title=f"{table} Table Report")
        profile.to_file(f'/opt/airflow/data_profiles/{table}_profile.html')

with DAG(dag_id="transform_dag", start_date= days_ago(1), schedule_interval=None) as dag:
  
  with TaskGroup(group_id="Basic_Transformation") as basic_transformation:

    create_date_hierarchy = MySqlOperator(
      task_id="Create_Date_Hierarchy",
      mysql_conn_id=database,
      sql="""
      CREATE TEMPORARY TABLE temp_dates AS
        SELECT DISTINCT date FROM Inventory_Transactions;

      CREATE TEMPORARY TABLE temp_date_hierarchy AS
        SELECT
            date,
            DAY(date) AS day,
            MONTH(date) AS month,
            YEAR(date) AS year,
            QUARTER(date) AS quarter,
            WEEK(date) AS week
        FROM temp_dates;

        CREATE TABLE IF NOT EXISTS Date (
            date DATE PRIMARY KEY,
            day INT,
            month INT,
            year INT,
            quarter INT,
            week INT
        );

        INSERT INTO Date (date, day, month, year, quarter, week)
          SELECT date, day, month, year, quarter, week FROM temp_date_hierarchy;

        DROP TEMPORARY TABLE IF EXISTS temp_dates;
        DROP TEMPORARY TABLE IF EXISTS temp_date_hierarchy;
      """,
    )

    populate_transaction_type = MySqlOperator(
      task_id="Populate_Transaction_Type",
      mysql_conn_id=database,
      sql="""
      INSERT INTO Transaction_Type (transaction_type)
      SELECT DISTINCT transaction_type
      FROM Inventory_Transactions;

      UPDATE Inventory_Transactions it JOIN Transaction_Type tt ON it.transaction_type = tt.transaction_type
      SET it.transaction_type_id = tt.transaction_type_id;
      """,
    )

    handle_null_foreignKey = MySqlOperator(
      task_id="Handle_Null_ForeignKey",
      mysql_conn_id=database,
      sql="""
      INSERT IGNORE INTO Customer (customer_id, customer_name, company_name, phone, email, address, city, province, country)
      VALUES ('N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A');

      INSERT IGNORE INTO Supplier (supplier_id, contact_name, company_name, phone, email, address, city, province, country)
      VALUES ('N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A');

      UPDATE Inventory_Transactions
      SET customer_id = 'N/A'
      WHERE customer_id IS NULL;

      UPDATE Inventory_Transactions
      SET supplier_id = 'N/A'
      WHERE supplier_id IS NULL;
      """,
    )

    create_derive_facts = MySqlOperator(
      task_id="Derive_Facts",
      mysql_conn_id=database,
      sql="""
      UPDATE staging_area.Inventory_Transactions
      SET total_cost = (quantity * cost) + IFNULL(freight_charges, 0);
      """,
    )
  
  generate_profile_task = PythonOperator(
    task_id='Generate_Data_Profiles',
    python_callable=generate_profile,
  )

  load_star_schema = TriggerDagRunOperator(
    task_id="Load_Star_Schema",
    trigger_dag_id="load_dag"
  )

  basic_transformation >> generate_profile_task >> load_star_schema







  

