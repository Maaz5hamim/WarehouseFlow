from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

database = "mini_motors_production"

product_query = """
INSERT IGNORE INTO staging_area.Product (product_id, product_name, product_description, category_name, category_description, brand_name)
SELECT p.product_id, p.product_name, p.product_description, c.category_name, c.category_description, b.brand_name FROM Product p JOIN Product_Category c using(category_id) JOIN Brand b using(brand_id);"""

supplier_query = """
INSERT IGNORE INTO staging_area.Supplier (supplier_id, company_name, contact_name, phone, email, city, province, country, address)
SELECT s.supplier_id, s.company_name, s.contact_name, s.phone, s.email, ci.city_name as city, pr.province_name as province, co.country_name as country, s.address
FROM Supplier s JOIN Cities ci ON s.city_id = ci.city_id JOIN Provinces pr ON ci.province_id = pr.province_id JOIN Countries co ON pr.country_id = co.country_id;"""

customer_query = """
INSERT IGNORE INTO staging_area.Customer (customer_id, customer_name, company_name, phone, email, city, province, country, address)
SELECT c.customer_id, c.customer_name, c.company_name, c.phone, c.email, ci.city_name as city, pr.province_name as province, co.country_name as country, c.address
FROM Customer c JOIN Cities ci ON c.city_id = ci.city_id JOIN Provinces pr ON ci.province_id = pr.province_id JOIN Countries co ON pr.country_id = co.country_id;"""

warehouse_query = """
INSERT IGNORE INTO staging_area.Warehouse (warehouse_id, address, city, province, country, capacity)
SELECT w.warehouse_id, w.address, c.city_name as city, pr.province_name as province, co.country_name as country, w.capacity
FROM Warehouse w JOIN Cities c ON w.city_id = c.city_id JOIN Provinces pr ON c.province_id = pr.province_id JOIN Countries co ON pr.country_id = co.country_id;"""

inventory_transactions_purchases_query ="""
INSERT IGNORE INTO staging_area.Inventory_Transactions (date, db_id, supplier_id, product_id, warehouse_id, quantity, cost, delay, freight_charges, transaction_type)
With OrderTotals As 
(
	SELECT po.order_id, SUM(pod.units_bought * pod.unit_cost_price) AS total_cost
	FROM Purchase_Order po JOIN Purchase_Order_Details pod ON po.order_id = pod.order_id
	GROUP BY po.order_id
)
SELECT 
    po.order_date AS date,
    po.order_id AS db_id,
    po.supplier_id,
    pod.product_id,
    po.shipment_destination AS warehouse_id,
    pod.units_bought AS quantity,
    pod.unit_cost_price AS cost,
    DATEDIFF(po.actual_shipment, po.expected_shipment) AS delay,
    (po.freight_charges * (pod.units_bought * pod.unit_cost_price) / ot.total_cost) AS freight_charges,
    'Purchases' AS transaction_type
FROM Purchase_Order po LEFT JOIN Purchase_Order_Details pod USING (order_id) LEFT JOIN OrderTotals ot ON po.order_id = ot.order_id
WHERE po.order_status = 'Shipped';
"""

inventory_transactions_sales_query ="""
INSERT IGNORE INTO staging_area.Inventory_Transactions (date, db_id, customer_id, product_id, warehouse_id, quantity, cost, delay, freight_charges, transaction_type)
With OrderTotals As 
(
	SELECT st.transaction_id, SUM(td.units_sold * td.unit_selling_price) AS total_cost
	FROM Sales_Transaction st JOIN Transaction_Details td using (transaction_id)
	GROUP BY st.transaction_id
)
SELECT 
    st.transaction_date AS date,
    st.transaction_id AS db_id,
    st.customer_id,
    td.product_id,
    st.warehouse_id,
    td.units_sold AS quantity,
    td.unit_selling_price AS cost,
    IF(sh.shipment_status = 'Shipped', DATEDIFF(sh.delivered_date, st.expected_delivery), null) AS delay,
    (sh.freight_charges * (td.units_sold * td.unit_selling_price) / ot.total_cost) AS freight_charges,
    'Sales' AS transaction_type
FROM Sales_Transaction st LEFT JOIN Transaction_Details td USING (transaction_id) LEFT JOIN Shipment sh using(transaction_id) LEFT JOIN OrderTotals ot using (transaction_id)
WHERE st.transaction_status = 'Shipped';
"""

with DAG(dag_id="extract_dag", start_date= days_ago(1), schedule_interval=None) as dag:

    with TaskGroup(group_id="Extract_Dimensions") as extract_dimensions:

        extract_product_task = MySqlOperator(
            task_id="extract_product_dimension",
            mysql_conn_id=database,
            sql=product_query,
        )

        extract_supplier_task = MySqlOperator(
            task_id="extract_supplier_dimension",
            mysql_conn_id=database,
            sql=supplier_query,
        )

        extract_customer_task = MySqlOperator(
            task_id="extract_customer_dimension",
            mysql_conn_id=database,
            sql=customer_query,
        )

        extract_warehouse_task = MySqlOperator(
            task_id="extract_warehouse_dimension",
            mysql_conn_id=database,
            sql=warehouse_query,
        )

    with TaskGroup(group_id="Extract_Transactional_Data") as extract_facts:

        extract_inventory_purchases_task = MySqlOperator(
            task_id = 'extract_inventory_purchases',
            mysql_conn_id = database,
            sql = inventory_transactions_purchases_query,
        )

        extract_inventory_sales_task = MySqlOperator(
            task_id = 'extract_inventory_sales',
            mysql_conn_id = database,
            sql = inventory_transactions_sales_query,
        )

    initiate_transformation = TriggerDagRunOperator(
        task_id="Initiate_Transformation",
        trigger_dag_id="transform_dag"
    )

    extract_dimensions >> extract_facts >> initiate_transformation



# SELECT 
#     po.order_date as date,
# 	po.order_id as db_id,
#     po.supplier_id,
#     pod.product_id,
#     po.shipment_destination as warehouse_id,
#     pod.units_bought AS quantity,
#     pod.unit_cost_price AS cost,
# 	IF(po.order_status = 'Shipped', 1, 0) AS fullfilled,
#     IF(po.order_status = 'Shipped', DATEDIFF(po.actual_shipment, po.expected_shipment), null) AS delay,
#     po.freight_charges,
#     'Purchases' AS transaction_type
# FROM 
#     Purchase_Order po
#     JOIN Purchase_Order_Details pod using(order_id)

# SELECT 
#     st.transaction_date AS date,
#     st.transaction_id AS db_id,
#     st.customer_id,
#     td.product_id,
#     st.warehouse_id,
#     td.units_sold AS quantity,
#     td.unit_selling_price AS cost,
#     IF(sh.shipment_status = 'Shipped', 1, 0) AS fullfilled,
#     IF(sh.shipment_status = 'Shipped', DATEDIFF(sh.delivered_date, st.expected_delivery), null) AS delay,
#     'Sales' AS transaction_type
# FROM 
#     Sales_Transaction st
#     JOIN Transaction_Details td USING (transaction_id)
#     LEFT JOIN Shipment sh using(transaction_id);

# SELECT s.supplier_id, s.company_name, s.contact_name, s.phone, s.email, ci.city_name as city, pr.province_name as province, co.country_name as country, s.address
# FROM Supplier s
# JOIN Cities ci ON s.city_id = ci.city_id
# JOIN Provinces pr ON ci.province_id = pr.province_id
# JOIN Countries co ON pr.country_id = co.country_id;

# SELECT c.customer_id, c.company_name, c.customer_name, c.phone, c.email, ci.city_name as city, pr.province_name as province, co.country_name as country, c.address
# FROM Customer c
# JOIN Cities ci ON c.city_id = ci.city_id
# JOIN Provinces pr ON ci.province_id = pr.province_id
# JOIN Countries co ON pr.country_id = co.country_id;

# SELECT w.warehouse_id, w.address, c.city_name as city, pr.province_name as province, co.country_name as country, w.capacity
# FROM Warehouse w
# JOIN Cities c ON w.city_id = c.city_id
# JOIN Provinces pr ON c.province_id = pr.province_id
# JOIN Countries co ON pr.country_id = co.country_id;

# SELECT p.product_id, p.product_name, p.product_description, c.category_name, c.category_description, b.brand_name 
# FROM Product p JOIN Product_Category c using(category_id) JOIN Brand b using(brand_id);



# -- CREATE TABLE `Customer` (
# --   `cust_id` varchar(20) NOT NULL,
# --   `cust_name` varchar(50) DEFAULT NULL,
# --   `cust_company` varchar(50) DEFAULT NULL,
# --   `cust_phone` varchar(20) DEFAULT NULL,
# --   `cust_email` varchar(50) DEFAULT NULL,
# --   `cust_address` varchar(200) DEFAULT NULL,
# --   `cust_city` varchar(100) DEFAULT NULL,
# --   `cust_province` varchar(100) DEFAULT NULL,
# --   `cust_country` varchar(100) DEFAULT NULL,
# --   PRIMARY KEY (`cust_id`)
# -- );

# -- CREATE TABLE `Date` (
# --   `date_id` date Not NULL,
# --   `date_day` int DEFAULT NULL,
# --   `date_month` int DEFAULT NULL,
# --   `date_year` int DEFAULT NULL,
# --   `date_quarter` int DEFAULT NULL,
# --   `date_week` int DEFAULT NULL,
# --   PRIMARY KEY (`date_id`)
# -- )

# -- CREATE TABLE `Product` (
# --   `prod_id` varchar(20) NOT NULL,
# --   `prod_name` varchar(100) DEFAULT NULL,
# --   `prod_description` varchar(2000) DEFAULT NULL,
# --   `prod_category` varchar(100) DEFAULT NULL,
# --   `prod_category_description` varchar(200) DEFAULT NULL,
# --   `prod_brand` varchar(100) DEFAULT NULL,
# --   PRIMARY KEY (`prod_id`)
# -- );

# -- CREATE TABLE `Supplier` (
# --   `sup_id` varchar(20) NOT NULL,
# --   `sup_company` varchar(100) DEFAULT NULL,
# --   `sup_name` varchar(50) DEFAULT NULL,
# --   `sup_phone` varchar(20) DEFAULT NULL,
# --   `sup_email` varchar(100) DEFAULT NULL,
# --   `sup_city` varchar(100) DEFAULT NULL,
# --   `sup_province` varchar(100) DEFAULT NULL,
# --   `sup_country` varchar(100) DEFAULT NULL,
# --   `sup_address` varchar(1000) DEFAULT NULL,
# --   PRIMARY KEY (`sup_id`)
# -- );

# -- CREATE TABLE `Warehouse` (
# --   `wh_id` varchar(20) NOT NULL,
# --   `wh_address` varchar(200) DEFAULT NULL,
# --   `wh_city` varchar(100) DEFAULT NULL,
# --   `wh_province` varchar(100) DEFAULT NULL,
# --   `wh_country` varchar(100) DEFAULT NULL,
# --   `wh_capacity` int DEFAULT NULL,
# --   PRIMARY KEY (`wh_id`)
# -- )

# -- CREATE TABLE Transaction_Type (
# --     type_id INT AUTO_INCREMENT PRIMARY KEY,
# --     type_description VARCHAR(50) NOT NULL,
# -- );


# -- INSERT INTO Transaction_Type (trans_type, trans_description) VALUES
# -- ('Purchases', 'Acquisition of goods from supplier'),
# -- ('Sales', 'Transfer of ownership of goods to customer');

# -- ALTER TABLE Inventory_Transactions
# -- ADD COLUMN transaction_type_id INT;

# -- Step 4: Update Fact Table with Transaction Type IDs
# UPDATE Inventory_Transactions it
# JOIN Transaction_Type tt ON it.transaction_type = tt.transaction_type_name
# SET it.transaction_type_id = tt.transaction_type_id;