# WarehouseFlow — Mini Motors

> End-to-end data pipeline: OLTP production database → staging area → star-schema data warehouse → business intelligence snapshots, orchestrated with Apache Airflow on Docker.

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## What It Does

This pipeline ingests transactional data from a production OLTP database (Mini Motors — a fictional automotive parts retailer), cleans and reshapes it into a dimensional star schema, and produces flat CSV snapshots and analytical query results ready for BI tools or reporting dashboards.

The entire workflow is automated in Apache Airflow: five DAGs chain together so running the first one kicks off the entire pipeline end-to-end.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Docker Compose Environment                          │
│                                                                             │
│   ┌──────────────────────┐          ┌──────────────────────────────────┐   │
│   │   mysql container    │          │      sleek-airflow container     │   │
│   │   (port 4000:3306)   │          │         (port 8080:8080)         │   │
│   │                      │          │                                  │   │
│   │  ┌────────────────┐  │◄────────►│  ┌──────────┐  ┌────────────┐  │   │
│   │  │  production    │  │  MySQL   │  │ Airflow  │  │    DAGs    │  │   │
│   │  │  (OLTP)        │  │  hooks   │  │ Scheduler│  │ (5 DAGs)   │  │   │
│   │  ├────────────────┤  │          │  └──────────┘  └────────────┘  │   │
│   │  │  staging_area  │  │          │                                  │   │
│   │  ├────────────────┤  │          │  ./Airflow  ──►  /opt/airflow   │   │
│   │  │  data_warehouse│  │          │  (volume mount)                  │   │
│   │  └────────────────┘  │          └──────────────────────────────────┘   │
│   │                      │                                                  │
│   │  ./ProductionDB ──►  │                                                  │
│   │  /var/lib/mysql       │                                                  │
│   └──────────────────────┘                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Database Layout

Three MySQL schemas live inside the same container:

```
MySQL (port 4000)
│
├── production              ← OLTP source (raw transactional data)
│   ├── Sales_Transaction   (+ Transaction_Details, Shipment)
│   ├── Purchase_Order      (+ Purchase_Order_Details)
│   ├── Inventory
│   ├── Product             (+ Product_Category, Brand, Product_Promotion)
│   ├── Supplier            (+ Product_Supplier)
│   ├── Customer
│   ├── Warehouse
│   ├── Employee            (+ Employee_Role, Department)
│   └── Geography           (Cities, Provinces, Countries)
│
├── staging_area            ← Flattened, denormalized intermediate tables
│   ├── Inventory_Transactions  (combined purchases + sales rows)
│   ├── Product
│   ├── Supplier
│   ├── Customer
│   ├── Warehouse
│   ├── Date                (generated during transformation)
│   └── Transaction_Type    (generated during transformation)
│
└── data_warehouse          ← Star schema (final analytical layer)
    ├── Inventory_Transactions_Fact   ← fact table
    ├── Product                       ← dim
    ├── Supplier                      ← dim
    ├── Customer                      ← dim
    ├── Warehouse                     ← dim
    ├── Date                          ← dim
    └── Transaction_Type              ← dim
```

---

## Star Schema

```
                        ┌───────────────┐
                        │   dim_Date    │
                        │───────────────│
                        │ date (PK)     │
                        │ day           │
                        │ week          │
                        │ month         │
                        │ quarter       │
                        │ year          │
                        └──────┬────────┘
                               │
          ┌────────────────────┼───────────────────────┐
          │                    │                        │
┌─────────┴──────┐   ┌─────────▼──────────────┐   ┌───┴──────────────┐
│  dim_Supplier  │   │                        │   │  dim_Customer    │
│────────────────│   │  Inventory_Transactions│   │──────────────────│
│ sup_id (PK)    ├──►│        _Fact           │◄──┤ cust_id (PK)     │
│ sup_company    │   │────────────────────────│   │ cust_name        │
│ sup_name       │   │ date_id  (FK → Date)   │   │ cust_company     │
│ sup_phone      │   │ type_id  (FK → TxType) │   │ cust_city        │
│ sup_city       │   │ sup_id   (FK → Sup)    │   │ cust_province    │
│ sup_province   │   │ cust_id  (FK → Cust)   │   │ cust_country     │
│ sup_country    │   │ prod_id  (FK → Prod)   │   └──────────────────┘
└────────────────┘   │ wh_id    (FK → Wh)     │
                     │ quantity               │   ┌──────────────────┐
┌────────────────┐   │ cost                   │   │  dim_Product     │
│  dim_Warehouse │   │ delay (days)           │   │──────────────────│
│────────────────│   │ freight_charges        │◄──┤ prod_id (PK)     │
│ wh_id (PK)     ├──►│ total_cost             │   │ prod_name        │
│ wh_address     │   │ inventory_transaction_id│  │ prod_category    │
│ wh_city        │   └────────────┬───────────┘   │ prod_brand       │
│ wh_capacity    │                │               └──────────────────┘
└────────────────┘                │
                        ┌─────────▼──────┐
                        │dim_Transaction │
                        │     _Type      │
                        │────────────────│
                        │ type_id (PK)   │
                        │ type_description│
                        │ (Purchases /   │
                        │  Sales)        │
                        └────────────────┘
```

---

## DAG Pipeline

Five Airflow DAGs run in sequence. The first is triggered manually; each one chains to the next automatically using `TriggerDagRunOperator`.

```
  [Manual Trigger]
        │
        ▼
┌───────────────────────────────────────┐
│           extract_dag                 │
│                                       │
│  ┌─────────────────────────────────┐  │
│  │    Extract_Dimensions (group)   │  │
│  │  ┌──────────┐ ┌──────────────┐ │  │
│  │  │ Product  │ │   Supplier   │ │  │
│  │  ├──────────┤ ├──────────────┤ │  │
│  │  │ Customer │ │  Warehouse   │ │  │
│  │  └──────────┘ └──────────────┘ │  │
│  └──────────────┬──────────────────┘  │
│                 │                     │
│  ┌──────────────▼──────────────────┐  │
│  │  Extract_Transactional_Data     │  │
│  │  ┌────────────┐ ┌────────────┐  │  │
│  │  │  Purchases │ │   Sales    │  │  │
│  │  └────────────┘ └────────────┘  │  │
│  └──────────────┬──────────────────┘  │
│                 │                     │
│     [Trigger → transform_dag]         │
└─────────────────┼─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│           transform_dag               │
│                                       │
│  ┌─────────────────────────────────┐  │
│  │    Basic_Transformation (group) │  │
│  │  ┌──────────────────────────┐   │  │
│  │  │  Create Date Hierarchy   │   │  │
│  │  ├──────────────────────────┤   │  │
│  │  │ Populate Transaction Type│   │  │
│  │  ├──────────────────────────┤   │  │
│  │  │  Handle Null Foreign Keys│   │  │
│  │  ├──────────────────────────┤   │  │
│  │  │     Derive Facts         │   │  │
│  │  │ (total_cost = qty*cost   │   │  │
│  │  │  + freight)              │   │  │
│  │  └──────────────────────────┘   │  │
│  └──────────────┬──────────────────┘  │
│                 │                     │
│     Generate Data Profiles (ydata)    │
│                 │                     │
│     [Trigger → load_dag]              │
└─────────────────┼─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│             load_dag                  │
│                                       │
│  ┌─────────────────────────────────┐  │
│  │     Load_Dimensions (group)     │  │
│  │  Product │ Supplier │ Customer  │  │
│  │  Warehouse │ Date │ Tx_Type     │  │
│  └──────────────┬──────────────────┘  │
│                 │                     │
│     Load Inventory_Transactions_Fact  │
│                 │                     │
│     [Trigger → snapshot_dag]          │
└─────────────────┼─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│           snapshot_dag                │
│                                       │
│  Procurement_Mart_Snapshot (CSV)      │
│  Revenue_Mart_Snapshot     (CSV)      │
│  Inventory_Mart_Snapshot   (CSV)      │
│         │                             │
│  Update_Version_History               │
│  (tracks net qty/value change         │
│   across pipeline runs)               │
│                                       │
│     [Trigger → dimensional_query_dag] │
└─────────────────┼─────────────────────┘
                  │
                  ▼
┌───────────────────────────────────────┐
│       dimensional_query_dag           │
│                                       │
│  TurnoverRate_By_ProductCategory      │
│  LateShipment_by_Supplier             │
│  GrossMargin_Per_Product              │
│                                       │
│  (results saved as CSV snapshots)     │
└───────────────────────────────────────┘
```

---

## DAG Reference

| DAG | Reads From | Writes To | Key Work |
|---|---|---|---|
| `extract_dag` | `production` | `staging_area` | Denormalizes dimensions (resolves city/province/country FKs); combines purchase orders and sales transactions into a single `Inventory_Transactions` fact staging table |
| `transform_dag` | `staging_area` | `staging_area` | Generates `Date` hierarchy; populates `Transaction_Type`; fills null FK placeholders (`N/A` sentinel rows); derives `total_cost`; runs ydata-profiling on all staging tables |
| `load_dag` | `staging_area` | `data_warehouse` | Bulk-inserts all six dimensions and the fact table into the star schema using `INSERT IGNORE` for idempotency |
| `snapshot_dag` | `data_warehouse` | CSV files | Exports three data mart views (Procurement, Revenue, Inventory) as flat CSVs; appends a versioned inventory summary row to `Version_History.csv` |
| `dimensional_query_dag` | `data_warehouse` | CSV files | Runs three analytical queries: inventory turnover by category, late vs. early shipment % by supplier, gross margin per product |

---

## Analytical Queries

### Inventory Turnover Rate by Product Category
```sql
SELECT p.prod_category,
       SUM(itf.quantity) / COUNT(DISTINCT itf.date_id) AS InventoryTurnoverRate
FROM Product p
JOIN Inventory_Transactions_Fact itf ON p.prod_id = itf.prod_id
GROUP BY p.prod_category;
```

### Late Shipment % by Supplier
```sql
SELECT s.sup_name,
       SUM(CASE WHEN itf.Delay < 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS EarlyShipmentPct,
       SUM(CASE WHEN itf.Delay > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS LateShipmentPct
FROM Supplier s
JOIN Inventory_Transactions_Fact itf ON s.sup_id = itf.sup_id
GROUP BY s.sup_name;
```

### Gross Margin per Product
```sql
SELECT p.Prod_Name,
       (SUM(itf.Quantity * itf.Cost) - SUM(itf.Freight_Charges)) AS GrossMargin
FROM Product p
JOIN Inventory_Transactions_Fact itf ON p.prod_id = itf.prod_id
GROUP BY p.prod_name;
```

---

## Project Structure

```
ETL_Pipeline/
├── docker-compose.yaml          # Spins up MySQL + Airflow containers
│
├── ProductionDB/                # MySQL data directory (volume-mounted into container)
│   ├── production/              # OLTP source tables
│   ├── staging_area/            # Intermediate staging tables
│   └── data_warehouse/          # Star schema tables
│
└── Airflow/                     # Volume-mounted into /opt/airflow in the container
    ├── dockerfile               # Custom Airflow image (seek-airflow)
    ├── dags/
    │   ├── extract_dag.py           # DAG 1 — Extract from production → staging
    │   ├── transformation_dag.py    # DAG 2 — Transform within staging
    │   ├── load_dag.py              # DAG 3 — Load staging → data warehouse
    │   ├── snapshot.py              # DAG 4 — Export mart snapshots + version history
    │   └── dimensional_query_dag.py # DAG 5 — Run analytical queries
    └── snapshots/               # Output CSVs
        ├── Procurement_snapshot.csv
        ├── Revenue_snapshot.csv
        └── Inventory_snapshot.csv
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### 1 — Build the custom Airflow image

```bash
cd Airflow
docker build -t seek-airflow .
```

### 2 — Start the stack

```bash
cd ..
docker compose up -d
```

This starts two containers:
- **mysql** on `localhost:4000` — serves all three schemas
- **sleek-airflow** on `localhost:8080` — Airflow webserver + scheduler

### 3 — Open Airflow UI

Navigate to [http://localhost:8080](http://localhost:8080).

The admin password is printed in `Airflow/standalone_admin_password.txt`.

### 4 — Set up MySQL connections in Airflow

In the Airflow UI go to **Admin → Connections** and create three connections:

| Conn ID | Host | Port | Schema |
|---|---|---|---|
| `mini_motors_production` | `mysql` | `3306` | `production` |
| `mini_motors_staging` | `mysql` | `3306` | `staging_area` |
| `mini_motors_warehouse` | `mysql` | `3306` | `data_warehouse` |

Use `root` / `root` for all three (as set in `docker-compose.yaml`).

### 5 — Run the pipeline

Trigger `extract_dag` manually from the Airflow UI. The remaining four DAGs run automatically in sequence.

When complete, the output CSVs land in `Airflow/snapshots/`.

---

## Tech Stack

| Tool | Role |
|---|---|
| **Apache Airflow** | Pipeline orchestration — DAG scheduling, task dependencies, cross-DAG triggering |
| **MySQL 8** | Both the OLTP source and the analytical data warehouse (three schemas, one instance) |
| **Docker Compose** | Reproducible local environment — one command to start everything |
| **Python / pandas** | Data profiling (`ydata_profiling`), snapshot export, version history tracking |
| **ydata-profiling** | Automated HTML data quality reports generated on all staging tables after transformation |
