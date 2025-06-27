# Data Warehouse Project

## 📌 Objective

To design and implement a modern **data warehouse** for **XYZ Company**, which operates across multiple geographic locations and receives data from various source systems like **CRM** and **ERP**. The raw data is supplied in **flat file format**, which needs to be ingested, transformed, and modeled for business intelligence and analytics use cases.

---

## 🧠 Business Understanding

- The core business goal is to **integrate disparate data sources**, cleanse and transform the data using best data engineering practices.
- The processed and modeled data should support various reporting and analytics functions across business units.
- Ensuring **data quality**, **consistency**, and **traceability** is key for supporting decision-making.

---

## 🏗️ Data Architecture: Medallion Architecture

We adopted the **Medallion Architecture** — a layered approach to data processing — to improve **data reliability**, **scalability**, and **modularity**.

### 🥉 Bronze Layer
- **Purpose**: Raw data ingestion from source systems (CRM, ERP).
- **Operation**: Full load (truncate and reload).
- **Actions**: No transformation or business logic is applied.
- **Storage**: Raw data as-is (schema-on-read).
- **Naming Convention**: `bronze_[table_name].[column_name]`

### 🥈 Silver Layer
- **Purpose**: Cleaned, standardized, and enriched data.
- **Actions**:
  - Data cleaning
  - Data standardization
  - Data transformation
  - Data integration (e.g., joins across systems)
  - Data modeling (normalized/staged structure)
  - Data profiling, validation, and quality checks
- **Storage**: Fully transformed tables, ready for analytics.
- **Naming Convention**: `silver_[table_name].[column_name]`

### 🥇 Gold Layer
- **Purpose**: Business-ready layer for consumption by analysts, BI tools, ML engineers, etc.
- **Actions**:
  - Business rule application
  - Final data aggregations and lookups
  - Final validation and quality checks
- **Output**: Only **views** are created on top of silver layer tables (no physical tables).
- **Naming Convention**: `vw_[business_entity]_[metric]`

---

## 🧱 Data Modeling

A robust **data modeling strategy** is implemented to ensure the warehouse supports performant, scalable analytics.

### ✅ Techniques & Best Practices Used
- **Schema Design**: Implemented **Star Schema** (Fact & Dimension tables) for reporting.
- **Entity Modeling**: Clear separation of dimensions and facts.
- **Relationships**: Proper cardinality and referential integrity (PK/FK constraints).
- **Indexing**: Indexes created on primary and frequently joined fields.
- **Surrogate Keys**: Used where necessary to ensure stable joins.
- **Slowly Changing Dimensions (SCD)**: Support for SCD Type 1 and Type 2 where applicable.
- **Date/Time Dimensions**: Created dedicated calendar dimensions for date-based analysis.

---

## ⚙️ Data Ingestion

- **Source Format**: Flat files (CSV, Excel, etc.) from CRM and ERP systems.
- **Load Type**: Full load for now (can be upgraded to incremental).
- **Validation**: All loads are validated using row counts, null checks, and reference checks.

---

## 🔍 Data Quality & Profiling

- **Profiling Tools**: Custom scripts or third-party tools used to analyze column-wise data quality.
- **Checks Implemented**:
  - Null value analysis
  - Outlier detection
  - Referential integrity
  - Duplicate records
  - Range checks for metrics

---

## 🧪 Testing Strategy

- Unit testing of transformations and joins.
- Validation of business logic at gold layer.
- Row count matching between source → bronze → silver → gold.
- Comparison against sample reports for verification.

---

## 📊 Reporting & Analytics Readiness

- Final views in **gold layer** are structured for **Power BI / Tableau / ML model ingestion**.
- Granularity and dimensionality allow for slicing and dicing across key business dimensions (e.g., location, product, customer, date).


