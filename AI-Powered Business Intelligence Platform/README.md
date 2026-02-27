# 🚀 AI-Powered Business Intelligence Platform

A **production-grade AI-powered BI platform** portfolio project that simulates an enterprise-level retail analytics system.

## 🌟 Core Architecture & Features

### 🔹 1. Dataset Preparation (Enterprise-Level Data Simulation)

Designed and generated an **industry-standard synthetic dataset** that mimics large-scale retail and supply chain operations of a big company.

**The dataset includes:**
* Customers
* Products
* Vendors
* Orders
* Order Items
* Payments
* Shipments
* Inventory
* Pricing history
* Marketing campaigns

**The data is:**
* Multi-table relational
* Time-series based
* Region-aware
* Business logic driven (realistic constraints & relationships)

*This completely simulates a real production OLTP system.*

---

### 🔹 2. Data Warehouse Architecture (PostgreSQL)

All data is stored in **PostgreSQL (PgSQL)** and structured using a modern layered warehouse architecture:

#### 🏗 Warehouse Layers:
* **Raw Layer** → Direct ingestion from source
* **Staging Layer** → Data cleaning, transformation, and processing
* **Warehouse Layer** → Core business tables
* **Analytics Layer** → KPI and business metric tables

**Implementation Details:**
* **STAR SCHEMA design**
* Fact tables (e.g., `fact_sales`)
* Dimension tables (e.g., `dim_customer`, `dim_product`, `dim_time`, `dim_region`)
* Optimized joins and indexing for analytics performance

*This perfectly mirrors enterprise-level analytics engineering practices.*

---

### 🔹 3. Demand Forecasting (ML Layer)

From the data warehouse:
* Connected to PostgreSQL historical sales data
* Performed feature engineering (time features, lags, rolling averages)
* Trained demand forecasting models (e.g., Prophet / XGBoost)
* Evaluated model performance
* Saved the trained base model for reuse

*This enables predictive analytics for future demand planning.*

---

### 🔹 4. Forecasting API (Production Simulation)

To simulate a real AI-powered system:
* Built an API endpoint (Flask/FastAPI)
* When the API is triggered:
  * It fetches the latest data from the Data Warehouse
  * Loads the saved base model
  * Generates new demand forecasts
  * Returns predictions in real-time

*This demonstrates model deployment, API-based inference, and production-level ML integration.*

---

## 🎯 Project Value

This project showcases:
* End-to-end data engineering
* Enterprise data warehouse design
* Analytics engineering best practices
* Machine learning model training
* ML model deployment via API
* Real-world production simulation

It reflects the complete lifecycle:
> **Data → Warehouse → ML → API → Predictive Business Intelligence**
