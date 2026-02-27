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

---

## 🛠️ Installation & Setup Guide

Follow these steps to set up the project locally.

### 1. Prerequisites
Ensure you have the following installed:
* Python 3.9+
* PostgreSQL & MySQL (or Docker to run them)
* Git

### 2. Clone the Repository
```bash
git clone https://github.com/your-username/ai-powered-bi-platform.git
cd ai-powered-bi-platform
```

### 3. Create a Virtual Environment
```bash
python -m venv env
# On Windows:
env\Scripts\activate
# On macOS/Linux:
source env/bin/activate
```

### 4. Install Dependencies
```bash
pip install -r requirements.txt
```
*(If you don't have a `requirements.txt` yet, you can install the main packages directly: `pip install pandas numpy scikit-learn flask sqlalchemy python-dotenv psycopg2-binary mysql-connector-python joblib`)*

### 5. Setup Environment Variables
Create a `.env` file in the root directory (or use the existing one) with your database credentials:
```env
# PostgreSQL Credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ai_bi_project
DB_USER=postgres
DB_PASSWORD=your_postgres_password

# MySQL Credentials
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DB=flagship_project
```

### 6. Run the Application
Start the Flask API server:
```bash
python ML_Layer/Demand_Forecasting/app.py
```
The server will start running on `http://127.0.0.1:5000`. You can now trigger the forecasting API by making a GET or POST request to `/forecast`.
