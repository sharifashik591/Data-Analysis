-- ============================================================
-- CREATE STAR SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS star;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- DATE DIMENSION
CREATE TABLE star.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT,
    month INT,
    month_name VARCHAR(20),
    quarter INT,
    year INT,
    week_number INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);


-- PRODUCT DIMENSION
CREATE TABLE star.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(12,2),
    status VARCHAR(50)
);


-- REGION DIMENSION
CREATE TABLE star.dim_region (
    region_key SERIAL PRIMARY KEY,
    region_id VARCHAR(50),
    region_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    sales_zone VARCHAR(100)
);


-- CUSTOMER DIMENSION
CREATE TABLE star.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(200),
    segment VARCHAR(100),
    industry VARCHAR(100),
    customer_type VARCHAR(100),
    join_date DATE,
    status VARCHAR(50)
);


-- DEPARTMENT DIMENSION
CREATE TABLE star.dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id VARCHAR(50),
    department_name VARCHAR(100),
    cost_center VARCHAR(50),
    manager_name VARCHAR(100)
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- SALES FACT
CREATE TABLE star.fact_sales (
    sales_key SERIAL PRIMARY KEY,

    date_key INT,
    product_key INT,
    region_key INT,
    customer_key INT,
    department_key INT,

    order_id VARCHAR(50),

    quantity INT,
    unit_price NUMERIC(12,2),

    sales_amount NUMERIC(12,2),
    discount_amount NUMERIC(12,2),
    cost_amount NUMERIC(12,2),
    profit_amount NUMERIC(12,2),

    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_key)
        REFERENCES star.dim_date(date_key),

    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_key)
        REFERENCES star.dim_product(product_key),

    CONSTRAINT fk_sales_region
        FOREIGN KEY (region_key)
        REFERENCES star.dim_region(region_key),

    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES star.dim_customer(customer_key),

    CONSTRAINT fk_sales_department
        FOREIGN KEY (department_key)
        REFERENCES star.dim_department(department_key)
);



-- EXPENSE FACT
CREATE TABLE star.fact_expenses (
    expense_key SERIAL PRIMARY KEY,

    date_key INT,
    region_key INT,
    department_key INT,

    expense_type VARCHAR(100),
    expense_amount NUMERIC(12,2),
    budget_amount NUMERIC(12,2),

    CONSTRAINT fk_expense_date
        FOREIGN KEY (date_key)
        REFERENCES star.dim_date(date_key),

    CONSTRAINT fk_expense_region
        FOREIGN KEY (region_key)
        REFERENCES star.dim_region(region_key),

    CONSTRAINT fk_expense_department
        FOREIGN KEY (department_key)
        REFERENCES star.dim_department(department_key)
);



-- FORECAST FACT
CREATE TABLE star.fact_forecast (
    forecast_key SERIAL PRIMARY KEY,

    date_key INT,
    product_key INT,
    region_key INT,
    department_key INT,

    forecast_revenue NUMERIC(12,2),
    forecast_profit NUMERIC(12,2),
    forecast_quantity INT,

    CONSTRAINT fk_forecast_date
        FOREIGN KEY (date_key)
        REFERENCES star.dim_date(date_key),

    CONSTRAINT fk_forecast_product
        FOREIGN KEY (product_key)
        REFERENCES star.dim_product(product_key),

    CONSTRAINT fk_forecast_region
        FOREIGN KEY (region_key)
        REFERENCES star.dim_region(region_key),

    CONSTRAINT fk_forecast_department
        FOREIGN KEY (department_key)
        REFERENCES star.dim_department(department_key)
);


-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

CREATE INDEX idx_sales_date ON star.fact_sales(date_key);
CREATE INDEX idx_sales_product ON star.fact_sales(product_key);
CREATE INDEX idx_sales_region ON star.fact_sales(region_key);

CREATE INDEX idx_expense_date ON star.fact_expenses(date_key);
CREATE INDEX idx_expense_department ON star.fact_expenses(department_key);

CREATE INDEX idx_forecast_date ON star.fact_forecast(date_key);
CREATE INDEX idx_forecast_product ON star.fact_forecast(product_key);
