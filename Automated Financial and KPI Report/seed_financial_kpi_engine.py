"""
Automated Financial & KPI Reporting Engine - PostgreSQL seed script

What this script does
---------------------
1. Connects to PostgreSQL
2. Creates these tables:
   - calendar
   - customers
   - products
   - orders
   - sales
   - expenses
   - targets_forecast
3. Generates realistic synthetic data
4. Inserts it into PostgreSQL

Install:
    pip install psycopg[binary] faker python-dotenv

Expected .env file:
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=financial_kpi_report
    DB_USER=postgres
    DB_PASSWORD=1234
"""

from __future__ import annotations

import os
import random
import calendar as pycalendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from faker import Faker
from psycopg import connect
from dotenv import load_dotenv


# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "port": int(os.environ["DB_PORT"]),
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
}

# -----------------------------
# Config
# -----------------------------
SEED = 42
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 12, 31)

NUM_CUSTOMERS = 2500
NUM_PRODUCTS = 120

ORDER_STATUSES = [
    "delivered",
    "delivered",
    "delivered",
    "delivered",
    "shipped",
    "cancelled",
    "returned",
]

CHANNELS = ["Online", "Retail", "Partner", "Mobile App"]
PAYMENT_METHODS = ["Card", "Cash", "Bank Transfer", "Wallet"]

REGIONS = {
    "North": ["Rangpur", "Rajshahi", "Bogura", "Dinajpur"],
    "Central": ["Dhaka", "Gazipur", "Narayanganj", "Mymensingh"],
    "South": ["Khulna", "Barishal", "Jashore", "Patuakhali"],
    "East": ["Chattogram", "Cumilla", "Noakhali", "Sylhet"],
}

CUSTOMER_SEGMENTS = ["Consumer", "Corporate", "SMB"]

EXPENSE_DEPARTMENTS = ["Sales", "Marketing", "Operations", "HR", "IT", "Finance"]

EXPENSE_CATEGORIES = {
    "Sales": ["Travel", "Incentives", "Client Entertainment"],
    "Marketing": ["Ads", "Events", "Content", "Agency"],
    "Operations": ["Logistics", "Packaging", "Warehouse", "Utilities"],
    "HR": ["Recruitment", "Training", "Benefits"],
    "IT": ["Cloud", "Software", "Hardware", "Support"],
    "Finance": ["Audit", "Bank Charges", "Advisory"],
}

PRODUCT_CATALOG = {
    "Electronics": [
        ("Smartphone", 320, 0.18),
        ("Laptop", 780, 0.16),
        ("Tablet", 260, 0.15),
        ("Headphones", 55, 0.28),
        ("Smartwatch", 110, 0.22),
        ("Monitor", 180, 0.20),
    ],
    "Home & Living": [
        ("Blender", 48, 0.26),
        ("Vacuum Cleaner", 145, 0.21),
        ("Rice Cooker", 65, 0.24),
        ("Air Fryer", 95, 0.23),
        ("Desk Lamp", 25, 0.35),
        ("Storage Box", 16, 0.40),
    ],
    "Fashion": [
        ("T-Shirt", 18, 0.48),
        ("Jeans", 35, 0.42),
        ("Sneakers", 58, 0.38),
        ("Jacket", 65, 0.36),
        ("Backpack", 32, 0.39),
        ("Sunglasses", 22, 0.44),
    ],
    "Beauty": [
        ("Face Wash", 10, 0.52),
        ("Perfume", 42, 0.46),
        ("Shampoo", 12, 0.49),
        ("Lipstick", 11, 0.55),
        ("Lotion", 9, 0.50),
        ("Serum", 18, 0.48),
    ],
    "Groceries": [
        ("Coffee", 8, 0.30),
        ("Tea", 6, 0.32),
        ("Pasta", 4, 0.27),
        ("Olive Oil", 9, 0.25),
        ("Biscuits", 3, 0.34),
        ("Cereal", 5, 0.29),
    ],
}


@dataclass
class ProductRecord:
    product_id: str
    sku: str
    product_name: str
    category: str
    subcategory: str
    brand: str
    base_price: float
    unit_cost: float
    launch_date: date
    is_active: bool


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def month_start(dt: date) -> date:
    return dt.replace(day=1)


def quarter_num(dt: date) -> int:
    return ((dt.month - 1) // 3) + 1


def safe_round(v: float, n: int = 2) -> float:
    return round(float(v), n)


def generate_calendar_rows(start: date, end: date):
    rows = []
    for d in daterange(start, end):
        rows.append(
            (
                d,
                d.year,
                d.month,
                d.day,
                pycalendar.month_name[d.month],
                pycalendar.day_name[d.weekday()],
                quarter_num(d),
                d.isoweekday(),
                d.weekday() >= 5,
                month_start(d),
            )
        )
    return rows


def generate_customers(n: int):
    rows = []
    customer_ids = []

    region_names = list(REGIONS.keys())
    region_weights = [0.38, 0.30, 0.15, 0.17]

    for i in range(1, n + 1):
        region = random.choices(region_names, weights=region_weights, k=1)[0]
        city = random.choice(REGIONS[region])
        segment = random.choices(CUSTOMER_SEGMENTS, weights=[0.72, 0.12, 0.16], k=1)[0]
        signup = fake.date_between(start_date="-3y", end_date=END_DATE)
        age_group = random.choices(
            ["18-24", "25-34", "35-44", "45-54", "55+"],
            weights=[0.14, 0.34, 0.28, 0.16, 0.08],
            k=1,
        )[0]

        first_name = fake.first_name()
        last_name = fake.last_name()
        customer_id = f"CUST{i:05d}"
        customer_ids.append(customer_id)

        rows.append(
            (
                customer_id,
                first_name,
                last_name,
                fake.unique.email(),
                fake.phone_number()[:30],
                city,
                region,
                segment,
                age_group,
                signup,
                random.random() > 0.03,
            )
        )

    return customer_ids, rows


def generate_products(n: int):
    rows = []
    product_records = []

    brand_pool = ["Nova", "UrbanLeaf", "PrimeX", "Aster", "Nimbus", "Vista", "Lumo", "Zenix", "PureHome"]

    combos = []
    for category, subs in PRODUCT_CATALOG.items():
        for sub in subs:
            combos.append((category, *sub))

    for i in range(1, n + 1):
        category, subcategory, avg_price, margin = random.choice(combos)
        brand = random.choice(brand_pool)
        adjective = random.choice(["Plus", "Max", "Lite", "Pro", "Air", "Elite", "Core", "Flex"])
        product_name = f"{brand} {subcategory} {adjective}"
        sku = f"SKU-{category[:3].upper()}-{i:04d}"

        price_variation = random.uniform(0.82, 1.28)
        base_price = safe_round(avg_price * price_variation, 2)
        unit_cost = safe_round(base_price * (1 - margin), 2)
        launch_date = fake.date_between(start_date="-4y", end_date=END_DATE)
        is_active = random.random() > 0.06

        product_id = f"PROD{i:04d}"
        rec = ProductRecord(
            product_id=product_id,
            sku=sku,
            product_name=product_name,
            category=category,
            subcategory=subcategory,
            brand=brand,
            base_price=base_price,
            unit_cost=unit_cost,
            launch_date=launch_date,
            is_active=is_active,
        )
        product_records.append(rec)

        rows.append(
            (
                product_id,
                sku,
                product_name,
                category,
                subcategory,
                brand,
                base_price,
                unit_cost,
                launch_date,
                is_active,
            )
        )

    return product_records, rows


def daily_order_volume(d: date) -> int:
    base = 36 if d.year == 2024 else 45

    weekday_factor = {
        0: 1.02,
        1: 1.05,
        2: 1.07,
        3: 1.06,
        4: 1.12,
        5: 0.88,
        6: 0.78,
    }[d.weekday()]

    month_factor = {
        1: 0.92,
        2: 0.95,
        3: 1.00,
        4: 1.01,
        5: 1.03,
        6: 1.04,
        7: 1.00,
        8: 1.02,
        9: 1.06,
        10: 1.10,
        11: 1.18,
        12: 1.28,
    }[d.month]

    promo_bump = 1.25 if (d.month in [11, 12] and d.day in range(20, 31)) else 1.0
    noise = random.uniform(0.82, 1.18)

    vol = int(base * weekday_factor * month_factor * promo_bump * noise)
    return max(vol, 8)


def choose_products_for_order(products: list[ProductRecord], order_date: date):
    weighted_products = []
    for p in products:
        if not p.is_active and random.random() < 0.8:
            continue

        category_weight = {
            "Electronics": 0.8,
            "Home & Living": 1.0,
            "Fashion": 1.3,
            "Beauty": 1.4,
            "Groceries": 1.7,
        }.get(p.category, 1.0)

        seasonal_weight = 1.0
        if p.category == "Fashion" and order_date.month in [10, 11, 12]:
            seasonal_weight = 1.18
        if p.category == "Electronics" and order_date.month in [11, 12]:
            seasonal_weight = 1.24
        if p.category == "Home & Living" and order_date.month in [4, 5]:
            seasonal_weight = 1.10

        weighted_products.append((p, category_weight * seasonal_weight))

    line_count = random.choices([1, 2, 3, 4, 5], weights=[38, 30, 18, 10, 4], k=1)[0]
    chosen = random.choices(
        [x[0] for x in weighted_products],
        weights=[x[1] for x in weighted_products],
        k=line_count,
    )
    return chosen


def generate_orders_and_sales(customer_ids: list[str], products: list[ProductRecord], start: date, end: date):
    orders_rows = []
    sales_rows = []

    order_counter = 1
    sales_counter = 1

    for d in daterange(start, end):
        num_orders = daily_order_volume(d)

        for _ in range(num_orders):
            order_id = f"ORD{order_counter:07d}"
            order_counter += 1

            customer_id = random.choice(customer_ids)
            region = random.choices(list(REGIONS.keys()), weights=[0.38, 0.30, 0.15, 0.17], k=1)[0]
            city = random.choice(REGIONS[region])
            channel = random.choices(CHANNELS, weights=[0.48, 0.22, 0.12, 0.18], k=1)[0]
            payment_method = random.choices(PAYMENT_METHODS, weights=[0.56, 0.12, 0.20, 0.12], k=1)[0]
            status = random.choice(ORDER_STATUSES)

            order_ts = datetime.combine(d, datetime.min.time()) + timedelta(
                hours=random.randint(8, 22),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )

            if status == "delivered":
                delivery_date = d + timedelta(days=random.randint(1, 6))
            elif status == "shipped":
                delivery_date = d + timedelta(days=random.randint(2, 7))
            else:
                delivery_date = None

            line_products = choose_products_for_order(products, d)
            gross_order_value = 0.0
            total_discount = 0.0

            for p in line_products:
                quantity = random.choices([1, 2, 3, 4, 5], weights=[54, 26, 11, 6, 3], k=1)[0]

                if p.category == "Electronics":
                    discount_pct = random.choices([0.00, 0.03, 0.05, 0.08, 0.10], weights=[20, 24, 28, 18, 10], k=1)[0]
                elif p.category == "Groceries":
                    discount_pct = random.choices([0.00, 0.02, 0.05, 0.07], weights=[36, 28, 24, 12], k=1)[0]
                else:
                    discount_pct = random.choices([0.00, 0.05, 0.10, 0.15], weights=[28, 38, 24, 10], k=1)[0]

                market_noise = random.uniform(0.98, 1.04)
                unit_price = safe_round(p.base_price * market_noise, 2)
                discount_amount = safe_round(unit_price * quantity * discount_pct, 2)
                gross_revenue = safe_round(unit_price * quantity, 2)
                net_revenue = safe_round(gross_revenue - discount_amount, 2)
                cogs = safe_round(p.unit_cost * quantity, 2)

                if status == "cancelled":
                    net_revenue = 0.0
                    gross_revenue = 0.0
                    discount_amount = 0.0
                    cogs = 0.0
                elif status == "returned":
                    net_revenue = safe_round(-0.75 * net_revenue, 2)
                    gross_revenue = safe_round(-0.75 * gross_revenue, 2)
                    cogs = safe_round(-0.75 * cogs, 2)

                gross_profit = safe_round(net_revenue - cogs, 2)

                sales_rows.append(
                    (
                        f"SALE{sales_counter:09d}",
                        order_id,
                        p.product_id,
                        d,
                        quantity,
                        unit_price,
                        safe_round(discount_pct * 100, 2),
                        gross_revenue,
                        net_revenue,
                        cogs,
                        gross_profit,
                        region,
                        city,
                        channel,
                    )
                )
                sales_counter += 1

                gross_order_value += net_revenue
                total_discount += discount_amount

            orders_rows.append(
                (
                    order_id,
                    customer_id,
                    order_ts,
                    status,
                    channel,
                    payment_method,
                    region,
                    city,
                    len(line_products),
                    safe_round(gross_order_value, 2),
                    safe_round(total_discount, 2),
                    delivery_date,
                )
            )

    return orders_rows, sales_rows


def generate_expenses(start: date, end: date):
    rows = []
    expense_id = 1

    for d in daterange(start, end):
        num_entries = random.choices([3, 4, 5, 6], weights=[18, 34, 30, 18], k=1)[0]

        for _ in range(num_entries):
            dept = random.choice(EXPENSE_DEPARTMENTS)
            category = random.choice(EXPENSE_CATEGORIES[dept])

            base = {
                "Sales": 280,
                "Marketing": 380,
                "Operations": 340,
                "HR": 180,
                "IT": 260,
                "Finance": 150,
            }[dept]

            multiplier = 1.0
            if dept == "Marketing" and d.month in [11, 12]:
                multiplier *= 1.35
            if dept == "Operations" and d.weekday() in [4, 5]:
                multiplier *= 1.08
            if d.day >= 25:
                multiplier *= 1.05

            amount = safe_round(base * random.uniform(0.55, 1.60) * multiplier, 2)

            rows.append(
                (
                    f"EXP{expense_id:08d}",
                    d,
                    dept,
                    category,
                    amount,
                    random.choice(list(REGIONS.keys())),
                    fake.company(),
                    fake.sentence(nb_words=4)[:100],
                )
            )
            expense_id += 1

    return rows


def generate_targets_forecast(products: list[ProductRecord], start: date, end: date):
    rows = []
    tf_id = 1

    categories = sorted({p.category for p in products})
    month_cursor = month_start(start)
    last_month = month_start(end)

    while month_cursor <= last_month:
        for region in REGIONS.keys():
            region_factor = {
                "Central": 1.18,
                "North": 0.95,
                "South": 0.84,
                "East": 1.02,
            }[region]

            for category in categories:
                category_factor = {
                    "Electronics": 1.22,
                    "Home & Living": 0.92,
                    "Fashion": 1.00,
                    "Beauty": 0.86,
                    "Groceries": 0.74,
                }.get(category, 1.0)

                seasonal_factor = {
                    1: 0.93,
                    2: 0.96,
                    3: 0.99,
                    4: 1.00,
                    5: 1.02,
                    6: 1.03,
                    7: 1.00,
                    8: 1.01,
                    9: 1.05,
                    10: 1.10,
                    11: 1.18,
                    12: 1.26,
                }[month_cursor.month]

                growth_factor = 1.00 if month_cursor.year == 2024 else 1.12

                revenue_target = 18000 * region_factor * category_factor * seasonal_factor * growth_factor * random.uniform(0.93, 1.07)
                orders_target = int(190 * region_factor * seasonal_factor * random.uniform(0.90, 1.08))

                forecast_revenue = revenue_target * random.uniform(0.96, 1.05)
                forecast_profit = forecast_revenue * random.uniform(0.22, 0.34)

                rows.append(
                    (
                        f"TF{tf_id:07d}",
                        month_cursor,
                        region,
                        category,
                        safe_round(revenue_target, 2),
                        orders_target,
                        safe_round(forecast_revenue, 2),
                        safe_round(forecast_profit, 2),
                    )
                )
                tf_id += 1

        if month_cursor.month == 12:
            month_cursor = date(month_cursor.year + 1, 1, 1)
        else:
            month_cursor = date(month_cursor.year, month_cursor.month + 1, 1)

    return rows


DDL_SQL = """
DROP TABLE IF EXISTS sales CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS expenses CASCADE;
DROP TABLE IF EXISTS targets_forecast CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS calendar CASCADE;

CREATE TABLE calendar (
    date_key DATE PRIMARY KEY,
    year_num INT NOT NULL,
    month_num INT NOT NULL,
    day_num INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    quarter_num INT NOT NULL,
    iso_weekday INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    month_start DATE NOT NULL
);

CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(60) NOT NULL,
    last_name VARCHAR(60) NOT NULL,
    email VARCHAR(120) UNIQUE NOT NULL,
    phone VARCHAR(30),
    city VARCHAR(60),
    region VARCHAR(30),
    segment VARCHAR(20),
    age_group VARCHAR(20),
    signup_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    sku VARCHAR(30) UNIQUE NOT NULL,
    product_name VARCHAR(120) NOT NULL,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50) NOT NULL,
    brand VARCHAR(50) NOT NULL,
    base_price NUMERIC(12,2) NOT NULL,
    unit_cost NUMERIC(12,2) NOT NULL,
    launch_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    order_ts TIMESTAMP NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    sales_channel VARCHAR(20) NOT NULL,
    payment_method VARCHAR(20) NOT NULL,
    region VARCHAR(30) NOT NULL,
    city VARCHAR(60) NOT NULL,
    item_count INT NOT NULL,
    order_amount NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL,
    delivery_date DATE
);

CREATE TABLE sales (
    sales_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    product_id VARCHAR(20) NOT NULL REFERENCES products(product_id),
    sales_date DATE NOT NULL REFERENCES calendar(date_key),
    quantity INT NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    discount_pct NUMERIC(5,2) NOT NULL,
    gross_revenue NUMERIC(12,2) NOT NULL,
    net_revenue NUMERIC(12,2) NOT NULL,
    cogs NUMERIC(12,2) NOT NULL,
    gross_profit NUMERIC(12,2) NOT NULL,
    region VARCHAR(30) NOT NULL,
    city VARCHAR(60) NOT NULL,
    sales_channel VARCHAR(20) NOT NULL
);

CREATE TABLE expenses (
    expense_id VARCHAR(20) PRIMARY KEY,
    expense_date DATE NOT NULL REFERENCES calendar(date_key),
    department VARCHAR(30) NOT NULL,
    category VARCHAR(40) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    region VARCHAR(30),
    vendor_name VARCHAR(120),
    notes VARCHAR(200)
);

CREATE TABLE targets_forecast (
    tf_id VARCHAR(20) PRIMARY KEY,
    target_month DATE NOT NULL,
    region VARCHAR(30) NOT NULL,
    category VARCHAR(50) NOT NULL,
    revenue_target NUMERIC(14,2) NOT NULL,
    orders_target INT NOT NULL,
    forecast_revenue NUMERIC(14,2) NOT NULL,
    forecast_profit NUMERIC(14,2) NOT NULL
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_ts ON orders(order_ts);
CREATE INDEX idx_sales_order_id ON sales(order_id);
CREATE INDEX idx_sales_product_id ON sales(product_id);
CREATE INDEX idx_sales_sales_date ON sales(sales_date);
CREATE INDEX idx_expenses_expense_date ON expenses(expense_date);
CREATE INDEX idx_targets_month ON targets_forecast(target_month);
"""

INSERT_CALENDAR = """
INSERT INTO calendar (
    date_key, year_num, month_num, day_num, month_name, day_name,
    quarter_num, iso_weekday, is_weekend, month_start
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_CUSTOMERS = """
INSERT INTO customers (
    customer_id, first_name, last_name, email, phone, city, region,
    segment, age_group, signup_date, is_active
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_PRODUCTS = """
INSERT INTO products (
    product_id, sku, product_name, category, subcategory, brand,
    base_price, unit_cost, launch_date, is_active
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_ORDERS = """
INSERT INTO orders (
    order_id, customer_id, order_ts, order_status, sales_channel,
    payment_method, region, city, item_count, order_amount,
    discount_amount, delivery_date
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_SALES = """
INSERT INTO sales (
    sales_id, order_id, product_id, sales_date, quantity, unit_price,
    discount_pct, gross_revenue, net_revenue, cogs, gross_profit,
    region, city, sales_channel
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_EXPENSES = """
INSERT INTO expenses (
    expense_id, expense_date, department, category, amount, region,
    vendor_name, notes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_TARGETS = """
INSERT INTO targets_forecast (
    tf_id, target_month, region, category, revenue_target,
    orders_target, forecast_revenue, forecast_profit
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def chunked(seq, size=5000):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def main():
    print("Generating calendar...")
    calendar_rows = generate_calendar_rows(START_DATE, END_DATE)

    print("Generating customers...")
    customer_ids, customer_rows = generate_customers(NUM_CUSTOMERS)

    print("Generating products...")
    product_records, product_rows = generate_products(NUM_PRODUCTS)

    print("Generating orders and sales...")
    order_rows, sales_rows = generate_orders_and_sales(customer_ids, product_records, START_DATE, END_DATE)

    print("Generating expenses...")
    expense_rows = generate_expenses(START_DATE, END_DATE)

    print("Generating targets/forecast...")
    target_rows = generate_targets_forecast(product_records, START_DATE, END_DATE)

    print("Connecting to PostgreSQL...")
    with connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            print("Creating tables...")
            cur.execute(DDL_SQL)

            print("Inserting calendar...")
            for batch in chunked(calendar_rows, 2000):
                cur.executemany(INSERT_CALENDAR, batch)

            print("Inserting customers...")
            for batch in chunked(customer_rows, 2000):
                cur.executemany(INSERT_CUSTOMERS, batch)

            print("Inserting products...")
            for batch in chunked(product_rows, 2000):
                cur.executemany(INSERT_PRODUCTS, batch)

            print("Inserting orders...")
            for batch in chunked(order_rows, 5000):
                cur.executemany(INSERT_ORDERS, batch)

            print("Inserting sales...")
            for batch in chunked(sales_rows, 5000):
                cur.executemany(INSERT_SALES, batch)

            print("Inserting expenses...")
            for batch in chunked(expense_rows, 5000):
                cur.executemany(INSERT_EXPENSES, batch)

            print("Inserting targets_forecast...")
            for batch in chunked(target_rows, 5000):
                cur.executemany(INSERT_TARGETS, batch)

        conn.commit()

    print("\nDone.")
    print(f"calendar rows:         {len(calendar_rows):,}")
    print(f"customers rows:        {len(customer_rows):,}")
    print(f"products rows:         {len(product_rows):,}")
    print(f"orders rows:           {len(order_rows):,}")
    print(f"sales rows:            {len(sales_rows):,}")
    print(f"expenses rows:         {len(expense_rows):,}")
    print(f"targets_forecast rows: {len(target_rows):,}")


if __name__ == "__main__":
    main()