import os
from pathlib import Path
from datetime import timedelta

import numpy as np
import pandas as pd

np.random.seed(42)
rng = np.random.default_rng(42)

OUT = "ecommerce_dataset"
Path(OUT).mkdir(exist_ok=True)

N_CUSTOMERS = 200
N_PRODUCTS = 50
N_ORDERS = 600
N_SESSIONS = 1000
N_CAMPAIGNS = 8

START = pd.Timestamp("2024-01-01")
END = pd.Timestamp("2025-12-31")


def make_id(prefix, n, width=5):
    return f"{prefix}{str(n).zfill(width)}"


def rand_date(start=START, end=END):
    days = (end - start).days
    return start + pd.Timedelta(days=int(rng.integers(0, days + 1)))


def money(x):
    return round(float(x), 2)


# =========================
# CUSTOMERS
# =========================

first_names = ["Ahmed", "Rahim", "Karim", "Nadia", "Sajib", "Mim", "Ayesha", "Hasan", "Rafi", "Sadia"]
last_names = ["Khan", "Rahman", "Islam", "Hossain", "Ahmed", "Mia", "Akter", "Sultana"]
cities = ["Dhaka", "Chattogram", "Sylhet", "Rajshahi", "Khulna", "Barishal"]
channels = ["Facebook", "Google", "Organic", "Referral", "YouTube", "Instagram"]

customers = []

for i in range(1, N_CUSTOMERS + 1):
    customers.append({
        "customer_id": make_id("CUST", i),
        "customer_name": f"{rng.choice(first_names)} {rng.choice(last_names)}",
        "gender": rng.choice(["Male", "Female"]),
        "age": int(rng.integers(18, 61)),
        "city": rng.choice(cities),
        "country": "Bangladesh",
        "signup_date": rand_date(pd.Timestamp("2023-01-01"), pd.Timestamp("2024-12-31")),
        "acquisition_channel": rng.choice(channels),
        "email_subscribed": rng.choice(["Yes", "No"], p=[0.7, 0.3])
    })

customers = pd.DataFrame(customers)


# =========================
# PRODUCTS
# =========================

categories = {
    "Electronics": ["Mobile Accessories", "Audio", "Computer Accessories"],
    "Fashion": ["Men Clothing", "Women Clothing", "Footwear"],
    "Home & Living": ["Kitchen", "Decor", "Furniture"],
    "Beauty": ["Skin Care", "Hair Care", "Fragrance"]
}

brands = {
    "Electronics": ["Samsung", "Sony", "Xiaomi", "Anker"],
    "Fashion": ["Apex", "Bata", "Yellow", "Le Reve"],
    "Home & Living": ["RFL", "Vision", "Hatil", "Regal"],
    "Beauty": ["Dove", "Nivea", "Garnier", "Rexona"]
}

products = []

for i in range(1, N_PRODUCTS + 1):
    category = rng.choice(list(categories.keys()), p=[0.35, 0.30, 0.20, 0.15])
    cost_price = money(rng.uniform(250, 6000))
    selling_price = money(cost_price * rng.uniform(1.25, 1.90))

    products.append({
        "product_id": make_id("PROD", i),
        "product_name": f"{rng.choice(brands[category])} {rng.choice(categories[category])} Item {i}",
        "category": category,
        "sub_category": rng.choice(categories[category]),
        "brand": rng.choice(brands[category]),
        "cost_price": cost_price,
        "selling_price": selling_price,
        "supplier_id": make_id("SUP", int(rng.integers(1, 8)), 3),
        "launch_date": rand_date(pd.Timestamp("2023-01-01"), pd.Timestamp("2024-06-30"))
    })

products = pd.DataFrame(products)


# =========================
# MARKETING CAMPAIGNS
# =========================

campaign_names = [
    "New Year Sale", "Ramadan Offer", "Eid Mega Sale", "Summer Deals",
    "Back to School", "11.11 Sale", "Black Friday", "Year End Sale"
]

campaigns = []

for i in range(1, N_CAMPAIGNS + 1):
    start_date = START + pd.Timedelta(days=(i - 1) * 90)
    end_date = start_date + pd.Timedelta(days=int(rng.integers(10, 25)))
    cost = money(rng.uniform(30000, 150000))
    impressions = int(rng.integers(50000, 400000))
    clicks = int(impressions * rng.uniform(0.02, 0.06))

    campaigns.append({
        "campaign_id": make_id("CAMP", i, 3),
        "campaign_name": campaign_names[i - 1],
        "channel": rng.choice(["Facebook", "Google", "YouTube", "Instagram"]),
        "start_date": start_date,
        "end_date": end_date,
        "campaign_cost": cost,
        "impressions": impressions,
        "clicks": clicks
    })

campaigns = pd.DataFrame(campaigns)


def get_campaign(order_date):
    active = campaigns[
        (campaigns["start_date"] <= order_date)
        & (campaigns["end_date"] >= order_date)
    ]

    if len(active) > 0 and rng.random() < 0.45:
        return rng.choice(active["campaign_id"].values)

    return None


# =========================
# ORDERS
# =========================

coupon_rate = {
    None: 0.00,
    "WELCOME10": 0.10,
    "EID15": 0.15,
    "FLASH5": 0.05,
    "VIP20": 0.20
}

orders = []

for i in range(1, N_ORDERS + 1):
    customer = customers.sample(1, random_state=int(rng.integers(1, 999999))).iloc[0]
    order_date = rand_date(max(START, customer["signup_date"]), END)

    status = rng.choice(["Completed", "Cancelled", "Returned"], p=[0.84, 0.09, 0.07])

    if status == "Cancelled":
        delivery_date = pd.NaT
        delivery_status = "Cancelled"
    else:
        delivery_days = int(rng.integers(1, 8))
        delivery_date = order_date + pd.Timedelta(days=delivery_days)
        delivery_status = "Delayed" if delivery_days > 5 else "Delivered"

    orders.append({
        "order_id": make_id("ORD", i),
        "customer_id": customer["customer_id"],
        "order_date": order_date,
        "order_status": status,
        "payment_method": rng.choice(["Card", "Cash on Delivery", "bKash", "Nagad"], p=[0.25, 0.30, 0.30, 0.15]),
        "device_type": rng.choice(["Mobile", "Desktop", "App"], p=[0.52, 0.24, 0.24]),
        "shipping_city": customer["city"],
        "shipping_country": "Bangladesh",
        "delivery_date": delivery_date,
        "delivery_status": delivery_status,
        "coupon_code": rng.choice(list(coupon_rate.keys()), p=[0.55, 0.15, 0.12, 0.12, 0.06]),
        "campaign_id": get_campaign(order_date)
    })

orders = pd.DataFrame(orders)


# =========================
# ORDER ITEMS
# =========================

order_items = []
item_id = 1

for _, order in orders.iterrows():
    available_products = products[products["launch_date"] <= order["order_date"]]
    selected = available_products.sample(
        n=int(rng.choice([1, 2, 3], p=[0.55, 0.30, 0.15])),
        random_state=int(rng.integers(1, 999999))
    )

    discount_rate = coupon_rate[order["coupon_code"]]

    for _, product in selected.iterrows():
        qty = int(rng.choice([1, 2, 3], p=[0.75, 0.20, 0.05]))
        gross = money(product["selling_price"] * qty)
        discount = money(gross * discount_rate)
        total = money(gross - discount)
        cost = money(product["cost_price"] * qty)
        profit = money(total - cost)

        order_items.append({
            "order_item_id": make_id("ITEM", item_id),
            "order_id": order["order_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": money(product["selling_price"]),
            "gross_amount": gross,
            "discount_amount": discount,
            "line_total": total,
            "product_cost": cost,
            "line_profit": profit
        })

        item_id += 1

order_items = pd.DataFrame(order_items)


# =========================
# ORDER TOTALS
# =========================

summary = order_items.groupby("order_id").agg(
    items_total=("line_total", "sum"),
    total_discount=("discount_amount", "sum"),
    item_profit=("line_profit", "sum")
).reset_index()

orders = orders.merge(summary, on="order_id", how="left")

orders["shipping_fee"] = np.where(
    orders["order_status"] == "Cancelled",
    0,
    np.where(orders["items_total"] >= 7000, 0, np.where(orders["shipping_city"] == "Dhaka", 80, 120))
)

orders["shipping_cost"] = np.where(
    orders["order_status"] == "Cancelled",
    0,
    np.where(orders["shipping_city"] == "Dhaka", 55, 90)
)

orders["order_total"] = (orders["items_total"] + orders["shipping_fee"]).round(2)
orders["gross_profit_before_return"] = (
    orders["item_profit"] + orders["shipping_fee"] - orders["shipping_cost"]
).round(2)

orders.loc[orders["order_status"] == "Cancelled", "gross_profit_before_return"] = 0


# =========================
# RETURNS
# =========================

returns = []
returned_orders = orders[orders["order_status"] == "Returned"]

for i, (_, order) in enumerate(returned_orders.iterrows(), start=1):
    item = order_items[order_items["order_id"] == order["order_id"]].sample(
        1,
        random_state=int(rng.integers(1, 999999))
    ).iloc[0]

    returned_qty = int(rng.integers(1, item["quantity"] + 1))
    refund = money((item["line_total"] / item["quantity"]) * returned_qty)
    returned_profit = money((item["line_profit"] / item["quantity"]) * returned_qty)

    returns.append({
        "return_id": make_id("RET", i),
        "order_id": order["order_id"],
        "product_id": item["product_id"],
        "return_date": order["delivery_date"] + pd.Timedelta(days=int(rng.integers(1, 15))),
        "returned_quantity": returned_qty,
        "return_reason": rng.choice(["Damaged Product", "Wrong Size", "Late Delivery", "Quality Issue", "Customer Changed Mind"]),
        "refund_amount": refund,
        "returned_profit": returned_profit,
        "return_processing_cost": money(rng.choice([50, 80, 120])),
        "return_status": rng.choice(["Approved", "Pending"], p=[0.88, 0.12])
    })

returns = pd.DataFrame(returns)

if len(returns) > 0:
    ret_summary = returns.groupby("order_id").agg(
        refund_total=("refund_amount", "sum"),
        returned_profit_total=("returned_profit", "sum"),
        return_processing_cost_total=("return_processing_cost", "sum")
    ).reset_index()
else:
    ret_summary = pd.DataFrame(columns=["order_id", "refund_total", "returned_profit_total", "return_processing_cost_total"])

orders = orders.merge(ret_summary, on="order_id", how="left")
orders[["refund_total", "returned_profit_total", "return_processing_cost_total"]] = orders[
    ["refund_total", "returned_profit_total", "return_processing_cost_total"]
].fillna(0)

orders["net_revenue"] = np.where(
    orders["order_status"] == "Cancelled",
    0,
    orders["order_total"] - orders["refund_total"]
).round(2)

orders["net_profit"] = np.where(
    orders["order_status"] == "Cancelled",
    0,
    orders["gross_profit_before_return"] - orders["returned_profit_total"] - orders["return_processing_cost_total"]
).round(2)


# =========================
# PAYMENTS
# =========================

fee_rate = {
    "Card": 0.020,
    "Cash on Delivery": 0.010,
    "bKash": 0.015,
    "Nagad": 0.015
}

payments = []

for i, (_, order) in enumerate(orders.iterrows(), start=1):
    if order["order_status"] == "Cancelled":
        status = rng.choice(["Failed", "Cancelled"])
        paid = 0
        fee = 0
        refund = 0
    elif order["order_status"] == "Returned":
        status = "Refunded"
        paid = order["order_total"]
        fee = money(paid * fee_rate[order["payment_method"]])
        refund = order["refund_total"]
    else:
        status = "Success"
        paid = order["order_total"]
        fee = money(paid * fee_rate[order["payment_method"]])
        refund = 0

    payments.append({
        "payment_id": make_id("PAY", i),
        "order_id": order["order_id"],
        "payment_date": order["order_date"],
        "payment_method": order["payment_method"],
        "payment_status": status,
        "amount_paid": money(paid),
        "transaction_fee": fee,
        "refund_amount": money(refund)
    })

payments = pd.DataFrame(payments)


# =========================
# SESSIONS
# =========================

sessions = []
campaign_channel = dict(zip(campaigns["campaign_id"], campaigns["channel"]))

for i, (_, order) in enumerate(orders.iterrows(), start=1):
    sessions.append({
        "session_id": make_id("SES", i),
        "customer_id": order["customer_id"],
        "session_date": order["order_date"],
        "traffic_source": campaign_channel.get(order["campaign_id"], rng.choice(channels)),
        "campaign_id": order["campaign_id"],
        "device_type": order["device_type"],
        "pages_viewed": int(rng.integers(3, 18)),
        "session_duration_minutes": round(float(rng.uniform(4, 28)), 1),
        "added_to_cart": "Yes",
        "checkout_started": "Yes",
        "order_placed": "Yes",
        "order_id": order["order_id"]
    })

for i in range(len(sessions) + 1, N_SESSIONS + 1):
    added = rng.random() < 0.40
    checkout = added and rng.random() < 0.45

    sessions.append({
        "session_id": make_id("SES", i),
        "customer_id": rng.choice(customers["customer_id"]),
        "session_date": rand_date(),
        "traffic_source": rng.choice(channels),
        "campaign_id": None,
        "device_type": rng.choice(["Mobile", "Desktop", "App"]),
        "pages_viewed": int(rng.integers(1, 12)),
        "session_duration_minutes": round(float(rng.uniform(1, 18)), 1),
        "added_to_cart": "Yes" if added else "No",
        "checkout_started": "Yes" if checkout else "No",
        "order_placed": "No",
        "order_id": None
    })

sessions = pd.DataFrame(sessions)


# =========================
# INVENTORY
# =========================

valid_order_ids = orders[orders["order_status"] != "Cancelled"]["order_id"]
sold_qty = order_items[order_items["order_id"].isin(valid_order_ids)].groupby("product_id")["quantity"].sum()

if len(returns) > 0:
    returned_qty = returns.groupby("product_id")["returned_quantity"].sum()
else:
    returned_qty = pd.Series(dtype=int)

inventory = []

for i, (_, product) in enumerate(products.iterrows(), start=1):
    sold = int(sold_qty.get(product["product_id"], 0))
    returned = int(returned_qty.get(product["product_id"], 0))
    beginning = int(rng.integers(max(80, sold), max(150, sold + 200)))
    restocked = int(rng.integers(20, 150))
    ending = beginning + restocked - sold + returned

    inventory.append({
        "inventory_id": make_id("INV", i),
        "product_id": product["product_id"],
        "warehouse_location": rng.choice(["Dhaka", "Chattogram", "Gazipur"]),
        "beginning_stock": beginning,
        "units_sold": sold,
        "units_returned": returned,
        "units_restocked": restocked,
        "ending_stock": ending,
        "stockout_risk": "Yes" if ending < 30 else "No"
    })

inventory = pd.DataFrame(inventory)


# =========================
# REVIEWS
# =========================

reviews = []
reviewable = orders[orders["order_status"].isin(["Completed", "Returned"])].sample(frac=0.38, random_state=42)

review_text = {
    1: "Very poor experience",
    2: "Not satisfied with the product",
    3: "Average product quality",
    4: "Good product and service",
    5: "Excellent product, highly recommended"
}

for i, (_, order) in enumerate(reviewable.iterrows(), start=1):
    item = order_items[order_items["order_id"] == order["order_id"]].sample(
        1,
        random_state=int(rng.integers(1, 999999))
    ).iloc[0]

    rating = int(rng.choice([1, 2, 3], p=[0.35, 0.45, 0.20])) if order["order_status"] == "Returned" else int(rng.choice([3, 4, 5], p=[0.20, 0.45, 0.35]))

    reviews.append({
        "review_id": make_id("REV", i),
        "customer_id": order["customer_id"],
        "order_id": order["order_id"],
        "product_id": item["product_id"],
        "rating": rating,
        "review_text": review_text[rating],
        "review_date": order["delivery_date"] + pd.Timedelta(days=int(rng.integers(1, 25)))
    })

reviews = pd.DataFrame(reviews)


# =========================
# CAMPAIGN PERFORMANCE
# =========================

campaign_result = orders[
    (orders["campaign_id"].notna()) & (orders["order_status"] != "Cancelled")
].groupby("campaign_id").agg(
    conversions=("order_id", "count"),
    revenue_generated=("net_revenue", "sum")
).reset_index()

campaigns = campaigns.merge(campaign_result, on="campaign_id", how="left")
campaigns["conversions"] = campaigns["conversions"].fillna(0).astype(int)
campaigns["revenue_generated"] = campaigns["revenue_generated"].fillna(0).round(2)
campaigns["clicks"] = campaigns.apply(lambda x: max(x["clicks"], x["conversions"] * 12), axis=1)
campaigns["roas"] = (campaigns["revenue_generated"] / campaigns["campaign_cost"]).round(2)
campaigns["conversion_rate"] = (campaigns["conversions"] / campaigns["clicks"]).round(4)


# =========================
# CUSTOMER SEGMENT
# =========================

customer_summary = orders[orders["order_status"] != "Cancelled"].groupby("customer_id").agg(
    total_orders=("order_id", "count"),
    total_spent=("net_revenue", "sum")
).reset_index()

customers = customers.merge(customer_summary, on="customer_id", how="left")
customers["total_orders"] = customers["total_orders"].fillna(0).astype(int)
customers["total_spent"] = customers["total_spent"].fillna(0).round(2)

customers["customer_segment"] = np.select(
    [
        (customers["total_orders"] >= 6) | (customers["total_spent"] >= 30000),
        customers["total_orders"] >= 2
    ],
    ["Premium", "Regular"],
    default="New"
)


# =========================
# SAVE CSV FILES
# =========================

tables = {
    "customers.csv": customers,
    "products.csv": products,
    "orders.csv": orders,
    "order_items.csv": order_items,
    "payments.csv": payments,
    "returns.csv": returns,
    "marketing_campaigns.csv": campaigns,
    "sessions.csv": sessions,
    "inventory.csv": inventory,
    "reviews.csv": reviews
}

for file_name, df in tables.items():
    df = df.copy()

    for col in df.columns:
        if "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    df.to_csv(os.path.join(OUT, file_name), index=False)

print("Dataset generated successfully.")
print(f"Saved in folder: {OUT}")

for file_name, df in tables.items():
    print(f"{file_name}: {len(df)} rows")