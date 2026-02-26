import pandas as pd
import os

# Get the directory where model.py is located
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "warehouse_sales_export.csv")

df = pd.read_csv(file_path)
# df['order_date'] = pd.to_datetime(df['order_date'])

print(df.head())
print(df.info())
print(df.describe())