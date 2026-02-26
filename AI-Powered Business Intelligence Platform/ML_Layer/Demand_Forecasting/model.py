import pandas as pd

df = pd.read_csv("ML_layer/Demand_Forecasting/warehouse_sales_export.csv")
# df['order_date'] = pd.to_datetime(df['order_date'])

print(df.head())
print(df.info())
print(df.describe())