import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Try loading from the env file in AI-Powered Business Intelligence Platform
dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'AI-Powered Business Intelligence Platform', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"[OK] Loaded credentials from: {dotenv_path}")
else:
    load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")
DB_NAME = "ecom_projects"

# Detect which PostgreSQL DB driver is installed
driver = "psycopg2"
try:
    import psycopg2
except ImportError:
    try:
        import psycopg
        driver = "psycopg"
    except ImportError:
        pass

print(f"[INFO] Using driver: {driver}")

# 1. Connect to postgres (default DB) to check and create DB if needed
postgres_url = f"postgresql+{driver}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
temp_engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")

try:
    with temp_engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'ecom_projects'"))
        exists = result.scalar()
        if not exists:
            print(f"[INFO] Database '{DB_NAME}' does not exist. Creating...")
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
            print(f"[OK] Database '{DB_NAME}' created successfully.")
        else:
            print(f"[OK] Database '{DB_NAME}' already exists.")
except Exception as e:
    print(f"[WARNING] Error checking/creating database: {e}")
    print("Trying to proceed with direct connection...")
finally:
    temp_engine.dispose()

# 2. Connect to the ecom_projects database
target_url = f"postgresql+{driver}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(target_url)

# 3. Define dataset folder and map files to tables
dataset_dir = os.path.join(os.path.dirname(__file__), 'ecommerce_dataset')
print(f"[INFO] Looking for dataset CSV files in: {dataset_dir}")

data_files = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
    "returns": "returns.csv",
    "marketing_campaigns": "marketing_campaigns.csv",
    "sessions": "sessions.csv",
    "inventory": "inventory.csv",
    "reviews": "reviews.csv"
}

try:
    with engine.begin() as connection:
        print("[OK] Connection established. Uploading datasets...")
        for table_name, file_name in data_files.items():
            file_path = os.path.join(dataset_dir, file_name)
            if os.path.exists(file_path):
                print(f"[INFO] Uploading {file_name} to table '{table_name}'...")
                df = pd.read_csv(file_path)
                
                # Convert date columns back to datetimes so Pandas handles the SQL schema conversion properly
                for col in df.columns:
                    if "date" in col:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                
                # Upload to Postgres
                df.to_sql(
                    table_name,
                    con=connection,
                    if_exists='replace',
                    index=False,
                    chunksize=5000
                )
                print(f"   [OK] Successfully uploaded {len(df)} rows.")
            else:
                print(f"   [WARNING] File not found: {file_path}")
                
    print("\n[SUCCESS] All datasets pushed successfully to database 'ecom_projects'!")
except Exception as connection_error:
    print(f"[ERROR] Failed to push dataset: {connection_error}")
finally:
    engine.dispose()
