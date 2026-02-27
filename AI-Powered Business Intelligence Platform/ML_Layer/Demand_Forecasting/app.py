import os
import joblib
import pandas as pd
import numpy as np
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# -----------------------
# Load env
# -----------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

MODEL_PATH = "saved_model/best_model.pkl"
MODEL_NAME = "best_5_models_v1"

# 👇 Change this to your real history source (table/view)
# Must have columns: date, category, region, quantity
HISTORY_TABLE = "analytics.ml_daily_demand_category_region"  # example name

# Where to save forecast
FORECAST_TABLE = "analytics.demand_forecast_15d"


def get_db_engine():
    connection_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_url, pool_pre_ping=True)


engine = get_db_engine()
model = joblib.load(MODEL_PATH)

app = Flask(__name__)


# -----------------------
# Feature function (same as training)
# -----------------------
def make_features(data: pd.DataFrame) -> pd.DataFrame:
    d = data.sort_values(["category", "region", "date"]).copy()

    d["dow"] = d["date"].dt.dayofweek
    d["month"] = d["date"].dt.month
    d["is_weekend"] = (d["dow"] >= 5).astype(int)

    g = d.groupby(["category", "region"], sort=False)
    d["lag_1"] = g["quantity"].shift(1)
    d["lag_7"] = g["quantity"].shift(7)
    d["lag_14"] = g["quantity"].shift(14)
    d["rollmean_7"] = g["quantity"].shift(1).rolling(7).mean()

    return d


# -----------------------
# Pull history from PGSQL
# -----------------------
def load_history_from_db(days_back: int = 365) -> pd.DataFrame:
    """
    Loads last N days of history from HISTORY_TABLE.
    """
    q = f"""
        SELECT date, category, region, quantity
        FROM {HISTORY_TABLE}
        WHERE date >= (CURRENT_DATE - INTERVAL '{days_back} days')
        ORDER BY category, region, date;
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)

    df["date"] = pd.to_datetime(df["date"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)
    df["category"] = df["category"].astype(str)
    df["region"] = df["region"].astype(str)

    return df


# -----------------------
# Forecast next N days (recursive)
# -----------------------
def forecast_recursive(df: pd.DataFrame, horizon: int = 15) -> pd.DataFrame:
    df = df.sort_values(["category", "region", "date"]).reset_index(drop=True)
    last_date = df["date"].max()
    future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=horizon, freq="D")

    forecasts = []

    for (cat, reg), g in df.groupby(["category", "region"], sort=False):
        g = g.sort_values("date").copy()

        for d in future_dates:
            # add future row
            g = pd.concat([g, pd.DataFrame([{
                "date": d, "category": cat, "region": reg, "quantity": np.nan
            }])], ignore_index=True)

            gg = make_features(g)
            row = gg.iloc[-1]

            if pd.isna(row["lag_14"]) or pd.isna(row["rollmean_7"]):
                yhat = 0.0
            else:
                X_row = pd.DataFrame([{
                    "category": cat,
                    "region": reg,
                    "dow": int(row["dow"]),
                    "month": int(row["month"]),
                    "is_weekend": int(row["is_weekend"]),
                    "lag_1": float(row["lag_1"]),
                    "lag_7": float(row["lag_7"]),
                    "lag_14": float(row["lag_14"]),
                    "rollmean_7": float(row["rollmean_7"]),
                }])

                yhat = float(model.predict(X_row)[0])
                yhat = max(0.0, yhat)

            g.loc[g.index[-1], "quantity"] = yhat

            forecasts.append({
                "forecast_date": d.date(),
                "category": cat,
                "region": reg,
                "predicted_quantity": yhat
            })

    return pd.DataFrame(forecasts)


# -----------------------
# Save forecast to DB
# -----------------------
def save_forecast_to_db(forecast_df: pd.DataFrame):
    """
    Inserts forecasts into FORECAST_TABLE.
    """
    out = forecast_df.copy()
    out["model_name"] = MODEL_NAME
    out.to_sql(
        name=FORECAST_TABLE.split(".")[1],
        con=engine,
        schema=FORECAST_TABLE.split(".")[0],
        if_exists="append",
        index=False
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/forecast", methods=["POST"])
def forecast():
    """
    Body (optional):
      {
        "horizon": 15,
        "days_back": 365
      }
    """
    payload = request.get_json(silent=True) or {}
    horizon = int(payload.get("horizon", 15))
    days_back = int(payload.get("days_back", 365))

    # 1) load history from DB
    history_df = load_history_from_db(days_back=days_back)
    if history_df.empty:
        return jsonify({"error": "No history data found in DB."}), 400

    # 2) forecast
    forecast_df = forecast_recursive(history_df, horizon=horizon)

    # 3) save to DB
    save_forecast_to_db(forecast_df)

    # 4) return response
    return jsonify({
        "horizon": horizon,
        "saved_rows": len(forecast_df),
        "forecast": forecast_df.to_dict(orient="records")
    }), 200


if __name__ == "__main__":
    # Production: gunicorn -w 2 -b 0.0.0.0:5000 app:app
    app.run(host="0.0.0.0", port=5000, debug=True)