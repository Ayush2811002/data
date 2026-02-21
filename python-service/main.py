
from fastapi import FastAPI
import pandas as pd
import mysql.connector
import os
import uvicorn
app = FastAPI()

# ✅ AI BUSINESS SUMMARY
@app.post("/generate-summary")
def generate_summary(table: dict):

    summary = f"This table '{table['tableName']}' stores business data with {len(table['columns'])} attributes."

    return {
        "tableName": table["tableName"],
        "businessSummary": summary
    }

# ✅ TIMESTAMP DETECTION ENGINE 🔥
def detect_timestamp_column(df):

    timestamp_candidates = [
        "created_at",
        "created_on",
        "updated_at",
        "last_updated",
        "modified_date",
        "modified_on",
        "created",
        "created_date",
        "last_updated_date"
    ]

    for col in df.columns:
        if col.lower() in timestamp_candidates:
            return col

    return None

# ✅ DATA QUALITY + FRESHNESS + RISK ENGINE 😈🔥
@app.post("/analyze-data")
def analyze_data(config: dict):

    conn = mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

    results = []

    for table in config["tables"]:

        table_name = table["tableName"]

        # ✅ PERFORMANCE SAFE SAMPLING 🔥
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 200", conn)

        column_metrics = []

        for col in df.columns:

            completeness = df[col].notnull().mean() * 100 if len(df) > 0 else 0
            uniqueness = df[col].nunique() / len(df) * 100 if len(df) > 0 else 0

            # ✅ NaN SAFE FIX 🔥
            completeness = 0 if pd.isna(completeness) else completeness
            uniqueness = 0 if pd.isna(uniqueness) else uniqueness

            column_metrics.append({
                "column": col,
                "completeness": round(completeness, 2),
                "uniqueness": round(uniqueness, 2)
            })

        # ✅ FRESHNESS ENGINE 🔥🔥🔥
        timestamp_column = detect_timestamp_column(df)

        freshness_info = {
            "lastUpdated": None,
            "status": "UNKNOWN"
        }

        if timestamp_column:

            latest_time = pd.read_sql(
                f"SELECT MAX({timestamp_column}) as lastUpdated FROM {table_name}",
                conn
            )

            last_updated = latest_time.iloc[0]["lastUpdated"]

            if last_updated:
                freshness_info["lastUpdated"] = str(last_updated)
                freshness_info["status"] = "ACTIVE"
            else:
                freshness_info["status"] = "NO DATA"

        else:
            freshness_info["status"] = "NO TIMESTAMP"

        # ✅ AI RISK ENGINE 😈🔥🔥🔥
        risks = []

        # ✅ COLUMN RISKS
        for metric in column_metrics:

            if metric["completeness"] < 50:
                risks.append(
                    f"Column '{metric['column']}' has low completeness ({metric['completeness']}%) → Missing value risk"
                )

            if metric["uniqueness"] < 10:
                risks.append(
                    f"Column '{metric['column']}' has very low uniqueness ({metric['uniqueness']}%) → Duplicate-heavy field"
                )

        # ✅ TABLE RISKS
        if len(df) == 0:
            risks.append("Table contains no sampled data → Dataset inactive")

        # ✅ FRESHNESS RISKS
        if freshness_info["status"] == "NO DATA":
            risks.append("Freshness check indicates NO DATA → Table not actively updated")

        if freshness_info["status"] == "NO TIMESTAMP":
            risks.append("No timestamp column detected → Freshness monitoring unavailable")

        # ✅ STRUCTURAL RISKS 😈
        for col in df.columns:
            if "payload" in col.lower():
                risks.append(
                    f"Column '{col}' appears to store payload data → Potential heavy storage / logging field"
                )

        results.append({
            "tableName": table_name,
            "metrics": column_metrics,
            "freshness": freshness_info,
            "risks": risks
        })

    conn.close()

    return results
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
