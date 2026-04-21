import csv
import os
import pandas as pd
from datetime import date

DIM_FILE = "output/company_dim.csv"

DIM_FIELDS = [
    "company_id", "company_name", "industry", "industry_conf",
    "type", "type_conf", "type_source", "size", "size_conf", "first_seen_date",
]


def load_company_dim() -> dict:

    try:
        from tools.database import load_company_dim_from_snowflake
        data = load_company_dim_from_snowflake()
        if data:
            return data
    except Exception as e:
        print(f"Snowflake load failed, falling back to local CSV: {e}")


    if not os.path.exists(DIM_FILE):
        return {}
    data = {}
    with open(DIM_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data[row["company_name"].lower()] = row
    print(f"Loaded {len(data)} companies from local CSV")
    return data


def save_company_dim(rows: list):
    # save to csv
    os.makedirs("output", exist_ok=True)
    with open(DIM_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DIM_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    # write to Snowflake
    try:
        from tools.database import save_company_dim as sf_save
        sf_save(rows)
    except Exception as e:
        print(f"Snowflake save failed: {e}")


def read_jobs_from_df(df: pd.DataFrame) -> dict:
    companies = {}
    for _, row in df.iterrows():
        name = str(row.get("company") or "").strip()
        desc = str(row.get("description") or "").strip()
        if name and (name not in companies or len(desc) > len(companies[name])):
            companies[name] = desc
    return companies