import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
import json
import os
import math

load_dotenv()


def get_connection():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(), password=None
        )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )


def clean_val(val):
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if str(val).lower() == 'nan':
        return None
    return val


def clean_skills_dict(d):
    if not isinstance(d, dict):
        return {}
    return {k: (0 if (isinstance(v, float) and math.isnan(v)) else int(v)) for k, v in d.items()}


def save_raw_jobs(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_JOBS (
            job_id VARCHAR PRIMARY KEY,
            source_keyword VARCHAR,
            scrape_time_utc VARCHAR,
            title VARCHAR,
            company VARCHAR,
            location VARCHAR,
            classification VARCHAR,
            work_type VARCHAR,
            salary VARCHAR,
            posted_raw VARCHAR,
            posted_date DATE,
            description TEXT,
            url VARCHAR
        )
    """)

    saved = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO RAW_JOBS (
                    job_id, source_keyword, scrape_time_utc, title,
                    company, location, classification, work_type,
                    salary, posted_raw, posted_date, description, url
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM RAW_JOBS WHERE job_id = %s
                )
            """, (
                clean_val(row["job_id"]),
                clean_val(row["source_keyword"]),
                clean_val(row["scrape_time_utc"]),
                clean_val(row["title"]),
                clean_val(row["company"]),
                clean_val(row["location"]),
                clean_val(row["classification"]),
                clean_val(row["work_type"]),
                clean_val(row["salary"]),
                clean_val(row["posted_raw"]),
                str(row["posted_date"]) if clean_val(row.get("posted_date")) else None,
                clean_val(row["description"]),
                clean_val(row["url"]),
                clean_val(row["job_id"])
            ))
            if cursor.rowcount > 0:
                saved += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"Failed to insert job_id {row['job_id']}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Raw jobs - Saved: {saved} new, Skipped: {skipped} existing")


def save_clean_jobs(df: pd.DataFrame):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CLEAN_JOBS (
            job_id VARCHAR PRIMARY KEY,
            source_keyword VARCHAR,
            scrape_time_utc VARCHAR,
            title VARCHAR,
            company VARCHAR,
            location VARCHAR,
            city VARCHAR,
            region_standardised VARCHAR,
            island VARCHAR,
            classification VARCHAR,
            work_type VARCHAR,
            salary VARCHAR,
            posted_date DATE,
            description TEXT,
            url VARCHAR,
            skills_dict VARIANT,
            company_industry VARCHAR,
            company_type VARCHAR,
            company_size VARCHAR,
            role_standardised VARCHAR,
            role_subtype VARCHAR
        )
    """)

    saved = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            skills = clean_skills_dict(row.get("skills_dict"))

            cursor.execute("""
                INSERT INTO CLEAN_JOBS (
                    job_id, source_keyword, scrape_time_utc, title,
                    company, location, city, region_standardised, island,
                    classification, work_type, salary, posted_date,
                    description, url, skills_dict,
                    company_industry, company_type, company_size,
                    role_standardised, role_subtype
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, PARSE_JSON(%s),
                       %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM CLEAN_JOBS WHERE job_id = %s
                )
            """, (
                clean_val(row["job_id"]),
                clean_val(row["source_keyword"]),
                clean_val(row["scrape_time_utc"]),
                clean_val(row["title"]),
                clean_val(row["company"]),
                clean_val(row["location"]),
                clean_val(row.get("city")),
                clean_val(row.get("region_standardised")),
                clean_val(row.get("island")),
                clean_val(row["classification"]),
                clean_val(row["work_type"]),
                clean_val(row["salary"]),
                str(row["posted_date"]) if clean_val(row.get("posted_date")) else None,
                clean_val(row["description"]),
                clean_val(row["url"]),
                json.dumps(skills),
                clean_val(row.get("industry")),
                clean_val(row.get("type")),
                clean_val(row.get("size")),
                clean_val(row.get("role_standardised")),
                clean_val(row.get("role_subtype")),
                clean_val(row["job_id"])
            ))
            if cursor.rowcount > 0:
                saved += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"Failed to insert job_id {row['job_id']}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Clean jobs - Saved: {saved} new, Skipped: {skipped} existing")


def save_company_dim(rows: list):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS COMPANY_DIM (
            company_id INTEGER,
            company_name VARCHAR PRIMARY KEY,
            industry VARCHAR,
            industry_conf FLOAT,
            type VARCHAR,
            type_conf FLOAT,
            type_source VARCHAR,
            size VARCHAR,
            size_conf FLOAT,
            first_seen_date DATE
        )
    """)

    updated = 0
    inserted = 0

    for row in rows:
        try:
            cursor.execute("""
                MERGE INTO COMPANY_DIM AS target
                USING (
                    SELECT %s AS company_name
                ) AS source
                ON target.company_name = source.company_name
                WHEN MATCHED THEN UPDATE SET
                    industry = %s,
                    industry_conf = %s,
                    type = %s,
                    type_conf = %s,
                    type_source = %s,
                    size = %s,
                    size_conf = %s
                WHEN NOT MATCHED THEN INSERT (
                    company_id, company_name, industry, industry_conf,
                    type, type_conf, type_source, size, size_conf, first_seen_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["company_name"],
                clean_val(row.get("industry")),
                clean_val(row.get("industry_conf")),
                clean_val(row.get("type")),
                clean_val(row.get("type_conf")),
                clean_val(row.get("type_source")),
                clean_val(row.get("size")),
                clean_val(row.get("size_conf")),
                clean_val(row.get("company_id")),
                row["company_name"],
                clean_val(row.get("industry")),
                clean_val(row.get("industry_conf")),
                clean_val(row.get("type")),
                clean_val(row.get("type_conf")),
                clean_val(row.get("type_source")),
                clean_val(row.get("size")),
                clean_val(row.get("size_conf")),
                clean_val(row.get("first_seen_date"))
            ))
            if cursor.rowcount == 1:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            print(f"Failed to upsert company {row.get('company_name')}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Company dim - Inserted: {inserted}, Updated: {updated}")


def load_company_dim_from_snowflake() -> dict:
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT company_id, company_name, industry, industry_conf,
                   type, type_conf, type_source, size, size_conf, first_seen_date
            FROM COMPANY_DIM
        """)
        rows = cursor.fetchall()
        columns = [
            "company_id", "company_name", "industry", "industry_conf",
            "type", "type_conf", "type_source", "size", "size_conf", "first_seen_date"
        ]
        data = {}
        for row in rows:
            record = dict(zip(columns, row))
            data[record["company_name"].lower()] = record
        print(f"Loaded {len(data)} companies from Snowflake")
        return data
    except Exception as e:
        print(f"Could not load from Snowflake, returning empty: {e}")
        return {}
    finally:
        cursor.close()
        conn.close()