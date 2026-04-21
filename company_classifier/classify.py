from dotenv import load_dotenv
load_dotenv()

import os
import pandas as pd
from openai import OpenAI

from company_classifier.rules import classify_type_by_rules
from company_classifier.ai_classifier import classify_company_ai
from company_classifier.io_utils import load_company_dim, save_company_dim, read_jobs_from_df
from company_classifier.utils import today

TAG_ALIASES = {
    "Small": "Small (50-199)",
    "SME": "SME (<50)",
    "Mid-size": "Mid-size (200-999)",
    "Enterprise": "Enterprise (1000+)",
    "Tech": "Technology",
    "Govt": "Government",
    "Education Sector": "Education",
    "Public": "Public Sector",
    "Private": "Private Sector",
    "Nonprofit": "Non-profit",
}

def normalize_label(label: str) -> str:
    label = label.strip()
    if label in TAG_ALIASES:
        return TAG_ALIASES[label]
    if label.lower() in ["other", "unknown", "unclear", "n/a", "none"]:
        return "Other"
    return label

def classify_single_company(client, company_name, desc):
    type_label, type_conf = classify_type_by_rules(company_name)
    ai_result = classify_company_ai(client, company_name, desc)

    industry_label = normalize_label(ai_result["industry"]["label"])
    industry_conf = ai_result["industry"]["confidence"]
    size_label = normalize_label(ai_result["size"]["label"])
    size_conf = ai_result["size"]["confidence"]

    if type_label:
        final_type_label = normalize_label(type_label)
        final_type_conf = type_conf
        source_type = "rules"
    else:
        final_type_label = normalize_label(ai_result["type"]["label"])
        final_type_conf = ai_result["type"]["confidence"]
        source_type = "ai"

    return {
        "industry": industry_label,
        "industry_conf": industry_conf,
        "type": final_type_label,
        "type_conf": final_type_conf,
        "type_source": source_type,
        "size": size_label,
        "size_conf": size_conf,
    }

def run_company_classification(df: pd.DataFrame) -> pd.DataFrame:
    """
    接收 clean_df，返回带有 company_dim 的 DataFrame
    同时把 company_dim 存成 CSV 备用
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    existing = load_company_dim()
    companies = read_jobs_from_df(df)

    new_companies = {
        name: desc for name, desc in companies.items()
        if name.lower() not in existing
    }

    print(f"Companies to classify: {len(new_companies)} new, {len(existing)} already cached")

    next_id = len(existing) + 1
    dim_rows = list(existing.values())

    for company_name, desc in new_companies.items():
        print(f"Classifying: {company_name}")
        result = classify_single_company(client, company_name, desc)
        dim_rows.append({
            "company_id": next_id,
            "company_name": company_name,
            "industry": result["industry"],
            "industry_conf": result["industry_conf"],
            "type": result["type"],
            "type_conf": result["type_conf"],
            "type_source": result["type_source"],
            "size": result["size"],
            "size_conf": result["size_conf"],
            "first_seen_date": today(),
        })
        next_id += 1

    save_company_dim(dim_rows)

    # 把分类结果 merge 回 df
    dim_df = pd.DataFrame(dim_rows)
    df = df.merge(
        dim_df[["company_name", "industry", "type", "size"]],
        left_on="company",
        right_on="company_name",
        how="left"
    ).drop(columns=["company_name"])

    print("Company classification done.")
    return df