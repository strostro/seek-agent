import re
import json
from dotenv import load_dotenv
from openai import OpenAI
import os

load_dotenv()


############################################################
# 1. Rule-based classify
############################################################

ROLE_RULES = [
    # Data Engineer
    ("data engineer", "Data Engineer", "Data Engineer"),
    ("analytics engineer", "Data Engineer", "Analytics Engineer"),
    ("data platform engineer", "Data Engineer", "Data Engineer"),
    ("data infrastructure", "Data Engineer", "Data Engineer"),

    # Data Scientist
    ("data scientist", "Data Scientist", "Data Scientist"),
    ("machine learning engineer", "Data Scientist", "ML Engineer"),
    ("ml engineer", "Data Scientist", "ML Engineer"),

    # AI Engineer
    ("ai engineer", "AI Engineer", "AI Engineer"),
    ("llm engineer", "AI Engineer", "AI Engineer"),
    ("generative ai", "AI Engineer", "AI Engineer"),

    # Business Analyst
    ("business analyst", "Business Analyst", "Business Analyst"),

    # Analyst subtypes
    ("bi analyst", "Analyst", "BI Analyst"),
    ("bi developer", "Analyst", "BI Analyst"),
    ("power bi developer", "Analyst", "BI Analyst"),
    ("reporting analyst", "Analyst", "BI Analyst"),
    ("insights analyst", "Analyst", "BI Analyst"),
    ("data analyst", "Analyst", "Data Analyst"),
    ("product analyst", "Analyst", "Product Analyst"),
    ("marketing analyst", "Analyst", "Marketing Analyst"),
    ("customer insight", "Analyst", "Marketing Analyst"),
    ("gis analyst", "Analyst", "GIS Analyst"),
    ("geospatial analyst", "Analyst", "GIS Analyst"),
    ("financial analyst", "Analyst", "Financial Analyst"),
    ("hr analyst", "Analyst", "HR Analyst"),
    ("people analyst", "Analyst", "HR Analyst"),
    ("workforce analyst", "Analyst", "HR Analyst"),
    ("research analyst", "Analyst", "Research Analyst"),
]


def classify_role_by_rules(title: str):
    """
    返回 (role, subtype) 如果匹配到规则
    返回 (None, None) 如果没有匹配，交给 LLM 处理
    """
    title_lower = title.lower()
    for keyword, role, subtype in ROLE_RULES:
        if keyword in title_lower:
            return role, subtype
    return None, None


############################################################
# 2. AI classify
############################################################

def classify_role_ai(client, title, description):
    prompt = f"""
You are classifying data-related job postings in the New Zealand job market.

You will be given:
- Job title
- Job description

Use BOTH title and description to determine the correct classification.

Job title: "{title}"

Job description:
\"\"\"
{description}
\"\"\"

You MUST classify the job into a role and subtype.
Never leave subtype empty. Always choose one valid subtype.

Output a JSON object with EXACTLY these fields:

- "role": one of:
  ["Data Engineer", "Data Scientist", "AI Engineer", "Analyst", "Business Analyst", "Other"]

- "subtype": based on role:
  If role = "Data Engineer":
      ["Data Engineer", "Analytics Engineer", "Other"]

  If role = "Data Scientist":
      ["Data Scientist", "ML Engineer", "Other"]

  If role = "AI Engineer":
      ["AI Engineer"]

  If role = "Business Analyst":
      ["Business Analyst"]

  If role = "Analyst":
      ["Data Analyst", "BI Analyst", "Product Analyst", "Marketing Analyst",
       "GIS Analyst", "Financial Analyst", "HR Analyst", "Research Analyst", "Other"]

  If role = "Other":
      ["Other"]

When role = "Analyst", you MUST apply these subtype rules using BOTH title and description:

- If the work is mainly about BI, dashboards, reporting, Power BI, visualisation, or "insights analyst":
    subtype = "BI Analyst"

- If the work is mainly about customer insights, marketing analytics, campaigns, CRM, segmentation, loyalty:
    subtype = "Marketing Analyst"

- If the work is mainly about product analytics, experiments, A/B testing, feature usage:
    subtype = "Product Analyst"

- If the work is mainly about GIS, geospatial, spatial analysis, ArcGIS, QGIS:
    subtype = "GIS Analyst"

- If the work is mainly about finance, FP&A, budgeting, forecasting, financial modelling:
    subtype = "Financial Analyst"

- If the work is mainly about people analytics, workforce analytics, HR analytics, talent analytics:
    subtype = "HR Analyst"

- If the work is mainly about research, evaluation, surveys, qualitative/quantitative studies:
    subtype = "Research Analyst"

- If none of the above clearly applies, but it is still an analyst role:
    subtype = "Data Analyst" or "Other"

Respond ONLY with JSON.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"^```json", "", raw).strip()
    raw = re.sub(r"```$", "", raw).strip()

    try:
        return json.loads(raw)
    except:
        return {"role": "Other", "subtype": "Other"}


############################################################
# 3. Alias mapping
############################################################

ROLE_ALIASES = {
    "Engineer": "Data Engineer",
}

SUBTYPE_ALIASES = {
    "Insights Analyst": "BI Analyst",
    "Reporting Analyst": "BI Analyst",
    "Customer Insight Analyst": "Marketing Analyst",
    "BI Developer": "BI Analyst",
}


def normalize_role(label):
    return ROLE_ALIASES.get(label, label)


def normalize_subtype(role, subtype):
    if not subtype:
        return "Other"

    if role == "Business Analyst":
        return "Business Analyst"

    if role == "AI Engineer":
        return "AI Engineer"

    if role == "Data Engineer":
        return subtype if subtype in ["Data Engineer", "Analytics Engineer", "Other"] else "Other"

    if role == "Data Scientist":
        return subtype if subtype in ["Data Scientist", "ML Engineer", "Other"] else "Other"

    if role == "Analyst":
        return SUBTYPE_ALIASES.get(subtype, subtype)

    return "Other"


############################################################
# 4. main
############################################################

def classify_role(title, description, client):
    # Step 1: rule-based 先跑
    role, subtype = classify_role_by_rules(title)
    if role:
        return role, subtype

    # Step 2: LLM fallback
    ai_result = classify_role_ai(client, title, description)
    role = normalize_role(ai_result["role"])
    subtype = ai_result["subtype"]

    title_has_analyst = (
        "analyst" in title.lower() or
        "specialist" in title.lower()
    )

    if role == "Analyst" and not title_has_analyst:
        role = "Other"
        subtype = "Other"

    subtype = normalize_subtype(role, subtype)
    return role, subtype



############################################################

def apply_role_classification(df, client):
    import sys
    import time

    total = len(df)
    processed = 0
    rule_hits = 0
    llm_hits = 0

    role_list = []
    subtype_list = []

    print(f"Classifying {total} roles...")

    for idx, row in df.iterrows():
        title = row["title"]
        description = row.get("description", "")

        # 检查是否 rule 命中
        rule_role, rule_subtype = classify_role_by_rules(title)
        if rule_role:
            rule_hits += 1
        else:
            llm_hits += 1

        role, subtype = classify_role(title, description, client)

        role_list.append(role)
        subtype_list.append(subtype)

        processed += 1

        bar_len = 30
        filled = int(bar_len * processed / total)
        bar = "#" * filled + "-" * (bar_len - filled)
        sys.stdout.write(f"\r[{bar}] {processed}/{total}  {role} / {subtype}")
        sys.stdout.flush()

        time.sleep(0.005)

    print(f"\nRole classification completed. Rules: {rule_hits}, LLM: {llm_hits}")

    df["role_standardised"] = role_list
    df["role_subtype"] = subtype_list

    return df