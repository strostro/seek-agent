import json
import re

AI_PROMPT_TEMPLATE = """
You are classifying companies in the New Zealand job market.

Company name: "{company_name}"

Job description excerpt:
\"\"\"
{description}
\"\"\"

Classify the company into EXACTLY ONE label for EACH dimension:

Industry (choose one):
- Government
- Finance
- Consulting
- Retail
- Technology
- Utilities
- Healthcare
- Education
- Manufacturing
- Construction
- Logistics & Supply Chain
- Agriculture & Farming
- Energy
- Media & Communications
- Hospitality & Tourism
- Professional Services
- Non-profit / NGO
- Other

Type (choose one):
- Public Sector
- Private Sector
- Non-profit
- Recruiter
- Other

Size (choose one):
- Enterprise (1000+)
- Mid-size (200–999)
- Small (50–199)
- SME (<50)

Respond ONLY with JSON:
{{
  "industry": {{"label": "...", "confidence": 0.0}},
  "type": {{"label": "...", "confidence": 0.0}},
  "size": {{"label": "...", "confidence": 0.0}},
  "reason": "..."
}}
"""

def classify_company_ai(client, company_name, desc):
    safe_desc = json.dumps(desc[:1500])[1:-1]

    prompt = AI_PROMPT_TEMPLATE.format(
        company_name=company_name.replace('"', "'"),
        description=safe_desc
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"^```json", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"```$", "", raw).strip()

    try:
        return json.loads(raw)
    except:
        return {
            "industry": {"label": "Other", "confidence": 0.0},
            "type": {"label": "Other", "confidence": 0.0},
            "size": {"label": "SME (<50)", "confidence": 0.0},
            "reason": "fallback"
        }