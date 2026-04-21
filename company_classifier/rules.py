# rules.py

import re

NZ_COMPANY_DICT = {
    # Banks
    "bnz": ("Finance", "Private Sector"),
    "bank of new zealand": ("Finance", "Private Sector"),
    "anz": ("Finance", "Private Sector"),
    "asb": ("Finance", "Private Sector"),
    "westpac": ("Finance", "Private Sector"),
    "kiwibank": ("Finance", "Private Sector"),

    # Government agencies
    "msd": ("Government", "Public Sector"),
    "inland revenue": ("Government", "Public Sector"),
    "ird": ("Government", "Public Sector"),
    "mbie": ("Government", "Public Sector"),
    "moh": ("Government", "Public Sector"),
    "moe": ("Government", "Public Sector"),
    "kainga ora": ("Government", "Public Sector"),
    "nz police": ("Government", "Public Sector"),
    "new zealand police": ("Government", "Public Sector"),
    "nzdf": ("Government", "Public Sector"),
    "new zealand defence force": ("Government", "Public Sector"),

    # Universities
    "university of auckland": ("Education", "Public Sector"),
    "victoria university of wellington": ("Education", "Public Sector"),
    "massey university": ("Education", "Public Sector"),
    "university of otago": ("Education", "Public Sector"),
    "aut": ("Education", "Public Sector"),

    # Utilities / Energy
    "northpower": ("Utilities", "Private Sector"),
    "vector": ("Utilities", "Private Sector"),
    "meridian energy": ("Energy", "Private Sector"),
    "genesis energy": ("Energy", "Private Sector"),
    "contact energy": ("Energy", "Private Sector"),
    "transpower": ("Utilities", "Public Sector"),

    # Retail groups
    "the warehouse group": ("Retail", "Private Sector"),
    "foodstuffs": ("Retail", "Private Sector"),
    "countdown": ("Retail", "Private Sector"),

    # Tech companies
    "datacom": ("Technology", "Private Sector"),
    "xero": ("Technology", "Private Sector"),
    "trademe": ("Technology", "Private Sector"),
    "spark": ("Technology", "Private Sector"),
    "2degrees": ("Technology", "Private Sector"),

    # Consulting
    "deloitte": ("Professional Services", "Private Sector"),
    "pwc": ("Professional Services", "Private Sector"),
    "kpmg": ("Professional Services", "Private Sector"),
    "ey": ("Professional Services", "Private Sector"),

    # SOE
    "nz post": ("Logistics & Supply Chain", "Public Sector"),
    "kiwirail": ("Logistics & Supply Chain", "Public Sector"),
    "air new zealand": ("Hospitality & Tourism", "Public Sector"),
}

RECRUITER_KEYWORDS = [
    "recruit", "recruitment", "staffing", "talent", "resourcing",
    "randstad", "hays", "robert half", "potentia", "adecco",
    "manpower", "kelly services", "hudson", "beyond recruitment",
    "absolute it", "madison", "convergence", "stellar", "frog",
]

PUBLIC_KEYWORDS = [
    "ministry", "department", "council", "government", "authority",
    "crown", "nz transport", "police", "defence", "corrections",
    "inland revenue", "treasury", "msd", "mbie", "moh", "epa",
    "kainga ora", "district health", "university", "school", "moe",
]


def classify_type_by_rules(company_name: str):
    name = company_name.lower().strip()

    if name in NZ_COMPANY_DICT:
        industry, ctype = NZ_COMPANY_DICT[name]
        return ctype, 0.95

    for kw in RECRUITER_KEYWORDS:
        if kw in name:
            return "Recruiter", 0.95

    for kw in PUBLIC_KEYWORDS:
        if kw in name:
            return "Public Sector", 0.95

    return None, 0.0