import pandas as pd
import re


def load_skill_dict(path="config/skill_dictionary.csv"):
    skills_df = pd.read_csv(path)
    return dict(zip(skills_df["skill"], skills_df["pattern"]))


def extract_skills_from_text(text, SKILLS):
    text = str(text).lower()
    results = {}
    for skill, pattern in SKILLS.items():
        found = re.search(pattern, text)
        results[skill] = int(bool(found))
    return pd.Series(results)


def apply_skill_extraction(df, SKILLS):
    print("Extracting skills...")
    skill_matrix = df["description"].apply(
        lambda x: extract_skills_from_text(x, SKILLS)
    )
    return pd.concat([df, skill_matrix], axis=1)