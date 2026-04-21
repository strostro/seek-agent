# sync_company_dim.py
import pandas as pd
from tools.database import save_company_dim
from dotenv import load_dotenv

load_dotenv()

df = pd.read_csv("output/company_dim.csv")
rows = df.to_dict("records")
save_company_dim(rows)
print(f"Synced {len(rows)} companies to Snowflake")