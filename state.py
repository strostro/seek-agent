from typing import TypedDict, Optional
import pandas as pd

class PipelineState(TypedDict):
    scraped_df: Optional[pd.DataFrame]
    clean_df: Optional[pd.DataFrame]
    status: str