from langgraph.graph import StateGraph, END
from state import PipelineState
from tools.scraper import pull_seek_data
from tools.database import save_raw_jobs, save_clean_jobs
from data_cleaning.clean_location import clean_location
from data_cleaning.extract_skills import load_skill_dict, apply_skill_extraction
from company_classifier.classify import run_company_classification
from role_classifier.classify_role import apply_role_classification
from openai import OpenAI
import os


def scrape_node(state: PipelineState) -> PipelineState:
    print("Starting scrape...")
    df = pull_seek_data()
    print(f"Scraped {len(df)} jobs")
    return {**state, "scraped_df": df, "status": "scraped"}


def save_raw_node(state: PipelineState) -> PipelineState:
    print("Saving raw data to Snowflake...")
    save_raw_jobs(state["scraped_df"])
    return {**state, "status": "raw_saved"}



def clean_node(state: PipelineState) -> PipelineState:
    print("Cleaning data...")
    df = state["scraped_df"].copy()

    print("Cleaning location...")
    df = clean_location(df)

    print("Extracting skills...")
    SKILLS = load_skill_dict("config/skill_dictionary.csv")
    df = apply_skill_extraction(df, SKILLS)
    df["skills_dict"] = df.apply(
        lambda row: {k: int(row.get(k, 0)) for k in SKILLS.keys()},
        axis=1,
    )

    print("Classifying companies...")
    df = run_company_classification(df)

    print("Classifying roles...")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    df = apply_role_classification(df, client)

    print(f"Cleaning done: {len(df)} jobs")
    return {**state, "clean_df": df, "status": "cleaned"}

def save_clean_node(state: PipelineState) -> PipelineState:
    print("Saving clean data to Snowflake...")
    save_clean_jobs(state["clean_df"])
    return {**state, "status": "saved"}

def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("scrape", scrape_node)
    graph.add_node("save_raw", save_raw_node)
    graph.add_node("clean", clean_node)
    graph.add_node("save_clean", save_clean_node)
    graph.set_entry_point("scrape")
    graph.add_edge("scrape", "save_raw")
    graph.add_edge("save_raw", "clean")
    graph.add_edge("clean", "save_clean")
    graph.add_edge("save_clean", END)
    return graph.compile()