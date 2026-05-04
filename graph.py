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
    save_raw_jobs(df)
    print("Raw data saved to Snowflake")
    return {**state, "scraped_df": df, "status": "scraped"}


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

    print(f"Cleaning done: {len(df)} jobs")
    return {**state, "clean_df": df, "status": "cleaned"}


def classify_node(state: PipelineState) -> PipelineState:
    print("Classifying...")
    df = state["clean_df"].copy()

    print("Classifying companies (ReAct agent)...")
    df = run_company_classification(df)

    print("Classifying roles (LLM)...")
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    df = apply_role_classification(df, client)

    print("Saving clean data to Snowflake...")
    save_clean_jobs(df)
    print(f"Done: {len(df)} jobs saved")

    return {**state, "clean_df": df, "status": "saved"}


def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("scrape", scrape_node)
    graph.add_node("clean", clean_node)
    graph.add_node("classify", classify_node)
    graph.set_entry_point("scrape")
    graph.add_edge("scrape", "clean")
    graph.add_edge("clean", "classify")
    graph.add_edge("classify", END)
    return graph.compile()