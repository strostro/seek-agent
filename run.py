from graph import build_graph

graph = build_graph()

initial_state = {
    "scraped_df": None,
    "status": "start"
}

result = graph.invoke(initial_state)

print(f"\nFinal status: {result['status']}")

raw_df = result["scraped_df"]
raw_df.to_csv("seek_output.csv", index=False)
print(f"Raw jobs saved locally: {len(raw_df)}")

clean_df = result["clean_df"]
clean_df.to_csv("output/jobs_clean.csv", index=False)
print(f"Clean jobs saved locally: {len(clean_df)}")