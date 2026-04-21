import pandas as pd

NZ_CITIES = [
    "Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga",
    "Dunedin", "Queenstown", "Napier", "Hastings", "Palmerston North",
    "Nelson", "New Plymouth", "Rotorua", "Whangarei", "Invercargill",
    "Taupo", "Timaru"
]

NZ_REGIONS = [
    "Northland", "Auckland", "Waikato", "Bay of Plenty", "Gisborne",
    "Hawke's Bay", "Taranaki", "Manawatū-Whanganui", "Wellington",
    "Tasman", "Nelson", "Marlborough", "West Coast", "Canterbury",
    "Otago", "Southland"
]

NORTH_ISLAND = [
    "Northland", "Auckland", "Waikato", "Bay of Plenty",
    "Gisborne", "Hawke's Bay", "Taranaki",
    "Manawatū-Whanganui", "Wellington"
]


def extract_city(text):
    if pd.isna(text):
        return "Unknown"
    text_lower = text.lower()
    
    if "mount wellington" in text_lower:
        return "Auckland"

    for city in NZ_CITIES:
        if city.lower() in text_lower:
            return city
    return "Other"


def standardise_region(text):
    if pd.isna(text):
        return "Unknown"
    text_lower = text.lower()
    for region in NZ_REGIONS:
        if region.lower() in text_lower:
            return region
    return "Other"


def clean_location(df):
    df[["suburb", "region_raw"]] = df["location"].str.split(",", n=1, expand=True)
    df["suburb"] = df["suburb"].str.strip()
    df["region_raw"] = df["region_raw"].str.strip()
    df["region_raw"] = df["region_raw"].fillna(df["suburb"])

    df["city"] = df["suburb"].apply(extract_city)
    df.loc[df["city"] == "Other", "city"] = df.loc[df["city"] == "Other", "location"].apply(extract_city)

    df["region_standardised"] = df["region_raw"].apply(standardise_region)

    df["island"] = df["region_standardised"].apply(
        lambda x: "North Island" if x in NORTH_ISLAND
        else "South Island" if x not in ["Other", "Unknown"]
        else "Unknown"
    )

    return df