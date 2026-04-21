import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from datetime import timedelta

st.set_page_config(
    page_title="NZ Data Job Market",
    page_icon="📊",
    layout="wide"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #F4F5F7; }
    [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] {
        background-color: #FFFFFF !important;
        border-right: 1px solid #E5E7EB !important;
    }
    [data-testid="stSidebar"] section { padding-top: 1.5rem; }
    [data-testid="stHeader"] { background: transparent; }
    .card {
        background: #FFFFFF;
        border: 1px solid #E5E7EB;
        border-radius: 12px;
        padding: 20px 24px;
        margin-bottom: 16px;
        position: relative;
    }
    .card-tag {
        position: absolute;
        top: 16px;
        right: 16px;
        background: #F3F4F6;
        color: #9CA3AF;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        padding: 3px 8px;
        border-radius: 6px;
    }
    .kpi-card {
        background: #FFFFFF;
        border: 1px solid #E5E7EB;
        border-radius: 12px;
        padding: 20px 24px;
    }
    .kpi-tag {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        color: #9CA3AF;
        margin-bottom: 8px;
    }
    .kpi-number {
        font-size: 40px;
        font-weight: 700;
        color: #111827;
        line-height: 1;
        margin-bottom: 4px;
    }
    .chart-title {
        font-size: 15px;
        font-weight: 600;
        color: #111827;
        margin-bottom: 2px;
    }
    .chart-subtitle {
        font-size: 12px;
        color: #9CA3AF;
        margin-bottom: 14px;
    }
    .page-title {
        font-size: 28px;
        font-weight: 700;
        color: #111827;
        margin-bottom: 2px;
    }
    .page-subtitle {
        font-size: 13px;
        color: #9CA3AF;
        margin-bottom: 0;
    }
    .sidebar-filter-label {
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        color: #9CA3AF;
        margin-bottom: 12px;
        margin-top: 8px;
    }
    .sidebar-about {
        font-size: 12px;
        color: #9CA3AF;
        line-height: 1.6;
        margin-top: 8px;
    }
    .stButton > button {
        width: 100%;
        border: 1px solid #E5E7EB;
        background: #FFFFFF;
        color: #374151 !important;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 500;
        padding: 8px;
    }
    .stButton > button:hover {
        background: #F9FAFB;
        border-color: #D1D5DB;
    }
</style>
""", unsafe_allow_html=True)

PRIMARY    = "#0F9B8E"
GRAY_TRACK = "#F3F4F6"
TEXT_DARK  = "#111827"
TEXT_MUTED = "#9CA3AF"

SUBTYPE_COLORS = {
    "Data Engineer":      "#0F9B8E",
    "Analytics Engineer": "#34D399",
    "Data Scientist":     "#6366F1",
    "ML Engineer":        "#A5B4FC",
    "AI Engineer":        "#0EA5E9",
    "Data Analyst":       "#0F766E",
    "BI Analyst":         "#F97316",
    "Marketing Analyst":  "#FBBF24",
    "Product Analyst":    "#F59E0B",
    "GIS Analyst":        "#84CC16",
    "Financial Analyst":  "#D946EF",
    "HR Analyst":         "#EC4899",
    "Research Analyst":   "#06B6D4",
    "Other":              "#D1D5DB",
}

SKILL_COLORS = {
    "programming": PRIMARY,
    "tool":        "#0EA5E9",
    "concept":     "#6366F1",
}

@st.cache_data(ttl=3600)
def load_data():
    private_key_str = st.secrets["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_str.encode(), password=None
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    conn = snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=pkb,
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )
    df_jobs    = pd.read_sql("SELECT * FROM JOBS_CLEAN", conn)
    df_skills  = pd.read_sql("SELECT * FROM JOB_SKILL", conn)
    df_company = pd.read_sql("SELECT * FROM COMPANY_DIM_FINAL", conn)
    conn.close()
    df_jobs.columns    = df_jobs.columns.str.lower()
    df_skills.columns  = df_skills.columns.str.lower()
    df_company.columns = df_company.columns.str.lower()
    return df_jobs, df_skills, df_company

@st.cache_data
def load_skill_dict():
    return pd.read_csv("config/skill_dictionary.csv")

df_jobs, df_skills, df_company = load_data()
skill_dict = load_skill_dict()
df_jobs["posted_date"] = pd.to_datetime(df_jobs["posted_date"], errors="coerce")

df_jobs_company = df_jobs.merge(
    df_company[["company_name", "company_type", "industry", "size"]],
    left_on="company", right_on="company_name", how="left"
)

def make_hbar(df, x_col, y_col, color=PRIMARY, height=300):
    df_sorted = df.sort_values(x_col, ascending=True).copy()
    max_val = df_sorted[x_col].max()
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[max_val * 1.15] * len(df_sorted),
        y=df_sorted[y_col],
        orientation="h",
        marker_color=GRAY_TRACK,
        showlegend=False,
        hoverinfo="skip",
    ))
    fig.add_trace(go.Bar(
        x=df_sorted[x_col],
        y=df_sorted[y_col],
        orientation="h",
        marker_color=color,
        text=df_sorted[x_col].astype(int),
        textposition="outside",
        textfont=dict(size=12, color=TEXT_MUTED),
        cliponaxis=False,
        showlegend=False,
    ))
    fig.update_layout(
        barmode="overlay",
        height=height,
        margin=dict(t=0, b=0, l=0, r=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False,
                   showline=False, zeroline=False,
                   range=[0, max_val * 1.3]),
        yaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=13, color=TEXT_DARK)),
        bargap=0.4,
    )
    return fig

with st.sidebar:
    st.markdown("""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:24px">
            <div style="width:40px;height:40px;background:#0F9B8E;border-radius:10px;
            display:flex;align-items:center;justify-content:center;
            color:white;font-weight:700;font-size:14px">NZ</div>
            <div>
                <div style="font-weight:600;font-size:14px;color:#111827">Data Job Market</div>
                <div style="font-size:11px;color:#9CA3AF">V1.0 · SEEK NZ</div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.markdown('<div class="sidebar-filter-label">FILTERS</div>', unsafe_allow_html=True)
    roles = ["All roles"] + sorted(df_jobs["role_standardised"].dropna().unique().tolist())
    selected_role = st.selectbox("Role type", roles)
    regions = ["All regions"] + sorted(
        df_jobs["region_standardised"].dropna()
        .loc[lambda x: ~x.isin(["Other", "Unknown"])].unique().tolist()
    )
    selected_region = st.selectbox("Region", regions)
    types = ["All types"] + sorted(df_company["company_type"].dropna().unique().tolist())
    selected_type = st.selectbox("Company type", types)
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Reset filters"):
        st.rerun()
    st.markdown("---")
    st.markdown("""
        <div style="font-size:12px;font-weight:600;color:#374151;margin-bottom:4px">About</div>
        <div class="sidebar-about">
            A personal analytics project tracking data-industry job postings on Seek NZ.
            Scraped weekly, deduplicated by title + company.
        </div>
    """, unsafe_allow_html=True)

filtered = df_jobs_company.copy()
if selected_role != "All roles":
    filtered = filtered[filtered["role_standardised"] == selected_role]
if selected_region != "All regions":
    filtered = filtered[filtered["region_standardised"] == selected_region]
if selected_type != "All types":
    filtered = filtered[filtered["company_type"] == selected_type]

filtered_skills = df_skills[df_skills["job_id"].isin(filtered["job_id"])]

col_title, col_date = st.columns([3, 1])
with col_title:
    st.markdown('<div class="page-title">NZ Data Job Market</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Based on Seek NZ · Updated weekly · 14 days of history</div>',
        unsafe_allow_html=True
    )
with col_date:
    if df_jobs["posted_date"].notna().any():
        last_updated = df_jobs["posted_date"].max().strftime("%B %d, %Y")
        st.markdown(
            f'<div style="text-align:right;padding-top:16px">'
            f'<span style="color:#0F9B8E;font-size:13px;font-weight:500">'
            f'● Last updated {last_updated}</span></div>',
            unsafe_allow_html=True
        )

st.markdown("<br>", unsafe_allow_html=True)

k1, k2 = st.columns(2)
with k1:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-tag">TOTAL JOBS</div>
            <div class="kpi-number">{len(filtered)}</div>
            <div style="font-size:12px;color:{TEXT_MUTED}">Last 14 days</div>
        </div>
    """, unsafe_allow_html=True)
with k2:
    st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-tag">COMPANIES HIRING</div>
            <div class="kpi-number">{filtered["company"].nunique()}</div>
            <div style="font-size:12px;color:{TEXT_MUTED}">Unique employers</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

col_ct, col_ind = st.columns(2)

with col_ct:
    type_counts = filtered["company_type"].value_counts().reset_index()
    type_counts.columns = ["type", "count"]
    type_counts = type_counts[type_counts["type"].notna()]
    total = type_counts["count"].sum()
    st.markdown(f"""
        <div class="card">
            <span class="card-tag">SECTOR</span>
            <div class="chart-title">Company type</div>
            <div class="chart-subtitle">Share by sector</div>
    """, unsafe_allow_html=True)
    if len(type_counts) > 0:
        fig_donut = go.Figure(go.Pie(
            labels=type_counts["type"],
            values=type_counts["count"],
            hole=0.6,
            marker_colors=[PRIMARY, "#374151", "#F59E0B", "#E5E7EB", "#A78BFA"],
            textinfo="none",
            hovertemplate="%{label}: %{value}<extra></extra>",
        ))
        fig_donut.add_annotation(
            text=f"<b>{total}</b><br><span style='font-size:10px'>POSTINGS</span>",
            x=0.18, y=0.5, showarrow=False,
            font=dict(size=16, color=TEXT_DARK),
            xref="paper", yref="paper"
        )
        legend_rows = ""
        for _, row in type_counts.iterrows():
            pct = round(row["count"] / total * 100, 1)
            legend_rows += f"""
                <tr>
                    <td style="padding:4px 8px 4px 0">
                        <span style="display:inline-block;width:10px;height:10px;
                        border-radius:2px;background:{PRIMARY};margin-right:6px"></span>
                        {row['type']}
                    </td>
                    <td style="padding:4px 8px;color:{TEXT_MUTED};font-size:12px">{pct}%</td>
                    <td style="padding:4px 0;font-weight:600;font-size:13px">{int(row['count'])}</td>
                </tr>
            """
        col_donut, col_legend = st.columns([1, 1])
        with col_donut:
            fig_donut.update_layout(
                height=200, showlegend=False,
                margin=dict(t=0, b=0, l=0, r=0),
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_donut, use_container_width=True)
        with col_legend:
            st.markdown(
                f'<table style="width:100%;font-size:13px;color:{TEXT_DARK};'
                f'margin-top:24px">{legend_rows}</table>',
                unsafe_allow_html=True
            )
    st.markdown('</div>', unsafe_allow_html=True)

with col_ind:
    industry_counts = filtered["industry"].value_counts().reset_index()
    industry_counts.columns = ["industry", "count"]
    industry_counts = industry_counts[
        industry_counts["industry"].notna() &
        ~industry_counts["industry"].isin(["Other", "Unknown"])
    ]
    n_ind = len(industry_counts)
    st.markdown(f"""
        <div class="card">
            <span class="card-tag">{n_ind} industries</span>
            <div class="chart-title">Industry breakdown</div>
            <div class="chart-subtitle">Postings by industry vertical</div>
    """, unsafe_allow_html=True)
    if n_ind > 0:
        bar_h = max(260, n_ind * 30)
        st.plotly_chart(
            make_hbar(industry_counts, "count", "industry", PRIMARY, height=bar_h),
            use_container_width=True
        )
    st.markdown('</div>', unsafe_allow_html=True)

if df_jobs["posted_date"].notna().any():
    max_date = df_jobs["posted_date"].max()
    min_date = max_date - timedelta(days=13)
    all_dates = pd.date_range(start=min_date, end=max_date, freq="D")
    date_df = pd.DataFrame({"posted_date": all_dates})
    daily_counts = filtered.groupby("posted_date").size().reset_index(name="count")
    daily_counts = date_df.merge(daily_counts, on="posted_date", how="left").fillna(0)
    daily_counts["count"] = daily_counts["count"].astype(int)
    daily_counts["date_label"] = daily_counts["posted_date"].dt.strftime("%a").str.upper()
    daily_counts["is_weekend"] = daily_counts["posted_date"].dt.dayofweek >= 5
    st.markdown(f"""
        <div class="card">
            <span class="card-tag">14D</span>
            <div class="chart-title">Job posting trend</div>
            <div class="chart-subtitle">Daily postings over the last 14 days</div>
    """, unsafe_allow_html=True)
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Bar(
        x=daily_counts["date_label"],
        y=daily_counts["count"],
        marker_color=[GRAY_TRACK if w else PRIMARY
                      for w in daily_counts["is_weekend"]],
        hovertemplate="%{x}: %{y} jobs<extra></extra>",
    ))
    fig_trend.update_layout(
        height=240,
        margin=dict(t=0, b=0, l=0, r=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=12, color=TEXT_MUTED)),
        yaxis=dict(showgrid=False, showline=False,
                   showticklabels=False, zeroline=False),
        bargap=0.25,
    )
    st.plotly_chart(fig_trend, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

role_subtype_counts = (
    filtered
    .groupby(["role_standardised", "role_subtype"])
    .size().reset_index(name="count")
)
role_order = (
    role_subtype_counts.groupby("role_standardised")["count"]
    .sum().sort_values(ascending=True).index.tolist()
)
all_subtypes = sorted(role_subtype_counts["role_subtype"].dropna().unique().tolist())
n_roles = len(role_order)
max_role_val = role_subtype_counts.groupby("role_standardised")["count"].sum().max()

st.markdown(f"""
    <div class="card">
        <span class="card-tag">{n_roles} roles</span>
        <div class="chart-title">Role distribution</div>
        <div class="chart-subtitle">Breakdown by role family and specialization</div>
""", unsafe_allow_html=True)

if len(role_subtype_counts) > 0:
    fig_role = go.Figure()

    for subtype in all_subtypes:
        df_sub = (
            role_subtype_counts[role_subtype_counts["role_subtype"] == subtype]
            .set_index("role_standardised").reindex(role_order).fillna(0).reset_index()
        )
        df_sub.columns = ["role_standardised", "role_subtype", "count"]
        fig_role.add_trace(go.Bar(
            name=subtype,
            x=df_sub["count"],
            y=df_sub["role_standardised"],
            orientation="h",
            marker_color=SUBTYPE_COLORS.get(subtype, "#E5E7EB"),
            hovertemplate="%{y} · " + subtype + ": %{x}<extra></extra>",
        ))

    role_totals = (
        role_subtype_counts.groupby("role_standardised")["count"]
        .sum().reindex(role_order).reset_index()
    )
    fig_role.add_trace(go.Scatter(
        x=role_totals["count"] + max_role_val * 0.03,
        y=role_totals["role_standardised"],
        mode="text",
        text=role_totals["count"].astype(int),
        textfont=dict(size=12, color=TEXT_MUTED),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig_role.update_layout(
        barmode="stack",
        height=max(280, n_roles * 60),
        margin=dict(t=0, b=120, l=0, r=60),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False,
                   showline=False, zeroline=False),
        yaxis=dict(showgrid=False, showline=False,
                   tickfont=dict(size=13, color=TEXT_DARK),
                   categoryorder="array", categoryarray=role_order),
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.4,
            xanchor="left", x=0,
            font=dict(size=11, color=TEXT_DARK),
            bgcolor="rgba(0,0,0,0)",
        ),
        bargap=0.4,
    )
    st.plotly_chart(fig_role, use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

skill_counts = filtered_skills.groupby("skill")["job_id"].count().reset_index()
skill_counts.columns = ["skill", "count"]
skill_counts = skill_counts.merge(
    skill_dict[["skill", "category"]], on="skill", how="left"
)

categories = [
    ("programming", "Programming", "Languages & scripting",  "PROG"),
    ("tool",        "Tools",       "Platforms & products",   "TOOLS"),
    ("concept",     "Concepts",    "Methods & domain skills","CONCEPTS"),
]

cols = st.columns(3)
for i, (cat_key, cat_label, cat_sub, cat_tag) in enumerate(categories):
    with cols[i]:
        cat_df = skill_counts[skill_counts["category"] == cat_key].nlargest(10, "count").copy()
        n_cat = len(cat_df)
        cat_df["skill_display"] = cat_df["skill"].str.replace("_", " ").str.title()
        st.markdown(f"""
            <div class="card">
                <span class="card-tag">{cat_tag}</span>
                <div class="chart-title">{cat_label}</div>
                <div class="chart-subtitle">{cat_sub}</div>
        """, unsafe_allow_html=True)
        if n_cat > 0:
            st.plotly_chart(
                make_hbar(cat_df, "count", "skill_display",
                          SKILL_COLORS[cat_key],
                          height=max(280, n_cat * 36)),
                use_container_width=True
            )
        else:
            st.info("No data")
        st.markdown('</div>', unsafe_allow_html=True)

REGION_COORDS = {
    "Auckland":           (-36.8485, 174.7633),
    "Wellington":         (-41.2866, 174.7756),
    "Canterbury":         (-43.5321, 172.6362),
    "Waikato":            (-37.7870, 175.2793),
    "Bay of Plenty":      (-37.6878, 176.1651),
    "Otago":              (-45.8788, 170.5028),
    "Manawatū-Whanganui": (-40.3523, 175.6082),
    "Nelson":             (-41.2706, 173.2840),
    "Southland":          (-46.4132, 168.3538),
    "Northland":          (-35.7275, 174.3166),
    "Taranaki":           (-39.0556, 174.0752),
    "Hawke's Bay":        (-39.4928, 176.9120),
    "Marlborough":        (-41.5134, 173.9612),
    "Tasman":             (-41.2706, 172.9847),
    "West Coast":         (-42.4502, 171.2103),
    "Gisborne":           (-38.6623, 178.0176),
}

region_counts = filtered["region_standardised"].value_counts().reset_index()
region_counts.columns = ["region", "count"]
region_counts = region_counts[
    region_counts["region"].notna() &
    ~region_counts["region"].isin(["Other", "Unknown"])
]
region_counts["lat"] = region_counts["region"].map(
    lambda x: REGION_COORDS.get(x, (None, None))[0]
)
region_counts["lon"] = region_counts["region"].map(
    lambda x: REGION_COORDS.get(x, (None, None))[1]
)
region_map = region_counts.dropna(subset=["lat", "lon"])

city_counts = filtered["city"].value_counts().reset_index()
city_counts.columns = ["city", "count"]
city_counts = city_counts[
    city_counts["city"].notna() &
    ~city_counts["city"].isin(["Other", "Unknown"])
]
n_cities = len(city_counts)

col_map, col_city = st.columns([2, 3])

with col_map:
    st.markdown(f"""
        <div class="card">
            <div class="chart-title">Postings by region</div>
            <div class="chart-subtitle">Geographic spread across NZ</div>
    """, unsafe_allow_html=True)
    if len(region_map) > 0:
        fig_map = px.scatter_mapbox(
            region_map,
            lat="lat", lon="lon",
            size="count",
            hover_name="region",
            hover_data={"count": True, "lat": False, "lon": False},
            color="count",
            color_continuous_scale=[[0, "#CCFBF1"], [1, PRIMARY]],
            size_max=50,
            zoom=4,
            center={"lat": -41.5, "lon": 174.0},
            mapbox_style="carto-positron"
        )
        fig_map.update_layout(
            height=420,
            margin=dict(t=0, b=0, l=0, r=0),
            coloraxis_showscale=False,
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_map, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col_city:
    st.markdown(f"""
        <div class="card">
            <span class="card-tag">{n_cities} cities</span>
            <div class="chart-title">Postings by city</div>
            <div class="chart-subtitle">Top hiring cities</div>
    """, unsafe_allow_html=True)
    if n_cities > 0:
        st.plotly_chart(
            make_hbar(city_counts, "count", "city", PRIMARY,
                      height=max(300, n_cities * 36)),
            use_container_width=True
        )
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f'<p style="text-align:center;color:{TEXT_MUTED};font-size:12px">'
    f'Data source: Seek NZ · Built with Streamlit & Snowflake</p>',
    unsafe_allow_html=True
)