import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import math
from cryptography.hazmat.primitives import serialization
from datetime import timedelta

st.set_page_config(
    page_title="NZ Data Job Market",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# Design tokens — dark sidebar + coloured accent
# ============================================================

# Primary accent — teal family
ACCENT      = "#14B8A6"   # teal-500
ACCENT_DARK = "#0F766E"   # teal-700
ACCENT_SOFT = "#CCFBF1"   # teal-100

# Neutrals
BG_PAGE    = "#F8FAFC"    # slate-50
BG_CARD    = "#FFFFFF"
BG_SIDEBAR = "#0F172A"    # slate-900
BORDER     = "#E2E8F0"    # slate-200
TRACK      = "#F1F5F9"    # slate-100
TEXT_1     = "#0F172A"    # primary text
TEXT_2     = "#475569"    # secondary
TEXT_3     = "#94A3B8"    # tertiary / muted

# Categorical palette for donut — discrete, high contrast
CATEGORICAL = [
    "#14B8A6",  # teal
    "#6366F1",  # indigo
    "#F59E0B",  # amber
    "#EC4899",  # pink
    "#0EA5E9",  # sky
    "#8B5CF6",  # violet
    "#10B981",  # emerald
    "#F43F5E",  # rose
]

# Per-subtype colours — engineer family cool, analyst family warm
SUBTYPE_COLORS = {
    # Engineer family — teal / blue
    "Data Engineer":      "#0F766E",
    "Analytics Engineer": "#14B8A6",
    "ML Engineer":        "#0EA5E9",
    "AI Engineer":        "#6366F1",
    # Scientist
    "Data Scientist":     "#8B5CF6",
    # Analyst family — warm tones
    "Data Analyst":       "#F59E0B",
    "BI Analyst":         "#F97316",
    "Marketing Analyst":  "#EC4899",
    "Product Analyst":    "#F43F5E",
    "GIS Analyst":        "#84CC16",
    "Financial Analyst":  "#EAB308",
    "HR Analyst":         "#A855F7",
    "Research Analyst":   "#06B6D4",
    "Other":              "#CBD5E1",
}

# ============================================================
# Global CSS
# ============================================================

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}

    .stApp {{ background-color: {BG_PAGE}; }}

    /* Main content area padding */
    .block-container {{
        padding-top: 2rem !important;
        padding-bottom: 3rem !important;
        max-width: 1400px;
    }}

    /* ========== Dark sidebar ========== */
    [data-testid="stSidebar"] {{
        background-color: {BG_SIDEBAR} !important;
        border-right: none !important;
    }}
    [data-testid="stSidebar"] * {{
        color: #E2E8F0 !important;
    }}
    [data-testid="stSidebar"] section {{
        padding-top: 1.5rem;
    }}

    /* Sidebar selectbox styling */
    [data-testid="stSidebar"] [data-baseweb="select"] > div {{
        background-color: #1E293B !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        color: #F1F5F9 !important;
    }}
    [data-testid="stSidebar"] [data-baseweb="select"] svg {{
        fill: #94A3B8 !important;
    }}
    [data-testid="stSidebar"] label {{
        color: #94A3B8 !important;
        font-size: 12px !important;
        font-weight: 500 !important;
    }}

    /* Sidebar reset button */
    [data-testid="stSidebar"] .stButton > button {{
        width: 100%;
        background: transparent !important;
        border: 1px solid #334155 !important;
        color: #E2E8F0 !important;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 500;
        padding: 8px;
    }}
    [data-testid="stSidebar"] .stButton > button:hover {{
        background: #1E293B !important;
        border-color: {ACCENT} !important;
        color: {ACCENT} !important;
    }}

    /* Sidebar divider */
    [data-testid="stSidebar"] hr {{
        border-color: #1E293B !important;
        margin: 1.5rem 0 !important;
    }}

    [data-testid="stHeader"] {{ background: transparent; }}

    /* ========== Native container card styling ========== */
    [data-testid="stVerticalBlockBorderWrapper"] {{
        background: {BG_CARD} !important;
        border: 1px solid {BORDER} !important;
        border-radius: 14px !important;
        padding: 22px 24px !important;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03) !important;
    }}

    /* Nested border wrappers must not re-apply card styles */
    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {{
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
        box-shadow: none !important;
    }}

    /* Card header */
    .card-header {{
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        margin-bottom: 14px;
    }}
    .card-title {{
        font-size: 15px;
        font-weight: 600;
        color: {TEXT_1};
        margin: 0;
        line-height: 1.3;
    }}
    .card-subtitle {{
        font-size: 12px;
        color: {TEXT_3};
        margin-top: 2px;
    }}
    .card-tag {{
        background: {TRACK};
        color: {TEXT_2};
        font-size: 10px;
        font-weight: 600;
        letter-spacing: 0.06em;
        padding: 4px 9px;
        border-radius: 6px;
        text-transform: uppercase;
        white-space: nowrap;
    }}
    .card-tag.accent {{
        background: {ACCENT_SOFT};
        color: {ACCENT_DARK};
    }}

    /* ========== KPI cards ========== */
    /* Compact horizontal layout — icon left, content right */
    .kpi-row {{
        display: flex;
        align-items: center;
        gap: 14px;
    }}
    .kpi-icon {{
        width: 44px;
        height: 44px;
        border-radius: 10px;
        background: {ACCENT_SOFT};
        color: {ACCENT_DARK};
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
    }}
    .kpi-icon svg {{
        display: block;
    }}
    .kpi-body {{
        flex: 1;
        min-width: 0;
    }}
    .kpi-label {{
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        color: {TEXT_2};
        text-transform: uppercase;
        margin-bottom: 4px;
    }}
    .kpi-value-row {{
        display: flex;
        align-items: baseline;
        gap: 10px;
    }}
    .kpi-value {{
        font-size: 30px;
        font-weight: 700;
        color: {TEXT_1};
        line-height: 1;
        letter-spacing: -0.02em;
    }}
    .kpi-hint {{
        font-size: 12px;
        color: {TEXT_3};
    }}
    .kpi-delta {{
        font-size: 11px;
        font-weight: 600;
        padding: 3px 7px;
        border-radius: 5px;
    }}
    .kpi-delta.up {{
        background: #DCFCE7;
        color: #15803D;
    }}
    .kpi-delta.neutral {{
        background: {TRACK};
        color: {TEXT_2};
    }}

    /* ========== Page header ========== */
    .page-title {{
        font-size: 28px;
        font-weight: 700;
        color: {TEXT_1};
        letter-spacing: -0.02em;
        margin-bottom: 4px;
    }}
    .page-subtitle {{
        font-size: 13px;
        color: {TEXT_2};
        margin-bottom: 24px;
    }}
    .update-badge {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: {BG_CARD};
        border: 1px solid {BORDER};
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 12px;
        color: {TEXT_2};
        font-weight: 500;
    }}
    .live-dot {{
        width: 6px; height: 6px;
        border-radius: 50%;
        background: {ACCENT};
        box-shadow: 0 0 0 3px {ACCENT_SOFT};
    }}

    /* ========== Sidebar logo ========== */
    .sb-logo {{
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 0 0 8px 0;
        margin-bottom: 8px;
    }}
    .sb-logo-icon {{
        width: 40px; height: 40px;
        background: linear-gradient(135deg, {ACCENT} 0%, {ACCENT_DARK} 100%);
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white !important;
        font-weight: 700;
        font-size: 14px;
    }}
    .sb-logo-title {{
        font-weight: 600; font-size: 14px;
        color: #F1F5F9 !important;
    }}
    .sb-logo-sub {{
        font-size: 11px;
        color: #64748B !important;
    }}
    .sb-section-label {{
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.08em;
        color: #64748B !important;
        text-transform: uppercase;
        margin: 4px 0 12px 0;
    }}
    .sb-about {{
        font-size: 12px;
        color: #94A3B8 !important;
        line-height: 1.6;
    }}

    /* Hide hamburger menu & Streamlit footer */
    #MainMenu, footer {{ visibility: hidden; }}
</style>
""", unsafe_allow_html=True)

# ============================================================
# Data loading
# ============================================================

@st.cache_data(ttl=3600)
def load_data():
    private_key_str = st.secrets["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_str.encode(),
        password=None
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

# ============================================================
# Shared chart helpers
# ============================================================

def hbar_chart(df, x_col, y_col, color=ACCENT, height=300):
    """
    Track-style horizontal bar chart.

    All grey tracks are the same full width (= max value). The coloured bar
    overlays the track proportionally. Count labels are pinned to a fixed
    x position at the right edge of the track so every row aligns.
    """
    df_sorted = df.sort_values(x_col, ascending=True).copy()
    max_val = df_sorted[x_col].max()
    if max_val == 0:
        max_val = 1  # guard against division by zero

    track_len = max_val  # track length = reference 100% for all rows
    track_len = max_val
    x_range = track_len * 1.18  # 18% padding on right for labels

    fig = go.Figure()

    # Layer 1: grey full-width track (same length for every row)
    fig.add_trace(go.Bar(
        x=[track_len] * len(df_sorted),
        y=df_sorted[y_col],
        orientation="h",
        marker=dict(color=TRACK, line=dict(width=0)),
        width=0.5,
        showlegend=False,
        hoverinfo="skip",
    ))

    # Layer 2: coloured data bar overlaid on the track
    fig.add_trace(go.Bar(
        x=df_sorted[x_col],
        y=df_sorted[y_col],
        orientation="h",
        marker=dict(color=color, line=dict(width=0)),
        width=0.5,
        showlegend=False,
        hovertemplate="<b>%{y}</b><br>%{x} postings<extra></extra>",
    ))

    # Layer 3: count labels pinned to a fixed right position
    fig.add_trace(go.Scatter(
        x=[track_len * 1.04] * len(df_sorted),
        y=df_sorted[y_col],
        mode="text",
        text=df_sorted[x_col].astype(int),
        textfont=dict(size=12, color=TEXT_2, family="Inter"),
        textposition="middle right",
        showlegend=False,
        hoverinfo="skip",
        cliponaxis=False,
    ))

    fig.update_layout(
        barmode="overlay",
        height=height,
        margin=dict(t=4, b=4, l=0, r=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(
            showgrid=False, showticklabels=False,
            showline=False, zeroline=False,
            range=[0, x_range],
            fixedrange=True,
        ),
        yaxis=dict(
            showgrid=False, showline=False,
            tickfont=dict(size=13, color=TEXT_1, family="Inter"),
            fixedrange=True,
        ),
        bargap=0.0,
    )
    return fig

def card_header(title, subtitle, tag=None, tag_accent=False):
    """Render a consistent card header: title + subtitle on the left, optional tag on the right."""
    tag_cls = "card-tag accent" if tag_accent else "card-tag"
    tag_html = f'<span class="{tag_cls}">{tag}</span>' if tag else ""
    st.markdown(f"""
        <div class="card-header">
            <div>
                <div class="card-title">{title}</div>
                <div class="card-subtitle">{subtitle}</div>
            </div>
            {tag_html}
        </div>
    """, unsafe_allow_html=True)

# ============================================================
# Sidebar — dark theme + session state
# ============================================================

# ============================================================
# Filter session state
# ============================================================

# Initialise session state keys
for key, default in [
    ("flt_role", "All roles"),
    ("flt_region", "All regions"),
    ("flt_type", "All types"),
    ("flt_subtype", "All subtypes"),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# Pre-compute subtype lists per role (runs once at startup)
role_to_subtypes = (
    df_jobs.dropna(subset=["role_standardised", "role_subtype"])
    .groupby("role_standardised")["role_subtype"]
    .apply(lambda s: sorted(s.unique().tolist()))
    .to_dict()
)

def on_role_change():
    """Reset subtype state when role changes and the old value is no longer valid."""
    new_role = st.session_state["flt_role"]
    if new_role == "All roles":
        st.session_state["flt_subtype"] = "All subtypes"
        return
    valid_subtypes = role_to_subtypes.get(new_role, [])
    if st.session_state["flt_subtype"] not in valid_subtypes:
        st.session_state["flt_subtype"] = "All subtypes"

with st.sidebar:
    st.markdown("""
        <div class="sb-logo">
            <div class="sb-logo-icon">NZ</div>
            <div>
                <div class="sb-logo-title">Data Job Market</div>
                <div class="sb-logo-sub">v1.0 · Seek NZ</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    st.markdown('<div class="sb-section-label">Filters</div>', unsafe_allow_html=True)

    roles = ["All roles"] + sorted(df_jobs["role_standardised"].dropna().unique().tolist())
    st.selectbox(
        "Role type", roles,
        key="flt_role",
        on_change=on_role_change,
    )

    # Cascading subtype filter — only shown when selected role has 2+ subtypes
    current_role = st.session_state["flt_role"]
    current_subtypes = role_to_subtypes.get(current_role, [])
    show_subtype_filter = (
        current_role != "All roles" and len(current_subtypes) >= 2
    )

    if show_subtype_filter:
        subtype_options = ["All subtypes"] + current_subtypes
        st.selectbox(
            f"{current_role} subtype", subtype_options,
            key="flt_subtype",
        )
    else:
        # Guard: clear stale subtype state when the filter is hidden
        if st.session_state["flt_subtype"] != "All subtypes":
            st.session_state["flt_subtype"] = "All subtypes"

    regions = ["All regions"] + sorted(
        df_jobs["region_standardised"].dropna()
        .loc[lambda x: ~x.isin(["Other", "Unknown"])].unique().tolist()
    )
    st.selectbox("Region", regions, key="flt_region")

    types = ["All types"] + sorted(df_company["company_type"].dropna().unique().tolist())
    st.selectbox("Company type", types, key="flt_type")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    if st.button("Reset filters"):
        st.session_state["flt_role"] = "All roles"
        st.session_state["flt_subtype"] = "All subtypes"
        st.session_state["flt_region"] = "All regions"
        st.session_state["flt_type"] = "All types"
        st.rerun()

    st.markdown("---")

    st.markdown("""
        <div class="sb-section-label">About</div>
        <div class="sb-about">
            A personal analytics project tracking data-industry job
            postings on Seek NZ. Scraped weekly, deduplicated by
            title + company.
        </div>
    """, unsafe_allow_html=True)

# Read filter values from session state
selected_role    = st.session_state["flt_role"]
selected_subtype = st.session_state["flt_subtype"]
selected_region  = st.session_state["flt_region"]
selected_type    = st.session_state["flt_type"]

# ============================================================
# Apply filters
# ============================================================

filtered = df_jobs_company.copy()
if selected_role != "All roles":
    filtered = filtered[filtered["role_standardised"] == selected_role]
if selected_subtype != "All subtypes":
    filtered = filtered[filtered["role_subtype"] == selected_subtype]
if selected_region != "All regions":
    filtered = filtered[filtered["region_standardised"] == selected_region]
if selected_type != "All types":
    filtered = filtered[filtered["company_type"] == selected_type]

filtered_skills = df_skills[df_skills["job_id"].isin(filtered["job_id"])]

# ============================================================
# Header
# ============================================================

col_title, col_date = st.columns([3, 1])
with col_title:
    st.markdown('<div class="page-title">NZ Data Job Market</div>',
                unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">'
        'Tracking data roles across New Zealand · 14 day rolling window'
        '</div>',
        unsafe_allow_html=True
    )
with col_date:
    if df_jobs["posted_date"].notna().any():
        last_updated = df_jobs["posted_date"].max().strftime("%b %d, %Y")
        st.markdown(
            f'<div style="text-align:right;padding-top:8px">'
            f'<span class="update-badge">'
            f'<span class="live-dot"></span>Updated {last_updated}'
            f'</span></div>',
            unsafe_allow_html=True
        )

# ============================================================
# 1. KPI cards
# ============================================================

total_now = len(filtered)
companies_now = filtered["company"].nunique()

# Lucide-style inline SVG icons — stroke uses currentColor to inherit from .kpi-icon
ICON_POSTINGS = """
<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24"
     fill="none" stroke="currentColor" stroke-width="2"
     stroke-linecap="round" stroke-linejoin="round">
    <rect width="8" height="4" x="8" y="2" rx="1" ry="1"/>
    <path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/>
    <path d="M12 11h4"/>
    <path d="M12 16h4"/>
    <path d="M8 11h.01"/>
    <path d="M8 16h.01"/>
</svg>
"""

ICON_COMPANIES = """
<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24"
     fill="none" stroke="currentColor" stroke-width="2"
     stroke-linecap="round" stroke-linejoin="round">
    <path d="M6 22V4a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v18Z"/>
    <path d="M6 12H4a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2"/>
    <path d="M18 9h2a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-2"/>
    <path d="M10 6h4"/>
    <path d="M10 10h4"/>
    <path d="M10 14h4"/>
    <path d="M10 18h4"/>
</svg>
"""

k1, k2 = st.columns(2, gap="medium")

# Self-drawn cards avoid Streamlit border-wrapper inconsistencies across versions
KPI_CARD_STYLE = (
    f"background: {BG_CARD};"
    f"border: 1px solid {BORDER};"
    f"border-radius: 14px;"
    f"padding: 22px 24px;"
    f"box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);"
)

with k1:
    st.markdown(f"""
        <div style="{KPI_CARD_STYLE}">
            <div class="kpi-row">
                <div class="kpi-icon">{ICON_POSTINGS}</div>
                <div class="kpi-body">
                    <div class="kpi-label">Total postings</div>
                    <div class="kpi-value-row">
                        <div class="kpi-value">{total_now:,}</div>
                        <div class="kpi-hint">jobs</div>
                    </div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
        <div style="{KPI_CARD_STYLE}">
            <div class="kpi-row">
                <div class="kpi-icon">{ICON_COMPANIES}</div>
                <div class="kpi-body">
                    <div class="kpi-label">Companies hiring</div>
                    <div class="kpi-value-row">
                        <div class="kpi-value">{companies_now:,}</div>
                        <div class="kpi-hint">companies</div>
                    </div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ============================================================
# 2. Company type (donut) + Industry breakdown (hbar)
# ============================================================

col_ct, col_ind = st.columns(2, gap="medium")

with col_ct:
    with st.container(border=True):
        type_counts = filtered["company_type"].value_counts().reset_index()
        type_counts.columns = ["type", "count"]
        type_counts = type_counts[type_counts["type"].notna()]
        total = int(type_counts["count"].sum())

        card_header(
            "Company type",
            "Share by sector",
            tag=f"{len(type_counts)} sectors",
        )

        if len(type_counts) > 0:
            # Assign palette colours consistently
            palette = CATEGORICAL[:len(type_counts)]

            fig_donut = go.Figure(go.Pie(
                labels=type_counts["type"],
                values=type_counts["count"],
                hole=0.68,
                marker=dict(colors=palette, line=dict(color="white", width=2)),
                textinfo="none",
                sort=False,
                hovertemplate="<b>%{label}</b><br>%{value} postings (%{percent})<extra></extra>",
            ))
            fig_donut.add_annotation(
                text=f"<b style='font-size:24px;color:{TEXT_1}'>{total}</b>"
                     f"<br><span style='font-size:10px;color:{TEXT_3};"
                     f"letter-spacing:0.08em'>POSTINGS</span>",
                x=0.5, y=0.5, showarrow=False,
                xref="paper", yref="paper",
            )
            fig_donut.update_layout(
                height=260,
                showlegend=False,
                margin=dict(t=0, b=0, l=0, r=0),
                paper_bgcolor="rgba(0,0,0,0)",
            )

            col_d, col_l = st.columns([5, 4])
            with col_d:
                st.plotly_chart(fig_donut, use_container_width=True,
                                config={"displayModeBar": False})
            with col_l:
                st.markdown("<div style='height:28px'></div>",
                            unsafe_allow_html=True)
                legend_html = ""
                for (_, row), color in zip(type_counts.iterrows(), palette):
                    pct = round(row["count"] / total * 100, 1)
                    legend_html += f"""
                        <div style="display:flex;align-items:center;
                                    justify-content:space-between;
                                    padding:6px 0;
                                    border-bottom:1px solid {TRACK};
                                    font-size:13px">
                            <div style="display:flex;align-items:center;gap:8px">
                                <span style="width:10px;height:10px;
                                             border-radius:3px;
                                             background:{color};
                                             display:inline-block"></span>
                                <span style="color:{TEXT_1}">{row['type']}</span>
                            </div>
                            <div style="color:{TEXT_3};font-size:12px">
                                <span style="color:{TEXT_1};font-weight:600;
                                             margin-right:6px">
                                    {int(row['count'])}
                                </span>{pct}%
                            </div>
                        </div>
                    """
                st.markdown(legend_html, unsafe_allow_html=True)

with col_ind:
    with st.container(border=True):
        industry_counts = filtered["industry"].value_counts().reset_index()
        industry_counts.columns = ["industry", "count"]
        industry_counts = industry_counts[
            industry_counts["industry"].notna() &
            ~industry_counts["industry"].isin(["Other", "Unknown"])
        ].head(8)
        n_ind = len(industry_counts)

        card_header(
            "Industry breakdown",
            "Top industry verticals hiring",
            tag=f"{n_ind} shown",
        )

        if n_ind > 0:
            bar_h = max(260, n_ind * 34)
            st.plotly_chart(
                hbar_chart(industry_counts, "count", "industry",
                           ACCENT, height=bar_h),
                use_container_width=True,
                config={"displayModeBar": False},
            )

# ============================================================
# 3. 14-day posting trend
# ============================================================

if df_jobs["posted_date"].notna().any():
    max_date = df_jobs["posted_date"].max()
    min_date = max_date - timedelta(days=13)
    all_dates = pd.date_range(start=min_date, end=max_date, freq="D")
    date_df = pd.DataFrame({"posted_date": all_dates})

    daily = filtered.groupby("posted_date").size().reset_index(name="count")
    daily = date_df.merge(daily, on="posted_date", how="left").fillna(0)
    daily["count"] = daily["count"].astype(int)
    daily["dow"] = daily["posted_date"].dt.strftime("%a")
    daily["is_weekend"] = daily["posted_date"].dt.dayofweek >= 5
    daily["label"] = daily["posted_date"].dt.strftime("%b %d")
    daily_avg = int(daily["count"].mean())

    with st.container(border=True):
        card_header(
            "Posting volume trend",
            f"Daily postings · avg {daily_avg}/day over 14 days",
            tag="14D",
            tag_accent=True,
        )

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Bar(
            x=daily["posted_date"],
            y=daily["count"],
            marker=dict(
                # Weekends rendered in soft accent to distinguish from workdays
                color=[ACCENT_SOFT if w else ACCENT for w in daily["is_weekend"]],
                line=dict(width=0),
            ),
            customdata=list(zip(daily["label"], daily["dow"])),
            hovertemplate="<b>%{customdata[0]} (%{customdata[1]})</b>"
                          "<br>%{y} postings<extra></extra>",
        ))
        # Dotted average line
        fig_trend.add_hline(
            y=daily_avg,
            line=dict(color=TEXT_3, width=1, dash="dot"),
            annotation_text=f"avg {daily_avg}",
            annotation_position="top right",
            annotation_font=dict(size=10, color=TEXT_3),
        )
        fig_trend.update_layout(
            height=240,
            margin=dict(t=8, b=8, l=0, r=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                showgrid=False, showline=False,
                tickfont=dict(size=11, color=TEXT_3),
                tickformat="%a<br>%b %d",
                tickmode="array",
                tickvals=daily["posted_date"],
            ),
            yaxis=dict(
                showgrid=True, gridcolor=TRACK, gridwidth=1,
                showline=False, zeroline=False,
                tickfont=dict(size=11, color=TEXT_3),
            ),
            bargap=0.3,
        )
        st.plotly_chart(fig_trend, use_container_width=True,
                        config={"displayModeBar": False})

# ============================================================
# 4. Role distribution — stacked hbar with grey track
# ============================================================

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

# Identify self-contained subtypes: sole subtype under its role, not shared elsewhere
# These are redundant in the legend and will be hidden
subtype_role_map = (
    role_subtype_counts.groupby("role_subtype")["role_standardised"]
    .nunique().to_dict()
)
role_subtype_counts_map = (
    role_subtype_counts.groupby("role_standardised")["role_subtype"]
    .nunique().to_dict()
)

def is_redundant_subtype(subtype):
    """Return True if the subtype maps 1-to-1 with its parent role."""
    if subtype_role_map.get(subtype, 0) != 1:
        return False
        # Look up which role owns this subtype
    its_role = role_subtype_counts[
        role_subtype_counts["role_subtype"] == subtype
    ]["role_standardised"].iloc[0]
    return role_subtype_counts_map.get(its_role, 0) == 1

with st.container(border=True):
    card_header(
        "Role distribution",
        "Breakdown by role family and specialization",
        tag=f"{n_roles} families",
    )

    if len(role_subtype_counts) > 0:
        role_totals = (
            role_subtype_counts.groupby("role_standardised")["count"]
            .sum().reindex(role_order)
        )
        max_total = role_totals.max()
        track_len = max_total
        x_range = track_len * 1.15

        fig_role = go.Figure()

        # Grey full-width track (independent offsetgroup, not part of the stack)
        fig_role.add_trace(go.Bar(
            x=[track_len] * len(role_order),
            y=role_order,
            orientation="h",
            marker=dict(color=TRACK, line=dict(width=0)),
            width=0.5,
            showlegend=False,
            hoverinfo="skip",
            offsetgroup="track",
        ))

        # One coloured trace per subtype, all in 'bars' offsetgroup so they stack
        for subtype in all_subtypes:
            df_sub = (
                role_subtype_counts[role_subtype_counts["role_subtype"] == subtype]
                .set_index("role_standardised")
                .reindex(role_order).fillna(0).reset_index()
            )
            redundant = is_redundant_subtype(subtype)

            # Hover text: redundant subtypes show count only
            if redundant:
                hover_tpl = "<b>%{y}</b><br>%{x} postings<extra></extra>"
            else:
                hover_tpl = ("<b>%{y}</b><br>" + subtype +
                             ": %{x}<extra></extra>")

            fig_role.add_trace(go.Bar(
                name=subtype,
                x=df_sub["count"],
                y=df_sub["role_standardised"],
                orientation="h",
                marker=dict(
                    color=SUBTYPE_COLORS.get(subtype, "#CBD5E1"),
                    line=dict(width=0),
                ),
                hovertemplate=hover_tpl,
                width=0.5,
                offsetgroup="bars",
                showlegend=not redundant,  # hide redundant subtypes from the legend
            ))

        # Count labels pinned to a fixed right position
        fig_role.add_trace(go.Scatter(
            x=[track_len * 1.04] * len(role_order),
            y=role_order,
            mode="text",
            text=[f"<b>{int(role_totals[r])}</b>" for r in role_order],
            textfont=dict(size=12, color=TEXT_1, family="Inter"),
            textposition="middle right",
            showlegend=False,
            hoverinfo="skip",
            cliponaxis=False,
        ))

        fig_role.update_layout(
            barmode="stack",
            height=max(300, n_roles * 58),
            margin=dict(t=4, b=80, l=0, r=8),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                showgrid=False, showticklabels=False,
                showline=False, zeroline=False,
                range=[0, x_range],
                fixedrange=True,
            ),
            yaxis=dict(
                showgrid=False, showline=False,
                tickfont=dict(size=13, color=TEXT_1, family="Inter"),
                categoryorder="array", categoryarray=role_order,
                fixedrange=True,
            ),
            legend=dict(
                orientation="h", yanchor="top", y=-0.08,
                xanchor="left", x=0,
                font=dict(size=11, color=TEXT_2, family="Inter"),
                bgcolor="rgba(0,0,0,0)",
                itemsizing="constant",
            ),
        )
        st.plotly_chart(fig_role, use_container_width=True,
                        config={"displayModeBar": False})

# ============================================================
# 5. Top skills — programming / tools / concepts
# ============================================================

skill_counts = filtered_skills.groupby("skill")["job_id"].count().reset_index()
skill_counts.columns = ["skill", "count"]
skill_counts = skill_counts.merge(
    skill_dict[["skill", "category"]], on="skill", how="left"
)

categories = [
    ("programming", "Programming",  "Languages & scripting",    "#6366F1"),
    ("tool",        "Tools",        "Platforms & products",     ACCENT),
    ("concept",     "Concepts",     "Methods & domain skills",  "#F59E0B"),
]

cols = st.columns(3, gap="medium")
for i, (cat_key, cat_label, cat_sub, cat_color) in enumerate(categories):
    with cols[i]:
        with st.container(border=True):
            cat_df = (
                skill_counts[skill_counts["category"] == cat_key]
                .nlargest(10, "count").copy()
            )
            n_cat = len(cat_df)
            cat_df["skill_display"] = (
                cat_df["skill"].str.replace("_", " ").str.title()
            )

            card_header(cat_label, cat_sub, tag=f"TOP {n_cat}")

            if n_cat > 0:
                st.plotly_chart(
                    hbar_chart(cat_df, "count", "skill_display",
                               cat_color, height=max(300, n_cat * 34)),
                    use_container_width=True,
                    config={"displayModeBar": False},
                )
            else:
                st.markdown(
                    f"<div style='color:{TEXT_3};text-align:center;"
                    f"padding:40px 0;font-size:13px'>No data</div>",
                    unsafe_allow_html=True,
                )

# ============================================================
# 6. Geographic distribution — map + city bar
# ============================================================

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

# Map and city bar share the same height so the columns align
MAP_HEIGHT = 460

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
region_map = region_counts.dropna(subset=["lat", "lon"]).copy()

city_counts = filtered["city"].value_counts().reset_index()
city_counts.columns = ["city", "count"]
city_counts = city_counts[
    city_counts["city"].notna() &
    ~city_counts["city"].isin(["Other", "Unknown"])
].head(10)  # top 10 cities
n_cities = len(city_counts)

col_map, col_city = st.columns([3, 2], gap="medium")

with col_map:
    with st.container(border=True):
        card_header(
            "Geographic distribution",
            "Postings across New Zealand regions",
            tag=f"{len(region_map)} regions",
            tag_accent=True,
        )

        if len(region_map) > 0:
            # Bubble size: sqrt mapping so circle area is proportional to count
            max_count = region_map["count"].max()
            MIN_SIZE = 6   # small enough to distinguish; still hoverable
            MAX_SIZE = 52
            region_map["size_scaled"] = region_map["count"].apply(
                lambda c: MIN_SIZE + (math.sqrt(c) / math.sqrt(max_count))
                                     * (MAX_SIZE - MIN_SIZE)
            )
            # Halo ring slightly larger than the core dot
            region_map["halo_size"] = region_map["size_scaled"] * 1.8

            # Labels only for the top 6 regions to avoid clutter
            region_map_sorted = region_map.sort_values("count", ascending=False)
            top_labels = region_map_sorted.head(6).copy()
            other_points = region_map_sorted.iloc[6:].copy()

            fig_map = go.Figure()

            # Layer 1: outer halo (low-opacity accent circle)
            fig_map.add_trace(go.Scattermap(
                lat=region_map["lat"],
                lon=region_map["lon"],
                mode="markers",
                marker=dict(
                    size=region_map["halo_size"],
                    color=ACCENT,
                    opacity=0.18,
                ),
                hoverinfo="skip",
                showlegend=False,
            ))

            # Layer 2: solid core dot
            fig_map.add_trace(go.Scattermap(
                lat=region_map["lat"],
                lon=region_map["lon"],
                mode="markers",
                marker=dict(
                    size=region_map["size_scaled"],
                    color=ACCENT_DARK,
                    opacity=0.92,
                ),
                text=region_map["region"],
                customdata=region_map["count"],
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "%{customdata} postings"
                    "<extra></extra>"
                ),
                showlegend=False,
            ))

            # Layer 3: text labels to the right of bubbles (avoids halo overlap)
            fig_map.add_trace(go.Scattermap(
                lat=top_labels["lat"],
                lon=top_labels["lon"],
                mode="text",
                text=[
                    f"<b>{r}</b> · {int(c)}"
                    for r, c in zip(top_labels["region"], top_labels["count"])
                ],
                textfont=dict(
                    size=12,
                    color=TEXT_1,
                    family="Inter",
                ),
                textposition="middle right",
                hoverinfo="skip",
                showlegend=False,
            ))

            fig_map.update_layout(
                height=MAP_HEIGHT,
                margin=dict(t=0, b=0, l=0, r=0),
                paper_bgcolor="rgba(0,0,0,0)",
                map=dict(
                    style="carto-positron",  # light neutral basemap, no API token required
                    zoom=4.3,
                    center={"lat": -41.0, "lon": 174.0},
                ),
                showlegend=False,
            )
            st.plotly_chart(
                fig_map,
                use_container_width=True,
                config={
                    "displayModeBar": False,
                    "scrollZoom": False,  # prevent accidental zoom while page-scrolling
                },
            )

with col_city:
    with st.container(border=True):
        card_header(
            "Top cities",
            "Cities with the most postings",
            tag=f"TOP {n_cities}",
        )

        if n_cities > 0:
            # Both columns use the same plotly height so the containers align.
            st.plotly_chart(
                hbar_chart(city_counts, "count", "city",
                           ACCENT, height=MAP_HEIGHT),
                use_container_width=True,
                config={"displayModeBar": False},
            )

# ============================================================
# Footer
# ============================================================

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
st.markdown(
    f'<p style="text-align:center;color:{TEXT_3};font-size:12px;'
    f'padding-top:16px;border-top:1px solid {BORDER}">'
    f'Data source: Seek NZ &nbsp;·&nbsp; Built with Streamlit + Snowflake + Plotly'
    f'</p>',
    unsafe_allow_html=True,
)