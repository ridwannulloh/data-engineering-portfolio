"""
Global Economic Indicators Dashboard
-------------------------------------
Streamlit app — reads from BigQuery, renders live charts.
Deployed on GCP Cloud Run.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Global Economic Indicators",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1e293b;
        border: 1px solid #475569;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 8px;
    }
    .metric-label { font-size: 13px; color: #94a3b8; margin-bottom: 4px; }
    .metric-value { font-size: 24px; font-weight: 700; color: #f1f5f9; }
    .metric-delta-pos { font-size: 12px; color: #4ade80; }
    .metric-delta-neg { font-size: 12px; color: #f87171; }
    .section-header {
        font-size: 18px; font-weight: 600; color: #ffffff;
        border-bottom: 2px solid #3b82f6;
        padding-bottom: 6px; margin-bottom: 16px;
    }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID")
TABLE_ID   = f"{PROJECT_ID}.economic_data.indicators"

INDICATOR_META = {
    "NY.GDP.MKTP.CD":    {"label": "GDP (Current USD)",          "unit": "USD", "format": "billions", "icon": "🏭"},
    "FP.CPI.TOTL.ZG":   {"label": "Inflation Rate",             "unit": "%",   "format": "percent",  "icon": "📈"},
    "SL.UEM.TOTL.ZS":   {"label": "Unemployment Rate",          "unit": "%",   "format": "percent",  "icon": "👷"},
    "NE.EXP.GNFS.ZS":   {"label": "Exports (% of GDP)",         "unit": "%",   "format": "percent",  "icon": "🚢"},
    "GC.DOD.TOTL.GD.ZS":{"label": "Government Debt (% of GDP)", "unit": "%",   "format": "percent",  "icon": "🏛️"},
}

COUNTRY_FLAGS = {
    "United States": "🇺🇸", "China": "🇨🇳", "Germany": "🇩🇪",
    "Japan": "🇯🇵", "Indonesia": "🇮🇩", "Brazil": "🇧🇷", "India": "🇮🇳",
    "Canada": "🇨🇦", "France": "🇫🇷", "Italy": "🇮🇹", "United Kingdom": "🇬🇧",
}

# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_data() -> pd.DataFrame:
    """Load all indicator data from BigQuery. Cached for 1 hour."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            country_code,
            country_name,
            indicator_code,
            indicator_name,
            year,
            value,
            loaded_at
        FROM `{TABLE_ID}`
        WHERE value IS NOT NULL
        ORDER BY year DESC
    """
    df = client.query(query).to_dataframe()
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def get_last_updated() -> str:
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT MAX(loaded_at) as ts FROM `{TABLE_ID}`"
    result = client.query(query).to_dataframe()
    ts = result["ts"].iloc[0]
    return ts.strftime("%d %b %Y %H:%M UTC") if ts else "Unknown"


def format_value(value: float, fmt: str) -> str:
    if fmt == "billions":
        if value >= 1e12:
            return f"${value/1e12:.2f}T"
        elif value >= 1e9:
            return f"${value/1e9:.2f}B"
        return f"${value:,.0f}"
    return f"{value:.2f}%"


# ── Load data ─────────────────────────────────────────────────────────────────
# Filter for G7 + Indonesia only
G7_PLUS_INDONESIA = [
    "Canada", "France", "Germany", "Italy", "Japan", 
    "United Kingdom", "United States", "Indonesia"
]

with st.spinner("Loading data from BigQuery..."):
    try:
        df = load_data()
        # Filter to only G7 + Indonesia
        df = df[df["country_name"].isin(G7_PLUS_INDONESIA)]
        last_updated = get_last_updated()
        data_ok = True
    except Exception as e:
        st.error(f"Could not connect to BigQuery: {e}")
        st.info("Make sure `GCP_PROJECT_ID` is set and the service account has BigQuery access.")
        data_ok = False
        st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_meta = st.columns([3, 1])
with col_title:
    st.markdown("## 🌐 Global Economic Indicators")
    st.markdown("*Live data · World Bank Open Data API · BigQuery + Cloud Run*")
with col_meta:
    st.markdown(f"<br><small>🕐 Updated: {last_updated}</small>", unsafe_allow_html=True)

st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔧 Filters")

    all_indicators = df["indicator_code"].unique().tolist()
    indicator_options = {
        INDICATOR_META[c]["icon"] + " " + INDICATOR_META[c]["label"]: c
        for c in all_indicators if c in INDICATOR_META
    }
    selected_label = st.selectbox("Indicator", list(indicator_options.keys()))
    selected_code  = indicator_options[selected_label]
    selected_meta  = INDICATOR_META[selected_code]

    st.markdown("---")
    all_countries = sorted(df["country_name"].unique().tolist())
    selected_countries = st.multiselect(
        "Countries",
        all_countries,
        default=all_countries,
    )

    st.markdown("---")
    year_min = int(df["year"].min())
    year_max = int(df["year"].max())
    year_range = st.slider("Year Range", year_min, year_max, (year_min, year_max))

    st.markdown("---")
    chart_type = st.radio("Chart Style", ["Line", "Area", "Bar"], horizontal=True)

    st.markdown("---")
    st.markdown(
        "**Source:** [World Bank Open Data](https://data.worldbank.org)\n\n"
        "**Stack:** Python · BigQuery · Streamlit · Cloud Run\n\n"
        "**Code:** [GitHub](https://github.com/ridwannulloh/data-engineering-portfolio)"
    )

# ── Filter data ───────────────────────────────────────────────────────────────
mask = (
    (df["indicator_code"] == selected_code) &
    (df["country_name"].isin(selected_countries)) &
    (df["year"].between(*year_range))
)
filtered = df[mask].copy()

if filtered.empty:
    st.warning("No data for this selection. Try adjusting filters.")
    st.stop()

latest_year  = int(filtered["year"].max())
latest_data  = filtered[filtered["year"] == latest_year]
prev_year    = latest_year - 1
prev_data    = filtered[filtered["year"] == prev_year]

# ── KPI Cards ─────────────────────────────────────────────────────────────────
st.markdown(f'<div class="section-header">{selected_meta["icon"]} {selected_meta["label"]} — {latest_year} Snapshot</div>', unsafe_allow_html=True)

kpi_cols = st.columns(len(latest_data))
for i, (_, row) in enumerate(latest_data.sort_values("country_name").iterrows()):
    country  = row["country_name"]
    flag     = COUNTRY_FLAGS.get(country, "🌍")
    val_fmt  = format_value(row["value"], selected_meta["format"])

    # compute YoY delta
    prev_row = prev_data[prev_data["country_name"] == country]
    if not prev_row.empty:
        delta = row["value"] - prev_row["value"].iloc[0]
        delta_pct = (delta / prev_row["value"].iloc[0]) * 100
        delta_str = f"{'▲' if delta > 0 else '▼'} {abs(delta_pct):.1f}% YoY"
        delta_cls = "metric-delta-pos" if delta > 0 else "metric-delta-neg"
    else:
        delta_str = "—"
        delta_cls = "metric-delta-pos"

    with kpi_cols[i]:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{flag} {country}</div>
            <div class="metric-value">{val_fmt}</div>
            <div class="{delta_cls}">{delta_str}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Trend Chart ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">📉 Historical Trend</div>', unsafe_allow_html=True)

chart_df = filtered.sort_values("year")

if chart_type == "Line":
    fig = px.line(
        chart_df, x="year", y="value", color="country_name",
        markers=True,
        labels={"value": selected_meta["label"], "year": "Year", "country_name": "Country"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
elif chart_type == "Area":
    fig = px.area(
        chart_df, x="year", y="value", color="country_name",
        labels={"value": selected_meta["label"], "year": "Year", "country_name": "Country"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
else:
    fig = px.bar(
        chart_df, x="year", y="value", color="country_name",
        barmode="group",
        labels={"value": selected_meta["label"], "year": "Year", "country_name": "Country"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )

fig.update_layout(
    height=420,
    legend_title_text="Country",
    plot_bgcolor="#0f172a",
    paper_bgcolor="#0f172a",
    xaxis=dict(showgrid=True, gridcolor="#1e293b", dtick=1, color="white"),
    yaxis=dict(showgrid=True, gridcolor="#1e293b", color="white"),
    margin=dict(t=30, b=40, l=60, r=20),
    font=dict(family="Inter, sans-serif", size=13, color="white"),
    legend=dict(font=dict(color="white")),
)
st.plotly_chart(fig, use_container_width=True)

# ── Comparison Bar + Heatmap ──────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.markdown('<div class="section-header">📊 Country Comparison (Latest)</div>', unsafe_allow_html=True)
    bar_df = latest_data.sort_values("value", ascending=True)
    fig_bar = px.bar(
        bar_df, x="value", y="country_name",
        orientation="h",
        color="value",
        color_continuous_scale="Blues",
        labels={"value": selected_meta["label"], "country_name": ""},
    )
    fig_bar.update_layout(
        height=320, showlegend=False, coloraxis_showscale=False,
        plot_bgcolor="#0f172a", paper_bgcolor="#0f172a",
        margin=dict(t=20, b=40, l=10, r=20),
        font=dict(family="Inter, sans-serif", size=13, color="white"),
        yaxis=dict(showgrid=False, color="white"),
        xaxis=dict(showgrid=True, gridcolor="#1e293b", color="white"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.markdown('<div class="section-header">🗺️ Year-over-Year Change</div>', unsafe_allow_html=True)
    # Build a pivot for heatmap: countries vs recent years
    pivot_years = sorted(filtered["year"].unique())[-6:]
    pivot_df    = (
        filtered[filtered["year"].isin(pivot_years)]
        .pivot_table(index="country_name", columns="year", values="value")
    )
    fig_heat = px.imshow(
        pivot_df,
        color_continuous_scale="RdYlGn",
        aspect="auto",
        labels=dict(color=selected_meta["label"]),
    )
    fig_heat.update_layout(
        height=320,
        margin=dict(t=20, b=40, l=10, r=20),
        font=dict(family="Inter, sans-serif", size=13, color="white"),
        plot_bgcolor="#0f172a", paper_bgcolor="#0f172a",
        xaxis=dict(color="white"),
        yaxis=dict(color="white"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ── Raw data table ─────────────────────────────────────────────────────────────
with st.expander("📋 View Raw Data"):
    display_df = (
        filtered[["country_name", "year", "value", "indicator_name"]]
        .sort_values(["country_name", "year"])
        .rename(columns={
            "country_name":  "Country",
            "year":          "Year",
            "value":         selected_meta["label"],
            "indicator_name":"Indicator",
        })
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    csv = display_df.to_csv(index=False)
    st.download_button("⬇️ Download CSV", csv, "economic_data.csv", "text/csv")

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "Built by Ridwan Nullah · Data Engineer  |  "
    "Stack: Python · World Bank API · BigQuery · Streamlit · GCP Cloud Run  |  "
    "Source: [GitHub](https://github.com/ridwannulloh/data-engineering-portfolio)"
)
