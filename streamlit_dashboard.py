import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ---- Page config ----
st.set_page_config(
    page_title="Crypto Anomaly Monitor",
    page_icon="⚡",
    layout="wide"
)

# ---- Custom CSS ----
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Inter:wght@300;400;600&display=swap');

    /* Force dark background everywhere */
    html, body { background-color: #0a0e1a !important; }
    .main, .block-container,
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewBlockContainer"],
    [data-testid="stVerticalBlock"],
    [data-testid="stMainBlockContainer"],
    section[data-testid="stMain"] {
        background-color: #0a0e1a !important;
        color: #e2e8f0 !important;
    }

    /* All text */
    html, body, p, span, div, label {
        font-family: 'Inter', sans-serif;
        color: #e2e8f0;
    }

    h1, h2, h3 {
        font-family: 'Space Mono', monospace !important;
        color: #f0f9ff !important;
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #111827 0%, #1a2235 100%) !important;
        border: 1px solid #1e3a5f !important;
        border-radius: 12px !important;
        padding: 16px !important;
        box-shadow: 0 4px 24px rgba(0,0,0,0.4) !important;
    }

    [data-testid="stMetricLabel"] {
        font-family: 'Space Mono', monospace !important;
        font-size: 11px !important;
        color: #64748b !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }

    [data-testid="stMetricValue"] {
        font-family: 'Space Mono', monospace !important;
        color: #38bdf8 !important;
        font-size: 28px !important;
    }

    /* Sidebar - dark */
    [data-testid="stSidebar"] {
        background-color: #0d1220 !important;
        border-right: 1px solid #1e3a5f !important;
    }

    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #f0f9ff !important;
    }

    [data-testid="collapsedControl"] {
        color: #e2e8f0 !important;
        background-color: #1e3a5f !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: #111827 !important;
        border: 1px solid #1e3a5f !important;
        color: #e2e8f0 !important;
        border-radius: 8px;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid #1e3a5f;
        border-radius: 8px;
        overflow: hidden;
    }

    /* Section headers */
    .section-header {
        font-family: 'Space Mono', monospace;
        font-size: 11px;
        color: #38bdf8;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 12px;
        padding-bottom: 8px;
        border-bottom: 1px solid #1e3a5f;
    }

    /* Alert badge */
    .alert-badge {
        display: inline-block;
        background: linear-gradient(90deg, #7c3aed, #db2777);
        color: white;
        font-family: 'Space Mono', monospace;
        font-size: 10px;
        padding: 3px 10px;
        border-radius: 20px;
        letter-spacing: 1px;
        text-transform: uppercase;
    }

    /* Header bar */
    .top-bar {
        border-bottom: 1px solid #1e3a5f;
        padding: 16px 0 12px 0;
        margin-bottom: 24px;
    }

    /* Caption text */
    [data-testid="stCaptionContainer"] p {
        color: #64748b !important;
    }

    /* Dropdown popup - force dark text so readable on white popup */
    [data-baseweb="popover"] * { color: #1e293b !important; }
    [data-baseweb="menu"] * { color: #1e293b !important; }
    [data-baseweb="select"] ul li { color: #1e293b !important; }
    li[role="option"] { color: #1e293b !important; }
    ul[role="listbox"] li { color: #1e293b !important; }
    li[role="option"] span { color: #1e293b !important; }
</style>
""", unsafe_allow_html=True)

# Extra dropdown fix injected separately for specificity
st.markdown("""
<style>
    li[role="option"] span { color: #1e293b !important; }
    li[role="option"] { color: #1e293b !important; }
</style>
""", unsafe_allow_html=True)

# ---- Snowpark session ----
session = get_active_session()

# ---- Header ----
st.markdown('<div class="top-bar">', unsafe_allow_html=True)
col_title, col_badge = st.columns([5, 1])
with col_title:
    st.markdown("# ⚡ CRYPTO ANOMALY MONITOR")
    st.markdown("<p style='color:#64748b; font-size:13px; margin-top:-8px;'>Real-time anomaly detection · CoinMarketCap · Kafka → Spark → Snowflake</p>", unsafe_allow_html=True)
with col_badge:
    st.markdown("<br><span class='alert-badge'>● Live</span>", unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

# ---- Load data ----
@st.cache_data(ttl=300)
def load_alerts():
    df = session.sql("""
        SELECT
            alert_id, event_id, id, event_ts, curr_price,
            mean, std, z_score, event_dt
        FROM raw_alerts
        ORDER BY event_ts DESC
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    df["event_dt"] = pd.to_datetime(df["event_dt"])
    return df

@st.cache_data(ttl=300)
def load_events():
    df = session.sql("""
        SELECT id, name, symbol, price, event_ts, volume_24h, event_dt
        FROM raw_events
        ORDER BY event_ts DESC
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

@st.cache_data(ttl=300)
def load_symbol_map():
    df = session.sql("""
        SELECT DISTINCT id, name, symbol FROM raw_events ORDER BY symbol
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df

alerts_df = load_alerts()
events_df = load_events()
symbol_map = load_symbol_map()

# ---- Sidebar filters ----
st.sidebar.markdown("## 🔍 Filters")

min_date = alerts_df["event_dt"].min()
max_date = alerts_df["event_dt"].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

z_threshold = st.sidebar.slider(
    "Min Z-score (severity)",
    min_value=3.0,
    max_value=float(alerts_df["z_score"].max()),
    value=3.0,
    step=0.1
)

all_coins = sorted(symbol_map["symbol"].tolist())
selected_coin = st.sidebar.selectbox(
    "Coin (for price chart)",
    options=all_coins,
    index=all_coins.index("BTC") if "BTC" in all_coins else 0
)

st.sidebar.markdown("---")
st.sidebar.markdown(f"<p style='color:#94a3b8; font-size:11px;'>Data refreshes every 5 min<br>Last load: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>", unsafe_allow_html=True)

# ---- Filter alerts ----
if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_alerts = alerts_df[
        (alerts_df["event_dt"] >= pd.Timestamp(start_date)) &
        (alerts_df["event_dt"] <= pd.Timestamp(end_date)) &
        (alerts_df["z_score"] >= z_threshold)
    ]
else:
    filtered_alerts = alerts_df[alerts_df["z_score"] >= z_threshold]

# ---- KPI metrics ----
st.markdown('<p class="section-header">Overview</p>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Alerts", f"{len(filtered_alerts):,}")

with col2:
    most_alerted = filtered_alerts["id"].value_counts().idxmax() if not filtered_alerts.empty else "—"
    symbol_match = symbol_map[symbol_map["id"] == most_alerted]["symbol"].values
    most_alerted_symbol = symbol_match[0] if len(symbol_match) > 0 else str(most_alerted)
    st.metric("Most Alerted Coin", most_alerted_symbol)

with col3:
    max_z = filtered_alerts["z_score"].max() if not filtered_alerts.empty else 0
    st.metric("Highest Z-score", f"{max_z:.2f}")

with col4:
    unique_coins = filtered_alerts["id"].nunique()
    st.metric("Coins Flagged", unique_coins)

st.markdown("<br>", unsafe_allow_html=True)

# ---- Price chart with anomalies ----
st.markdown('<p class="section-header">Price History with Anomalies</p>', unsafe_allow_html=True)

selected_id = symbol_map[symbol_map["symbol"] == selected_coin]["id"].values
if len(selected_id) > 0:
    coin_id = selected_id[0]
    coin_prices = events_df[events_df["id"] == coin_id].sort_values("event_ts")
    coin_alerts = filtered_alerts[filtered_alerts["id"] == coin_id]

    if not coin_prices.empty:
        import altair as alt

        # Y-axis zoom with padding so line sits in middle
        price_min = coin_prices["price"].min()
        price_max = coin_prices["price"].max()
        padding = (price_max - price_min) * 0.3 if price_max != price_min else price_min * 0.1
        y_min = price_min - padding
        y_max = price_max + padding

        price_chart = alt.Chart(coin_prices).mark_line(
            color="#38bdf8",
            strokeWidth=1.5
        ).encode(
            x=alt.X("event_ts:T", title="Time",
                    axis=alt.Axis(labelColor="#94a3b8", titleColor="#94a3b8")),
            y=alt.Y("price:Q", title="Price (USD)",
                    scale=alt.Scale(domain=[y_min, y_max]),
                    axis=alt.Axis(labelColor="#94a3b8", titleColor="#94a3b8")),
            tooltip=["event_ts:T", "price:Q"]
        )

        if not coin_alerts.empty:
            anomaly_points = alt.Chart(coin_alerts).mark_point(
                color="#f87171",
                size=100,
                shape="triangle-up",
                filled=True
            ).encode(
                x=alt.X("event_ts:T"),
                y=alt.Y("curr_price:Q", scale=alt.Scale(domain=[y_min, y_max])),
                tooltip=["event_ts:T", "curr_price:Q", "z_score:Q", "mean:Q", "std:Q"]
            )
            chart = (price_chart + anomaly_points).properties(height=320)
        else:
            chart = price_chart.properties(height=320)

        chart = chart.properties(
            background="#0f1729"
        ).configure_view(
            strokeOpacity=0
        ).configure_axis(
            gridColor="#1e3a5f",
            domainColor="#1e3a5f"
        )

        st.altair_chart(chart, use_container_width=True)
        st.caption(f"🔴 Red triangles = anomaly alerts (z-score >= {z_threshold:.1f})")
    else:
        st.info(f"No price data found for {selected_coin}")

st.markdown("<br>", unsafe_allow_html=True)

# ---- Alerts over time bar chart ----
st.markdown('<p class="section-header">Alert Volume by Date</p>', unsafe_allow_html=True)

if not filtered_alerts.empty:
    alerts_by_date = filtered_alerts.groupby("event_dt").size().reset_index(name="count")
    bar_chart = alt.Chart(alerts_by_date).mark_bar(
        color="#7c3aed",
        cornerRadiusTopLeft=4,
        cornerRadiusTopRight=4
    ).encode(
        x=alt.X("event_dt:T", title="Date",
                axis=alt.Axis(labelColor="#94a3b8", titleColor="#94a3b8")),
        y=alt.Y("count:Q", title="Number of Alerts",
                axis=alt.Axis(labelColor="#94a3b8", titleColor="#94a3b8")),
        tooltip=["event_dt:T", "count:Q"]
    ).properties(
        height=200,
        background="#0f1729"
    ).configure_view(
        strokeOpacity=0
    ).configure_axis(
        gridColor="#1e3a5f",
        domainColor="#1e3a5f"
    )
    st.altair_chart(bar_chart, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---- Recent alerts table ----
st.markdown('<p class="section-header">Recent Alerts</p>', unsafe_allow_html=True)

if not filtered_alerts.empty:
    display_alerts = filtered_alerts.merge(symbol_map[["id", "name", "symbol"]], on="id", how="left")
    display_alerts = display_alerts[[
        "event_ts", "symbol", "name", "curr_price", "mean", "std", "z_score"
    ]].sort_values("event_ts", ascending=False).head(50)

    display_alerts = display_alerts.rename(columns={
        "event_ts": "Time",
        "symbol": "Coin",
        "name": "Name",
        "curr_price": "Price at Alert",
        "mean": "Window Mean",
        "std": "Window StdDev",
        "z_score": "Z-Score"
    })

    for col in ["Price at Alert", "Window Mean", "Window StdDev", "Z-Score"]:
        display_alerts[col] = display_alerts[col].round(4)

    st.dataframe(
        display_alerts,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Z-Score": st.column_config.ProgressColumn(
                "Z-Score",
                min_value=0,
                max_value=float(filtered_alerts["z_score"].max()),
                format="%.2f"
            ),
            "Time": st.column_config.DatetimeColumn("Time", format="MMM D, HH:mm")
        }
    )
else:
    st.info("No alerts found for the selected filters.")

st.markdown("<br><hr style='border-color:#1e3a5f'>", unsafe_allow_html=True)
st.markdown("<p style='color:#64748b; font-size:11px; text-align:center;'>CSE 5114 · Linus Dannull & Victoria Cheung · Anomaly Detection Pipeline</p>", unsafe_allow_html=True)
