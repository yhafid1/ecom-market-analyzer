import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db.queries import (
    get_leaderboard,
    get_rising_declining,
    get_channel_comparison,
    get_channel_summary,
    get_niche_finder,
    get_category_timeseries,
    get_category_score_breakdown,
    get_trends_explorer,
    get_all_category_names,
)

# ─────────────────────────────────────────────
# Page config & global styling
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="E-Commerce Trends Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    /* Global font and background */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Hide default Streamlit header/footer */
    #MainMenu, footer, header { visibility: hidden; }

    /* Main container padding */
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 2px solid #f0f0f0;
        padding-bottom: 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 44px;
        padding: 0 20px;
        border-radius: 8px 8px 0 0;
        font-size: 14px;
        font-weight: 500;
        color: #666;
        background: transparent;
        border: none;
    }
    .stTabs [aria-selected="true"] {
        background: #f8f9ff;
        color: #1a1a2e;
        border-bottom: 2px solid #4361ee;
    }

    /* Metric cards */
    [data-testid="metric-container"] {
        background: #f8f9ff;
        border: 1px solid #e8eaf6;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="metric-container"] label {
        font-size: 12px;
        font-weight: 600;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 700;
        color: #1a1a2e;
    }

    /* Section headers */
    .section-header {
        font-size: 13px;
        font-weight: 600;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 24px 0 12px 0;
    }

    /* Recommendation badges */
    .badge-enter  { background:#e8f5e9; color:#2e7d32; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
    .badge-watch  { background:#fff8e1; color:#f57f17; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
    .badge-avoid  { background:#fce4ec; color:#c62828; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
    .badge-rising   { background:#e3f2fd; color:#1565c0; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
    .badge-declining{ background:#fce4ec; color:#c62828; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }
    .badge-stable   { background:#f3e5f5; color:#6a1b9a; padding:3px 10px; border-radius:20px; font-size:12px; font-weight:600; }

    /* Divider */
    hr { border: none; border-top: 1px solid #f0f0f0; margin: 24px 0; }

    /* Dataframe styling */
    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }

    /* Sidebar */
    [data-testid="stSidebar"] { background: #f8f9ff; }

    /* Score bar */
    .score-bar-wrap { background:#f0f0f0; border-radius:8px; height:8px; width:100%; }
    .score-bar-fill { border-radius:8px; height:8px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Color palette
# ─────────────────────────────────────────────

BLUE    = "#4361ee"
GREEN   = "#2ecc71"
RED     = "#e74c3c"
AMBER   = "#f39c12"
PURPLE  = "#9b59b6"
TEAL    = "#1abc9c"
GRAY    = "#95a5a6"

CATEGORY_COLORS = {
    "Electronics":             BLUE,
    "Home & Garden":           GREEN,
    "Clothing & Accessories":  PURPLE,
    "Sporting Goods":          TEAL,
    "Toys & Hobbies":          AMBER,
    "Health & Beauty":         "#e91e63",
    "Pet Supplies":            "#ff7043",
    "Automotive Parts":        GRAY,
    "Musical Instruments":     "#00bcd4",
    "Collectibles":            "#8d6e63",
}

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def score_color(score: float) -> str:
    if score >= 65:   return GREEN
    if score >= 40:   return AMBER
    return RED

def momentum_badge(momentum: str) -> str:
    m = str(momentum).lower()
    if m == "rising":   return '<span class="badge-rising">↑ Rising</span>'
    if m == "declining":return '<span class="badge-declining">↓ Declining</span>'
    return '<span class="badge-stable">→ Stable</span>'

def rec_badge(rec: str) -> str:
    r = str(rec).lower()
    if r == "enter": return '<span class="badge-enter">Enter</span>'
    if r == "watch": return '<span class="badge-watch">Watch</span>'
    return '<span class="badge-avoid">Avoid</span>'

def score_bar(score: float, color: str) -> str:
    return f"""
    <div class="score-bar-wrap">
      <div class="score-bar-fill" style="width:{score}%;background:{color};"></div>
    </div>"""

def empty_state(message: str):
    st.markdown(f"""
    <div style="text-align:center;padding:60px 0;color:#aaa;">
        <div style="font-size:40px;margin-bottom:12px;">📭</div>
        <div style="font-size:15px;">{message}</div>
    </div>""", unsafe_allow_html=True)

def safe_load(fn, *args, **kwargs) -> pd.DataFrame:
    try:
        df = fn(*args, **kwargs)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not load data: {e}")
        return pd.DataFrame()

def plotly_defaults(fig) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", size=13, color="#444"),
        margin=dict(l=0, r=0, t=32, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right",  x=1,
            font=dict(size=12),
        ),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#f0f0f0", zeroline=False)
    return fig

# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

col_title, col_meta = st.columns([3, 1])
with col_title:
    st.markdown("## 📊 E-Commerce Trends Analytics")
    st.markdown("<p style='color:#888;margin-top:-12px;font-size:14px;'>Tracking where retail is declining and where e-commerce is winning — by category</p>", unsafe_allow_html=True)
with col_meta:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↻ Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("<hr>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Rising vs Declining",
    "🏪 Retail vs E-commerce",
    "🎯 Niche Finder",
    "🔍 Product Deep Dive",
    "📉 Trends Explorer",
])


# ══════════════════════════════════════════════
# TAB 1 — Rising vs Declining
# ══════════════════════════════════════════════

with tab1:
    df_lead = safe_load(get_leaderboard)
    df_rd   = safe_load(get_rising_declining)

    if df_lead.empty:
        empty_state("No leaderboard data yet. Run the pipeline first.")
    else:
        # Summary metrics
        rising   = len(df_rd[df_rd["momentum"] == "Rising"])   if not df_rd.empty else 0
        declining= len(df_rd[df_rd["momentum"] == "Declining"]) if not df_rd.empty else 0
        top_cat  = df_lead.iloc[0]["category_name"] if not df_lead.empty else "—"
        top_score= df_lead.iloc[0]["opportunity_score"] if not df_lead.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Categories Tracked", len(df_lead))
        c2.metric("Rising",   f"↑ {rising}",   delta=None)
        c3.metric("Declining",f"↓ {declining}", delta=None)
        c4.metric("Top Category", top_cat)

        st.markdown("<hr>", unsafe_allow_html=True)
        left, right = st.columns([1.6, 1])

        with left:
            st.markdown('<p class="section-header">Opportunity Score Leaderboard</p>', unsafe_allow_html=True)

            for _, row in df_lead.iterrows():
                sc    = float(row.get("opportunity_score", 0))
                color = score_color(sc)
                delta = float(row.get("score_delta", 0))
                delta_str = f"<span style='color:{'#2ecc71' if delta>=0 else '#e74c3c'};font-size:12px;'>{'▲' if delta>=0 else '▼'} {abs(delta):.1f}</span>"
                mom   = str(row.get("momentum", "Stable"))
                rec   = str(row.get("recommendation", "watch"))

                with st.container():
                    r1, r2 = st.columns([3, 1])
                    with r1:
                        st.markdown(
                            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:4px;'>"
                            f"<span style='font-weight:600;font-size:15px;'>{row['category_name']}</span>"
                            f"{momentum_badge(mom)}"
                            f"{rec_badge(rec)}"
                            f"</div>",
                            unsafe_allow_html=True
                        )
                        st.markdown(score_bar(sc, color), unsafe_allow_html=True)
                    with r2:
                        st.markdown(
                            f"<div style='text-align:right;'>"
                            f"<span style='font-size:22px;font-weight:700;color:{color};'>{sc:.0f}</span>"
                            f"<span style='font-size:12px;color:#aaa;'> /100</span><br>"
                            f"{delta_str}"
                            f"</div>",
                            unsafe_allow_html=True
                        )
                    st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)

        with right:
            st.markdown('<p class="section-header">Score Breakdown</p>', unsafe_allow_html=True)

            if not df_lead.empty:
                score_cols = ["trend_score","buzz_score","demand_score","spend_score","competition_score"]
                score_labels = ["Trend","Buzz","Demand","Spend","Competition"]
                avg_scores = [df_lead[c].mean() for c in score_cols if c in df_lead.columns]

                fig_radar = go.Figure(go.Scatterpolar(
                    r=avg_scores,
                    theta=score_labels,
                    fill="toself",
                    fillcolor=f"rgba(67,97,238,0.15)",
                    line=dict(color=BLUE, width=2),
                ))
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, 100], gridcolor="#f0f0f0"),
                        angularaxis=dict(gridcolor="#f0f0f0"),
                        bgcolor="rgba(0,0,0,0)",
                    ),
                    paper_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    margin=dict(l=20, r=20, t=20, b=20),
                    height=300,
                )
                st.plotly_chart(fig_radar, use_container_width=True)

            st.markdown('<p class="section-header">Momentum Summary</p>', unsafe_allow_html=True)
            if not df_rd.empty:
                momentum_counts = df_rd["momentum"].value_counts().reset_index()
                momentum_counts.columns = ["momentum", "count"]
                color_map = {"Rising": GREEN, "Declining": RED, "Stable": PURPLE}
                fig_mom = px.bar(
                    momentum_counts, x="momentum", y="count",
                    color="momentum",
                    color_discrete_map=color_map,
                    height=220,
                )
                fig_mom = plotly_defaults(fig_mom)
                fig_mom.update_layout(showlegend=False, xaxis_title="", yaxis_title="")
                st.plotly_chart(fig_mom, use_container_width=True)


# ══════════════════════════════════════════════
# TAB 2 — Retail vs E-commerce
# ══════════════════════════════════════════════

with tab2:
    df_channel = safe_load(get_channel_summary)
    df_channel_ts = safe_load(get_channel_comparison)

    if df_channel.empty:
        empty_state("No channel data yet. Run the pipeline first.")
    else:
        # Summary metrics
        ecomm_dom  = len(df_channel[df_channel["channel_status"] == "E-comm dominant"])
        retail_dom = len(df_channel[df_channel["channel_status"] == "Retail dominant"])
        contested  = len(df_channel[df_channel["channel_status"] == "Contested"])
        early_mover= len(df_channel[df_channel["opportunity_label"] == "Early mover opportunity"])

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("E-comm Dominant",      ecomm_dom)
        c2.metric("Retail Dominant",      retail_dom)
        c3.metric("Contested",            contested)
        c4.metric("Early Mover Opportunities", early_mover)

        st.markdown("<hr>", unsafe_allow_html=True)
        left, right = st.columns(2)

        with left:
            st.markdown('<p class="section-header">E-commerce Share by Category</p>', unsafe_allow_html=True)
            df_sorted = df_channel.sort_values("avg_ecomm_share", ascending=True)
            fig_share = go.Figure()
            fig_share.add_trace(go.Bar(
                y=df_sorted["category_name"],
                x=df_sorted["avg_ecomm_share"],
                name="E-commerce",
                orientation="h",
                marker_color=BLUE,
            ))
            fig_share.add_trace(go.Bar(
                y=df_sorted["category_name"],
                x=df_sorted["avg_retail_share"],
                name="Retail",
                orientation="h",
                marker_color=GRAY,
            ))
            fig_share.update_layout(
                barmode="stack",
                height=380,
                xaxis=dict(ticksuffix="%", range=[0, 100]),
            )
            fig_share = plotly_defaults(fig_share)
            st.plotly_chart(fig_share, use_container_width=True)

        with right:
            st.markdown('<p class="section-header">E-commerce Growth Rate by Category</p>', unsafe_allow_html=True)
            df_growth = df_channel.sort_values("avg_ecomm_growth", ascending=False)
            colors = [GREEN if v > 0 else RED for v in df_growth["avg_ecomm_growth"]]
            fig_growth = go.Figure(go.Bar(
                x=df_growth["category_name"],
                y=df_growth["avg_ecomm_growth"],
                marker_color=colors,
            ))
            fig_growth.update_layout(
                height=380,
                xaxis_tickangle=-35,
                yaxis=dict(ticksuffix="%"),
            )
            fig_growth = plotly_defaults(fig_growth)
            st.plotly_chart(fig_growth, use_container_width=True)

        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown('<p class="section-header">Channel Status by Category</p>', unsafe_allow_html=True)

        for _, row in df_channel.iterrows():
            ecomm = float(row.get("avg_ecomm_share", 0))
            retail = float(row.get("avg_retail_share", 0))
            growth = float(row.get("avg_ecomm_growth", 0))
            status = str(row.get("channel_status", ""))
            opp    = str(row.get("opportunity_label", ""))

            growth_color = GREEN if growth > 0 else RED
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:16px;padding:10px 0;border-bottom:1px solid #f5f5f5;'>"
                f"<div style='width:180px;font-weight:600;font-size:14px;'>{row['category_name']}</div>"
                f"<div style='flex:1;'>{score_bar(ecomm, BLUE)}"
                f"<div style='display:flex;justify-content:space-between;font-size:11px;color:#aaa;margin-top:3px;'>"
                f"<span>E-comm {ecomm:.0f}%</span><span>Retail {retail:.0f}%</span></div></div>"
                f"<div style='width:80px;text-align:right;font-size:13px;font-weight:600;color:{growth_color};'>"
                f"{'▲' if growth>0 else '▼'} {abs(growth):.1f}%</div>"
                f"<div style='width:160px;font-size:12px;color:#888;'>{opp}</div>"
                f"</div>",
                unsafe_allow_html=True
            )


# ══════════════════════════════════════════════
# TAB 3 — Niche Finder
# ══════════════════════════════════════════════

with tab3:
    st.markdown('<p class="section-header">Filters</p>', unsafe_allow_html=True)
    f1, f2, f3, f4 = st.columns(4)

    with f1:
        min_score = st.slider("Minimum opportunity score", 0, 100, 0, 5)
    with f2:
        rec_filter = st.selectbox("Recommendation", ["All", "enter", "watch", "avoid"])
    with f3:
        channel_filter = st.selectbox("Channel edge", ["All", "ecomm", "retail", "mixed"])
    with f4:
        price_filter = st.number_input("Max avg price ($)", min_value=0, max_value=10000, value=0, step=10)

    df_niche = safe_load(
        get_niche_finder,
        min_score=min_score,
        recommendation=None if rec_filter == "All" else rec_filter,
        channel_edge=None if channel_filter == "All" else channel_filter,
        max_price=None if price_filter == 0 else price_filter,
    )

    if df_niche.empty:
        empty_state("No niches match your filters. Try adjusting them.")
    else:
        st.markdown("<hr>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Categories matched", len(df_niche))
        c2.metric("Avg opportunity score", f"{df_niche['opportunity_score'].mean():.1f}")
        c3.metric("Enter recommendations", len(df_niche[df_niche["recommendation"] == "enter"]))

        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown('<p class="section-header">Niche Opportunity Table</p>', unsafe_allow_html=True)

        for _, row in df_niche.iterrows():
            sc     = float(row.get("opportunity_score", 0))
            color  = score_color(sc)
            rec    = str(row.get("recommendation", "watch"))
            ch     = str(row.get("channel_edge", "mixed"))
            price  = row.get("avg_price")
            st_pct = row.get("sell_through_pct")
            interest = row.get("avg_interest_30d")
            mentions = row.get("weekly_mentions")

            with st.expander(f"{row['category_name']}  —  Score: {sc:.0f}/100"):
                ex1, ex2, ex3, ex4 = st.columns(4)
                ex1.metric("Opportunity Score", f"{sc:.0f}/100")
                ex2.metric("Avg Price",         f"${price:.2f}" if price else "—")
                ex3.metric("Sell-through",      f"{st_pct:.1f}%" if st_pct else "—")
                ex4.metric("Weekly Mentions",   int(mentions) if mentions else "—")

                score_cols   = ["trend_score","buzz_score","demand_score","spend_score","competition_score"]
                score_labels = ["Trend","Buzz","Demand","Spend","Competition"]
                scores = [float(row.get(c, 0)) for c in score_cols]

                fig_scores = go.Figure(go.Bar(
                    x=score_labels,
                    y=scores,
                    marker_color=[BLUE, PURPLE, GREEN, AMBER, TEAL],
                ))
                fig_scores.update_layout(
                    height=200,
                    yaxis=dict(range=[0, 100]),
                    showlegend=False,
                )
                fig_scores = plotly_defaults(fig_scores)
                st.plotly_chart(fig_scores, use_container_width=True)

                st.markdown(
                    f"<div style='display:flex;gap:12px;margin-top:8px;'>"
                    f"{rec_badge(rec)}"
                    f"<span style='font-size:12px;color:#888;'>Channel: <b>{ch}</b></span>"
                    f"<span style='font-size:12px;color:#888;'>Search interest: <b>{interest:.0f}/100</b></span>"
                    f"</div>",
                    unsafe_allow_html=True
                )


# ══════════════════════════════════════════════
# TAB 4 — Product Deep Dive
# ══════════════════════════════════════════════

with tab4:
    all_cats = safe_load(get_all_category_names)
    if isinstance(all_cats, pd.DataFrame):
        cat_list = all_cats["category_name"].tolist() if not all_cats.empty and "category_name" in all_cats.columns else []
    elif isinstance(all_cats, list):
        cat_list = all_cats
    else:
        cat_list = []

    if not cat_list:
        empty_state("No categories available yet. Run the pipeline first.")
    else:
        selected_cat = st.selectbox("Select a category to analyze", cat_list)

        df_ts    = safe_load(get_category_timeseries, selected_cat)
        df_score = safe_load(get_category_score_breakdown, selected_cat)

        if df_score.empty and df_ts.empty:
            empty_state(f"No data found for {selected_cat}.")
        else:
            if not df_score.empty:
                row = df_score.iloc[0]
                sc  = float(row.get("opportunity_score", 0))
                rec = str(row.get("recommendation", "watch"))
                ch  = str(row.get("channel_edge", "mixed"))

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Opportunity Score",  f"{sc:.0f}/100")
                c2.metric("Recommendation",     rec.title())
                c3.metric("Channel Edge",       ch.title())
                c4.metric("Avg Price",          f"${float(row['avg_price']):.2f}" if row.get("avg_price") else "—")
                c5.metric("Sell-through",       f"{float(row['sell_through_pct']):.1f}%" if row.get("sell_through_pct") else "—")

                st.markdown("<hr>", unsafe_allow_html=True)

            if not df_ts.empty:
                df_ts["snapshot_date"] = pd.to_datetime(df_ts["snapshot_date"])

                left, right = st.columns(2)

                with left:
                    st.markdown('<p class="section-header">Sell-through Rate Over Time</p>', unsafe_allow_html=True)
                    fig_st = px.area(
                        df_ts, x="snapshot_date", y="sell_through_pct",
                        color_discrete_sequence=[BLUE],
                    )
                    fig_st.update_traces(fill="tozeroy", fillcolor=f"rgba(67,97,238,0.1)")
                    fig_st.update_layout(height=260, yaxis_title="Sell-through %", xaxis_title="")
                    fig_st = plotly_defaults(fig_st)
                    st.plotly_chart(fig_st, use_container_width=True)

                with right:
                    st.markdown('<p class="section-header">Average Price Over Time</p>', unsafe_allow_html=True)
                    fig_price = px.line(
                        df_ts, x="snapshot_date", y="avg_price",
                        color_discrete_sequence=[PURPLE],
                    )
                    fig_price.update_layout(height=260, yaxis_title="Avg Price ($)", xaxis_title="")
                    fig_price = plotly_defaults(fig_price)
                    st.plotly_chart(fig_price, use_container_width=True)

                left2, right2 = st.columns(2)

                with left2:
                    st.markdown('<p class="section-header">Search Interest Over Time</p>', unsafe_allow_html=True)
                    fig_interest = px.line(
                        df_ts, x="snapshot_date", y="search_interest",
                        color_discrete_sequence=[TEAL],
                    )
                    fig_interest.update_layout(height=260, yaxis_title="Interest Score", xaxis_title="")
                    fig_interest = plotly_defaults(fig_interest)
                    st.plotly_chart(fig_interest, use_container_width=True)

                with right2:
                    st.markdown('<p class="section-header">Reddit Mentions Over Time</p>', unsafe_allow_html=True)
                    fig_buzz = px.bar(
                        df_ts, x="snapshot_date", y="reddit_mentions",
                        color_discrete_sequence=[AMBER],
                    )
                    fig_buzz.update_layout(height=260, yaxis_title="Mentions", xaxis_title="")
                    fig_buzz = plotly_defaults(fig_buzz)
                    st.plotly_chart(fig_buzz, use_container_width=True)

                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown('<p class="section-header">Listing & Sales Volume</p>', unsafe_allow_html=True)
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Bar(
                    x=df_ts["snapshot_date"], y=df_ts["listing_count"],
                    name="Listings", marker_color=GRAY, opacity=0.6,
                ))
                fig_vol.add_trace(go.Bar(
                    x=df_ts["snapshot_date"], y=df_ts["sold_count"],
                    name="Sold", marker_color=GREEN,
                ))
                fig_vol.update_layout(barmode="overlay", height=260, xaxis_title="", yaxis_title="Count")
                fig_vol = plotly_defaults(fig_vol)
                st.plotly_chart(fig_vol, use_container_width=True)


# ══════════════════════════════════════════════
# TAB 5 — Trends Explorer
# ══════════════════════════════════════════════

with tab5:
    all_cats2 = safe_load(get_all_category_names)
    if isinstance(all_cats2, pd.DataFrame):
        cat_list2 = all_cats2["category_name"].tolist() if not all_cats2.empty and "category_name" in all_cats2.columns else []
    elif isinstance(all_cats2, list):
        cat_list2 = all_cats2
    else:
        cat_list2 = []

    if not cat_list2:
        empty_state("No categories available yet. Run the pipeline first.")
    else:
        f1, f2, f3 = st.columns([2, 1, 1])

        with f1:
            selected_cats = st.multiselect(
                "Compare categories",
                cat_list2,
                default=cat_list2[:3] if len(cat_list2) >= 3 else cat_list2,
            )
        with f2:
            signal = st.selectbox("Signal", [
                "sell_through_pct",
                "avg_price",
                "listing_count",
                "search_interest",
                "reddit_mentions",
            ], format_func=lambda x: {
                "sell_through_pct": "Sell-through %",
                "avg_price":        "Avg Price ($)",
                "listing_count":    "Listing Count",
                "search_interest":  "Search Interest",
                "reddit_mentions":  "Reddit Mentions",
            }[x])
        with f3:
            days = st.selectbox("Time window", [30, 60, 90, 180], index=2, format_func=lambda x: f"Last {x} days")

        if not selected_cats:
            empty_state("Select at least one category to compare.")
        else:
            df_explorer = safe_load(get_trends_explorer, selected_cats, signal, days)

            if df_explorer.empty:
                empty_state("No trend data available for the selected filters.")
            else:
                df_explorer["snapshot_date"] = pd.to_datetime(df_explorer["snapshot_date"])

                signal_label = {
                    "sell_through_pct": "Sell-through %",
                    "avg_price":        "Avg Price ($)",
                    "listing_count":    "Listing Count",
                    "search_interest":  "Search Interest (0-100)",
                    "reddit_mentions":  "Reddit Mentions",
                }.get(signal, signal)

                color_seq = list(CATEGORY_COLORS.values())

                fig_explorer = px.line(
                    df_explorer,
                    x="snapshot_date",
                    y="value",
                    color="category_name",
                    color_discrete_sequence=color_seq,
                    labels={"value": signal_label, "snapshot_date": "", "category_name": "Category"},
                )
                fig_explorer.update_traces(line=dict(width=2.5))
                fig_explorer.update_layout(height=420)
                fig_explorer = plotly_defaults(fig_explorer)
                st.plotly_chart(fig_explorer, use_container_width=True)

                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown('<p class="section-header">Latest Values</p>', unsafe_allow_html=True)

                latest = df_explorer.sort_values("snapshot_date").groupby("category_name").last().reset_index()
                latest = latest.sort_values("value", ascending=False)

                cols = st.columns(min(len(latest), 5))
                for i, (_, row) in enumerate(latest.iterrows()):
                    if i < len(cols):
                        cols[i].metric(row["category_name"], f"{row['value']:.1f}")
