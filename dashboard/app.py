import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db.queries import (
    get_leaderboard,
    get_score_history,
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
# Palette — a single source of truth for both the injected CSS below
# and every Plotly chart. Muted / desaturated on purpose: this mirrors
# the same "ink" (neutral) + "accent" (teal) system used across the
# rest of the portfolio, rather than default bright dashboard colors.
# ─────────────────────────────────────────────

INK_950 = "#0D1116"
INK_900 = "#171D24"
INK_700 = "#3A4450"
INK_600 = "#495563"
INK_500 = "#5D6B7A"
INK_400 = "#7C8896"
INK_300 = "#A8B0BA"
INK_200 = "#CDD2D8"
INK_100 = "#E7E9EC"
INK_50  = "#F5F6F7"

ACCENT_700 = "#2A5355"
ACCENT_600 = "#326668"
ACCENT_500 = "#3F7E80"
ACCENT_400 = "#5C999B"
ACCENT_300 = "#82B3B4"
ACCENT_100 = "#D6E7E7"
ACCENT_50  = "#EEF5F5"

POSITIVE      = ACCENT_600   # enter / rising / e-commerce winning
POSITIVE_SOFT = ACCENT_50
WARNING       = "#9C6B2E"    # watch / stable — muted ochre, not candy amber
WARNING_SOFT  = "#F5EDE1"
NEGATIVE      = "#9C4A42"    # avoid / declining — muted brick, not candy red
NEGATIVE_SOFT = "#F5E9E7"

# A 10-color categorical palette for the product categories — desaturated
# and harmonious so a 10-line comparison chart stays legible rather than
# turning into a rainbow.
CATEGORY_COLORS = {
    "Electronics":             "#3F7E80",  # teal (matches accent)
    "Home & Garden":           "#6B8E4E",  # muted olive
    "Clothing & Accessories":  "#8B6BAE",  # muted plum
    "Sporting Goods":          "#4A7BA6",  # muted steel blue
    "Toys & Hobbies":          "#C08A3E",  # muted ochre
    "Health & Beauty":         "#B5657A",  # muted rose
    "Pet Supplies":            "#C17A4A",  # muted terracotta
    "Automotive Parts":        "#7C8896",  # neutral gray-blue
    "Musical Instruments":     "#4E9494",  # muted cyan
    "Collectibles":            "#8C6E4A",  # muted brown
}

# ─────────────────────────────────────────────
# Page config & global styling
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="E-Commerce Trends Analytics",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {{
        font-family: 'IBM Plex Sans', ui-sans-serif, system-ui, sans-serif;
    }}

    #MainMenu, footer, header {{ visibility: hidden; }}

    .block-container {{ padding-top: 2rem; padding-bottom: 2rem; }}

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        border-bottom: 1px solid {INK_200};
        padding-bottom: 0;
    }}
    .stTabs [data-baseweb="tab"] {{
        height: 42px;
        padding: 0 18px;
        border-radius: 6px 6px 0 0;
        font-size: 13.5px;
        font-weight: 500;
        color: {INK_500};
        background: transparent;
        border: none;
        transition: color 0.15s ease, background-color 0.15s ease;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: {INK_700};
        background: {INK_50};
    }}
    .stTabs [aria-selected="true"] {{
        background: {ACCENT_50} !important;
        color: {ACCENT_700} !important;
        border-bottom: 2px solid {ACCENT_500};
    }}

    /* Metric cards — flat, bordered, with a subtle hover lift instead
       of a heavy shadow-everywhere look */
    [data-testid="metric-container"] {{
        background: #ffffff;
        border: 1px solid {INK_200};
        border-radius: 8px;
        padding: 14px 18px;
        transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
    }}
    [data-testid="metric-container"]:hover {{
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(13, 17, 22, 0.08);
        border-color: {ACCENT_300};
    }}
    [data-testid="metric-container"] label {{
        font-size: 11.5px;
        font-weight: 600;
        color: {INK_500};
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }}
    [data-testid="metric-container"] [data-testid="stMetricValue"] {{
        font-family: 'IBM Plex Mono', ui-monospace, monospace;
        font-size: 26px;
        font-weight: 600;
        color: {INK_900};
    }}

    .section-header {{
        font-size: 12px;
        font-weight: 600;
        color: {INK_500};
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin: 22px 0 12px 0;
    }}

    /* Recommendation / momentum badges — small bordered chips, not
       heavy saturated pills */
    .badge {{
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 2px 9px;
        border-radius: 5px;
        font-size: 11.5px;
        font-weight: 500;
        border: 1px solid transparent;
    }}
    .badge-enter,   .badge-rising   {{ background: {POSITIVE_SOFT}; color: {POSITIVE}; border-color: {ACCENT_100}; }}
    .badge-watch,   .badge-stable   {{ background: {WARNING_SOFT};  color: {WARNING};  border-color: #E8D9BE; }}
    .badge-avoid,   .badge-declining{{ background: {NEGATIVE_SOFT}; color: {NEGATIVE}; border-color: #E8CFC9; }}

    hr {{ border: none; border-top: 1px solid {INK_100}; margin: 22px 0; }}

    [data-testid="stDataFrame"] {{ border-radius: 8px; overflow: hidden; }}
    [data-testid="stSidebar"] {{ background: {INK_50}; }}

    /* Score bar — subtle rounded track with a soft inner shadow for depth */
    .score-bar-wrap {{
        background: {INK_100};
        border-radius: 6px;
        height: 7px;
        width: 100%;
        box-shadow: inset 0 1px 2px rgba(13, 17, 22, 0.06);
        overflow: hidden;
    }}
    .score-bar-fill {{
        border-radius: 6px;
        height: 7px;
        transition: width 0.3s ease;
    }}

    /* Expander rows — subtle hover to signal interactivity, no scale/bounce */
    [data-testid="stExpander"] {{
        border: 1px solid {INK_200} !important;
        border-radius: 8px !important;
        transition: border-color 0.15s ease;
    }}
    [data-testid="stExpander"]:hover {{
        border-color: {ACCENT_300} !important;
    }}

    /* Leaderboard row divider list */
    .lb-row {{
        border-bottom: 1px solid {INK_100};
        padding: 14px 0;
        transition: background-color 0.15s ease;
    }}
    .lb-row:hover {{ background-color: {INK_50}; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def score_color(score: float) -> str:
    if score >= 65: return POSITIVE
    if score >= 40: return WARNING
    return NEGATIVE

def momentum_badge(momentum: str) -> str:
    m = str(momentum).lower()
    if m == "rising":    return '<span class="badge badge-rising">&#9650; Rising</span>'
    if m == "declining": return '<span class="badge badge-declining">&#9660; Declining</span>'
    return '<span class="badge badge-stable">Stable</span>'

def rec_badge(rec: str) -> str:
    r = str(rec).lower()
    if r == "enter": return '<span class="badge badge-enter">Enter</span>'
    if r == "watch": return '<span class="badge badge-watch">Watch</span>'
    return '<span class="badge badge-avoid">Avoid</span>'

def score_bar(score: float, color: str) -> str:
    return f"""
    <div class="score-bar-wrap">
      <div class="score-bar-fill" style="width:{score}%;background:{color};"></div>
    </div>"""

def empty_state(message: str):
    st.markdown(f"""
    <div style="text-align:center;padding:56px 0;color:{INK_400};">
        <div style="font-size:13px;letter-spacing:0.04em;">{message}</div>
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
        font=dict(family="IBM Plex Sans", size=13, color=INK_700),
        margin=dict(l=0, r=0, t=32, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right",  x=1,
            font=dict(size=12),
        ),
        hoverlabel=dict(
            bgcolor=INK_900,
            bordercolor=INK_700,
            font=dict(family="IBM Plex Sans", size=12, color="#ffffff"),
        ),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor=INK_100, zeroline=False)
    return fig

def sparkline(values: list[float], color: str) -> go.Figure:
    """A minimal, axis-free trend line used inline on the leaderboard —
    real per-category opportunity_score history, not a decorative fake."""
    fig = go.Figure(go.Scatter(
        y=values,
        mode="lines",
        line=dict(color=color, width=2),
        fill="tozeroy",
        fillcolor=color.replace(")", ", 0.12)").replace("rgb", "rgba") if color.startswith("rgb") else None,
        hoverinfo="skip",
    ))
    fig.update_layout(
        height=40,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig

# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────

col_title, col_meta = st.columns([3, 1])
with col_title:
    st.markdown("## E-Commerce Trends Analytics")
    st.markdown(f"<p style='color:{INK_500};margin-top:-12px;font-size:14px;'>Tracking where retail is declining and where e-commerce is winning — by category</p>", unsafe_allow_html=True)
with col_meta:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.markdown("<hr>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Rising vs Declining",
    "Retail vs E-commerce",
    "Niche Finder",
    "Product Deep Dive",
    "Trends Explorer",
])


# ══════════════════════════════════════════════
# TAB 1 — Rising vs Declining
# ══════════════════════════════════════════════

with tab1:
    df_lead    = safe_load(get_leaderboard)
    df_rd      = safe_load(get_rising_declining)
    df_history = safe_load(get_score_history)

    if df_lead.empty:
        empty_state("No leaderboard data yet. Run the pipeline first.")
    else:
        # Summary metrics
        rising   = len(df_rd[df_rd["momentum"] == "Rising"])   if not df_rd.empty else 0
        declining= len(df_rd[df_rd["momentum"] == "Declining"]) if not df_rd.empty else 0
        top_cat  = df_lead.iloc[0]["category_name"] if not df_lead.empty else "—"

        # ---- Unique effect #1: an overall market-health gauge ----------
        # A single indicator summarizing the average opportunity score
        # across every tracked category, with a delta vs the prior
        # snapshot average — the kind of hero KPI a real BI tool leads
        # with, rather than another plain metric tile.
        gauge_col, metrics_col = st.columns([1, 2])

        with gauge_col:
            avg_score = float(df_lead["opportunity_score"].mean())
            avg_delta = float(df_lead["score_delta"].mean()) if "score_delta" in df_lead.columns else 0.0

            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=avg_score,
                number={"suffix": "", "font": {"family": "IBM Plex Mono", "size": 34, "color": INK_900}},
                delta={"reference": avg_score - avg_delta, "increasing": {"color": POSITIVE}, "decreasing": {"color": NEGATIVE}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": INK_300, "tickfont": {"size": 10, "color": INK_400}},
                    "bar": {"color": score_color(avg_score), "thickness": 0.28},
                    "bgcolor": "rgba(0,0,0,0)",
                    "borderwidth": 0,
                    "steps": [
                        {"range": [0, 40],  "color": NEGATIVE_SOFT},
                        {"range": [40, 65], "color": WARNING_SOFT},
                        {"range": [65, 100],"color": POSITIVE_SOFT},
                    ],
                },
                title={"text": "Overall Market Opportunity", "font": {"family": "IBM Plex Sans", "size": 13, "color": INK_500}},
            ))
            fig_gauge.update_layout(
                height=220,
                margin=dict(l=20, r=20, t=50, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Sans", color=INK_700),
            )
            st.plotly_chart(fig_gauge, use_container_width=True)

        with metrics_col:
            m1, m2, m3 = st.columns(3)
            m1.metric("Categories Tracked", len(df_lead))
            m2.metric("Rising",   rising)
            m3.metric("Declining",declining)
            st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
            m4, m5 = st.columns(2)
            m4.metric("Top Category", top_cat)
            m5.metric("Top Score", f"{float(df_lead.iloc[0]['opportunity_score']):.0f}/100")

        st.markdown("<hr>", unsafe_allow_html=True)
        left, right = st.columns([1.6, 1])

        with left:
            st.markdown('<p class="section-header">Opportunity Score Leaderboard</p>', unsafe_allow_html=True)

            for _, row in df_lead.iterrows():
                sc    = float(row.get("opportunity_score", 0))
                color = score_color(sc)
                delta = float(row.get("score_delta", 0))
                delta_str = f"<span style='color:{POSITIVE if delta>=0 else NEGATIVE};font-size:12px;'>{'&#9650;' if delta>=0 else '&#9660;'} {abs(delta):.1f}</span>"
                mom   = str(row.get("momentum", "Stable"))
                rec   = str(row.get("recommendation", "watch"))
                cat_name = row["category_name"]

                st.markdown("<div class='lb-row'>", unsafe_allow_html=True)
                r1, r2, r3 = st.columns([2.6, 1, 0.9])
                with r1:
                    st.markdown(
                        f"<div style='display:flex;align-items:center;gap:9px;margin-bottom:5px;'>"
                        f"<span style='font-weight:600;font-size:14.5px;color:{INK_900};'>{cat_name}</span>"
                        f"{momentum_badge(mom)}"
                        f"{rec_badge(rec)}"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                    st.markdown(score_bar(sc, color), unsafe_allow_html=True)
                with r2:
                    # ---- Unique effect #2: a real per-category sparkline ----
                    hist = df_history[df_history["category_name"] == cat_name]["opportunity_score"].tolist() if not df_history.empty else []
                    if len(hist) >= 2:
                        fig_spark = sparkline(hist, color)
                        st.plotly_chart(fig_spark, use_container_width=True, config={"displayModeBar": False})
                    else:
                        st.markdown("<div style='height:40px;'></div>", unsafe_allow_html=True)
                with r3:
                    st.markdown(
                        f"<div style='text-align:right;'>"
                        f"<span style='font-family:\"IBM Plex Mono\";font-size:20px;font-weight:600;color:{color};'>{sc:.0f}</span>"
                        f"<span style='font-size:11px;color:{INK_400};'> /100</span><br>"
                        f"{delta_str}"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                st.markdown("</div>", unsafe_allow_html=True)

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
                    fillcolor="rgba(63,126,128,0.15)",
                    line=dict(color=ACCENT_500, width=2),
                ))
                fig_radar.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, 100], gridcolor=INK_100),
                        angularaxis=dict(gridcolor=INK_100),
                        bgcolor="rgba(0,0,0,0)",
                    ),
                    paper_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    margin=dict(l=20, r=20, t=20, b=20),
                    height=280,
                    font=dict(family="IBM Plex Sans", color=INK_700),
                )
                st.plotly_chart(fig_radar, use_container_width=True)

            st.markdown('<p class="section-header">Momentum Summary</p>', unsafe_allow_html=True)
            if not df_rd.empty:
                momentum_counts = df_rd["momentum"].value_counts().reset_index()
                momentum_counts.columns = ["momentum", "count"]
                color_map = {"Rising": POSITIVE, "Declining": NEGATIVE, "Stable": WARNING}
                fig_mom = px.bar(
                    momentum_counts, x="momentum", y="count",
                    color="momentum",
                    color_discrete_map=color_map,
                    height=200,
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
                marker_color=ACCENT_500,
            ))
            fig_share.add_trace(go.Bar(
                y=df_sorted["category_name"],
                x=df_sorted["avg_retail_share"],
                name="Retail",
                orientation="h",
                marker_color=INK_300,
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
            colors = [POSITIVE if v > 0 else NEGATIVE for v in df_growth["avg_ecomm_growth"]]
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

            growth_color = POSITIVE if growth > 0 else NEGATIVE
            st.markdown(
                f"<div style='display:flex;align-items:center;gap:16px;padding:10px 0;border-bottom:1px solid {INK_100};'>"
                f"<div style='width:180px;font-weight:600;font-size:14px;color:{INK_900};'>{row['category_name']}</div>"
                f"<div style='flex:1;'>{score_bar(ecomm, ACCENT_500)}"
                f"<div style='display:flex;justify-content:space-between;font-size:11px;color:{INK_400};margin-top:3px;'>"
                f"<span>E-comm {ecomm:.0f}%</span><span>Retail {retail:.0f}%</span></div></div>"
                f"<div style='width:80px;text-align:right;font-size:13px;font-weight:600;color:{growth_color};'>"
                f"{'&#9650;' if growth>0 else '&#9660;'} {abs(growth):.1f}%</div>"
                f"<div style='width:160px;font-size:12px;color:{INK_500};'>{opp}</div>"
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
                    marker_color=[ACCENT_500, "#8B6BAE", POSITIVE, WARNING, "#4A7BA6"],
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
                    f"<span style='font-size:12px;color:{INK_500};'>Channel: <b>{ch}</b></span>"
                    f"<span style='font-size:12px;color:{INK_500};'>Search interest: <b>{interest:.0f}/100</b></span>"
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
                cat_color = CATEGORY_COLORS.get(selected_cat, ACCENT_500)

                left, right = st.columns(2)

                with left:
                    st.markdown('<p class="section-header">Sell-through Rate Over Time</p>', unsafe_allow_html=True)
                    fig_st = px.area(
                        df_ts, x="snapshot_date", y="sell_through_pct",
                        color_discrete_sequence=[cat_color],
                    )
                    fig_st.update_traces(fill="tozeroy", fillcolor="rgba(63,126,128,0.10)")
                    fig_st.update_layout(height=260, yaxis_title="Sell-through %", xaxis_title="")
                    fig_st = plotly_defaults(fig_st)
                    st.plotly_chart(fig_st, use_container_width=True)

                with right:
                    st.markdown('<p class="section-header">Average Price Over Time</p>', unsafe_allow_html=True)
                    fig_price = px.line(
                        df_ts, x="snapshot_date", y="avg_price",
                        color_discrete_sequence=["#8B6BAE"],
                    )
                    fig_price.update_layout(height=260, yaxis_title="Avg Price ($)", xaxis_title="")
                    fig_price = plotly_defaults(fig_price)
                    st.plotly_chart(fig_price, use_container_width=True)

                left2, right2 = st.columns(2)

                with left2:
                    st.markdown('<p class="section-header">Search Interest Over Time</p>', unsafe_allow_html=True)
                    fig_interest = px.line(
                        df_ts, x="snapshot_date", y="search_interest",
                        color_discrete_sequence=["#4E9494"],
                    )
                    fig_interest.update_layout(height=260, yaxis_title="Interest Score", xaxis_title="")
                    fig_interest = plotly_defaults(fig_interest)
                    st.plotly_chart(fig_interest, use_container_width=True)

                with right2:
                    st.markdown('<p class="section-header">Reddit Mentions Over Time</p>', unsafe_allow_html=True)
                    fig_buzz = px.bar(
                        df_ts, x="snapshot_date", y="reddit_mentions",
                        color_discrete_sequence=[WARNING],
                    )
                    fig_buzz.update_layout(height=260, yaxis_title="Mentions", xaxis_title="")
                    fig_buzz = plotly_defaults(fig_buzz)
                    st.plotly_chart(fig_buzz, use_container_width=True)

                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown('<p class="section-header">Listing & Sales Volume</p>', unsafe_allow_html=True)
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Bar(
                    x=df_ts["snapshot_date"], y=df_ts["listing_count"],
                    name="Listings", marker_color=INK_300, opacity=0.7,
                ))
                fig_vol.add_trace(go.Bar(
                    x=df_ts["snapshot_date"], y=df_ts["sold_count"],
                    name="Sold", marker_color=POSITIVE,
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

                color_seq = [CATEGORY_COLORS.get(c, ACCENT_500) for c in selected_cats]

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
