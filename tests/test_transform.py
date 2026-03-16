import pytest
import pandas as pd
from datetime import date
from etl.transform import (
    transform_category_trends,
    transform_search_signals,
    transform_social_buzz,
    transform_consumer_spend,
    build_retail_vs_ecomm,
)


# ─── Fixtures ────────────────────────────────

@pytest.fixture
def sample_trends():
    return pd.DataFrame([{
        "category_name": "electronics",
        "category_id":   "58058",
        "source":        "ebay",
        "snapshot_date": "2024-06-01",
        "listing_count": 1500,
        "avg_price":     "129.99",
        "sold_count":    300,
        "sell_through":  "0.20",
        "rank_position": None,
    }])

@pytest.fixture
def sample_signals():
    return pd.DataFrame([{
        "keyword":        "Consumer Electronics",
        "category_name":  "Electronics",
        "snapshot_date":  "2024-06-01",
        "interest_score": "85",
        "geo":            "us",
    }])

@pytest.fixture
def sample_buzz():
    return pd.DataFrame([{
        "category_name":  "Electronics",
        "keyword":        "Electronics",
        "subreddit":      "flipping",
        "snapshot_date":  "2024-06-01",
        "mention_count":  "42",
        "avg_score":      "18.5",
    }])

@pytest.fixture
def sample_spend():
    return pd.DataFrame([{
        "category_name":    "Electronics",
        "bls_category":     "Consumer electronics",
        "year":             "2023",
        "avg_annual_spend": "1200.00",
        "yoy_change":       "0.04",
    }])


# ─── Transform: category_trends ──────────────

def test_transform_category_trends_normalizes_name(sample_trends):
    result = transform_category_trends(sample_trends)
    assert result["category_name"].iloc[0] == "Electronics"

def test_transform_category_trends_converts_price(sample_trends):
    result = transform_category_trends(sample_trends)
    assert result["avg_price"].dtype == float
    assert result["avg_price"].iloc[0] == 129.99

def test_transform_category_trends_clips_sell_through(sample_trends):
    sample_trends["sell_through"] = "1.5"
    result = transform_category_trends(sample_trends)
    assert result["sell_through"].iloc[0] <= 1.0

def test_transform_category_trends_deduplicates(sample_trends):
    doubled = pd.concat([sample_trends, sample_trends], ignore_index=True)
    result = transform_category_trends(doubled)
    assert len(result) == 1

def test_transform_category_trends_empty():
    result = transform_category_trends(pd.DataFrame())
    assert result.empty


# ─── Transform: search_signals ───────────────

def test_transform_search_signals_lowercases_keyword(sample_signals):
    result = transform_search_signals(sample_signals)
    assert result["keyword"].iloc[0] == "consumer electronics"

def test_transform_search_signals_uppercases_geo(sample_signals):
    result = transform_search_signals(sample_signals)
    assert result["geo"].iloc[0] == "US"

def test_transform_search_signals_clips_score(sample_signals):
    sample_signals["interest_score"] = "150"
    result = transform_search_signals(sample_signals)
    assert result["interest_score"].iloc[0] <= 100


# ─── Transform: social_buzz ──────────────────

def test_transform_social_buzz_casts_mention_count(sample_buzz):
    result = transform_social_buzz(sample_buzz)
    assert result["mention_count"].dtype == int
    assert result["mention_count"].iloc[0] == 42

def test_transform_social_buzz_handles_zero_score(sample_buzz):
    sample_buzz["avg_score"] = None
    result = transform_social_buzz(sample_buzz)
    assert result["avg_score"].iloc[0] == 0.0


# ─── Transform: consumer_spend ───────────────

def test_transform_consumer_spend_filters_old_years(sample_spend):
    sample_spend["year"] = "1995"
    result = transform_consumer_spend(sample_spend)
    assert result.empty

def test_transform_consumer_spend_valid_year(sample_spend):
    result = transform_consumer_spend(sample_spend)
    assert len(result) == 1
    assert result["year"].iloc[0] == 2023


# ─── build_retail_vs_ecomm ───────────────────

def test_build_retail_vs_ecomm_shares_sum_to_one(sample_spend):
    spend_clean = transform_consumer_spend(sample_spend)
    result = build_retail_vs_ecomm(spend_clean, pd.DataFrame())
    for _, row in result.iterrows():
        total = round(row["ecomm_share"] + row["retail_share"], 4)
        assert abs(total - 1.0) < 0.01

def test_build_retail_vs_ecomm_returns_empty_on_no_spend():
    result = build_retail_vs_ecomm(pd.DataFrame(), pd.DataFrame())
    assert result.empty
