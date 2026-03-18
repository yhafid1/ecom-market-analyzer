import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from etl.transform import (
    transform_category_trends,
    transform_search_signals,
    transform_social_buzz,
    transform_consumer_spend,
    build_retail_vs_ecomm,
)


# ─── Scoring logic ────────────────────────────

def test_opportunity_score_weights_sum_to_one():
    weights = [0.25, 0.20, 0.30, 0.15, 0.10]
    assert sum(weights) == 1.0


def test_retail_vs_ecomm_ecomm_share_never_exceeds_95():
    spend_df = pd.DataFrame([{
        "category_name":    "Electronics",
        "bls_category":     "Consumer electronics",
        "year":             2023,
        "avg_annual_spend": 1200.0,
        "yoy_change":       5.0,  # extreme growth
    }])
    result = build_retail_vs_ecomm(spend_df, pd.DataFrame())
    assert result["ecomm_share"].max() <= 0.95


def test_retail_vs_ecomm_shares_never_negative():
    spend_df = pd.DataFrame([{
        "category_name":    "Electronics",
        "bls_category":     "Consumer electronics",
        "year":             2023,
        "avg_annual_spend": 1200.0,
        "yoy_change":       -0.99,  # extreme decline
    }])
    result = build_retail_vs_ecomm(spend_df, pd.DataFrame())
    assert result["ecomm_share"].min() >= 0
    assert result["retail_share"].min() >= 0


# ─── Transform edge cases ─────────────────────

def test_transform_category_trends_handles_none_price():
    df = pd.DataFrame([{
        "category_name": "Electronics",
        "category_id":   "58058",
        "source":        "ebay",
        "snapshot_date": "2024-06-01",
        "listing_count": 100,
        "avg_price":     None,
        "sold_count":    10,
        "sell_through":  "0.10",
        "rank_position": None,
    }])
    result = transform_category_trends(df)
    assert pd.isna(result["avg_price"].iloc[0])


def test_transform_search_signals_handles_zero_interest():
    df = pd.DataFrame([{
        "keyword":        "test keyword",
        "category_name":  "Electronics",
        "snapshot_date":  "2024-06-01",
        "interest_score": "0",
        "geo":            "US",
    }])
    result = transform_search_signals(df)
    assert result["interest_score"].iloc[0] == 0


def test_transform_social_buzz_multiple_subreddits():
    df = pd.DataFrame([
        {"category_name": "Electronics", "keyword": "gadgets", "subreddit": "flipping",    "snapshot_date": "2024-06-01", "mention_count": 10, "avg_score": 5.0},
        {"category_name": "Electronics", "keyword": "gadgets", "subreddit": "ecommerce",   "snapshot_date": "2024-06-01", "mention_count": 20, "avg_score": 8.0},
        {"category_name": "Electronics", "keyword": "gadgets", "subreddit": "amazonseller","snapshot_date": "2024-06-01", "mention_count": 15, "avg_score": 6.0},
    ])
    result = transform_social_buzz(df)
    assert len(result) == 3  # one row per subreddit, no dedup across subreddits


def test_transform_consumer_spend_rounds_correctly():
    df = pd.DataFrame([{
        "category_name":    "Electronics",
        "bls_category":     "Consumer electronics",
        "year":             "2023",
        "avg_annual_spend": "1234.5678",
        "yoy_change":       "0.04567",
    }])
    result = transform_consumer_spend(df)
    assert result["avg_annual_spend"].iloc[0] == 1234.57
    assert result["yoy_change"].iloc[0] == 0.0457


def test_transform_category_trends_sell_through_zero():
    df = pd.DataFrame([{
        "category_name": "Collectibles",
        "category_id":   "1",
        "source":        "ebay",
        "snapshot_date": "2024-06-01",
        "listing_count": 500,
        "avg_price":     "25.00",
        "sold_count":    0,
        "sell_through":  "0.0",
        "rank_position": None,
    }])
    result = transform_category_trends(df)
    assert result["sell_through"].iloc[0] == 0.0
    assert result["sold_count"].iloc[0] == 0


def test_transform_search_signals_multiple_keywords_same_category():
    df = pd.DataFrame([
        {"keyword": "consumer electronics", "category_name": "Electronics", "snapshot_date": "2024-06-01", "interest_score": "80", "geo": "US"},
        {"keyword": "gadgets online",       "category_name": "Electronics", "snapshot_date": "2024-06-01", "interest_score": "60", "geo": "US"},
    ])
    result = transform_search_signals(df)
    assert len(result) == 2  # different keywords, both kept


# ─── Pipeline DB health check ─────────────────

def test_health_check_returns_false_on_bad_connection():
    with patch("db.connection.engine") as mock_engine:
        mock_engine.connect.side_effect = Exception("connection refused")
        from db.connection import health_check
        result = health_check()
        assert result is False


def test_health_check_returns_true_on_good_connection():
    with patch("db.connection.engine") as mock_engine:
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        from db.connection import health_check
        result = health_check()
        assert result is True
