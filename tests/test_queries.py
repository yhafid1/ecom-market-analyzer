import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


def test_niche_finder_builds_min_score_filter():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_niche_finder
        get_niche_finder(min_score=60)
        call_args = mock_query.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert ":min_score" in sql
        assert params["min_score"] == 60


def test_niche_finder_adds_recommendation_filter():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_niche_finder
        get_niche_finder(min_score=0, recommendation="enter")
        sql = mock_query.call_args[0][0]
        assert "recommendation = :recommendation" in sql


def test_niche_finder_adds_channel_filter():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_niche_finder
        get_niche_finder(min_score=0, channel_edge="ecomm")
        sql = mock_query.call_args[0][0]
        assert "channel_edge = :channel_edge" in sql


def test_niche_finder_adds_price_filter():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_niche_finder
        get_niche_finder(min_score=0, max_price=100)
        sql = mock_query.call_args[0][0]
        assert "avg_price <= :max_price" in sql


def test_niche_finder_no_extra_filters_by_default():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_niche_finder
        get_niche_finder()
        sql = mock_query.call_args[0][0]
        assert "recommendation = :recommendation" not in sql
        assert "channel_edge = :channel_edge" not in sql
        assert "avg_price <= :max_price" not in sql


def test_trends_explorer_rejects_invalid_signal():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_trends_explorer
        get_trends_explorer(["Electronics"], signal="malicious_column")
        sql = mock_query.call_args[0][0]
        assert "sell_through_pct AS value" in sql


def test_trends_explorer_accepts_valid_signal():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_trends_explorer
        get_trends_explorer(["Electronics"], signal="search_interest")
        sql = mock_query.call_args[0][0]
        assert "search_interest AS value" in sql


def test_trends_explorer_builds_placeholders():
    with patch("db.queries._query", return_value=pd.DataFrame()) as mock_query:
        from db.queries import get_trends_explorer
        get_trends_explorer(["Electronics", "Pet Supplies", "Toys & Hobbies"])
        sql = mock_query.call_args[0][0]
        assert ":cat0" in sql
        assert ":cat1" in sql
        assert ":cat2" in sql
