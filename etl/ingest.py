import os
import time
import requests
import praw
import pandas as pd
from datetime import date, timedelta
from pytrends.request import TrendReq
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# eBay Browse API
# ─────────────────────────────────────────────

EBAY_CATEGORIES = {
    "Electronics":          "58058",
    "Home & Garden":        "11700",
    "Clothing & Accessories": "11450",
    "Sporting Goods":       "888",
    "Toys & Hobbies":       "220",
    "Health & Beauty":      "26395",
    "Pet Supplies":         "1281",
    "Automotive Parts":     "6000",
    "Musical Instruments":  "619",
    "Collectibles":         "1",
}

def _get_ebay_token() -> str:
    import base64
    credentials = base64.b64encode(
        f"{os.getenv('EBAY_APP_ID')}:{os.getenv('EBAY_CLIENT_SECRET')}".encode()
    ).decode()
    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data="grant_type=client_credentials&scope=https://api.ebay.com/oauth/api_scope",
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_ebay_category_data() -> pd.DataFrame:
    token = _get_ebay_token()
    headers = {"Authorization": f"Bearer {token}", "X-EBAY-C-MARKETPLACE-ID": "EBAY_US"}
    rows = []
    today = date.today()

    for category_name, category_id in EBAY_CATEGORIES.items():
        try:
            resp = requests.get(
                "https://api.ebay.com/buy/browse/v1/item_summary/search",
                headers=headers,
                params={
                    "category_ids": category_id,
                    "limit": 200,
                    "filter": "buyingOptions:{FIXED_PRICE}",
                    "sort": "bestMatch",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("itemSummaries", [])

            prices = [
                float(i["price"]["value"])
                for i in items
                if "price" in i
            ]
            sold_count = sum(
                1 for i in items
                if i.get("buyingOptions") and "FIXED_PRICE" in i["buyingOptions"]
            )

            rows.append({
                "category_name":  category_name,
                "category_id":    category_id,
                "source":         "ebay",
                "snapshot_date":  today,
                "listing_count":  data.get("total", len(items)),
                "avg_price":      round(sum(prices) / len(prices), 2) if prices else None,
                "sold_count":     sold_count,
                "sell_through":   round(sold_count / len(items), 4) if items else None,
                "rank_position":  None,
            })
            time.sleep(0.5)  # be polite with rate limits

        except Exception as e:
            print(f"eBay fetch failed for {category_name}: {e}")

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Rainforest API — Amazon Best Sellers
# ─────────────────────────────────────────────

AMAZON_CATEGORIES = [
    "bestsellers_electronics",
    "bestsellers_home",
    "bestsellers_clothing",
    "bestsellers_sports",
    "bestsellers_toys",
    "bestsellers_health",
    "bestsellers_pet_supplies",
    "bestsellers_automotive",
    "bestsellers_musical_instruments",
]

def fetch_amazon_bsr_data() -> pd.DataFrame:
    api_key = os.getenv("RAINFOREST_API_KEY")
    if not api_key:
        print("Rainforest API key not set — skipping Amazon BSR fetch")
        return pd.DataFrame()

    rows = []
    today = date.today()

    for list_type in AMAZON_CATEGORIES:
        try:
            resp = requests.get(
                "https://api.rainforestapi.com/request",
                params={
                    "api_key":   api_key,
                    "type":      "bestsellers",
                    "list_type": list_type,
                    "amazon_domain": "amazon.com",
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            bestsellers = data.get("bestsellers", [])

            category_name = list_type.replace("bestsellers_", "").replace("_", " ").title()
            for rank, item in enumerate(bestsellers[:50], start=1):
                rows.append({
                    "category_name":  category_name,
                    "category_id":    list_type,
                    "source":         "amazon_bsr",
                    "snapshot_date":  today,
                    "listing_count":  None,
                    "avg_price":      item.get("price", {}).get("value"),
                    "sold_count":     None,
                    "sell_through":   None,
                    "rank_position":  rank,
                })
            time.sleep(1.0)

        except Exception as e:
            print(f"Rainforest fetch failed for {list_type}: {e}")

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Google Trends — pytrends
# ─────────────────────────────────────────────

TREND_KEYWORDS = {
    "Electronics":            ["consumer electronics", "gadgets online"],
    "Home & Garden":          ["home decor", "garden supplies"],
    "Clothing & Accessories": ["online clothing", "fashion accessories"],
    "Sporting Goods":         ["sports equipment", "fitness gear"],
    "Toys & Hobbies":         ["toys online", "hobby supplies"],
    "Health & Beauty":        ["health products", "beauty supplies"],
    "Pet Supplies":           ["pet products", "pet accessories"],
    "Automotive Parts":       ["auto parts online", "car accessories"],
    "Musical Instruments":    ["musical instruments", "guitar accessories"],
    "Collectibles":           ["collectibles buy", "rare items online"],
}

def fetch_google_trends_data() -> pd.DataFrame:
    pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
    rows = []
    today = date.today()

    for category_name, keywords in TREND_KEYWORDS.items():
        for keyword in keywords:
            try:
                pytrends.build_payload([keyword], cat=0, timeframe="today 3-m", geo="US")
                interest_df = pytrends.interest_over_time()

                if not interest_df.empty and keyword in interest_df.columns:
                    latest_score = int(interest_df[keyword].iloc[-1])
                    rows.append({
                        "keyword":        keyword,
                        "category_name":  category_name,
                        "snapshot_date":  today,
                        "interest_score": latest_score,
                        "geo":            "US",
                    })
                time.sleep(1.5)  # Google Trends rate limits aggressively

            except Exception as e:
                print(f"Google Trends failed for '{keyword}': {e}")

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Reddit — PRAW
# ─────────────────────────────────────────────

REDDIT_SUBREDDITS = [
    "flipping", "entrepreneur", "ecommerce",
    "amazonseller", "Ebay", "dropship",
]

REDDIT_SEARCH_TERMS = {
    "Electronics":            "electronics",
    "Home & Garden":          "home garden",
    "Clothing & Accessories": "clothing fashion",
    "Sporting Goods":         "sporting goods fitness",
    "Toys & Hobbies":         "toys hobbies",
    "Health & Beauty":        "health beauty",
    "Pet Supplies":           "pet supplies",
    "Automotive Parts":       "auto parts car",
    "Musical Instruments":    "musical instruments",
    "Collectibles":           "collectibles vintage",
}

def fetch_reddit_data() -> pd.DataFrame:
    reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "ecomm-intelligence/1.0"),
    )
    rows = []
    today = date.today()
    cutoff = today - timedelta(days=7)

    for subreddit_name in REDDIT_SUBREDDITS:
        for category_name, search_term in REDDIT_SEARCH_TERMS.items():
            try:
                subreddit = reddit.subreddit(subreddit_name)
                posts = list(subreddit.search(search_term, time_filter="week", limit=100))
                recent = [
                    p for p in posts
                    if date.fromtimestamp(p.created_utc) >= cutoff
                ]
                avg_score = (
                    sum(p.score for p in recent) / len(recent)
                    if recent else 0
                )
                rows.append({
                    "category_name":  category_name,
                    "keyword":        search_term,
                    "subreddit":      subreddit_name,
                    "snapshot_date":  today,
                    "mention_count":  len(recent),
                    "avg_score":      round(avg_score, 2),
                })
                time.sleep(0.5)

            except Exception as e:
                print(f"Reddit fetch failed for r/{subreddit_name} / {category_name}: {e}")

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# BLS Consumer Expenditure — static CSV
# ─────────────────────────────────────────────

BLS_CATEGORY_MAP = {
    "Apparel and services":             "Clothing & Accessories",
    "Entertainment":                    "Toys & Hobbies",
    "Personal care products":           "Health & Beauty",
    "Household furnishings & equipment":"Home & Garden",
    "Vehicles":                         "Automotive Parts",
    "Pets, toys, hobbies":              "Pet Supplies",
    "Reading":                          "Collectibles",
    "Sports & recreation equipment":    "Sporting Goods",
}

def fetch_bls_data(csv_path: str = "data/bls_consumer_expenditure.csv") -> pd.DataFrame:
    try:
        raw = pd.read_csv(csv_path)
        rows = []

        for _, row in raw.iterrows():
            bls_cat = str(row.get("category", "")).strip()
            category_name = BLS_CATEGORY_MAP.get(bls_cat, bls_cat)
            year = int(row.get("year", 0))
            spend = float(row.get("avg_annual_expenditure", 0))
            prev_spend = float(row.get("prev_year_expenditure", spend))
            yoy = ((spend - prev_spend) / prev_spend) if prev_spend else None

            rows.append({
                "category_name":     category_name,
                "bls_category":      bls_cat,
                "year":              year,
                "avg_annual_spend":  round(spend, 2),
                "yoy_change":        round(yoy, 4) if yoy is not None else None,
            })

        return pd.DataFrame(rows)

    except FileNotFoundError:
        print(f"BLS CSV not found at {csv_path} — skipping")
        return pd.DataFrame()
