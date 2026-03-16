import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from db.connection import get_engine


def _upsert(df: pd.DataFrame, table: str, conflict_cols: list[str]) -> int:
    if df.empty:
        print(f"  [load] {table}: nothing to load")
        return 0

    engine = get_engine()
    records = df.to_dict(orient="records")
    inserted = 0

    with engine.begin() as conn:
        for record in records:
            stmt = (
                insert(__import__("sqlalchemy", fromlist=["Table"])
                       .__class__)  # placeholder — using raw SQL below
            )

        stmt = text(f"""
            INSERT INTO {table} ({", ".join(record.keys())})
            VALUES ({", ".join(f":{k}" for k in record.keys())})
            ON CONFLICT ({", ".join(conflict_cols)}) DO UPDATE SET
            {", ".join(f"{k} = EXCLUDED.{k}" for k in record.keys() if k not in conflict_cols)}
        """)

        for record in records:
            try:
                conn.execute(stmt, record)
                inserted += 1
            except Exception as e:
                print(f"  [load] {table} row error: {e}")

    print(f"  [load] {table}: {inserted}/{len(records)} rows upserted")
    return inserted


def load_category_trends(df: pd.DataFrame) -> int:
    return _upsert(df, "category_trends", ["category_name", "source", "snapshot_date"])


def load_search_signals(df: pd.DataFrame) -> int:
    return _upsert(df, "search_signals", ["keyword", "snapshot_date", "geo"])


def load_social_buzz(df: pd.DataFrame) -> int:
    return _upsert(df, "social_buzz", ["keyword", "subreddit", "snapshot_date"])


def load_consumer_spend(df: pd.DataFrame) -> int:
    return _upsert(df, "consumer_spend", ["category_name", "year"])


def load_retail_vs_ecomm(df: pd.DataFrame) -> int:
    return _upsert(df, "retail_vs_ecomm", ["category_name", "period_date", "source"])


def load_niche_scores(df: pd.DataFrame) -> int:
    return _upsert(df, "niche_scores", ["category_name", "scored_at"])
