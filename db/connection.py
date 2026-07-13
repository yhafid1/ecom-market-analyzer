import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

def get_connection_string() -> str:
    """
    Resolve the SQLAlchemy connection string.

    Priority:
    1. DATABASE_URL env var (used by hosted Postgres providers like Neon/
       Supabase/Render, and by Streamlit Community Cloud secrets) — a
       standard postgres:// or postgresql:// URL, transparently upgraded
       to the psycopg2 driver.
    2. Discrete DB_USER/DB_PASSWORD/DB_HOST/DB_PORT/DB_NAME vars (used by
       local Docker Compose dev, unchanged from before).
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Normalize scheme so SQLAlchemy always uses the psycopg2 driver,
        # regardless of how the hosting provider formats the URL.
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return database_url

    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', 5432)}"
        f"/{os.getenv('DB_NAME')}"
    )

engine = create_engine(get_connection_string(), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

def get_engine():
    return engine

def health_check() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"DB health check failed: {e}")
        return False
