# Deployment Guide

This document is a step-by-step guide for taking this project from
"runs locally with Docker Compose" to "publicly viewable Streamlit
dashboard backed by a hosted Postgres instance, seeded with synthetic
demo data."

**Status: not yet deployed.** Nothing below has been executed against a
real cloud account — this repo does not contain any cloud credentials.
Everything here is written so a human with a browser and ~10-15 minutes
can go through it manually. See the "Live Demo" section in
[README.md](README.md) for the current state.

The dashboard (`dashboard/app.py`) reads from 4 materialized views
(`mv_category_leaderboard`, `mv_channel_comparison`, `mv_niche_finder`,
`mv_trends_explorer`) which are built on top of 6 base tables defined in
[`db/schema.sql`](db/schema.sql). For a public demo, we don't want to
require eBay/Reddit/Google Trends/Rainforest API keys, so instead of
running the real ETL pipeline (`etl/pipeline.py`), we run
[`db/seed.py`](db/seed.py) — a script that generates a small, synthetic,
internally-consistent dataset (10 categories x ~10 days) and loads it
into the same schema, then creates and refreshes the materialized views.

---

## Part A — Create a free hosted Postgres instance

You can use either **Neon** or **Supabase**; both have a free tier
sufficient for this project (a few thousand rows across 6 small tables).
Steps below use Neon; Supabase differs only in where you find the
connection string (noted inline).

### A1. Create the database

1. Go to https://neon.tech and sign up / log in (GitHub or Google SSO
   both work, no credit card required for the free tier).
2. Click **Create a project**. Pick any project name (e.g.
   `ecom-market-analyzer`) and a region close to where your Streamlit
   app will run (e.g. a US region, since Streamlit Community Cloud
   currently runs in the US).
3. Neon provisions a default database (commonly named `neondb`) and
   gives you a **connection string** immediately after creation, of the
   form:
   ```
   postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require
   ```
   Copy this — you'll need it twice (once to run the schema/seed
   locally against it, once as the `DATABASE_URL` secret in Streamlit
   Community Cloud).

   **Supabase alternative:** Create a project at https://supabase.com,
   then go to **Project Settings → Database → Connection string** and
   copy the **URI** (not the "pooled" one for this step — use the
   direct connection for running schema/seed scripts). Supabase
   connection strings also end in `?sslmode=require` or use port 6543
   for the pooler; either works with SQLAlchemy/psycopg2.

### A2. Run schema.sql against the hosted database

You need `psql` installed locally, or use the SQL editor built into the
Neon/Supabase web console (both have one — this is often easier than
installing `psql`).

**Option 1 — web SQL editor (simplest, no local tools needed):**
1. Open the Neon console → your project → **SQL Editor** (or Supabase
   → **SQL Editor**).
2. Paste the entire contents of `db/schema.sql` and run it. This
   creates the 6 base tables (`category_trends`, `retail_vs_ecomm`,
   `search_signals`, `social_buzz`, `consumer_spend`, `niche_scores`)
   plus their indexes.

**Option 2 — psql from your machine:**
```bash
psql "postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require" \
  -f db/schema.sql
```

### A3. Seed the demo dataset

From the project root, with Python and the project's dependencies
installed locally (`pip install -r requirements.txt`), set `DATABASE_URL`
to point at your new hosted database and run the seed script:

```bash
# macOS/Linux
export DATABASE_URL="postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require"
python -m db.seed

# Windows PowerShell
$env:DATABASE_URL = "postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require"
python -m db.seed
```

This will:
1. Health-check the connection.
2. Generate ~10 days of synthetic data across all 10 tracked categories
   for `category_trends`, `search_signals`, `social_buzz`, plus a few
   `retail_vs_ecomm` snapshots, 3 years of `consumer_spend`, and 5
   scoring snapshots in `niche_scores` (enough for the "Rising vs
   Declining" momentum badges to have something to compare against).
3. Upsert all of it into the base tables using the same
   `etl/load.py` upsert helpers the real pipeline uses.
4. Create (if missing) and `REFRESH` the 4 materialized views the
   dashboard reads from.

To wipe and re-seed later (e.g. to reset the public demo to a clean
state), run:
```bash
python -m db.seed --reset
```

Verify it worked by connecting with `psql` or the web SQL editor and
running:
```sql
SELECT COUNT(*) FROM mv_category_leaderboard;  -- should return 10
```

---

## Part B — Deploy the dashboard to Streamlit Community Cloud

Streamlit Community Cloud deploys directly from a public GitHub
repository — there is no separate build step to configure; it reads
`requirements.txt` from the repo root and runs the app file you specify.

1. Push this repository to a **public** GitHub repo (Community Cloud's
   free tier requires the repo to be public, or you connect a private
   repo under a paid/team plan — for a portfolio demo, public is fine
   since there are no real secrets in the code, only in the connection
   string you'll enter as a secret).
2. Go to https://share.streamlit.io and sign in with your GitHub
   account (this authorizes Streamlit to see your repos).
3. Click **Create app** → **"Deploy a public app from GitHub"**.
4. Fill in:
   - **Repository:** `<your-github-username>/ecom-market-analyzer`
   - **Branch:** `main` (or whichever branch you want live)
   - **Main file path:** `dashboard/app.py`
   - App URL: you can customize the subdomain here
     (`https://<your-chosen-name>.streamlit.app`).
5. Before clicking Deploy, open **"Advanced settings"** and set the
   **Secrets** block (this is Streamlit's equivalent of environment
   variables, using TOML format) to:
   ```toml
   DATABASE_URL = "postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require"
   ```
   This is the same connection string from Part A, and
   `db/connection.py` already reads `DATABASE_URL` first (see below) —
   no code changes are needed to wire this up.
6. Click **Deploy**. Streamlit Community Cloud will install
   `requirements.txt` and launch `streamlit run dashboard/app.py`. First
   deploy typically takes 2-5 minutes.
7. Once live, the app sleeps after a period of inactivity on the free
   tier and wakes on the next visit (a "waking up" screen appears for a
   few seconds) — this is expected free-tier behavior, not a bug.

**Updating secrets later:** App → **Settings (⋮ menu) → Secrets** —
edit and save; Streamlit restarts the app automatically to pick up the
new value.

**Updating the deployed app:** Community Cloud auto-redeploys on every
push to the tracked branch. No manual redeploy step is needed for code
changes; only `DATABASE_URL`/schema changes require re-running Part A.

### Why this works without code changes

`db/connection.py`'s `get_connection_string()` checks `DATABASE_URL`
first and falls back to the discrete `DB_HOST`/`DB_USER`/etc. vars used
by local Docker Compose. Streamlit Community Cloud's secrets are
exposed as environment variables to the running process, so setting
`DATABASE_URL` in the Secrets panel is sufficient — nothing else in
`dashboard/app.py` or `db/queries.py` needs to change.

---

## Part C — Alternative: Fly.io (if you want the whole stack containerized)

Fly.io is a reasonable alternative if you'd rather deploy the existing
`Dockerfile` as-is (e.g. to also self-host Postgres via a Fly Postgres
app, or to avoid Streamlit Community Cloud's sleep-on-idle behavior).

1. Install the Fly CLI (`flyctl`) and run `fly auth login`.
2. From the project root, run `fly launch` — it detects the existing
   `Dockerfile` and proposes a Fly app config (`fly.toml`). Decline
   Fly's offer to also provision a new Postgres unless you want to use
   Fly Postgres instead of Neon/Supabase; either works since the app
   only needs a `DATABASE_URL`.
3. Set the connection string as a Fly secret (never commit it to
   `fly.toml`):
   ```bash
   fly secrets set DATABASE_URL="postgresql://<user>:<password>@<endpoint-hostname>/<dbname>?sslmode=require"
   ```
4. Fly's default `Dockerfile` `CMD` already runs
   `streamlit run dashboard/app.py --server.address=0.0.0.0`, which
   binds to all interfaces — required for Fly's proxy to reach it.
   Streamlit also needs `--server.port` to match whatever port Fly
   expects (`8501` by default, matching `EXPOSE 8501` in the
   `Dockerfile`); confirm the generated `fly.toml`'s `[[services]]` /
   `internal_port` is set to `8501`.
5. Deploy with `fly deploy`.
6. Run `db/seed.py` once against the same `DATABASE_URL` (from your
   local machine, per Part A3) before or after the first deploy — Fly
   doesn't run it automatically.

Fly.io requires a credit card on file even for the free/hobby
allowances (as of 2026), which is why Streamlit Community Cloud (Part
B) is the recommended first option for a no-cost portfolio demo.

---

## Checklist — what's left for the human owner

- [ ] Create a Neon or Supabase account (free tier)
- [ ] Run `db/schema.sql` against the hosted instance
- [ ] Run `python -m db.seed` against the hosted instance (with
      `DATABASE_URL` set)
- [ ] Push this repo to a public GitHub repository
- [ ] Create a Streamlit Community Cloud account and deploy
      `dashboard/app.py`, setting `DATABASE_URL` in the app's Secrets
- [ ] Update the "Live Demo" section in `README.md` with the real URL
      and 1-2 screenshots once the app is confirmed working
