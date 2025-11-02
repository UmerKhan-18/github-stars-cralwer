# src/config.py
import os

# Crawl limits
MAX_REPOS = int(os.getenv("MAX_REPOS", 100_000))  # default 100k (configurable)
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 100))      # GraphQL page size (max 100)

# GitHub / DB
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_API_URL = "https://api.github.com/graphql"

# Postgres connection (SQLAlchemy URL)
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE", "crawler")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
)

# Other
SLEEP_BETWEEN_PAGES = float(os.getenv("SLEEP_BETWEEN_PAGES", 1.0))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "output/repos_dump.csv")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
