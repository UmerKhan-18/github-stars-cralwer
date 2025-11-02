# src/github_api.py
import requests
import time
from datetime import datetime, timezone
from src.config import GITHUB_API_URL, GITHUB_TOKEN, PAGE_SIZE, SLEEP_BETWEEN_PAGES

if not GITHUB_TOKEN:
    raise RuntimeError("GITHUB_TOKEN environment variable is required")

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}

# GraphQL query with rateLimit info included
QUERY = """
query ($cursor: String, $pageSize: Int!) {
  search(query: "stars:>0", type: REPOSITORY, first: $pageSize, after: $cursor) {
    repositoryCount
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        ... on Repository {
          id
          nameWithOwner
          stargazerCount
          updatedAt
        }
      }
    }
  }
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""

def run_query(variables):
    resp = requests.post(GITHUB_API_URL, json={"query": QUERY, "variables": variables}, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"GitHub API returned {resp.status_code}: {resp.text}")
    j = resp.json()
    if "errors" in j:
        # propagate helpful error
        raise RuntimeError(f"GitHub API errors: {j['errors']}")
    return j["data"]

def fetch_page(cursor=None, page_size=PAGE_SIZE):
    """Fetch one page of repositories and return (repos_list, page_info, rate_limit)."""
    data = run_query({"cursor": cursor, "pageSize": page_size})
    edges = data["search"]["edges"]
    repos = [edge["node"] for edge in edges]
    page_info = data["search"]["pageInfo"]
    rate_limit = data["rateLimit"]
    return repos, page_info, rate_limit

def handle_rate_limit(rate_limit):
    remaining = rate_limit.get("remaining", 0)
    reset_at = rate_limit.get("resetAt")
    if remaining <= 1 and reset_at:
        # compute seconds until reset
        reset_dt = datetime.fromisoformat(reset_at.rstrip("Z")).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        wait_seconds = (reset_dt - now).total_seconds() + 5  # small buffer
        if wait_seconds > 0:
            print(f"⚠️ Rate limit low (remaining={remaining}). Sleeping {int(wait_seconds)}s until reset.")
            time.sleep(wait_seconds)
    else:
        # polite sleep between pages to reduce hitting limits quickly
        time.sleep(SLEEP_BETWEEN_PAGES)
