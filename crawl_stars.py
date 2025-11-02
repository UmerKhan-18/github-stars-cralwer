import os
import requests
import time
import json
import psycopg2

# GitHub GraphQL API endpoint
GITHUB_API_URL = "https://api.github.com/graphql"
TOKEN = os.environ.get("GITHUB_TOKEN")

if not TOKEN:
    print("❌ Error: GITHUB_TOKEN not found.")
    exit(1)

HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# GraphQL query
QUERY = """
query ($cursor: String) {
  search(query: "stars:>10000", type: REPOSITORY, first: 20, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
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
}
"""

def run_query(query, variables=None):
    response = requests.post(GITHUB_API_URL, json={"query": query, "variables": variables}, headers=HEADERS)
    if response.status_code != 200:
        print(f"❌ Query failed: {response.status_code} - {response.text}")
        return None
    return response.json()

def upsert_repo(cur, repo):
    """Insert or update a repo in the database"""
    full_name = repo["nameWithOwner"]
    owner, name = full_name.split("/")
    cur.execute("""
        INSERT INTO repos (repo_id, full_name, owner, name, stargazers_count, last_crawled, metadata)
        VALUES (%s, %s, %s, %s, %s, now(), %s)
        ON CONFLICT (repo_id) DO UPDATE
        SET stargazers_count = EXCLUDED.stargazers_count,
            last_crawled = now(),
            metadata = repos.metadata || EXCLUDED.metadata;
    """, (
        repo["id"],
        full_name,
        owner,
        name,
        repo["stargazerCount"],
        json.dumps({})
    ))

def main():
    # --- connect to Postgres (in GitHub Actions, these env vars will be set) ---
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "crawler"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )
    cur = conn.cursor()

    cursor = None
    total_fetched = 0

    print("🚀 Fetching repositories from GitHub and saving to Postgres...")

    while total_fetched < 20:
        data = run_query(QUERY, {"cursor": cursor})
        if not data:
            break

        repos = data["data"]["search"]["edges"]
        for edge in repos:
            repo = edge["node"]
            upsert_repo(cur, repo)
            total_fetched += 1
            print(f"✅ Saved {repo['nameWithOwner']} ({repo['stargazerCount']}⭐)")

        conn.commit()

        # Pagination
        page_info = data["data"]["search"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]

        # Small sleep to be nice to API
        time.sleep(1)

    cur.close()
    conn.close()
    print(f"\n✅ Done! Total {total_fetched} repos saved to database.")

if __name__ == "__main__":
    main()
