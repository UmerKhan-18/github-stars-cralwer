# src/crawler.py
from src.db import SessionLocal, Repo
from src.github_api import fetch_page, handle_rate_limit
from src.config import MAX_REPOS, PAGE_SIZE
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select
from datetime import datetime
import os
import pandas as pd

def upsert_repo(session, repo_node):
    """Create or update a Repo ORM object from GraphQL node."""
    repo_id = repo_node["id"]
    full_name = repo_node["nameWithOwner"]
    owner, name = full_name.split("/", 1)
    stars = repo_node.get("stargazerCount", 0)

    # Build ORM object
    repo = Repo(
        repo_id=repo_id,
        full_name=full_name,
        owner=owner,
        name=name,
        stargazers_count=stars,
        last_crawled=datetime.utcnow(),
        metadata={}
    )
    # session.merge will insert or update based on primary key
    session.merge(repo)

def crawl_and_persist():
    """Main crawling loop. Returns how many repos fetched."""
    session = SessionLocal()
    cursor = None
    total = 0
    try:
        while total < MAX_REPOS:
            repos, page_info, rate_limit = fetch_page(cursor=cursor, page_size=PAGE_SIZE)
            if not repos:
                break
            for node in repos:
                upsert_repo(session, node)
                total += 1
                if total % 100 == 0:
                    print(f"Progress: {total} repos processed.")
                if total >= MAX_REPOS:
                    break
            # Commit after each page for checkpointing
            session.commit()
            handle_rate_limit(rate_limit)
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return total
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

def export_csv(output_path):
    """Export all repos to CSV using pandas (via SQL)."""
    from src.db import engine
    query = "SELECT repo_id, full_name, owner, name, stargazers_count, last_crawled, metadata FROM repos ORDER BY stargazers_count DESC"
    df = pd.read_sql(query, engine)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"CSV exported to: {output_path}")
