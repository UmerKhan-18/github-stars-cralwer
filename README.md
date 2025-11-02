# GitHub Stars Crawler

This project automatically crawls public GitHub repositories using the **GitHub GraphQL API** and stores their star counts in a **PostgreSQL database**.  
It was developed as part of a **Software Engineering Take-Home Assignment**.

---

## Overview

The system connects to the GitHub GraphQL API, fetches repositories in batches, and writes their details (ID, owner, name, star count, etc.) into a PostgreSQL database.  
Everything runs automatically inside **GitHub Actions**, so no local setup or database installation is required.

---

## Features

- Fetches up to **100,000 repositories** via the **GraphQL API**
- Stores repository data in **PostgreSQL**
- Handles **pagination**, **rate limits**, and **retries**
- Uses **UPSERT** for efficient updates (no duplicates)
- Runs completely in **GitHub Actions**
- Generates and uploads a **database dump (artifact)** after every run

---

## Architecture

GitHub GraphQL API → Python Crawler → PostgreSQL (Service Container) → Database Dump Artifact


---

## Database Schema

The schema is defined in `src/schema.sql`.

```sql
CREATE TABLE IF NOT EXISTS repos (
  repo_id TEXT PRIMARY KEY,
  full_name TEXT NOT NULL,
  owner TEXT NOT NULL,
  name TEXT NOT NULL,
  stargazers_count BIGINT,
  last_crawled TIMESTAMP WITH TIME ZONE DEFAULT now(),
  metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_repos_last_crawled ON repos(last_crawled);
```
---

### Why this design:

repo_id → unique GitHub identifier (used for upserts)

metadata → flexible JSON column for future fields like forks, issues, PRs

ON CONFLICT (repo_id) DO UPDATE → ensures efficient updates

Crawler (crawl_stars.py)
The crawler connects to GitHub’s GraphQL API using the built-in GitHub Actions token (${{ secrets.GITHUB_TOKEN }}).

### Responsibilities
Fetch repository data using GraphQL

Insert or update rows in PostgreSQL

Handle pagination and rate limits

Commit data after each batch

# GitHub Actions Workflow
The entire pipeline runs automatically using the workflow file:
.github/workflows/crawl.yml

## Workflow Steps
Checkout the repository

Setup Python

Start PostgreSQL service container

Apply database schema (setup.sql)

Run crawler script (crawl_stars.py)

Dump database contents to dump.sql

Upload dump as an artifact

Trigger Options
Manual: Run from the Actions tab → “crawl-stars” → Run workflow
Automatic: Scheduled daily at 02:00 UTC

Output Artifact (dump.sql)
After each run, a file named dump.sql is uploaded as a GitHub Actions artifact.

sql
INSERT INTO public.repos (repo_id, full_name, owner, name, stargazers_count, last_crawled, metadata)
VALUES ('MDEwOlJlcG9zaXRvcnkyMzI1Mjk4', 'torvalds/linux', 'torvalds', 'linux', 206128, '2025-11-02 02:09:10+00', '{}');


# Technologies Used
Python 3.10

PostgreSQL 15

GitHub Actions

GitHub GraphQL API

Libraries: requests, psycopg2-binary

Project Structure
pgsql
.
├── crawl_stars.py
├── sql/
│   └── setup.sql
├── .github/
│   └── workflows/
│       └── crawl.yml
├── requirements.txt
└── README.md


# Future Improvements

1. Extend to Pull Requests, Issues & Comments
Add new tables:

sql
CREATE TABLE pull_requests (pr_id TEXT PRIMARY KEY, repo_id TEXT, metadata JSONB, last_updated TIMESTAMP);
CREATE TABLE issues (issue_id TEXT PRIMARY KEY, repo_id TEXT, metadata JSONB, last_updated TIMESTAMP);
CREATE TABLE comments (comment_id TEXT PRIMARY KEY, pr_id TEXT, metadata JSONB, last_updated TIMESTAMP);
Use last_updated for incremental updates.

2. Scale for 500M+ Repositories
Shard repositories across multiple workers

Use message queues (e.g., Kafka) between crawler and database

Store cold data in S3/Parquet, hot data in Postgres

Rotate tokens for distributed rate-limit management

3. Monitoring & Reliability
Add detailed logs & metrics

Slack/email notifications for job status

Add checkpoints to resume interrupted crawls
