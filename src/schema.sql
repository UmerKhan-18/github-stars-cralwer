-- =====================================
-- Database Schema for GitHub Stars Crawler
-- =====================================

CREATE TABLE IF NOT EXISTS repos (
  repo_id TEXT PRIMARY KEY,            -- GitHub global node ID
  full_name TEXT NOT NULL,             -- e.g. "torvalds/linux"
  owner TEXT NOT NULL,                 -- e.g. "torvalds"
  name TEXT NOT NULL,                  -- e.g. "linux"
  stargazers_count BIGINT,             -- total stars
  last_crawled TIMESTAMP WITH TIME ZONE DEFAULT now(), -- when data was last updated
  metadata JSONB DEFAULT '{}'          -- flexible JSON field for extra info
);

-- Index for faster lookups by time (useful for updates)
CREATE INDEX IF NOT EXISTS idx_repos_last_crawled ON repos(last_crawled);
