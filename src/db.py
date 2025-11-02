# src/db.py
from sqlalchemy import create_engine, Column, String, BigInteger, TIMESTAMP, JSON, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
from src.config import DATABASE_URL

Base = declarative_base()

class Repo(Base):
    __tablename__ = "repos"

    repo_id = Column(String, primary_key=True)                 # GitHub global node id
    full_name = Column(String, nullable=False)                 # owner/name
    owner = Column(String, nullable=False)
    name = Column(String, nullable=False)
    stargazers_count = Column(BigInteger)
    last_crawled = Column(TIMESTAMP(timezone=True), server_default=func.now())
    metadata = Column(JSON, server_default=text("'{}'::jsonb"))

# Engine & Session
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def create_schema():
    """Create DB tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
