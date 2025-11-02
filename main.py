# main.py
import sys
from src.db import create_schema
from src.crawler import crawl_and_persist, export_csv
from src.config import OUTPUT_CSV

def main():
    print("Initializing DB schema (if needed)...")
    create_schema()

    print("Starting crawl...")
    total = crawl_and_persist()
    print(f"Done crawling. Total repositories processed: {total}")

    print("Exporting CSV...")
    export_csv(OUTPUT_CSV)
    print("All done. CSV ready for artifact upload.")

if __name__ == "__main__":
    main()
