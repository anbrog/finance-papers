#!/usr/bin/env python3
"""Get working papers for authors from an author list CSV file"""
import requests
import sqlite3
import json
import os
import sys
import csv
from datetime import datetime
import time

BASE_URL = "https://api.openalex.org/works"
DB_DIR = '../out/data'

def read_author_list(csv_file):
    """Read authors from CSV file"""
    authors = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            authors.append({
                'rank': int(row['Rank']),
                'name': row['Author Name'],
                'paper_count': int(row['Paper Count'])
            })
    return authors

def fetch_working_papers_for_author(author_name, year=None):
    """Fetch working papers (preprints) from OpenAlex for a specific author"""
    # Check if author_name contains an OpenAlex ID (format: "Name|A12345678")
    if '|' in author_name:
        author_display_name, author_id = author_name.split('|', 1)
        author_name = author_display_name.strip()
        author_id = author_id.strip()
    else:
        # Search for the author and get their OpenAlex ID first
        author_search_url = "https://api.openalex.org/authors"
        
        # Build filter
        filters = f"display_name.search:{author_name}"
        
        params = {
            "filter": filters,
            "per-page": 1
        }
        
        mailto = os.getenv("OPENALEX_MAILTO")
        if mailto:
            params["mailto"] = mailto
        
        try:
            resp = requests.get(author_search_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if not data.get("results"):
                return []
            
            author_id = data["results"][0]["id"].split('/')[-1]  # Extract author ID
        except requests.RequestException as e:
            print(f"Error fetching author ID for {author_name}: {e}")
            return []
    
    # Now search for working papers by this author
    # Working papers are type:report, or exclude journal articles to get preprints/dissertations
    mailto = os.getenv("OPENALEX_MAILTO")
    
    try:
        # Search for non-journal works (reports, dissertations, etc.)
        work_filters = f"authorships.author.id:{author_id},type:!article"
        if year:
            # Search from 2 years before to current to catch "forthcoming" papers
            year_int = int(year) if isinstance(year, str) else year
            work_filters += f",from_publication_date:{year_int-1}-01-01"
        
        work_params = {
            "filter": work_filters,
            "per-page": 200
        }
        if mailto:
            work_params["mailto"] = mailto
        
        resp = requests.get(BASE_URL, params=work_params, timeout=30)
        resp.raise_for_status()
        work_data = resp.json()
        
        papers = []
        for work in work_data.get("results", []):
            primary_loc = work.get("primary_location") or {}
            source = primary_loc.get("source") or {}
            papers.append({
                "openalex_id": work.get("id"),
                "title": work.get("title"),
                "publication_date": work.get("publication_date"),
                "doi": work.get("doi"),
                "author_name": author_name,
                "type": work.get("type"),
                "primary_location": source.get("display_name"),
                "cited_by_count": work.get("cited_by_count", 0)
            })
        
        return papers
    
    except requests.RequestException as e:
        print(f"Error fetching data for {author_name}: {e}")
        return []

def save_working_papers_to_db(papers, db_filename='working_papers.db'):
    """Save working papers to SQLite database"""
    db_filepath = os.path.join(DB_DIR, db_filename)
    
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS working_papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            author_name TEXT,
            type TEXT,
            primary_location TEXT,
            cited_by_count INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_openalex_id ON working_papers(openalex_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_author ON working_papers(author_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_date ON working_papers(publication_date)')
    
    new_count = 0
    duplicate_count = 0
    
    for paper in papers:
        openalex_id = paper['openalex_id']
        
        # Check for duplicates
        cursor.execute('SELECT id FROM working_papers WHERE openalex_id = ?', (openalex_id,))
        if cursor.fetchone():
            duplicate_count += 1
            continue
        
        # Insert paper
        cursor.execute('''
            INSERT INTO working_papers (openalex_id, title, publication_date, doi, author_name, type, primary_location, cited_by_count, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            openalex_id,
            paper['title'],
            paper['publication_date'],
            paper['doi'],
            paper['author_name'],
            paper['type'],
            paper['primary_location'],
            paper.get('cited_by_count', 0),
            datetime.now().isoformat()
        ))
        new_count += 1
    
    conn.commit()
    conn.close()
    
    return new_count, duplicate_count

def main():
    if len(sys.argv) < 2:
        # No CSV provided - find the latest author_list CSV
        import glob
        pattern = os.path.join(DB_DIR, 'author_list_*.csv')
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            print("Usage: python3 get_wp.py <author_list_csv> [year]")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv 2024")
            print("\nError: No author_list CSV files found in ../out/data/")
            sys.exit(1)
        
        csv_file = max(csv_files, key=os.path.getmtime)
        print(f"No CSV provided - using latest author list: {os.path.basename(csv_file)}")
        year = None
    else:
        csv_file = sys.argv[1]
        year = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Expand glob pattern if provided
    import glob
    csv_files = glob.glob(csv_file)
    
    if not csv_files:
        print(f"Error: No files found matching '{csv_file}'")
        sys.exit(1)
    
    # Use the most recent file if multiple matches
    csv_file = max(csv_files, key=os.path.getmtime)
    
    if not os.path.exists(csv_file):
        print(f"Error: File not found: {csv_file}")
        sys.exit(1)
    
    print(f"Reading authors from: {csv_file}")
    authors = read_author_list(csv_file)
    print(f"Found {len(authors)} authors")
    
    year_label = f" ({year})" if year else " (all years)"
    print(f"Fetching working papers{year_label}...")
    
    all_papers = []
    total_authors = len(authors)
    
    for idx, author in enumerate(authors, 1):
        print(f"[{idx}/{total_authors}] {author['name']}...", end=' ', flush=True)
        papers = fetch_working_papers_for_author(author['name'], year)
        all_papers.extend(papers)
        print(f"{len(papers)} working papers")
        
        # Be nice to the API - rate limiting
        if idx < total_authors:
            time.sleep(0.1)  # 100ms delay between requests
    
    print(f"\nTotal working papers found: {len(all_papers)}")
    
    if all_papers:
        db_filename = f"working_papers{f'_{year}' if year else ''}.db"
        new_count, dup_count = save_working_papers_to_db(all_papers, db_filename)
        
        db_path = os.path.join(DB_DIR, db_filename)
        print(f"\n💾 Saved {new_count} new working papers to {db_path}")
        if dup_count > 0:
            print(f"🔄 Skipped {dup_count} duplicate working papers")
    else:
        print("\nNo working papers found.")

if __name__ == "__main__":
    main()
