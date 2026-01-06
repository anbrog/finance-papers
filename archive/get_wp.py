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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE_URL = "https://api.openalex.org/works"

# Rate limiting for OpenAlex API (10 req/sec max)
_last_request_time = 0
_request_lock = threading.Lock()

def _rate_limited_request(url, params=None, timeout=30, max_retries=3):
    """Make a rate-limited request with retry logic for 429 errors"""
    global _last_request_time

    for attempt in range(max_retries):
        # Ensure minimum 200ms between requests (across all threads)
        with _request_lock:
            now = time.time()
            elapsed = now - _last_request_time
            if elapsed < 0.2:
                time.sleep(0.2 - elapsed)
            _last_request_time = time.time()

        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                # Rate limited - wait and retry
                wait_time = 2 ** (attempt + 2)  # Exponential backoff: 4, 8, 16 seconds
                print(f"\n⚠️  Rate limited (429), waiting {wait_time}s before retry {attempt+1}/{max_retries}...", flush=True)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"\n❌ Request failed after {max_retries} retries: {e}", flush=True)
                raise
            time.sleep(1)

    print(f"\n❌ Rate limit: all {max_retries} retries exhausted", flush=True)
    return None

# Get DB_DIR relative to project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
DB_DIR = os.path.join(project_root, 'out', 'data')

def read_author_list(csv_file):
    """Read authors from CSV file"""
    authors = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            authors.append({
                'rank': int(row['Rank']),
                'name': row['Author Name'],
                'author_id': row.get('Author ID', '').strip() if 'Author ID' in row else None,
                'paper_count': int(row['Paper Count'])
            })
    return authors

def fetch_working_papers_for_author(author_name, author_id=None, year=None):
    """Fetch working papers (preprints) from OpenAlex for a specific author

    Args:
        author_name: Author's display name
        author_id: OpenAlex author ID (e.g., 'https://openalex.org/A1234567890')
        year: Optional year filter
    """
    author_affiliation = None
    mailto = os.getenv("OPENALEX_MAILTO")

    # If author_id is provided, use it directly but also fetch affiliation
    if author_id:
        # Extract just the ID part if full URL is provided
        if author_id.startswith('https://openalex.org/'):
            author_id = author_id.split('/')[-1]

        # Fetch author details to get affiliation
        try:
            author_url = f"https://api.openalex.org/authors/{author_id}"
            params = {"mailto": mailto} if mailto else None
            resp = _rate_limited_request(author_url, params=params)
            if not resp:
                raise requests.RequestException("Rate limit exceeded")
            author_data = resp.json()
            # Get last known institution
            institutions = author_data.get("last_known_institutions", [])
            if institutions:
                author_affiliation = institutions[0].get("display_name")
        except requests.RequestException:
            pass  # Continue without affiliation
    else:
        # No author ID - try to find author by name with additional filters
        # Filter by economics/business concepts to reduce false matches

        # Try multiple search strategies to find the right author
        search_attempts = [
            # Strategy 1: Search with economics/finance filter
            f"https://api.openalex.org/authors?search={requests.utils.quote(author_name)}&filter=x_concepts.id:C162324750|C41008148&per-page=1",
            # Strategy 2: Fallback to general search
            f"https://api.openalex.org/authors?search={requests.utils.quote(author_name)}&per-page=3"
        ]

        for attempt_url in search_attempts:
            if mailto:
                attempt_url += f"&mailto={mailto}"

            try:
                resp = _rate_limited_request(attempt_url)
                if not resp:
                    continue
                data = resp.json()

                if data.get("results") and len(data["results"]) > 0:
                    # For general search, try to pick the most relevant author
                    # (highest works_count or most recent activity)
                    results = data["results"]
                    if len(results) > 1:
                        # Sort by works_count to get the most prolific author
                        results = sorted(results, key=lambda x: x.get("works_count", 0), reverse=True)

                    author_result = results[0]
                    author_id = author_result["id"].split('/')[-1]

                    # Extract affiliation from search result
                    institutions = author_result.get("last_known_institutions", [])
                    if institutions:
                        author_affiliation = institutions[0].get("display_name")

                    # Warn if using fallback without concept filter
                    if "x_concepts" not in attempt_url:
                        print(f"(using top match by works_count)...", end=' ', flush=True)

                    break
            except requests.RequestException as e:
                continue
        else:
            print(f"No OpenAlex profile found for {author_name}, skipping...")
            return []
    
    # Now search for working papers by this author
    # Working papers include: non-articles, posted-content, and papers from working paper repositories (SSRN, arXiv, etc.)

    # Collect papers from multiple sources
    all_papers = []
    seen_ids = set()

    # 1. Get all non-article works (dissertations, preprints, etc.)
    # 2. Get papers from working paper repositories (SSRN) even if marked as "article"
    filter_attempts = [
        ("type:!article", "all non-article works"),
        ("primary_location.source.id:S4210172589", "SSRN papers")  # SSRN source ID
    ]

    for type_filter, description in filter_attempts:
        try:
            work_filters = f"authorships.author.id:{author_id},{type_filter}"
            if year:
                # Search from 2 years before to current to catch "forthcoming" papers
                year_int = int(year) if isinstance(year, str) else year
                work_filters += f",from_publication_date:{year_int-1}-01-01"

            # Use cursor pagination to get ALL results
            cursor = "*"
            while cursor:
                work_params = {
                    "filter": work_filters,
                    "per-page": 200,
                    "cursor": cursor
                }
                if mailto:
                    work_params["mailto"] = mailto

                resp = _rate_limited_request(BASE_URL, params=work_params)
                if not resp:
                    print(f"Warning: Rate limit exceeded for {author_name}, skipping {description}")
                    break
                work_data = resp.json()

                for work in work_data.get("results", []):
                    openalex_id = work.get("id")

                    # Skip if we've already seen this paper
                    if openalex_id in seen_ids:
                        continue

                    seen_ids.add(openalex_id)

                    primary_loc = work.get("primary_location") or {}
                    source = primary_loc.get("source") or {}

                    # Extract topics
                    topics = work.get("topics", [])
                    topics_data = [{"name": t.get("display_name"), "id": t.get("id")} for t in topics]

                    all_papers.append({
                        "openalex_id": openalex_id,
                        "title": work.get("title"),
                        "publication_date": work.get("publication_date"),
                        "doi": work.get("doi"),
                        "author_name": author_name,
                        "author_affiliation": author_affiliation,
                        "type": work.get("type"),
                        "primary_location": source.get("display_name"),
                        "cited_by_count": work.get("cited_by_count", 0),
                        "topics_json": json.dumps(topics_data) if topics_data else None
                    })

                # Get next page cursor
                cursor = work_data.get("meta", {}).get("next_cursor")

        except requests.RequestException as e:
            print(f"Warning: Error fetching {description} for {author_name}: {e}")
            continue

    return all_papers

def save_working_papers_to_db(papers, db_filename='working_papers.db', clean=False):
    """Save working papers to SQLite database
    
    Args:
        papers: List of paper dictionaries
        db_filename: Database filename
        clean: If True, drop and recreate the table (removes all old data)
    """
    db_filepath = os.path.join(DB_DIR, db_filename)
    
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Drop table if clean flag is set
    if clean:
        print("🗑️  Cleaning database - removing all old working papers...")
        cursor.execute('DROP TABLE IF EXISTS working_papers')
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS working_papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            author_name TEXT,
            author_affiliation TEXT,
            type TEXT,
            primary_location TEXT,
            cited_by_count INTEGER DEFAULT 0,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add columns if they don't exist (for existing databases)
    cursor.execute("PRAGMA table_info(working_papers)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'author_affiliation' not in columns:
        cursor.execute('ALTER TABLE working_papers ADD COLUMN author_affiliation TEXT')
    if 'topics_json' not in columns:
        cursor.execute('ALTER TABLE working_papers ADD COLUMN topics_json TEXT')
    if 'authors_json' not in columns:
        cursor.execute('ALTER TABLE working_papers ADD COLUMN authors_json TEXT')

    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_openalex_id ON working_papers(openalex_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_author ON working_papers(author_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_date ON working_papers(publication_date)')

    new_count = 0
    duplicate_count = 0
    updated_count = 0

    for paper in papers:
        openalex_id = paper['openalex_id']
        author_name = paper['author_name']
        author_affiliation = paper.get('author_affiliation')

        # Check for duplicates
        cursor.execute('SELECT id, authors_json FROM working_papers WHERE openalex_id = ?', (openalex_id,))
        existing = cursor.fetchone()

        if existing:
            # Paper exists - add this author to authors_json if not already there
            existing_id, existing_authors_json = existing

            # Parse existing authors or create new list
            if existing_authors_json:
                try:
                    authors_list = json.loads(existing_authors_json)
                except json.JSONDecodeError:
                    authors_list = []
            else:
                authors_list = []

            # Check if this author is already in the list
            author_names_in_list = [a.get('name', '').lower() for a in authors_list]
            if author_name.lower() not in author_names_in_list:
                # Add new author
                authors_list.append({
                    'name': author_name,
                    'affiliation': author_affiliation
                })
                cursor.execute('''
                    UPDATE working_papers SET authors_json = ? WHERE id = ?
                ''', (json.dumps(authors_list), existing_id))
                updated_count += 1
            else:
                duplicate_count += 1
            continue

        # New paper - create initial authors_json with this author
        authors_list = [{'name': author_name, 'affiliation': author_affiliation}]

        # Insert paper
        cursor.execute('''
            INSERT INTO working_papers (openalex_id, title, publication_date, doi, author_name, author_affiliation, type, primary_location, cited_by_count, topics_json, authors_json, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            openalex_id,
            paper['title'],
            paper['publication_date'],
            paper['doi'],
            author_name,
            author_affiliation,
            paper['type'],
            paper['primary_location'],
            paper.get('cited_by_count', 0),
            paper.get('topics_json'),
            json.dumps(authors_list),
            datetime.now().isoformat()
        ))
        new_count += 1

    conn.commit()
    conn.close()

    return new_count, duplicate_count, updated_count

def main():
    # Parse arguments
    csv_file = None
    year = None
    max_authors = None
    clean = False
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--clean':
            clean = True
        elif arg.startswith('--'):
            try:
                max_authors = int(arg[2:])
            except ValueError:
                print(f"Invalid --N flag: {arg}")
                sys.exit(1)
        elif csv_file is None:
            csv_file = arg
        elif year is None:
            year = arg
        i += 1
    
    if csv_file is None:
        # No CSV provided - find the latest author_list CSV
        import glob
        pattern = os.path.join(DB_DIR, 'author_list_*.csv')
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            print("Usage: python3 get_wp.py <author_list_csv> [year] [--N] [--clean]")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv 2024")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv 2024 --50")
            print("Example: python3 get_wp.py author_list_top3_2024_top250_*.csv 2024 --clean")
            print("\nFlags:")
            print("  --N      Limit to first N authors")
            print("  --clean  Remove all old working papers before adding new ones")
            print("\nError: No author_list CSV files found in ../out/data/")
            sys.exit(1)
        
        csv_file = max(csv_files, key=os.path.getmtime)
        print(f"No CSV provided - using latest author list: {os.path.basename(csv_file)}")
    
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
    
    # Limit authors if --N flag provided
    if max_authors is not None and max_authors < len(authors):
        authors = authors[:max_authors]
        print(f"Limiting to first {max_authors} authors (from {len(read_author_list(csv_file))} total)")
    
    print(f"Processing {len(authors)} authors")
    
    year_label = f" ({year})" if year else " (all years)"
    print(f"Fetching working papers{year_label}...")

    all_papers = []
    total_authors = len(authors)

    # Thread-safe counter for progress
    progress_lock = threading.Lock()
    completed_count = [0]  # Use list to allow modification in nested function

    mailto = os.getenv("OPENALEX_MAILTO")

    def fetch_author_papers(author):
        """Fetch papers for a single author (thread worker)"""
        author_name = author['name']
        author_id = author.get('author_id')

        # Check if author has multiple IDs (comma-separated)
        if author_id and ',' in author_id:
            author_ids = [aid.strip() for aid in author_id.split(',')]

            # Fetch papers for each ID and aggregate (deduplicate)
            seen_ids = set()
            aggregated_papers = []

            for aid in author_ids:
                papers = fetch_working_papers_for_author(author_name, aid, year)
                for paper in papers:
                    if paper['openalex_id'] not in seen_ids:
                        seen_ids.add(paper['openalex_id'])
                        aggregated_papers.append(paper)

            return author_name, aggregated_papers
        else:
            # Single ID or no ID
            papers = fetch_working_papers_for_author(author_name, author_id, year)
            return author_name, papers

    # Determine number of workers based on mailto (rate limit)
    # 200ms delay - retry on 429 with exponential backoff
    max_workers = 10

    print(f"Using {max_workers} parallel workers" + (" (polite pool)" if mailto else " (rate-limited)") + "...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks with staggered start (100ms between each)
        future_to_author = {}
        for i, author in enumerate(authors):
            if i > 0 and i < max_workers:
                time.sleep(0.1)  # Stagger first batch of workers
            future_to_author[executor.submit(fetch_author_papers, author)] = author

        # Process results as they complete
        for future in as_completed(future_to_author):
            author = future_to_author[future]
            try:
                author_name, papers = future.result()
                all_papers.extend(papers)

                with progress_lock:
                    completed_count[0] += 1
                    print(f"[{completed_count[0]}/{total_authors}] {author_name}: {len(papers)} working papers")
            except Exception as e:
                with progress_lock:
                    completed_count[0] += 1
                    print(f"[{completed_count[0]}/{total_authors}] {author['name']}: Error - {e}")
    
    print(f"\nTotal working papers found: {len(all_papers)}")
    
    if all_papers:
        db_filename = f"working_papers{f'_{year}' if year else ''}.db"
        new_count, dup_count, updated_count = save_working_papers_to_db(all_papers, db_filename, clean)

        db_path = os.path.join(DB_DIR, db_filename)
        if clean:
            print(f"\n💾 Saved {new_count} working papers to {db_path} (database cleaned)")
        else:
            print(f"\n💾 Saved {new_count} new working papers to {db_path}")
            if updated_count > 0:
                print(f"✏️  Updated {updated_count} papers with additional co-authors")
            if dup_count > 0:
                print(f"🔄 Skipped {dup_count} duplicate working papers")
    else:
        print("\nNo working papers found.")

if __name__ == "__main__":
    main()
