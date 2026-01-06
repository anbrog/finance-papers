import requests
import sqlite3
import json
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE_URL = "https://api.openalex.org/works"

# Journal configurations
JOURNALS = {
    # Top 3 Finance Journals
    'jf': {
        'name': 'The Journal of Finance',
        'source_id': 'S5353659'
    },
    'rfs': {
        'name': 'Review of Financial Studies',
        'source_id': 'S170137484'
    },
    'jfe': {
        'name': 'Journal of Financial Economics',
        'source_id': 'S149240962'
    },
    # Top 5 Economics Journals
    'qje': {
        'name': 'The Quarterly Journal of Economics',
        'source_id': 'S203860005'
    },
    'aer': {
        'name': 'American Economic Review',
        'source_id': 'S23254222'
    },
    'ecma': {
        'name': 'Econometrica',
        'source_id': 'S95464858'
    },
    'jpe': {
        'name': 'Journal of Political Economy',
        'source_id': 'S95323914'
    },
    'restud': {
        'name': 'The Review of Economic Studies',
        'source_id': 'S88935262'
    }
}

# Journal group definitions
JOURNAL_GROUPS = {
    'top3': ['jf', 'rfs', 'jfe'],  # Top 3 Finance journals
    'econ5': ['qje', 'aer', 'ecma', 'jpe', 'restud'],  # Top 5 Economics journals
    'alltop': ['jf', 'rfs', 'jfe', 'qje', 'aer', 'ecma', 'jpe', 'restud'],  # All top journals
}

def get_filters(journal_key, year):
    """Generate filters for a specific journal and year"""
    if journal_key not in JOURNALS:
        raise ValueError(f"Unknown journal: {journal_key}. Available: {', '.join(JOURNALS.keys())}")
    
    return f"primary_location.source.id:{JOURNALS[journal_key]['source_id']},publication_year:{year}"

def fetch_articles(filters):
    """Fetch articles from OpenAlex API with given filters"""
    cursor = "*"
    mailto = os.getenv("OPENALEX_MAILTO")
    while cursor:
        params = {"filter": filters, "per-page": 200, "cursor": cursor}
        # Optional: include a mailto to be a good API citizen
        if mailto:
            params["mailto"] = mailto

        resp = requests.get(BASE_URL, params=params, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            # Print API error details to help diagnose bad filters or params
            content = None
            try:
                content = resp.json()
            except Exception:
                content = resp.text
            raise SystemExit(f"OpenAlex API error {resp.status_code}: {content}")
        data = resp.json()
        results = data.get("results", [])
        print(f"Fetched {len(results)} results in this batch", flush=True)
        for work in results:
            # Extract abstract from inverted index
            abstract_inverted = work.get("abstract_inverted_index", {})
            abstract_text = ""
            if abstract_inverted:
                # Reconstruct abstract from inverted index
                word_positions = []
                for word, positions in abstract_inverted.items():
                    for pos in positions:
                        word_positions.append((pos, word))
                word_positions.sort()
                abstract_text = " ".join([word for _, word in word_positions])
            
            # Extract topics (OpenAlex provides these as classified concepts)
            topics = work.get("topics", [])
            topics_data = [
                {
                    "name": t.get("display_name"),
                    "score": t.get("score"),
                    "subfield": t.get("subfield", {}).get("display_name") if t.get("subfield") else None,
                    "field": t.get("field", {}).get("display_name") if t.get("field") else None,
                }
                for t in topics[:5]  # Keep top 5 topics
            ]

            yield {
                "id": work.get("id"),
                "title": work.get("title"),
                "publication_date": work.get("publication_date"),
                "doi": work.get("doi"),
                "cited_by_count": work.get("cited_by_count", 0),
                "abstract": abstract_text,
                "topics": topics_data,
                "authors": [
                    {
                        "name": auth.get("author", {}).get("display_name"),
                        "orcid": auth.get("author", {}).get("orcid"),
                        "author_id": auth.get("author", {}).get("id"),
                        "institutions": [
                            inst.get("display_name") for inst in auth.get("institutions", [])
                        ],
                    }
                    for auth in work.get("authorships", [])
                ],
            }
        cursor = data.get("meta", {}).get("next_cursor")

def save_to_db(articles, db_filename='openalex_articles.db', force_update=False):
    """Save OpenAlex articles to SQLite database"""
    # Create output directory - use path relative to project root
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, 'out', 'data')
    os.makedirs(output_dir, exist_ok=True)
    db_filepath = os.path.join(output_dir, db_filename)
    
    # Connect to database
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS openalex_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            cited_by_count INTEGER DEFAULT 0,
            abstract TEXT,
            authors_json TEXT,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add topics_json column if it doesn't exist (for existing databases)
    try:
        cursor.execute('ALTER TABLE openalex_articles ADD COLUMN topics_json TEXT')
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_openalex_id ON openalex_articles(openalex_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_doi ON openalex_articles(doi)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_publication_date ON openalex_articles(publication_date)')
    
    new_count = 0
    duplicate_count = 0
    updated_count = 0
    
    for article in articles:
        openalex_id = article['id']
        
        # Check for duplicates
        cursor.execute('SELECT id FROM openalex_articles WHERE openalex_id = ?', (openalex_id,))
        existing = cursor.fetchone()
        
        if existing:
            if force_update:
                # Update existing record with new citation count, abstract, authors, and topics
                cursor.execute('''
                    UPDATE openalex_articles
                    SET cited_by_count = ?, abstract = ?, authors_json = ?, topics_json = ?, scraped_at = ?
                    WHERE openalex_id = ?
                ''', (
                    article.get('cited_by_count', 0),
                    article.get('abstract', ''),
                    json.dumps(article['authors']),
                    json.dumps(article.get('topics', [])),
                    datetime.now().isoformat(),
                    openalex_id
                ))
                updated_count += 1
            else:
                duplicate_count += 1
            continue

        # Insert article
        cursor.execute('''
            INSERT INTO openalex_articles (openalex_id, title, publication_date, doi, cited_by_count, abstract, authors_json, topics_json, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            openalex_id,
            article['title'],
            article['publication_date'],
            article['doi'],
            article.get('cited_by_count', 0),
            article.get('abstract', ''),
            json.dumps(article['authors']),
            json.dumps(article.get('topics', [])),
            datetime.now().isoformat()
        ))
        new_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n💾 Saved {new_count} new articles to {db_filepath}")
    if updated_count > 0:
        print(f"🔄 Updated {updated_count} existing articles")
    if duplicate_count > 0:
        print(f"⏭️  Skipped {duplicate_count} duplicate articles")
    
    return new_count, duplicate_count, updated_count


def main():
    """Main entry point for console script"""
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: get-papers <journal> [year] [--force]")
        print(f"Available journals: {', '.join(JOURNALS.keys())}")
        print(f"Journal groups: {', '.join(JOURNAL_GROUPS.keys())}")
        print("Example: get-papers jf 2024")
        print("Example: get-papers top3 2024 --force")
        print("Example: get-papers alltop 2024  # All finance + econ journals")
        print("\nOptions:")
        print("  --force    Update existing articles with new citation counts")
        sys.exit(1)

    journal_key = sys.argv[1].lower()
    year = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else "2024"
    force_update = '--force' in sys.argv

    # Handle journal groups (top3, econ5, alltop)
    if journal_key in JOURNAL_GROUPS:
        journals_to_fetch = JOURNAL_GROUPS[journal_key]
    elif journal_key in JOURNALS:
        journals_to_fetch = [journal_key]
    else:
        print(f"Error: Unknown journal '{journal_key}'")
        print(f"Available journals: {', '.join(JOURNALS.keys())}")
        print(f"Journal groups: {', '.join(JOURNAL_GROUPS.keys())}")
        sys.exit(1)
    
    # Thread-safe print lock
    print_lock = threading.Lock()

    def fetch_journal_articles(jkey):
        """Fetch articles for a single journal (thread worker)"""
        filters = get_filters(jkey, year)
        journal_name = JOURNALS[jkey]['name']

        with print_lock:
            print(f"[{jkey.upper()}] Starting fetch for {journal_name} ({year})...")

        articles = list(fetch_articles(filters))

        return jkey, journal_name, articles

    # Use parallel fetching if multiple journals
    mailto = os.getenv("OPENALEX_MAILTO")
    max_workers = min(len(journals_to_fetch), 8 if mailto else 3)  # More workers with mailto

    if len(journals_to_fetch) > 1:
        print(f"\nFetching {len(journals_to_fetch)} journals in parallel ({max_workers} workers)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_journal_articles, jkey): jkey for jkey in journals_to_fetch}

            for future in as_completed(futures):
                jkey, journal_name, articles = future.result()

                with print_lock:
                    print(f"\n{'='*80}")
                    print(f"[{jkey.upper()}] Completed: {journal_name} ({year})")
                    print(f"Total articles fetched: {len(articles)}")

                    if articles:
                        # Show example
                        article = articles[0]
                        print(f"  Example: {article['title'][:60]}...")
                        authors = [a['name'] for a in article['authors'] if a['name']]
                        print(f"  Authors: {', '.join(authors[:2])}{' ...' if len(authors) > 2 else ''}")
                    print('='*80)

                if articles:
                    db_filename = f'openalex_{jkey}_{year}.db'
                    save_to_db(articles, db_filename, force_update=force_update)
    else:
        # Single journal - fetch sequentially with verbose output
        jkey = journals_to_fetch[0]
        filters = get_filters(jkey, year)
        journal_name = JOURNALS[jkey]['name']

        print(f"\n{'='*80}")
        print(f"Fetching articles from {journal_name} ({year})")
        print(f"Filters: {filters}")
        print('='*80)

        articles = []
        for article in fetch_articles(filters):
            articles.append(article)
            # Print first 3 articles as examples
            if len(articles) <= 3:
                print(f"\nExample {len(articles)}:")
                print(f"  Title: {article['title']}")
                print(f"  Date: {article['publication_date']}")
                authors = [a['name'] for a in article['authors'] if a['name']]
                author_str = ', '.join(authors[:3])
                if len(authors) > 3:
                    author_str += f", ... ({len(authors)} total)"
                print(f"  Authors: {author_str}")

        print(f"\nTotal articles fetched: {len(articles)}")

        if articles:
            db_filename = f'openalex_{jkey}_{year}.db'
            save_to_db(articles, db_filename, force_update=force_update)


if __name__ == "__main__":
    main()
