import requests
import sqlite3
import json
import os
import sys
from datetime import datetime

BASE_URL = "https://api.openalex.org/works"

# Journal configurations
JOURNALS = {
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
    }
}

def get_filters(journal_key, year):
    """Generate filters for a specific journal and year"""
    if journal_key not in JOURNALS:
        raise ValueError(f"Unknown journal: {journal_key}. Available: {', '.join(JOURNALS.keys())}")
    
    return f"primary_location.source.id:{JOURNALS[journal_key]['source_id']},publication_year:{year}"

def fetch_articles(filters):
    """Fetch articles from OpenAlex API with given filters"""
    cursor = "*"
    while cursor:
        params = {"filter": filters, "per-page": 200, "cursor": cursor}
        # Optional: include a mailto to be a good API citizen (read from env if set)
        import os
        mailto = os.getenv("OPENALEX_MAILTO")
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
            yield {
                "id": work.get("id"),
                "title": work.get("title"),
                "publication_date": work.get("publication_date"),
                "doi": work.get("doi"),
                "cited_by_count": work.get("cited_by_count", 0),
                "authors": [
                    {
                        "name": auth.get("author", {}).get("display_name"),
                        "orcid": auth.get("author", {}).get("orcid"),
                        "institutions": [
                            inst.get("display_name") for inst in auth.get("institutions", [])
                        ],
                    }
                    for auth in work.get("authorships", [])
                ],
            }
        cursor = data.get("meta", {}).get("next_cursor")

def save_to_db(articles, db_filename='openalex_articles.db'):
    """Save OpenAlex articles to SQLite database"""
    # Create output directory
    output_dir = '../out/data'
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
            authors_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_openalex_id ON openalex_articles(openalex_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_doi ON openalex_articles(doi)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_publication_date ON openalex_articles(publication_date)')
    
    new_count = 0
    duplicate_count = 0
    
    for article in articles:
        openalex_id = article['id']
        
        # Check for duplicates
        cursor.execute('SELECT id FROM openalex_articles WHERE openalex_id = ?', (openalex_id,))
        if cursor.fetchone():
            duplicate_count += 1
            continue
        
        # Insert article
        cursor.execute('''
            INSERT INTO openalex_articles (openalex_id, title, publication_date, doi, cited_by_count, authors_json, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            openalex_id,
            article['title'],
            article['publication_date'],
            article['doi'],
            article.get('cited_by_count', 0),
            json.dumps(article['authors']),
            datetime.now().isoformat()
        ))
        new_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n💾 Saved {new_count} new articles to {db_filepath}")
    if duplicate_count > 0:
        print(f"🔄 Skipped {duplicate_count} duplicate articles")
    
    return new_count, duplicate_count

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 getpapers-openalex.py <journal> [year]")
        print(f"Available journals: {', '.join(JOURNALS.keys())}, top3")
        print("Example: python3 getpapers-openalex.py jf 2024")
        print("Example: python3 getpapers-openalex.py top3 2024")
        sys.exit(1)
    
    journal_key = sys.argv[1].lower()
    year = sys.argv[2] if len(sys.argv) > 2 else "2024"
    
    # Handle 'top3' to fetch all journals
    if journal_key == 'top3':
        journals_to_fetch = list(JOURNALS.keys())
    elif journal_key in JOURNALS:
        journals_to_fetch = [journal_key]
    else:
        print(f"Error: Unknown journal '{journal_key}'")
        print(f"Available journals: {', '.join(JOURNALS.keys())}, top3")
        sys.exit(1)
    
    # Fetch articles for each journal
    for jkey in journals_to_fetch:
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
            # Use journal-specific database filename
            db_filename = f'openalex_{jkey}_{year}.db'
            save_to_db(articles, db_filename)