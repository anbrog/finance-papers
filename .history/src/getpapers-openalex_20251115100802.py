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
        'source_id': 'S4210180129'
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
            INSERT INTO openalex_articles (openalex_id, title, publication_date, doi, authors_json, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            openalex_id,
            article['title'],
            article['publication_date'],
            article['doi'],
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
    print(f"Fetching articles with filters: {FILTERS}")
    
    articles = []
    for article in fetch_articles():
        articles.append(article)
    
    print(f"\nTotal articles fetched: {len(articles)}")
    
    if articles:
        save_to_db(articles)