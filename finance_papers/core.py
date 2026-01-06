"""Finance papers - fetch and analyze top journal publications.

A simple tool to:
1. Fetch articles from top finance/economics journals via OpenAlex API
2. Store them in SQLite databases
3. Rank authors by publication count
4. Fetch working papers for top authors
"""

import sqlite3
import json
import os
import csv
import glob
import shutil
import requests
import time
import threading
import unicodedata
from pathlib import Path
from contextlib import contextmanager
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator, Optional

# =============================================================================
# CONFIGURATION
# =============================================================================

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DB_DIR = PROJECT_ROOT / 'out' / 'data'
CONFIG_DIR = Path.home() / '.finance-papers'
CONTEXT_FILE = CONFIG_DIR / 'context.json'
ENV_FILE = PROJECT_ROOT / '.env'

# OpenAlex API
OPENALEX_BASE_URL = "https://api.openalex.org"
OPENALEX_MAILTO = os.environ.get('OPENALEX_MAILTO')

# Journal definitions
JOURNALS = {
    # Top 3 Finance
    'jf': {'name': 'The Journal of Finance', 'source_id': 'S5353659'},
    'rfs': {'name': 'Review of Financial Studies', 'source_id': 'S170137484'},
    'jfe': {'name': 'Journal of Financial Economics', 'source_id': 'S149240962'},
    # Top 5 Economics
    'qje': {'name': 'The Quarterly Journal of Economics', 'source_id': 'S203860005'},
    'aer': {'name': 'American Economic Review', 'source_id': 'S23254222'},
    'ecma': {'name': 'Econometrica', 'source_id': 'S95464858'},
    'jpe': {'name': 'Journal of Political Economy', 'source_id': 'S95323914'},
    'restud': {'name': 'The Review of Economic Studies', 'source_id': 'S88935262'},
}

JOURNAL_GROUPS = {
    'top3': ['jf', 'rfs', 'jfe'],
    'econ5': ['qje', 'aer', 'ecma', 'jpe', 'restud'],
    'alltop': ['jf', 'rfs', 'jfe', 'qje', 'aer', 'ecma', 'jpe', 'restud'],
}

# Author name normalization (variants -> canonical)
AUTHOR_NAME_FIXES = {
    'Jules H. van Binsbergen': 'Jules van Binsbergen',
    'ANTOINETTE SCHOAR': 'Antoinette Schoar',
}

# Authors to highlight in output
HIGHLIGHTED_AUTHORS = ['Andreas Brøgger']


# =============================================================================
# DATA TYPES
# =============================================================================

@dataclass
class Author:
    name: str
    openalex_id: Optional[str] = None
    paper_count: int = 0
    citations: int = 0
    affiliation: Optional[str] = None
    latest_paper: tuple = ('', '')  # (date, title)


@dataclass
class Paper:
    title: str
    authors: list
    year: Optional[int] = None
    pub_date: Optional[str] = None
    citations: int = 0
    abstract: Optional[str] = None
    doi: Optional[str] = None
    openalex_id: Optional[str] = None
    topics: list = field(default_factory=list)
    journal: Optional[str] = None


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

@contextmanager
def db_connection(path: Path):
    """Context manager for database connections."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def ensure_db_dir():
    """Ensure output directory exists."""
    DB_DIR.mkdir(parents=True, exist_ok=True)


def get_db_files(journals: list = None, years: list = None) -> list:
    """Find database files matching journal/year filters."""
    ensure_db_dir()

    if journals is None and years is None:
        # Default: all databases
        return list(DB_DIR.glob('openalex_*.db'))

    # Expand journal groups
    if journals:
        expanded = []
        for j in journals:
            if j in JOURNAL_GROUPS:
                expanded.extend(JOURNAL_GROUPS[j])
            else:
                expanded.append(j)
        journals = expanded

    db_files = []
    if journals and years:
        for j in journals:
            for y in years:
                path = DB_DIR / f'openalex_{j}_{y}.db'
                if path.exists():
                    db_files.append(path)
    elif journals:
        for j in journals:
            db_files.extend(DB_DIR.glob(f'openalex_{j}_*.db'))
    elif years:
        for y in years:
            db_files.extend(DB_DIR.glob(f'openalex_*_{y}.db'))

    return sorted(set(db_files))


def iter_articles(db_files: list) -> Iterator[dict]:
    """Iterate over all articles in given databases."""
    for db_file in db_files:
        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT openalex_id, title, publication_date, doi,
                       cited_by_count, abstract, authors_json, topics_json
                FROM openalex_articles
            ''')
            for row in cursor:
                yield {
                    'openalex_id': row['openalex_id'],
                    'title': row['title'],
                    'pub_date': row['publication_date'],
                    'doi': row['doi'],
                    'citations': row['cited_by_count'] or 0,
                    'abstract': row['abstract'],
                    'authors': json.loads(row['authors_json']) if row['authors_json'] else [],
                    'topics': json.loads(row['topics_json']) if row['topics_json'] else [],
                    'db_file': db_file,
                }


def save_articles(articles: list, journal: str, year: int, force_update: bool = False):
    """Save articles to database."""
    ensure_db_dir()
    db_path = DB_DIR / f'openalex_{journal}_{year}.db'

    with db_connection(db_path) as conn:
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

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_openalex_id ON openalex_articles(openalex_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_doi ON openalex_articles(doi)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_publication_date ON openalex_articles(publication_date)')

        new_count = 0
        updated_count = 0

        for article in articles:
            openalex_id = article['id']

            cursor.execute('SELECT id FROM openalex_articles WHERE openalex_id = ?', (openalex_id,))
            existing = cursor.fetchone()

            if existing:
                if force_update:
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
                cursor.execute('''
                    INSERT INTO openalex_articles
                    (openalex_id, title, publication_date, doi, cited_by_count, abstract, authors_json, topics_json, scraped_at)
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

    return new_count, updated_count


# =============================================================================
# UNIFIED PAPERS DATABASE
# =============================================================================

UNIFIED_DB_PATH = DB_DIR / 'papers.db'


def _create_unified_schema(conn):
    """Create the unified papers table schema."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            author_name TEXT,
            author_affiliation TEXT,
            author_openalex_id TEXT,
            source TEXT,
            journal TEXT,
            cited_by_count INTEGER DEFAULT 0,
            abstract TEXT,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(openalex_id, author_name)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_papers_openalex ON papers(openalex_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_papers_author ON papers(author_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_papers_source ON papers(source)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_papers_date ON papers(publication_date)')
    conn.commit()


def build_unified_db(rebuild: bool = False):
    """Build unified papers database from article DBs and working papers.

    Args:
        rebuild: If True, drop and recreate the table. If False, only add new entries.
    """
    ensure_db_dir()

    with db_connection(UNIFIED_DB_PATH) as conn:
        cursor = conn.cursor()

        if rebuild:
            cursor.execute('DROP TABLE IF EXISTS papers')
            conn.commit()

        _create_unified_schema(conn)

        # Import articles from all article DBs
        article_count = 0
        article_dbs = list(DB_DIR.glob('openalex_*_*.db'))
        for db_file in article_dbs:
            # Extract journal from filename: openalex_{journal}_{year}.db
            parts = db_file.stem.split('_')
            if len(parts) >= 3:
                journal = parts[1]
            else:
                journal = None

            with db_connection(db_file) as article_conn:
                article_cursor = article_conn.cursor()
                article_cursor.execute('''
                    SELECT openalex_id, title, publication_date, doi,
                           cited_by_count, abstract, authors_json, topics_json, scraped_at
                    FROM openalex_articles
                ''')
                for row in article_cursor:
                    authors = json.loads(row['authors_json']) if row['authors_json'] else []
                    for author in authors:
                        author_name = author.get('name')
                        if not author_name:
                            continue
                        author_name = normalize_name(author_name)
                        institutions = author.get('institutions', [])
                        affiliation = institutions[0] if institutions else ''
                        author_id = author.get('id')

                        try:
                            cursor.execute('''
                                INSERT OR IGNORE INTO papers
                                (openalex_id, title, publication_date, doi, author_name,
                                 author_affiliation, author_openalex_id, source, journal,
                                 cited_by_count, abstract, topics_json, scraped_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                row['openalex_id'],
                                row['title'],
                                row['publication_date'],
                                row['doi'],
                                author_name,
                                affiliation,
                                author_id,
                                'article',
                                journal,
                                row['cited_by_count'] or 0,
                                row['abstract'],
                                row['topics_json'],
                                row['scraped_at'],
                            ))
                            if cursor.rowcount > 0:
                                article_count += 1
                        except Exception:
                            pass

        conn.commit()
        print(f"Imported {article_count} author-paper entries from articles")

        # Import working papers
        wp_count = 0
        wp_db = DB_DIR / 'working_papers.db'
        if wp_db.exists():
            with db_connection(wp_db) as wp_conn:
                wp_cursor = wp_conn.cursor()
                wp_cursor.execute('''
                    SELECT openalex_id, title, publication_date, doi,
                           author_name, author_affiliation, cited_by_count,
                           topics_json, scraped_at
                    FROM working_papers
                    WHERE doi NOT LIKE '%10.1257/rct%' OR doi IS NULL
                ''')
                for row in wp_cursor:
                    author_name = row['author_name']
                    if not author_name:
                        continue
                    author_name = normalize_name(author_name)

                    try:
                        cursor.execute('''
                            INSERT OR IGNORE INTO papers
                            (openalex_id, title, publication_date, doi, author_name,
                             author_affiliation, author_openalex_id, source, journal,
                             cited_by_count, abstract, topics_json, scraped_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            row['openalex_id'],
                            row['title'],
                            row['publication_date'],
                            row['doi'],
                            author_name,
                            row['author_affiliation'] or '',
                            None,
                            'working-paper',
                            None,
                            row['cited_by_count'] or 0,
                            None,
                            row['topics_json'],
                            row['scraped_at'],
                        ))
                        if cursor.rowcount > 0:
                            wp_count += 1
                    except Exception:
                        pass

        conn.commit()
        print(f"Imported {wp_count} working paper entries")
        print(f"Total: {article_count + wp_count} entries in unified database")


def iter_unified_papers(source: str = None, journals: list = None, years: list = None) -> Iterator[dict]:
    """Iterate over papers in unified database.

    Args:
        source: Filter by 'article' or 'working-paper'
        journals: Filter by journal codes (articles only)
        years: Filter by publication years
    """
    if not UNIFIED_DB_PATH.exists():
        return

    with db_connection(UNIFIED_DB_PATH) as conn:
        cursor = conn.cursor()

        # Build query with filters
        query = '''
            SELECT openalex_id, title, publication_date, doi, author_name,
                   author_affiliation, source, journal, cited_by_count, topics_json
            FROM papers
            WHERE (doi NOT LIKE '%10.1257/rct%' OR doi IS NULL)
        '''
        params = []

        if source:
            query += ' AND source = ?'
            params.append(source)

        if journals:
            # Expand journal groups
            expanded = []
            for j in journals:
                if j in JOURNAL_GROUPS:
                    expanded.extend(JOURNAL_GROUPS[j])
                else:
                    expanded.append(j)
            placeholders = ','.join('?' * len(expanded))
            query += f' AND (journal IN ({placeholders}) OR source = ?)'
            params.extend(expanded)
            params.append('working-paper')  # Include WPs when filtering journals

        if years:
            year_conditions = ' OR '.join(['publication_date LIKE ?' for _ in years])
            query += f' AND ({year_conditions})'
            params.extend([f'{y}%' for y in years])

        query += ' ORDER BY publication_date DESC'

        cursor.execute(query, params)
        for row in cursor:
            topics = []
            if row['topics_json']:
                try:
                    topics = json.loads(row['topics_json'])
                except json.JSONDecodeError:
                    pass

            yield {
                'openalex_id': row['openalex_id'],
                'title': row['title'],
                'pub_date': row['publication_date'],
                'doi': row['doi'],
                'name': row['author_name'],
                'affiliation': row['author_affiliation'] or '',
                'source': row['source'],
                'journal': row['journal'],
                'citations': row['cited_by_count'] or 0,
                'topics': topics,
            }


# =============================================================================
# OPENALEX API
# =============================================================================

# Rate limiting
_last_request_time = 0
_request_lock = threading.Lock()


def _rate_limited_request(url: str, params: dict = None, timeout: int = 30, max_retries: int = 3):
    """Make a rate-limited request with retry logic."""
    global _last_request_time

    for attempt in range(max_retries):
        with _request_lock:
            now = time.time()
            elapsed = now - _last_request_time
            if elapsed < 0.2:  # 200ms minimum between requests
                time.sleep(0.2 - elapsed)
            _last_request_time = time.time()

        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait_time = 2 ** (attempt + 2)
                print(f"Rate limited, waiting {wait_time}s...", flush=True)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)

    return None


def reconstruct_abstract(inverted_index: dict) -> str:
    """Reconstruct abstract from OpenAlex inverted index format."""
    if not inverted_index:
        return ""
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort()
    return " ".join([word for _, word in word_positions])


def fetch_journal_articles(journal: str, year: int, force: bool = False) -> list:
    """Fetch articles from OpenAlex for a journal/year."""
    if journal not in JOURNALS:
        raise ValueError(f"Unknown journal: {journal}. Available: {', '.join(JOURNALS.keys())}")

    source_id = JOURNALS[journal]['source_id']
    filters = f"primary_location.source.id:{source_id},publication_year:{year}"

    articles = []
    cursor = "*"

    while cursor:
        params = {"filter": filters, "per-page": 200, "cursor": cursor}
        if OPENALEX_MAILTO:
            params["mailto"] = OPENALEX_MAILTO

        resp = _rate_limited_request(f"{OPENALEX_BASE_URL}/works", params=params)
        if not resp:
            break

        data = resp.json()
        results = data.get("results", [])

        for work in results:
            abstract = reconstruct_abstract(work.get("abstract_inverted_index", {}))
            topics = [
                {
                    "name": t.get("display_name"),
                    "score": t.get("score"),
                    "subfield": t.get("subfield", {}).get("display_name") if t.get("subfield") else None,
                    "field": t.get("field", {}).get("display_name") if t.get("field") else None,
                }
                for t in work.get("topics", [])[:5]
            ]

            articles.append({
                "id": work.get("id"),
                "title": work.get("title"),
                "publication_date": work.get("publication_date"),
                "doi": work.get("doi"),
                "cited_by_count": work.get("cited_by_count", 0),
                "abstract": abstract,
                "topics": topics,
                "authors": [
                    {
                        "name": auth.get("author", {}).get("display_name"),
                        "orcid": auth.get("author", {}).get("orcid"),
                        "author_id": auth.get("author", {}).get("id"),
                        "institutions": [inst.get("display_name") for inst in auth.get("institutions", [])],
                    }
                    for auth in work.get("authorships", [])
                ],
            })

        cursor = data.get("meta", {}).get("next_cursor")
        print(f"Fetched {len(articles)} articles...", end='\r', flush=True)

    print(f"Fetched {len(articles)} articles from {JOURNALS[journal]['name']} ({year})")
    return articles


def fetch_author_works(author_id: str, from_year: int = None) -> list:
    """Fetch working papers for an author."""
    if author_id.startswith('https://openalex.org/'):
        author_id = author_id.split('/')[-1]

    all_papers = []

    # Fetch non-article works and SSRN papers
    for type_filter in ["type:!article", "primary_location.source.id:S4210172589"]:
        work_filters = f"authorships.author.id:{author_id},{type_filter}"
        if from_year:
            work_filters += f",from_publication_date:{from_year - 1}-01-01"

        cursor = "*"
        while cursor:
            params = {"filter": work_filters, "per-page": 200, "cursor": cursor}
            if OPENALEX_MAILTO:
                params["mailto"] = OPENALEX_MAILTO

            resp = _rate_limited_request(f"{OPENALEX_BASE_URL}/works", params=params)
            if not resp:
                break

            data = resp.json()
            for work in data.get("results", []):
                # Skip AEA RCT Registry entries (trial registrations, not papers)
                doi = work.get("doi") or ""
                if "10.1257/rct" in doi:
                    continue

                all_papers.append({
                    "openalex_id": work.get("id"),
                    "title": work.get("title"),
                    "publication_date": work.get("publication_date"),
                    "doi": doi,
                    "type": work.get("type"),
                    "cited_by_count": work.get("cited_by_count", 0),
                })

            cursor = data.get("meta", {}).get("next_cursor")

    return all_papers


# =============================================================================
# AUTHOR RANKING
# =============================================================================

def normalize_name(name: str) -> str:
    """Normalize author name using mapping."""
    return AUTHOR_NAME_FIXES.get(name, name)


def normalize_for_search(text: str) -> str:
    """Normalize text for search matching.

    Converts special characters to ASCII equivalents:
    - ø -> o (so 'brogger' matches 'brøgger')
    - Also handles 'oe' -> 'o' (so 'broegger' matches 'brøgger')
    """
    if not text:
        return ''
    # First convert to lowercase
    text = text.lower()
    # Replace special characters that don't decompose well
    replacements = {
        'ø': 'o', 'œ': 'o', 'ö': 'o',
        'æ': 'a', 'ä': 'a', 'å': 'a',
        'ü': 'u', 'ß': 'ss',
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    # Normalize remaining unicode (é -> e, etc.)
    normalized = unicodedata.normalize('NFKD', text)
    ascii_text = normalized.encode('ascii', 'ignore').decode('ascii')
    # Handle common transliterations: oe -> o (for broegger -> brogger)
    ascii_text = ascii_text.replace('oe', 'o').replace('ae', 'a')
    return ascii_text


def get_topic_counts(journals: list = None, years: list = None,
                     author: str = None, title: str = None) -> tuple:
    """Get all topics with paper counts, sorted by prevalence.

    Optionally filter by author and/or title to show relevant topic counts.

    Returns:
        tuple: (topic_counts dict, total_paper_count int)
    """
    db_files = get_db_files(journals, years)
    topic_counts = defaultdict(int)
    total_papers = 0

    for article in iter_articles(db_files):
        # Filter by author if specified
        if author:
            author_names = [(a.get('name') or '').lower() for a in article.get('authors', []) if a]
            if not any(author.lower() in name for name in author_names):
                continue

        # Filter by title if specified
        if title:
            article_title = article.get('title', '').lower()
            if title.lower() not in article_title:
                continue

        total_papers += 1
        for topic in article.get('topics', []):
            name = topic.get('name')
            if name:
                topic_counts[name] += 1

    return dict(sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)), total_papers


def rank_authors(journals: list = None, years: list = None, top_n: int = 250,
                 by_citations: bool = False, topic: str = None, source: str = None) -> list:
    """Rank authors by publication count or citations.

    Args:
        journals: Filter by journal codes (for articles)
        years: Filter by publication years
        top_n: Number of top authors to return
        by_citations: Sort by citations instead of paper count
        topic: Filter by topic (partial match)
        source: Filter by 'article' or 'working-paper' (None = both)
    """
    if not UNIFIED_DB_PATH.exists():
        print("Unified database not found. Run 'finance-papers update --build-db' first.")
        return []

    author_stats = defaultdict(lambda: {'count': 0, 'citations': 0, 'latest': ('', ''), 'affiliation': ''})

    # Initialize highlighted authors
    for name in HIGHLIGHTED_AUTHORS:
        author_stats[name]

    # Iterate over papers (years filtering done in SQL by iter_unified_papers)
    for item in iter_unified_papers(source=source, journals=journals, years=years):
        # Filter by topic if specified
        if topic:
            item_topics = [t.get('name', '').lower() for t in item.get('topics', [])]
            if not any(topic.lower() in t for t in item_topics):
                continue

        name = item.get('name')
        if not name:
            continue

        name = normalize_name(name)
        stats = author_stats[name]
        stats['count'] += 1
        stats['citations'] += item.get('citations', 0) or 0

        pub_date = item.get('pub_date') or ''
        if pub_date > stats['latest'][0]:
            stats['latest'] = (pub_date, item.get('title') or '')
            if item.get('affiliation'):
                stats['affiliation'] = item['affiliation']

    # Sort
    if by_citations:
        ranked = sorted(author_stats.items(),
                       key=lambda x: (x[1]['citations'], x[1]['count']), reverse=True)
    else:
        ranked = sorted(author_stats.items(),
                       key=lambda x: (x[1]['count'], x[1]['citations']), reverse=True)

    # Convert to Author objects
    return [
        Author(
            name=name,
            paper_count=stats['count'],
            citations=stats['citations'],
            latest_paper=stats['latest'],
            affiliation=stats['affiliation'],
        )
        for name, stats in ranked[:top_n]
    ]


def export_author_csv(authors: list, output_path: Path = None, journals: str = 'top3',
                      years: str = None, top_n: int = 250):
    """Export author list to CSV."""
    if output_path is None:
        ensure_db_dir()
        timestamp = datetime.now().strftime('%Y%m%d')
        year_label = f"_{years}" if years else ""
        output_path = DB_DIR / f'author_list_{journals}{year_label}_top{top_n}_{timestamp}.csv'

    # Get author IDs from database
    db_files = get_db_files([journals] if journals else None, [int(years)] if years else None)
    author_ids = {}

    for article in iter_articles(db_files):
        for author in article['authors']:
            name = normalize_name(author.get('name', ''))
            if name and author.get('author_id'):
                if name not in author_ids:
                    author_ids[name] = set()
                author_ids[name].add(author['author_id'])

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Rank', 'Author Name', 'Paper Count', 'Citations', 'Author ID'])

        for i, author in enumerate(authors, 1):
            ids = author_ids.get(author.name, set())
            author_id = ','.join(sorted(ids)) if ids else ''
            writer.writerow([i, author.name, author.paper_count, author.citations, author_id])

    print(f"Saved {len(authors)} authors to {output_path}")
    return output_path


# =============================================================================
# PAPER QUERIES
# =============================================================================

def search_papers(author: str = None, title: str = None, journals: list = None,
                  years: list = None, topic: str = None, limit: int = None,
                  source: str = 'articles') -> list:
    """Search papers with filters.

    Args:
        author: Filter by author name
        title: Filter by title keyword
        journals: Filter by journals (articles only)
        years: Filter by years
        topic: Filter by topic
        limit: Maximum number of papers to return
        source: 'articles' or 'working-papers'
    """
    # Get iterator for the source
    if source == 'working-papers':
        items = iter_working_papers()
    else:
        db_files = get_db_files(journals, years)
        items = iter_articles(db_files)

    papers = []
    author_search = normalize_for_search(author) if author else None
    for item in items:
        # Filter by author (normalize both search term and names for matching)
        if author_search:
            author_names = [normalize_for_search(normalize_name(a.get('name') or '')) for a in item['authors']]
            if not any(author_search in name for name in author_names):
                continue

        # Filter by year (for working papers; articles are pre-filtered by db file)
        if years and source == 'working-papers':
            pub_date = item.get('pub_date')
            if not pub_date:
                continue
            paper_year = int(pub_date[:4])
            if paper_year not in years:
                continue

        # Filter by title
        if title:
            if title.lower() not in (item['title'] or '').lower():
                continue

        # Filter by topic
        if topic:
            item_topics = [t.get('name', '').lower() for t in item.get('topics', [])]
            if not any(topic.lower() in t for t in item_topics):
                continue

        # Get journal from item or extract from db_file
        journal = item.get('journal')
        if journal is None and 'db_file' in item:
            db_name = item['db_file'].name
            journal = db_name.split('_')[1] if '_' in db_name else None

        papers.append(Paper(
            title=item['title'],
            authors=[a.get('name') for a in item['authors'] if a.get('name')],
            pub_date=item['pub_date'],
            year=int(item['pub_date'][:4]) if item['pub_date'] else None,
            citations=item['citations'],
            abstract=item.get('abstract'),
            doi=item['doi'],
            openalex_id=item['openalex_id'],
            topics=item.get('topics', []),
            journal=journal,
        ))

        if limit and len(papers) >= limit:
            break

    # Sort by date descending
    papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return papers


def _short_source(primary_location: str) -> str:
    """Convert primary_location to short source name."""
    if not primary_location:
        return 'WP'
    loc = primary_location.lower()
    if 'ssrn' in loc:
        return 'SSRN'
    if 'arxiv' in loc:
        return 'arXiv'
    if 'repec' in loc:
        return 'RePEc'
    if 'nber' in loc:
        return 'NBER'
    if 'dataverse' in loc:
        return 'Data'
    if 'biorxiv' in loc:
        return 'bioRxiv'
    if 'econstor' in loc:
        return 'EconStor'
    if 'zenodo' in loc:
        return 'Zenodo'
    # Check for journal names (might be published versions)
    if 'journal of finance' in loc:
        return 'JF'
    if 'review of financial studies' in loc:
        return 'RFS'
    if 'journal of financial economics' in loc:
        return 'JFE'
    if 'econometrica' in loc:
        return 'Ecma'
    if 'american economic review' in loc:
        return 'AER'
    return 'WP'


def _wp_row_to_paper(row) -> Paper:
    """Convert a working papers database row to a Paper object."""
    topics_json = row['topics_json'] if 'topics_json' in row.keys() else None
    topics = json.loads(topics_json) if topics_json else []
    primary_loc = row['primary_location'] if 'primary_location' in row.keys() else None
    openalex_id = row['openalex_id'] if 'openalex_id' in row.keys() else None
    return Paper(
        title=row['title'],
        authors=[row['author_name']] if row['author_name'] else [],
        pub_date=row['publication_date'],
        citations=row['cited_by_count'] or 0,
        doi=row['doi'],
        openalex_id=openalex_id,
        journal=_short_source(primary_loc),
        topics=topics,
    )


def _article_row_to_paper(row, journal: str = None) -> Paper:
    """Convert an articles database row to a Paper object."""
    authors_data = json.loads(row['authors_json']) if row['authors_json'] else []
    topics = json.loads(row['topics_json']) if row['topics_json'] else []
    pub_date = row['publication_date']
    abstract = row['abstract'] if 'abstract' in row.keys() else None
    return Paper(
        title=row['title'],
        authors=[a.get('name') for a in authors_data if a.get('name')],
        pub_date=pub_date,
        year=int(pub_date[:4]) if pub_date else None,
        citations=row['cited_by_count'] or 0,
        abstract=abstract,
        doi=row['doi'],
        openalex_id=row['openalex_id'],
        topics=topics,
        journal=journal,
    )


def iter_working_papers():
    """Iterate over all working papers in the database.

    Yields dicts with keys matching iter_articles format for unified processing.
    """
    db_path = DB_DIR / 'working_papers.db'
    if not db_path.exists():
        return

    with db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT openalex_id, title, publication_date, doi, cited_by_count,
                   author_name, primary_location, topics_json
            FROM working_papers
            WHERE doi NOT LIKE '%10.1257/rct%' OR doi IS NULL
            ORDER BY publication_date DESC
        ''')

        for row in cursor:
            topics = json.loads(row['topics_json']) if row['topics_json'] else []
            yield {
                'openalex_id': row['openalex_id'],
                'title': row['title'],
                'pub_date': row['publication_date'],
                'doi': row['doi'],
                'citations': row['cited_by_count'] or 0,
                'authors': [{'name': row['author_name']}] if row['author_name'] else [],
                'topics': topics,
                'abstract': None,
                'journal': _short_source(row['primary_location']),
            }


def get_author_papers(author_name: str, journals: list = None, years: list = None) -> list:
    """Get all papers by an author."""
    return search_papers(author=author_name, journals=journals, years=years, limit=1000)


def get_recent_papers(journals: list = None, years: list = None, limit: int = 20,
                      source: str = 'articles') -> list:
    """Get recently added papers, ordered by scraped_at timestamp.

    Args:
        journals: List of journal keys to filter (for articles)
        years: List of years to filter (for articles)
        limit: Maximum number of papers to return
        source: 'articles' or 'working-papers'

    Returns:
        List of Paper objects ordered by most recently added
    """
    config = _SOURCE_CONFIG.get(source, _SOURCE_CONFIG['articles'])
    db_files = _get_db_files_for_source(source, journals, years)
    if not db_files:
        return []

    # Collect papers from all dbs
    all_papers = []
    for db_file in db_files:
        journal = _get_journal_from_db(db_file, source)

        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT {config['columns']}
                FROM {config['table']}
                ORDER BY publication_date DESC
                LIMIT ?
            ''', (limit,))

            for row in cursor:
                all_papers.append(config['converter'](row, journal))

    # Sort by publication date descending and take top N
    all_papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return all_papers[:limit]


def get_last_update_timestamp(journal: str = None, year: int = None) -> Optional[str]:
    """Get the most recent scraped_at timestamp from the database.

    If journal/year specified, checks that specific db.
    Otherwise checks all article dbs.
    """
    if journal and year:
        db_path = DB_DIR / f'openalex_{journal}_{year}.db'
        if not db_path.exists():
            return None
        db_files = [db_path]
    else:
        db_files = get_db_files()

    latest = None
    for db_file in db_files:
        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MAX(scraped_at) FROM openalex_articles')
            row = cursor.fetchone()
            if row and row[0]:
                if latest is None or row[0] > latest:
                    latest = row[0]

    return latest


# Source configurations for unified paper queries
_SOURCE_CONFIG = {
    'working-papers': {
        'table': 'working_papers',
        'columns': 'openalex_id, title, publication_date, doi, author_name, type, cited_by_count, primary_location, topics_json',
        'converter': lambda row, _: _wp_row_to_paper(row),
    },
    'articles': {
        'table': 'openalex_articles',
        'columns': 'openalex_id, title, publication_date, doi, cited_by_count, abstract, authors_json, topics_json',
        'converter': _article_row_to_paper,
    },
}


def _get_db_files_for_source(source: str, journals: list = None, years: list = None) -> list:
    """Get database files for a given source."""
    if source == 'working-papers':
        db_path = DB_DIR / 'working_papers.db'
        return [db_path] if db_path.exists() else []
    else:
        return get_db_files(journals, years)


def _get_journal_from_db(db_file: Path, source: str) -> str:
    """Extract journal name from database file."""
    if source == 'working-papers':
        return None  # Working papers use primary_location instead
    return db_file.name.split('_')[1] if '_' in db_file.name else None


def get_papers_from_last_update(journals: list = None, years: list = None,
                                source: str = 'articles') -> list:
    """Get papers from the most recent update batch.

    Returns all papers that share the latest scraped_at date,
    representing the most recent update that added new papers.
    """
    config = _SOURCE_CONFIG.get(source, _SOURCE_CONFIG['articles'])
    db_files = _get_db_files_for_source(source, journals, years)
    if not db_files:
        return []

    # Find the global latest date across all matching dbs
    latest_date = None
    for db_file in db_files:
        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DATE(MAX(scraped_at)) FROM {config['table']}")
            row = cursor.fetchone()
            if row and row[0]:
                if latest_date is None or row[0] > latest_date:
                    latest_date = row[0]

    if not latest_date:
        return []

    # Get all papers from that date
    papers = []
    for db_file in db_files:
        journal = _get_journal_from_db(db_file, source)

        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT {config['columns']}
                FROM {config['table']}
                WHERE DATE(scraped_at) = ?
                ORDER BY publication_date DESC
            ''', (latest_date,))

            for row in cursor:
                papers.append(config['converter'](row, journal))

    papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return papers


def get_papers_added_since(timestamp: str, journals: list = None, years: list = None) -> list:
    """Get papers added after a given timestamp.

    Used to show newly added papers after an update.
    """
    db_files = get_db_files(journals, years)
    papers = []

    for db_file in db_files:
        journal = db_file.name.split('_')[1] if '_' in db_file.name else None

        with db_connection(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT openalex_id, title, publication_date, doi,
                       cited_by_count, abstract, authors_json, topics_json
                FROM openalex_articles
                WHERE scraped_at > ?
                ORDER BY publication_date DESC
            ''', (timestamp,))

            for row in cursor:
                papers.append(_article_row_to_paper(row, journal))

    return papers


# =============================================================================
# WORKING PAPERS
# =============================================================================

def read_author_csv(csv_path: Path) -> list:
    """Read authors from CSV file."""
    authors = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            authors.append(Author(
                name=row['Author Name'],
                openalex_id=row.get('Author ID', '').strip() or None,
                paper_count=int(row.get('Paper Count', 0)),
                citations=int(row.get('Citations', 0)),
            ))
    return authors


def save_working_papers(papers: list, db_filename: str = 'working_papers.db', clean: bool = False):
    """Save working papers to database."""
    ensure_db_dir()
    db_path = DB_DIR / db_filename

    with db_connection(db_path) as conn:
        cursor = conn.cursor()

        if clean:
            cursor.execute('DROP TABLE IF EXISTS working_papers')

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
                cited_by_count INTEGER DEFAULT 0,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_author ON working_papers(author_name)')

        new_count = 0
        for paper in papers:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO working_papers
                    (openalex_id, title, publication_date, doi, author_name, type, cited_by_count, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    paper['openalex_id'],
                    paper['title'],
                    paper['publication_date'],
                    paper['doi'],
                    paper.get('author_name', ''),
                    paper['type'],
                    paper['cited_by_count'],
                    datetime.now().isoformat()
                ))
                if cursor.rowcount > 0:
                    new_count += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    return new_count


def update_working_papers(authors: list, year: int = None, max_authors: int = None,
                          clean: bool = False):
    """Fetch and store working papers for authors."""
    if max_authors:
        authors = authors[:max_authors]

    all_papers = []

    def fetch_for_author(author):
        if author.openalex_id:
            papers = fetch_author_works(author.openalex_id, year)
            for p in papers:
                p['author_name'] = author.name
            return author.name, papers
        return author.name, []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_for_author, a): a for a in authors}

        for i, future in enumerate(as_completed(futures), 1):
            author_name, papers = future.result()
            all_papers.extend(papers)
            print(f"[{i}/{len(authors)}] {author_name}: {len(papers)} papers")

    if all_papers:
        db_name = f"working_papers{'_' + str(year) if year else ''}.db"
        new_count = save_working_papers(all_papers, db_name, clean)
        print(f"Saved {new_count} new working papers")

    return all_papers


def rank_by_working_papers(top_n: int = 250, years: list = None, topic: str = None) -> list:
    """Rank authors by working paper count.

    Args:
        top_n: Number of top authors to return
        years: Filter by publication years (list of ints)
        topic: Filter by topic name (partial match)
    """
    return rank_authors(top_n=top_n, years=years, topic=topic, source='working-paper')


# =============================================================================
# DISPLAY HELPERS
# =============================================================================

def paginate(items: list, page_size: int = None, formatter=None, header: str = None,
             chat_callback=None):
    """Display items with pagination.

    Args:
        items: List of items to display
        page_size: Items per page (default: fit terminal)
        formatter: Function to format each item
        header: Header to repeat on each page
        chat_callback: Optional function to call when 'c' is pressed (for chat)
    """
    if page_size is None:
        page_size = max(5, shutil.get_terminal_size().lines - 8)

    total_pages = (len(items) + page_size - 1) // page_size
    current_page = 0

    def display_page(page_num):
        """Display a single page."""
        start = page_num * page_size
        end = min(start + page_size, len(items))
        batch = items[start:end]

        # Clear screen and show header
        print("\033[2J\033[H", end="")  # Clear screen, move to top
        if header:
            print(header)

        for item in batch:
            if formatter:
                print(formatter(item))
            else:
                print(item)

        # Navigation hint
        nav = []
        if page_num > 0:
            nav.append("\\ back")
        if page_num < total_pages - 1:
            nav.append("Enter next")
        if chat_callback:
            nav.append("c chat")
        nav.append("q exit")

        print(f"\n[Page {page_num + 1}/{total_pages}] [{end}/{len(items)}] ({' | '.join(nav)})")

    while True:
        display_page(current_page)

        key = _getch()
        if key == 'q' or key == '\x03':  # q or Ctrl+C
            break
        elif key == '\\' and current_page > 0:
            current_page -= 1
        elif key in ('\r', '\n', ' ') and current_page < total_pages - 1:
            current_page += 1
        elif key == 'c' and chat_callback:
            chat_callback()
            # After chat, continue showing papers
        elif current_page >= total_pages - 1:
            break


def shorten_affiliation(affiliation: str, max_len: int = 18) -> str:
    """Shorten institution name to fit display."""
    if not affiliation:
        return ''
    # Common shortenings
    s = affiliation
    s = s.replace('University of California', 'UC')
    s = s.replace('University of ', 'U ')
    s = s.replace(' University', '')
    s = s.replace('Massachusetts Institute of Technology', 'MIT')
    s = s.replace('California Institute of Technology', 'Caltech')
    s = s.replace('London School of Economics', 'LSE')
    s = s.replace(' Business School', ' Bus')
    s = s.replace(' School of Business', ' Bus')
    s = s.replace(' Graduate School', ' Grad')
    s = s.replace('National Bureau of Economic Research', 'NBER')
    if len(s) > max_len:
        s = s[:max_len-2] + '..'
    return s


def format_author_row(author: Author, rank: int, width: int = 80) -> str:
    """Format a single author row."""
    GRAY = "\033[38;5;250m"
    RESET = "\033[0m"
    BLUE = "\033[94m"

    # Split name into first name(s) and surname
    name_parts = author.name.split()
    if len(name_parts) > 1:
        firstname = ' '.join(name_parts[:-1])
        surname = name_parts[-1]
    else:
        firstname = ''
        surname = author.name

    # Truncate if needed
    full_name = author.name
    if len(full_name) > 20:
        full_name = full_name[:18] + '..'
        # Recalculate parts for truncated name
        name_parts = full_name.split()
        if len(name_parts) > 1:
            firstname = ' '.join(name_parts[:-1])
            surname = name_parts[-1]
        else:
            firstname = ''
            surname = full_name

    # Format name with firstname gray, surname white
    if firstname:
        name_formatted = f"{GRAY}{firstname} {RESET}{surname}"
        name_display_len = len(firstname) + 1 + len(surname)
    else:
        name_formatted = surname
        name_display_len = len(surname)

    # Pad to 20 chars (accounting for ANSI codes)
    padding = ' ' * max(0, 20 - name_display_len)
    name_formatted += padding

    affil = shorten_affiliation(author.affiliation or '', 10)
    date = author.latest_paper[0][:7] if author.latest_paper[0] else '       '
    title = author.latest_paper[1][:28] + '..' if len(author.latest_paper[1]) > 30 else author.latest_paper[1]

    if author.name in HIGHLIGHTED_AUTHORS:
        line = f"{rank:>4} {author.paper_count:>4} {BLUE}{author.citations:>6}{RESET}  {BLUE}{author.name:<20}{RESET} {BLUE}{affil:<10}{RESET} {BLUE}{date}{RESET} {title}"
    else:
        line = f"{rank:>4} {author.paper_count:>4} {GRAY}{author.citations:>6}{RESET}  {name_formatted} {GRAY}{affil:<10}{RESET} {GRAY}{date}{RESET} {title}"

    return line


def _getch():
    """Read a single character without requiring Enter."""
    import sys
    import tty
    import termios
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def _clear_screen():
    """Clear terminal screen."""
    print('\033[2J\033[H', end='')


def _pagination_input(prompt: str, allow_prev: bool = False):
    """Single-keypress pagination. Returns 'next', 'prev', 'quit', or author name."""
    print(prompt, end='', flush=True)
    ch = _getch()
    if ch == '\\' and allow_prev:
        print('\\')
        return 'prev'
    if ch in ('q', 'Q'):
        print('q')
        return 'quit'
    if ch in ('\r', '\n', 'n', 'N'):
        print()
        return 'next'
    if ch == '\x03':
        raise KeyboardInterrupt
    print(ch, end='', flush=True)
    return ch + input()


def _prompt_return_or_chat():
    """Prompt user to return (Enter) or chat (c). Returns True if chat requested."""
    print("Press Enter to return, or 'c' to chat: ", end='', flush=True)
    ch = _getch()
    print()  # New line after keypress
    return ch.lower() == 'c'


def format_paper(paper: Paper, index: int) -> str:
    """Format a single paper for display (compact: info + authors/topics + link)."""
    import shutil
    GRAY = "\033[90m"
    BROWN = "\033[38;5;180m"
    RESET = "\033[0m"

    term_width = shutil.get_terminal_size().columns

    # Extract surnames only (last word of each name)
    surnames = [a.split()[-1] if a.split() else a for a in paper.authors[:3]]
    authors = ', '.join(surnames)
    if len(paper.authors) > 3:
        authors += ' +'
    journal = f"[{paper.journal.upper()}] " if paper.journal else ''
    pub_date = paper.pub_date or 'N/A'

    # Line 1: index, journal, date, citations, title
    title = (paper.title or 'Untitled')[:50]
    line1 = f"{index:3}. {journal}{pub_date} ({paper.citations}) {title}"

    # Line 2: authors and topics (brown, shortened)
    base_line2 = f"     {authors}"
    line2 = base_line2
    if paper.topics:
        # Shorten topic names: remove filler words, use common abbreviations
        FILLER = {'and', 'of', 'the', 'in', 'for', 'on', 'to', 'a', 'an'}
        ABBREV = {
            'finance': 'Fin', 'financial': 'Fin', 'economics': 'Econ', 'economic': 'Econ',
            'market': 'Mkt', 'markets': 'Mkts', 'corporate': 'Corp', 'corporation': 'Corp',
            'investment': 'Inv', 'investments': 'Inv', 'investor': 'Inv', 'investors': 'Inv',
            'government': 'Gov', 'governance': 'Gov', 'international': 'Intl',
            'management': 'Mgmt', 'development': 'Dev', 'regulation': 'Reg',
            'monetary': 'Mon', 'policy': 'Pol', 'banking': 'Bank', 'behavior': 'Behav',
            'behavioral': 'Behav', 'sustainable': 'Sust', 'sustainability': 'Sust',
            'environmental': 'Env', 'volatility': 'Vol', 'dynamics': 'Dyn',
        }
        topic_names = []
        for t in paper.topics:
            name = t.get('name', '') if isinstance(t, dict) else str(t)
            if name:
                words = []
                for w in name.split():
                    wl = w.lower()
                    if wl in FILLER:
                        continue
                    # Use abbreviation if exists, otherwise truncate to 4 chars
                    if wl in ABBREV:
                        words.append(ABBREV[wl])
                    else:
                        words.append(w[:4])
                name = ' '.join(words[:4])
                topic_names.append(name)

        if topic_names:
            # Available space for topics
            available = term_width - len(base_line2) - 5
            if available > 10:
                # Build topics string, adding as many as fit
                topics_str = ''
                included = 0
                for i, topic in enumerate(topic_names):
                    if i == 0:
                        test = topic
                    else:
                        test = topics_str + '; ' + topic
                    # Reserve space for "+" if more topics remain
                    reserve = 2 if i < len(topic_names) - 1 else 0
                    if len(test) <= available - reserve:
                        topics_str = test
                        included += 1
                    else:
                        break
                # Add "+" if not all topics shown
                if included < len(topic_names):
                    topics_str += ' +'
                line2 += f" {BROWN}| {topics_str}{RESET}"

    lines = [line1, line2]
    if paper.doi:
        doi_url = paper.doi if paper.doi.startswith('http') else f"https://doi.org/{paper.doi}"
        lines.append(f"     {GRAY}{doi_url}{RESET}")
    return '\n'.join(lines)


def display_papers(papers: list = None, title: str = None, context_desc: str = None,
                   author: str = None, title_search: str = None, journals: list = None,
                   years: list = None, topic: str = None, limit: int = None,
                   offer_chat: bool = True):
    """Display papers with pagination and optional chat.

    Can either pass papers directly, or pass search parameters to fetch them.

    Args:
        papers: List of Paper objects (if None, will search using other params)
        title: Header title to display
        context_desc: Description for chat context
        author: Author name to search
        title_search: Title keyword to search
        journals: Journal filter
        years: Year filter
        topic: Topic filter
        limit: Max papers to fetch
        offer_chat: Whether to offer chat option at end
    """
    # Fetch papers if not provided
    if papers is None:
        papers = search_papers(author=author, title=title_search, journals=journals,
                              years=years, topic=topic, limit=limit)

    if not papers:
        search_term = author or title_search or "search"
        print(f"\nNo papers found for '{search_term}'")
        if offer_chat:
            input("Press Enter to return...")
        return

    # Build context description if not provided
    if context_desc is None:
        parts = []
        if author:
            parts.append(f"author: {author}")
        if title_search:
            parts.append(f"title: {title_search}")
        if topic:
            parts.append(f"topic: {topic}")
        context_desc = ', '.join(parts) if parts else "papers search"

    # Save to context for chat
    save_paper_context(papers, context_desc)

    # Build title if not provided
    if title is None:
        if author:
            title = f"Papers by {author}"
        elif topic:
            title = f"Papers on '{topic}'"
        else:
            title = "Papers"

    # Build header
    header = f"{'=' * 60}\n{title} ({len(papers)} found)\n{'=' * 60}\n"

    # Calculate pagination - 3 lines per paper (info, authors/topic, doi)
    terminal_lines = shutil.get_terminal_size().lines
    lines_per_paper = 3
    # Account for header (4 lines) and footer (2 lines)
    papers_per_page = max(3, (terminal_lines - 6) // lines_per_paper)

    # Display with pagination
    indexed_papers = list(enumerate(papers, 1))

    # Chat callback for 'c' key during pagination
    chat_cb = (lambda: chat_with_papers(papers, context_desc)) if offer_chat else None

    paginate(indexed_papers, page_size=papers_per_page,
             formatter=lambda item: format_paper(item[1], item[0]),
             header=header, chat_callback=chat_cb)


def _display_author_working_papers(author_name: str, years: list = None):
    """Display working papers by an author, optionally filtered by year."""
    papers = search_papers(author=author_name, source='working-papers', years=years)
    title = f"Working Papers by {author_name}"
    if years:
        title += f" ({min(years)}-{max(years)})" if len(years) > 1 else f" ({years[0]})"
    context_desc = f"working papers: {author_name}"
    display_papers(papers=papers, title=title, context_desc=context_desc, offer_chat=True)


def _find_author_match(query: str, authors: list) -> str:
    """Find best matching author name from query."""
    query_lower = query.lower().strip()

    # Exact match first
    for author in authors:
        if author.name.lower() == query_lower:
            return author.name

    # Partial match (query is substring of name)
    matches = []
    for author in authors:
        if query_lower in author.name.lower():
            matches.append(author.name)

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        # Multiple matches - show options
        print(f"\nMultiple matches for '{query}':")
        for i, name in enumerate(matches[:10], 1):
            print(f"  {i}. {name}")
        if len(matches) > 10:
            print(f"  ... and {len(matches) - 10} more")
        try:
            choice = input("Enter number or refine search: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(matches):
                return matches[int(choice) - 1]
        except (EOFError, KeyboardInterrupt):
            pass
        return None

    return None


def print_author_table(authors: list, title: str = "Author Rankings", paginated: bool = True,
                       journals: list = None, years: list = None, working_papers: bool = False,
                       topic: str = None):
    """Print formatted author table with optional pagination.

    Single-keypress navigation (no Enter required):
    - n or Enter: next page
    - \\ (backslash): previous page
    - q: quit
    - Or start typing an author name to view their papers

    Args:
        working_papers: If True, display working papers instead of journal articles
        years: Year filter to apply when viewing author's papers (for both articles and working papers)
        topic: Topic filter to apply when viewing author's papers
    """
    width = min(shutil.get_terminal_size().columns, 120)
    page_size = max(10, shutil.get_terminal_size().lines - 8)
    pubs_label = 'WPs' if working_papers else 'Pubs'

    def print_header():
        print(f"\n{title}\n")
        print(f"{'Rank':>4} {pubs_label:>4} {'Cites':>6}  {'Author':<20} {'Affil':<10} {'Latest Paper'}")
        print("=" * width)

    def print_page(start_idx: int):
        """Print a page of authors starting at start_idx."""
        batch = authors[start_idx:start_idx + page_size]
        for j, author in enumerate(batch, start_idx + 1):
            print(format_author_row(author, j, width))

    def prompt_for_author():
        """Prompt user to enter author name, return True to redisplay."""
        try:
            user_input = input("Enter author name (or press Enter to exit): ").strip()
            if user_input:
                match = _find_author_match(user_input, authors)
                if match:
                    if working_papers:
                        _display_author_working_papers(match, years)
                    else:
                        display_papers(author=match, journals=journals, years=years, topic=topic)
                    return True  # Redisplay
                else:
                    print(f"No author found matching '{user_input}'")
                    return True  # Stay and prompt again
        except EOFError:
            pass
        return False

    print_header()

    if not paginated or len(authors) <= page_size:
        for i, author in enumerate(authors, 1):
            print(format_author_row(author, i, width))
        print("=" * width)
        print(f"Total: {len(authors)} authors")
        # Allow author lookup even with single page
        while prompt_for_author():
            print_header()
            for i, author in enumerate(authors, 1):
                print(format_author_row(author, i, width))
            print("=" * width)
            print(f"Total: {len(authors)} authors")
    else:
        i = 0
        while i < len(authors):
            _clear_screen()
            print_header()
            print_page(i)

            if i + page_size < len(authors):
                try:
                    prev = "\\=prev, " if i > 0 else ""
                    result = _pagination_input(f"[{min(i + page_size, len(authors))}/{len(authors)}] {prev}Enter, q=quit, or name: ", i > 0)

                    if result == 'quit':
                        break
                    elif result == 'prev':
                        i = max(0, i - page_size)
                    elif result == 'next':
                        i += page_size
                    else:
                        # Author name entered
                        match = _find_author_match(result.strip(), authors)
                        if match:
                            if working_papers:
                                _display_author_working_papers(match, years)
                            else:
                                display_papers(author=match, journals=journals, years=years, topic=topic)
                        else:
                            print(f"No author found matching '{result}'")
                            input("Press Enter...")
                except (EOFError, KeyboardInterrupt):
                    break
            else:
                # Last page - show footer and allow author lookup or go back
                print("=" * width)
                print(f"Total: {len(authors)} authors")
                try:
                    prev = "\\=prev, " if i > 0 else ""
                    result = _pagination_input(f"{prev}Enter/name: ", i > 0)

                    if result == 'quit' or result == 'next':
                        break
                    elif result == 'prev':
                        i = max(0, i - page_size)
                    else:
                        # Author name entered
                        match = _find_author_match(result.strip(), authors)
                        if match:
                            if working_papers:
                                _display_author_working_papers(match, years)
                            else:
                                display_papers(author=match, journals=journals, years=years, topic=topic)
                        else:
                            print(f"No author found matching '{result}'")
                            input("Press Enter...")
                except (EOFError, KeyboardInterrupt):
                    pass
                break


# =============================================================================
# HIGH-LEVEL OPERATIONS
# =============================================================================

def update_articles(journals: list = None, years: list = None, force: bool = False):
    """Update journal articles from OpenAlex."""
    if journals is None:
        journals = ['top3']

    # Expand journal groups
    expanded = []
    for j in journals:
        if j in JOURNAL_GROUPS:
            expanded.extend(JOURNAL_GROUPS[j])
        elif j in JOURNALS:
            expanded.append(j)
        else:
            print(f"Unknown journal: {j}")
            continue

    if years is None:
        years = [datetime.now().year]

    # Get timestamp before update to find new papers later
    pre_update_timestamp = get_last_update_timestamp()

    total_new = 0
    total_updated = 0
    stats = []  # Track per-journal stats for summary

    for journal in expanded:
        for year in years:
            print(f"\nFetching {JOURNALS[journal]['name']} ({year})...")
            articles = fetch_journal_articles(journal, year, force)
            if articles:
                new, updated = save_articles(articles, journal, year, force)
                print(f"  New: {new}, Updated: {updated}")
                total_new += new
                total_updated += updated
                stats.append((journal.upper(), year, new, updated))

    # Print summary if multiple journals/years
    if len(stats) > 1:
        print(f"\n{'='*50}")
        print("Summary:")
        print(f"{'='*50}")
        for journal, year, new, updated in stats:
            if new or updated:
                print(f"  {journal:6} {year}: {new:3} new, {updated:3} updated")
        print(f"{'─'*50}")
        print(f"  {'Total':6}     : {total_new:3} new, {total_updated:3} updated")
        print(f"{'='*50}")

    # Show new papers from this update
    if total_new > 0:
        # Get papers added since pre_update_timestamp
        new_papers = get_papers_added_since(
            pre_update_timestamp or '1970-01-01',
            expanded,
            years
        )
        display_papers(papers=new_papers, title="New Papers Added", offer_chat=False)
    else:
        # No new papers - show most recent ones from last update that had new papers
        recent = get_recent_papers(expanded, years, limit=20)
        if recent:
            display_papers(papers=recent, title="Most Recent Papers (from previous updates)", offer_chat=False)


# =============================================================================
# PAPER CONTEXT FOR CHAT
# =============================================================================

def save_paper_context(papers: list, query_description: str = ""):
    """Save papers to context file for chat feature."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    context = {
        'query': query_description,
        'saved_at': datetime.now().isoformat(),
        'papers': [
            {
                'title': p.title,
                'authors': p.authors,
                'year': p.year,
                'pub_date': p.pub_date,
                'journal': p.journal,
                'abstract': p.abstract,
                'doi': p.doi,
                'citations': p.citations,
            }
            for p in papers
        ]
    }

    with open(CONTEXT_FILE, 'w', encoding='utf-8') as f:
        json.dump(context, f, indent=2)

    return len(papers)


def load_paper_context() -> tuple:
    """Load papers from context file. Returns (papers, query_description)."""
    if not CONTEXT_FILE.exists():
        return [], ""

    with open(CONTEXT_FILE, 'r', encoding='utf-8') as f:
        context = json.load(f)

    papers = [
        Paper(
            title=p['title'],
            authors=p['authors'],
            year=p.get('year'),
            pub_date=p.get('pub_date'),
            journal=p.get('journal'),
            abstract=p.get('abstract'),
            doi=p.get('doi'),
            citations=p.get('citations', 0),
        )
        for p in context.get('papers', [])
    ]

    return papers, context.get('query', '')


def clear_paper_context():
    """Clear the saved paper context."""
    if CONTEXT_FILE.exists():
        CONTEXT_FILE.unlink()


def export_papers_to_file(papers: list = None, output_path: Path = None) -> Path:
    """Export papers to a markdown file with titles, authors, abstracts, and DOIs."""
    if papers is None:
        papers, _ = load_paper_context()

    if not papers:
        return None

    if output_path is None:
        output_path = Path.cwd() / f"papers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    lines = [f"# Papers Export ({len(papers)} papers)", ""]
    lines.append(f"*Exported: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")

    for i, p in enumerate(papers, 1):
        authors = ', '.join(p.authors[:5]) if p.authors else 'Unknown'
        if p.authors and len(p.authors) > 5:
            authors += f' et al.'

        journal_info = f"{p.journal.upper()} " if p.journal else ""
        year_info = p.year or (p.pub_date[:4] if p.pub_date else "")

        lines.append(f"## {i}. {p.title}")
        lines.append(f"**Authors:** {authors}")
        lines.append(f"**Published:** {journal_info}{year_info}")
        lines.append(f"**Citations:** {p.citations}")

        if p.doi:
            doi_url = p.doi if p.doi.startswith('http') else f"https://doi.org/{p.doi}"
            lines.append(f"**DOI:** [{doi_url}]({doi_url})")

        if p.abstract:
            lines.append(f"\n**Abstract:**\n{p.abstract}")

        lines.append("\n---\n")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    return output_path


def format_papers_for_llm(papers: list, max_papers: int = 50) -> str:
    """Format papers as context for LLM."""
    lines = ["You have access to the following academic finance/economics papers:\n"]

    for i, p in enumerate(papers[:max_papers], 1):
        authors = ', '.join(p.authors[:5]) if p.authors else 'Unknown'
        if p.authors and len(p.authors) > 5:
            authors += f' et al. ({len(p.authors)} authors)'

        journal_info = f"{p.journal.upper()} " if p.journal else ""
        year_info = p.year or (p.pub_date[:4] if p.pub_date else "")

        lines.append(f"## Paper {i}: {p.title}")
        lines.append(f"**Authors:** {authors}")
        lines.append(f"**Published:** {journal_info}{year_info}")
        if p.doi:
            doi_url = p.doi if p.doi.startswith('http') else f"https://doi.org/{p.doi}"
            lines.append(f"**DOI:** {doi_url}")
        lines.append(f"**Citations:** {p.citations}")
        if p.abstract:
            lines.append(f"**Abstract:** {p.abstract}")
        lines.append("")

    if len(papers) > max_papers:
        lines.append(f"(Showing {max_papers} of {len(papers)} papers)")

    return '\n'.join(lines)


# =============================================================================
# API KEY MANAGEMENT
# =============================================================================

def get_anthropic_api_key() -> str:
    """Get Anthropic API key from environment or .env file."""
    # Check environment first
    key = os.environ.get('ANTHROPIC_API_KEY')
    if key:
        return key

    # Check .env file
    if ENV_FILE.exists():
        with open(ENV_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('ANTHROPIC_API_KEY='):
                    return line.split('=', 1)[1].strip().strip('"\'')

    return None


def save_anthropic_api_key(key: str):
    """Save Anthropic API key to .env file."""
    lines = []

    # Read existing .env if it exists
    if ENV_FILE.exists():
        with open(ENV_FILE, 'r') as f:
            lines = f.readlines()

    # Update or add the key
    key_found = False
    for i, line in enumerate(lines):
        if line.strip().startswith('ANTHROPIC_API_KEY='):
            lines[i] = f'ANTHROPIC_API_KEY={key}\n'
            key_found = True
            break

    if not key_found:
        lines.append(f'ANTHROPIC_API_KEY={key}\n')

    with open(ENV_FILE, 'w') as f:
        f.writelines(lines)

    # Also set in current environment
    os.environ['ANTHROPIC_API_KEY'] = key


# =============================================================================
# CHAT WITH PAPERS
# =============================================================================

def chat_with_papers(papers: list = None, query_description: str = ""):
    """Start an interactive chat session about papers using Claude."""
    try:
        import anthropic
    except ImportError:
        print("Error: anthropic package not installed.")
        print("Install with: pip install anthropic")
        return

    # Get or prompt for API key
    api_key = get_anthropic_api_key()
    if not api_key:
        print("Anthropic API key not found.")
        api_key = input("Enter your Anthropic API key: ").strip()
        if not api_key:
            print("No API key provided. Exiting.")
            return
        save_anthropic_api_key(api_key)
        print(f"API key saved to {ENV_FILE}")

    # Load papers from context if not provided
    if papers is None:
        papers, query_description = load_paper_context()

    if not papers:
        print("No papers in context. Run a papers query first.")
        print("Example: finance-papers papers -a 'Fama'")
        print("         finance-papers topic 'Asset Pricing'")
        return

    # Format papers for context
    paper_context = format_papers_for_llm(papers)

    # System prompt
    system_prompt = f"""You are a helpful research assistant specializing in academic finance and economics.
You have been given a set of academic papers to discuss. Answer questions about these papers,
summarize them, compare their findings, or help the user understand the research.

{paper_context}

When discussing papers, refer to them by author names and year. Be precise and academic in tone.
If asked about something not covered in the papers, say so clearly."""

    # Initialize client
    client = anthropic.Anthropic(api_key=api_key)

    # Print header
    print(f"\n{'='*60}")
    print(f"Chat about {len(papers)} papers")
    if query_description:
        print(f"Query: {query_description}")
    print(f"{'='*60}")
    print("Type 'quit' or 'exit' to end the chat.")
    print("Type 'papers' to list the papers in context.")
    print()

    # Chat loop
    messages = []

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting chat.")
            break

        if not user_input:
            continue

        if user_input.lower() in ('quit', 'exit', 'q'):
            print("Exiting chat.")
            break

        if user_input.lower() == 'papers':
            print(f"\nPapers in context ({len(papers)}):")
            for i, p in enumerate(papers, 1):
                authors = p.authors[0] if p.authors else 'Unknown'
                if p.authors and len(p.authors) > 1:
                    authors += f' et al.'
                year = p.year or (p.pub_date[:4] if p.pub_date else '')
                print(f"  {i}. {authors} ({year}): {p.title[:50]}...")
            print()
            continue

        # Add user message
        messages.append({"role": "user", "content": user_input})

        # Get response from Claude
        try:
            # Show thinking indicator
            print("\nThinking...", end='', flush=True)

            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2048,
                system=system_prompt,
                messages=messages
            )

            # Clear thinking indicator and show response
            print('\r' + ' ' * 12 + '\r', end='')  # Clear "Thinking..."

            assistant_message = response.content[0].text
            messages.append({"role": "assistant", "content": assistant_message})

            print(f"Claude: {assistant_message}\n")

        except anthropic.APIError as e:
            print(f"\nAPI Error: {e}\n")
            messages.pop()  # Remove failed user message
