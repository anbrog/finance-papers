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
CACHE_DIR = Path.home() / '.cache' / 'finance-papers'
CONTEXT_FILE = CONFIG_DIR / 'context.json'
ENV_FILE = PROJECT_ROOT / '.env'
READ_FILE = DB_DIR / 'read_papers.json'


def _load_dotenv(path: Path) -> None:
    """Populate os.environ from a KEY=VALUE .env file. Existing env wins."""
    if not path.exists():
        return
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except OSError:
        pass


_load_dotenv(ENV_FILE)

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

# Topic display abbreviations
TOPIC_FILLER_WORDS = {'and', 'of', 'the', 'in', 'for', 'on', 'to', 'a', 'an'}
TOPIC_ABBREVIATIONS = {
    'finance': 'Fin', 'financial': 'Fin', 'economics': 'Econ', 'economic': 'Econ',
    'market': 'Mkt', 'markets': 'Mkts', 'corporate': 'Corp', 'corporation': 'Corp',
    'investment': 'Inv', 'investments': 'Inv', 'investor': 'Inv', 'investors': 'Inv',
    'government': 'Gov', 'governance': 'Gov', 'international': 'Intl',
    'management': 'Mgmt', 'development': 'Dev', 'regulation': 'Reg',
    'monetary': 'Mon', 'policy': 'Pol', 'banking': 'Bank', 'behavior': 'Behav',
    'behavioral': 'Behav', 'sustainable': 'Sust', 'sustainability': 'Sust',
    'environmental': 'Env', 'volatility': 'Vol', 'dynamics': 'Dyn',
}


# =============================================================================
# READ TRACKING
# =============================================================================

def load_read_set() -> set:
    """Load the set of read paper openalex_ids."""
    if READ_FILE.exists():
        return set(json.loads(READ_FILE.read_text()))
    return set()


def save_read_set(read_set: set):
    """Persist the set of read paper openalex_ids."""
    READ_FILE.parent.mkdir(parents=True, exist_ok=True)
    READ_FILE.write_text(json.dumps(sorted(read_set)))


def toggle_read(openalex_id: str) -> bool:
    """Toggle read status for a paper. Returns new read state."""
    read_set = load_read_set()
    if openalex_id in read_set:
        read_set.discard(openalex_id)
        is_read = False
    else:
        read_set.add(openalex_id)
        is_read = True
    save_read_set(read_set)
    return is_read


# =============================================================================
# PEEK CACHE
# =============================================================================

def _peek_cache_path(source: str) -> Path:
    """Return cache file path for a peek source ('articles' or 'working-papers')."""
    return CACHE_DIR / f'peek_{source.replace("-", "_")}.json'


def save_peek_cache(papers: list, source: str):
    """Save peek results to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        'cached_at': datetime.now().isoformat(),
        'papers': [
            {
                'title': p.title,
                'authors': p.authors,
                'year': p.year,
                'pub_date': p.pub_date,
                'citations': p.citations,
                'abstract': p.abstract,
                'doi': p.doi,
                'openalex_id': p.openalex_id,
                'topics': p.topics,
                'journal': p.journal,
                'queried_author': p.queried_author,
            }
            for p in papers
        ],
    }
    _peek_cache_path(source).write_text(json.dumps(data))


def load_peek_cache(source: str, max_age_minutes: Optional[int] = 30) -> Optional[list]:
    """Load peek results from cache if fresh enough. Returns None if stale/missing.

    Pass max_age_minutes=None to load the cache regardless of age.
    """
    path = _peek_cache_path(source)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        cached_at = datetime.fromisoformat(data['cached_at'])
        age_minutes = (datetime.now() - cached_at).total_seconds() / 60
        if max_age_minutes is not None and age_minutes > max_age_minutes:
            return None
        return [
            Paper(
                title=p['title'],
                authors=p.get('authors', []),
                year=p.get('year'),
                pub_date=p.get('pub_date'),
                citations=p.get('citations', 0),
                abstract=p.get('abstract'),
                doi=p.get('doi'),
                openalex_id=p.get('openalex_id'),
                topics=p.get('topics', []),
                journal=p.get('journal'),
                queried_author=p.get('queried_author'),
            )
            for p in data.get('papers', [])
        ]
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def peek_cache_age_minutes(source: str) -> Optional[float]:
    """Return numeric age (minutes) of the cache, or None if no cache."""
    path = _peek_cache_path(source)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        cached_at = datetime.fromisoformat(data['cached_at'])
        return (datetime.now() - cached_at).total_seconds() / 60
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def peek_cache_age(source: str) -> Optional[str]:
    """Return human-readable age of the cache, or None if no cache."""
    path = _peek_cache_path(source)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        cached_at = datetime.fromisoformat(data['cached_at'])
        minutes = int((datetime.now() - cached_at).total_seconds() / 60)
        if minutes < 1:
            return "just now"
        elif minutes < 60:
            return f"{minutes}m ago"
        else:
            return f"{minutes // 60}h {minutes % 60}m ago"
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


# =============================================================================
# NTFY NOTIFICATIONS
# =============================================================================

def _ntfy_default_topic(working_papers: bool = False) -> str:
    """Topic name from $NTFY_TOPIC, else derived from the source."""
    env_topic = os.environ.get('NTFY_TOPIC')
    if env_topic:
        return env_topic
    return 'finance-papers-w' if working_papers else 'finance-papers'


def _ntfy_post(title: str, body: str, click: Optional[str] = None,
               priority: Optional[str] = None,
               working_papers: bool = False) -> bool:
    """Low-level ntfy.sh POST. Returns True on success, False on error."""
    topic = _ntfy_default_topic(working_papers)
    server = os.environ.get('NTFY_SERVER', 'https://ntfy.sh').rstrip('/')
    token = os.environ.get('NTFY_TOKEN')
    headers = {'Title': title, 'Tags': 'books'}
    if priority:
        headers['Priority'] = priority
    if click:
        headers['Click'] = click
    if token:
        headers['Authorization'] = f'Bearer {token}'
    try:
        resp = requests.post(f'{server}/{topic}',
                             data=body.encode('utf-8'),
                             headers=headers, timeout=10)
        return resp.ok
    except requests.RequestException:
        return False


def notify_ntfy_heartbeat(label: str = "Papers", since: str = "",
                          total_fetched: int = 0,
                          working_papers: bool = False) -> bool:
    """Send a 'still alive, no new papers' notification at low priority."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    title = f"No new {label.lower()}{since}"
    body = f"Checked at {ts}. Fetched {total_fetched} papers from API; none new vs cache."
    return _ntfy_post(title, body, priority='low', working_papers=working_papers)


NTFY_DEFAULT_CAP = 20


def notify_ntfy(papers: list, since: str = "", working_papers: bool = False) -> int:
    """Push one ntfy.sh notification per new paper, capped to NTFY_CAP (default 20).

    Topic default depends on `working_papers`: 'finance-papers-w' if True else
    'finance-papers'. Override with $NTFY_TOPIC.
    Server from $NTFY_SERVER (default: 'https://ntfy.sh'). Set NTFY_TOKEN for auth.
    Cap from $NTFY_CAP (default 20). When papers > cap, the first `cap` get
    individual notifications and one extra summary notification is sent for the rest.
    Returns the number of notifications successfully sent (individuals + summary).
    Network errors are swallowed so a cron call never crashes.
    """
    if not papers:
        return 0

    topic = _ntfy_default_topic(working_papers)
    server = os.environ.get('NTFY_SERVER', 'https://ntfy.sh').rstrip('/')
    token = os.environ.get('NTFY_TOKEN')
    try:
        cap = int(os.environ.get('NTFY_CAP', NTFY_DEFAULT_CAP))
    except ValueError:
        cap = NTFY_DEFAULT_CAP
    if cap < 0:
        cap = NTFY_DEFAULT_CAP
    url = f'{server}/{topic}'

    auth_header = {'Authorization': f'Bearer {token}'} if token else {}

    individuals = papers[:cap] if cap > 0 else []
    overflow = papers[cap:] if cap > 0 else papers

    sent = 0
    for p in individuals:
        authors = ', '.join((a.split()[-1] if a else '') for a in (p.authors or [])[:3])
        if p.authors and len(p.authors) > 3:
            authors += ' et al.'
        j = getattr(p, 'journal', '') or ''

        title_parts = []
        if j:
            title_parts.append(f"[{j}]")
        if authors:
            title_parts.append(authors)
        title_text = ' '.join(title_parts) or 'New paper'
        if since:
            title_text += f"{since}"

        body = (p.title or '').strip() or '(untitled)'

        headers = {'Title': title_text, 'Tags': 'books', **auth_header}
        doi = getattr(p, 'doi', None)
        if doi:
            headers['Click'] = doi if doi.startswith('http') else f'https://doi.org/{doi}'
        elif getattr(p, 'openalex_id', None):
            headers['Click'] = p.openalex_id

        try:
            resp = requests.post(url, data=body.encode('utf-8'),
                                 headers=headers, timeout=10)
            if resp.ok:
                sent += 1
        except requests.RequestException:
            pass

    if overflow:
        lines = []
        for p in overflow[:15]:
            authors = ', '.join((a.split()[-1] if a else '') for a in (p.authors or [])[:2])
            if p.authors and len(p.authors) > 2:
                authors += ' et al.'
            j = getattr(p, 'journal', '') or ''
            t = (p.title or '').strip()
            if len(t) > 80:
                t = t[:77] + '…'
            prefix = f"[{j}] " if j else ''
            lines.append(f"• {prefix}{authors} — {t}" if authors else f"• {prefix}{t}")
        if len(overflow) > 15:
            lines.append(f"… and {len(overflow) - 15} more not listed")
        summary_title = f"… and {len(overflow)} more new papers{since}"
        summary_body = '\n'.join(lines)
        summary_headers = {'Title': summary_title, 'Tags': 'books', **auth_header}
        try:
            resp = requests.post(url, data=summary_body.encode('utf-8'),
                                 headers=summary_headers, timeout=10)
            if resp.ok:
                sent += 1
        except requests.RequestException:
            pass

    return sent


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
    wp_count: int = 0


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
    queried_author: Optional[str] = None  # author whose query found this paper


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


def _journal_from_db_file(db_file: Path) -> str:
    """Extract journal code from database filename."""
    return db_file.name.split('_')[1] if '_' in db_file.name else None


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
            min_interval = 0.05 if OPENALEX_MAILTO else 0.1  # 20/s polite, 10/s free
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
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

    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] Fetched {len(articles)} articles from {JOURNALS[journal]['name']} ({year})")
    return articles


def fetch_author_works(author_id: str, from_year: int = None) -> list:
    """Fetch working papers for an author."""
    if author_id.startswith('https://openalex.org/'):
        author_id = author_id.split('/')[-1]

    all_papers = {}  # keyed by openalex_id to deduplicate

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
                oa_id = work.get("id")
                if oa_id in all_papers:
                    continue

                # Skip AEA RCT Registry entries (trial registrations, not papers)
                doi = work.get("doi") or ""
                if "10.1257/rct" in doi:
                    continue

                # Extract primary location source name
                primary_loc = work.get("primary_location") or {}
                source = primary_loc.get("source") or {}
                primary_location = source.get("display_name")

                # Extract topics
                topics = [
                    {
                        "name": t.get("display_name"),
                        "score": t.get("score"),
                    }
                    for t in work.get("topics", [])[:5]
                ]

                authors = [
                    auth.get("author", {}).get("display_name")
                    for auth in work.get("authorships", [])
                    if auth.get("author", {}).get("display_name")
                ]

                all_papers[oa_id] = {
                    "openalex_id": oa_id,
                    "title": work.get("title"),
                    "publication_date": work.get("publication_date"),
                    "doi": doi,
                    "type": work.get("type"),
                    "cited_by_count": work.get("cited_by_count", 0),
                    "primary_location": primary_location,
                    "topics": topics,
                    "authors": authors,
                }

            cursor = data.get("meta", {}).get("next_cursor")

    return list(all_papers.values())


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

    Returns:
        tuple: (topic_counts dict, total_paper_count int)
    """
    db_files = get_db_files(journals, years)
    topic_counts = defaultdict(int)
    total_papers = 0

    for article in iter_articles(db_files):
        if author:
            author_names = [(a.get('name') or '').lower() for a in article.get('authors', []) if a]
            if not any(author.lower() in name for name in author_names):
                continue

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


def _matches_topic(topics: list, topic_filter: str) -> bool:
    """Check if any topic matches the filter (case-insensitive partial match)."""
    topic_lower = topic_filter.lower()
    return any(topic_lower in (t.get('name', '') or '').lower() for t in topics)


def rank_authors(journals: list = None, years: list = None, top_n: int = 250,
                 by_citations: bool = False, topic: str = None, source: str = None) -> list:
    """Rank authors by publication count or citations.

    Iterates directly over per-journal article DBs and working_papers.db.

    Args:
        journals: Filter by journal codes (for articles)
        years: Filter by publication years
        top_n: Number of top authors to return
        by_citations: Sort by citations instead of paper count
        topic: Filter by topic (partial match)
        source: Filter by 'article' or 'working-paper' (None = both)
    """
    author_stats = defaultdict(lambda: {'count': 0, 'wp_count': 0, 'citations': 0, 'latest': ('', ''), 'affiliation': ''})

    # Initialize highlighted authors
    for name in HIGHLIGHTED_AUTHORS:
        author_stats[name]

    # Count articles (always — ranking is based on published papers only)
    if source is None or source == 'article':
        db_files = get_db_files(journals, years)
        seen_articles = set()
        for article in iter_articles(db_files):
            if topic and not _matches_topic(article.get('topics', []), topic):
                continue

            # Deduplicate across per-journal/per-year DBs
            oa_id = article.get('openalex_id')
            if oa_id and oa_id in seen_articles:
                continue
            if oa_id:
                seen_articles.add(oa_id)

            citations = article.get('citations', 0) or 0
            pub_date = article.get('pub_date') or ''
            title = article.get('title') or ''

            for author in article['authors']:
                name = normalize_name(author.get('name') or '')
                if not name:
                    continue
                stats = author_stats[name]
                stats['count'] += 1
                stats['citations'] += citations
                if pub_date > stats['latest'][0]:
                    stats['latest'] = (pub_date, title)
                    institutions = author.get('institutions', [])
                    if institutions:
                        stats['affiliation'] = institutions[0]

    # Count working papers (always — shown as a separate column, not used for ranking)
    if source is None or source == 'working-paper':
        for wp in iter_working_papers():
            # Year filter
            if years:
                wp_date = wp.get('pub_date') or ''
                if wp_date and int(wp_date[:4]) not in years:
                    continue

            if topic and not _matches_topic(wp.get('topics', []), topic):
                continue

            for author in wp['authors']:
                name = normalize_name(author.get('name') or '')
                if not name:
                    continue
                author_stats[name]['wp_count'] += 1

    # Sort by published papers only (articles), not WPs
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
            wp_count=stats['wp_count'],
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
        source: 'articles' or 'working-papers'
    """
    if source == 'working-papers':
        items = iter_working_papers()
    else:
        db_files = get_db_files(journals, years)
        items = iter_articles(db_files)

    papers = []
    author_search = normalize_for_search(author) if author else None
    for item in items:
        if author_search:
            author_names = [normalize_for_search(normalize_name(a.get('name') or '')) for a in item['authors']]
            if not any(author_search in name for name in author_names):
                continue

        if years and source == 'working-papers':
            pub_date = item.get('pub_date')
            if not pub_date:
                continue
            if int(pub_date[:4]) not in years:
                continue

        if title:
            if title.lower() not in (item['title'] or '').lower():
                continue

        if topic and not _matches_topic(item.get('topics', []), topic):
            continue

        journal = item.get('journal')
        if journal is None and 'db_file' in item:
            journal = _journal_from_db_file(item['db_file'])

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
    authors_json = row['authors_json'] if 'authors_json' in row.keys() else None
    if authors_json:
        authors = json.loads(authors_json)
    elif row['author_name']:
        authors = [row['author_name']]
    else:
        authors = []
    queried = row['author_name'] if row['author_name'] else None
    return Paper(
        title=row['title'],
        authors=authors,
        pub_date=row['publication_date'],
        citations=row['cited_by_count'] or 0,
        doi=row['doi'],
        openalex_id=openalex_id,
        journal=_short_source(primary_loc),
        topics=topics,
        queried_author=queried,
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

        # Use column-safe query: check what columns exist
        table_info = conn.execute("PRAGMA table_info(working_papers)").fetchall()
        col_names = {row['name'] for row in table_info}

        has_primary_location = 'primary_location' in col_names
        has_topics_json = 'topics_json' in col_names
        has_authors_json = 'authors_json' in col_names

        columns = ['openalex_id', 'title', 'publication_date', 'doi',
                    'cited_by_count', 'author_name']
        if has_primary_location:
            columns.append('primary_location')
        if has_topics_json:
            columns.append('topics_json')
        if has_authors_json:
            columns.append('authors_json')

        cursor.execute(f'''
            SELECT {', '.join(columns)}
            FROM working_papers
            WHERE doi NOT LIKE '%10.1257/rct%' OR doi IS NULL
            ORDER BY publication_date DESC
        ''')

        for row in cursor:
            primary_loc = row['primary_location'] if has_primary_location else None
            topics_json = row['topics_json'] if has_topics_json else None
            topics = json.loads(topics_json) if topics_json else []
            yield {
                'openalex_id': row['openalex_id'],
                'title': row['title'],
                'pub_date': row['publication_date'],
                'doi': row['doi'],
                'citations': row['cited_by_count'] or 0,
                'authors': [a if isinstance(a, dict) else {'name': a} for a in json.loads(row['authors_json'])] if has_authors_json and row['authors_json'] else ([{'name': row['author_name']}] if row['author_name'] else []),
                'topics': topics,
                'abstract': None,
                'journal': _short_source(primary_loc),
            }


def get_recent_papers(journals: list = None, years: list = None, limit: int = 20,
                      source: str = 'articles') -> list:
    """Get recently added papers, ordered by publication date."""
    papers = []

    if source == 'working-papers':
        db_path = DB_DIR / 'working_papers.db'
        if not db_path.exists():
            return []
        with db_connection(db_path) as conn:
            # Check available columns
            table_info = conn.execute("PRAGMA table_info(working_papers)").fetchall()
            col_names = {row['name'] for row in table_info}
            columns = ['openalex_id', 'title', 'publication_date', 'doi',
                        'author_name', 'type', 'cited_by_count']
            if 'primary_location' in col_names:
                columns.append('primary_location')
            if 'topics_json' in col_names:
                columns.append('topics_json')
            if 'authors_json' in col_names:
                columns.append('authors_json')

            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT {', '.join(columns)}
                FROM working_papers
                ORDER BY publication_date DESC
                LIMIT ?
            ''', (limit,))
            for row in cursor:
                papers.append(_wp_row_to_paper(row))
    else:
        db_files = get_db_files(journals, years)
        for db_file in db_files:
            journal = _journal_from_db_file(db_file)
            with db_connection(db_file) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT openalex_id, title, publication_date, doi,
                           cited_by_count, abstract, authors_json, topics_json
                    FROM openalex_articles
                    ORDER BY publication_date DESC
                    LIMIT ?
                ''', (limit,))
                for row in cursor:
                    papers.append(_article_row_to_paper(row, journal))

    papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return papers[:limit]


def get_last_update_timestamp(journal: str = None, year: int = None,
                              source: str = 'articles') -> Optional[str]:
    """Get the most recent scraped_at timestamp from the database."""
    if source == 'working-papers':
        db_path = DB_DIR / 'working_papers.db'
        if not db_path.exists():
            return None
        with db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MAX(scraped_at) FROM working_papers')
            row = cursor.fetchone()
            return row[0] if row and row[0] else None

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


def get_last_update_date(source: str = 'articles') -> Optional[str]:
    """Get the date of the most recent update as a human-readable string."""
    ts = get_last_update_timestamp(source=source)
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return ts[:10] if len(ts) >= 10 else ts


def get_previous_update_date(source: str = 'articles') -> Optional[str]:
    """Get the second-most-recent update date (the update before the latest).

    Returns a date string like '2025-04-28', or None if there's only one update.
    """
    if source == 'working-papers':
        db_path = DB_DIR / 'working_papers.db'
        if not db_path.exists():
            return None
        with db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT DATE(scraped_at) as d
                FROM working_papers
                ORDER BY d DESC
                LIMIT 2
            ''')
            dates = [row[0] for row in cursor]
            return dates[1] if len(dates) >= 2 else None
    else:
        # Collect all distinct scraped_at dates across article DBs
        all_dates = set()
        for db_file in get_db_files():
            with db_connection(db_file) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute('SELECT DISTINCT DATE(scraped_at) FROM openalex_articles')
                    all_dates.update(row[0] for row in cursor if row[0])
                except sqlite3.OperationalError:
                    pass
        if len(all_dates) < 2:
            return None
        sorted_dates = sorted(all_dates, reverse=True)
        return sorted_dates[1]


def get_papers_from_last_update(journals: list = None, years: list = None,
                                source: str = 'articles') -> list:
    """Get papers from the most recent update batch.

    Returns all papers that share the latest scraped_at date.
    """
    papers = []

    if source == 'working-papers':
        db_path = DB_DIR / 'working_papers.db'
        if not db_path.exists():
            return []

        with db_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DATE(MAX(scraped_at)) FROM working_papers")
            row = cursor.fetchone()
            if not row or not row[0]:
                return []
            latest_date = row[0]

            # Check available columns
            table_info = conn.execute("PRAGMA table_info(working_papers)").fetchall()
            col_names = {row['name'] for row in table_info}
            columns = ['openalex_id', 'title', 'publication_date', 'doi',
                        'author_name', 'type', 'cited_by_count']
            if 'primary_location' in col_names:
                columns.append('primary_location')
            if 'topics_json' in col_names:
                columns.append('topics_json')
            if 'authors_json' in col_names:
                columns.append('authors_json')

            cursor.execute(f'''
                SELECT {', '.join(columns)}
                FROM working_papers
                WHERE DATE(scraped_at) = ?
                ORDER BY publication_date DESC
            ''', (latest_date,))
            for row in cursor:
                papers.append(_wp_row_to_paper(row))
    else:
        db_files = get_db_files(journals, years)
        if not db_files:
            return []

        # Find latest date across all DBs
        latest_date = None
        for db_file in db_files:
            with db_connection(db_file) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DATE(MAX(scraped_at)) FROM openalex_articles")
                row = cursor.fetchone()
                if row and row[0]:
                    if latest_date is None or row[0] > latest_date:
                        latest_date = row[0]

        if not latest_date:
            return []

        for db_file in db_files:
            journal = _journal_from_db_file(db_file)
            with db_connection(db_file) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT openalex_id, title, publication_date, doi,
                           cited_by_count, abstract, authors_json, topics_json
                    FROM openalex_articles
                    WHERE DATE(scraped_at) = ?
                    ORDER BY publication_date DESC
                ''', (latest_date,))
                for row in cursor:
                    papers.append(_article_row_to_paper(row, journal))

    papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return papers


def peek_new_articles(journals: list = None, years: list = None) -> list:
    """Fetch articles from OpenAlex and return only those not already in the DB.

    Does NOT save anything — a read-only preview of what 'update' would add.
    """
    if journals is None:
        journals = ['top3']
    if years is None:
        years = [datetime.now().year]

    # Expand journal groups
    expanded = []
    for j in journals:
        if j in JOURNAL_GROUPS:
            expanded.extend(JOURNAL_GROUPS[j])
        elif j in JOURNALS:
            expanded.append(j)

    # Collect existing openalex_ids
    existing_ids = set()
    for journal in expanded:
        for year in years:
            db_path = DB_DIR / f'openalex_{journal}_{year}.db'
            if db_path.exists():
                with db_connection(db_path) as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute('SELECT openalex_id FROM openalex_articles')
                        existing_ids.update(row['openalex_id'] for row in cursor)
                    except sqlite3.OperationalError:
                        pass

    # Fetch from API and filter to new only
    new_papers = []
    for journal in expanded:
        for year in years:
            articles = fetch_journal_articles(journal, year)
            for article in articles:
                if article['id'] not in existing_ids:
                    topics = article.get('topics', [])
                    new_papers.append(Paper(
                        title=article['title'],
                        authors=[a.get('name') for a in article['authors'] if a.get('name')],
                        pub_date=article['publication_date'],
                        year=int(article['publication_date'][:4]) if article.get('publication_date') else None,
                        citations=article.get('cited_by_count', 0),
                        abstract=article.get('abstract'),
                        doi=article['doi'],
                        openalex_id=article['id'],
                        topics=topics,
                        journal=journal,
                    ))

    new_papers.sort(key=lambda p: p.pub_date or '', reverse=True)
    return new_papers


def get_papers_added_since(timestamp: str, journals: list = None, years: list = None) -> list:
    """Get papers added after a given timestamp."""
    db_files = get_db_files(journals, years)
    papers = []

    for db_file in db_files:
        journal = _journal_from_db_file(db_file)

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


def save_working_papers(papers: list, clean: bool = False):
    """Save working papers to database."""
    ensure_db_dir()
    db_path = DB_DIR / 'working_papers.db'

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
                primary_location TEXT,
                topics_json TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Add columns if upgrading from old schema
        existing_cols = {row['name'] for row in conn.execute("PRAGMA table_info(working_papers)").fetchall()}
        if 'primary_location' not in existing_cols:
            cursor.execute('ALTER TABLE working_papers ADD COLUMN primary_location TEXT')
        if 'topics_json' not in existing_cols:
            cursor.execute('ALTER TABLE working_papers ADD COLUMN topics_json TEXT')
        if 'authors_json' not in existing_cols:
            cursor.execute('ALTER TABLE working_papers ADD COLUMN authors_json TEXT')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wp_author ON working_papers(author_name)')

        new_count = 0
        for paper in papers:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO working_papers
                    (openalex_id, title, publication_date, doi, author_name, type,
                     cited_by_count, primary_location, topics_json, authors_json, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    paper['openalex_id'],
                    paper['title'],
                    paper['publication_date'],
                    paper['doi'],
                    paper.get('author_name', ''),
                    paper['type'],
                    paper['cited_by_count'],
                    paper.get('primary_location'),
                    json.dumps(paper.get('topics', [])),
                    json.dumps(paper.get('authors', [])),
                    datetime.now().isoformat()
                ))
                if cursor.rowcount > 0:
                    new_count += 1
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    return new_count


def _dedup_authors(authors: list) -> list:
    """Deduplicate authors by OpenAlex ID, keeping first occurrence."""
    seen_ids = set()
    unique = []
    for a in authors:
        if a.openalex_id:
            if a.openalex_id in seen_ids:
                continue
            seen_ids.add(a.openalex_id)
        unique.append(a)
    return unique


def update_working_papers(authors: list, year: int = None, max_authors: int = None,
                          clean: bool = False):
    """Fetch and store working papers for authors."""
    if max_authors:
        authors = authors[:max_authors]
    authors = _dedup_authors(authors)

    all_papers = []

    def fetch_for_author(author):
        if author.openalex_id:
            papers = fetch_author_works(author.openalex_id, year)
            for p in papers:
                p['author_name'] = author.name
            return author.name, papers
        return author.name, []

    t0 = time.time()
    workers = 20 if OPENALEX_MAILTO else 10
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_for_author, a): a for a in authors}

        for i, future in enumerate(as_completed(futures), 1):
            author_name, papers = future.result()
            all_papers.extend(papers)
            print(f"\r\033[K[{i}/{len(authors)}] {author_name}: {len(papers)} papers", end='', flush=True)

    elapsed = time.time() - t0
    print(f"\nFetched {len(all_papers)} working papers in {elapsed:.1f}s")
    if all_papers:
        new_count = save_working_papers(all_papers, clean)
        print(f"Saved {new_count} new working papers")

    return all_papers


def peek_new_working_papers(authors: list = None, year: int = None,
                            max_authors: int = None) -> list:
    """Fetch working papers from OpenAlex and return only those not already in the DB.

    Does NOT save anything — a read-only preview of what 'update -w' would add.
    """
    # Load author list from CSV if not provided
    if authors is None:
        pattern = str(DB_DIR / 'author_list_*.csv')
        csv_files = glob.glob(pattern)
        if not csv_files:
            print("No author list found. Run 'finance-papers rank -o' first.")
            return []
        csv_file = max(csv_files, key=lambda x: Path(x).stat().st_mtime)
        authors = read_author_csv(Path(csv_file))
        print(f"Using: {Path(csv_file).name} ({len(authors)} authors)")

    if max_authors:
        authors = authors[:max_authors]
    authors = _dedup_authors(authors)

    if year is None:
        year = datetime.now().year  # fetch_author_works subtracts 1, so this covers current + previous year

    # Collect existing openalex_ids from working papers DB
    existing_ids = set()
    db_path = DB_DIR / 'working_papers.db'
    if db_path.exists():
        with db_connection(db_path) as conn:
            try:
                cursor = conn.cursor()
                cursor.execute('SELECT openalex_id FROM working_papers')
                existing_ids.update(row['openalex_id'] for row in cursor)
            except sqlite3.OperationalError:
                pass

    # Fetch from API in parallel, filter to new only
    all_papers = []

    def fetch_for_author(author):
        if author.openalex_id:
            papers = fetch_author_works(author.openalex_id, year)
            for p in papers:
                p['author_name'] = author.name
            return author.name, papers
        return author.name, []

    t0 = time.time()
    workers = 20 if OPENALEX_MAILTO else 10
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_for_author, a): a for a in authors}
        for i, future in enumerate(as_completed(futures), 1):
            author_name, papers = future.result()
            new = [p for p in papers if p['openalex_id'] not in existing_ids]
            all_papers.extend(new)
            # Clear line and show progress
            print(f"\r\033[K[{i}/{len(authors)}] {author_name}", end='', flush=True)
            if new:
                print(f": {len(new)} new")

    elapsed = time.time() - t0
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\r\033[K[{ts}] Fetched {len(authors)} authors in {elapsed:.1f}s, {len(all_papers)} new papers")

    # Convert to Paper objects
    result = []
    for p in all_papers:
        topics = p.get('topics', [])
        primary_loc = p.get('primary_location')
        result.append(Paper(
            title=p['title'],
            authors=p.get('authors') or ([p.get('author_name', '')] if p.get('author_name') else []),
            pub_date=p['publication_date'],
            year=int(p['publication_date'][:4]) if p.get('publication_date') else None,
            citations=p.get('cited_by_count', 0),
            doi=p.get('doi'),
            openalex_id=p['openalex_id'],
            topics=topics,
            journal=_short_source(primary_loc),
            queried_author=p.get('author_name'),
        ))

    result.sort(key=lambda p: p.pub_date or '', reverse=True)
    return result


def rank_by_working_papers(top_n: int = 250, years: list = None, topic: str = None) -> list:
    """Rank authors by working paper count."""
    return rank_authors(top_n=top_n, years=years, topic=topic, source='working-paper')


# =============================================================================
# DISPLAY HELPERS
# =============================================================================

def paginate(items: list, page_size: int = None, formatter=None, header: str = None,
             chat_callback=None, next_callback=None, next_label: str = "next",
             read_callback=None, find_callback=None):
    """Display items with pagination.

    Args:
        items: List of items to display
        page_size: Items per page (default: fit terminal)
        formatter: Function to format each item
        header: Header to repeat on each page
        chat_callback: Optional function to call when 'c' is pressed (for chat)
        next_callback: Optional function to call when Enter is pressed on last page
        next_label: Label shown for next_callback in nav hint (e.g. "working papers")
        read_callback: Optional function to call when 'r' is pressed (toggle read)
        find_callback: Optional function to call when '/' is pressed. Should return
            an int item index to jump to (page containing that index), or None to stay.
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
        elif next_callback:
            nav.append(f"Enter {next_label}")
        if find_callback:
            nav.append("/ find")
        if read_callback:
            nav.append("r read")
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
        elif key in ('\r', '\n', ' ') and current_page >= total_pages - 1 and next_callback:
            next_callback()
            break
        elif key == 'r' and read_callback:
            read_callback()
            # Redisplay current page to show updated read marks
        elif key == '/' and find_callback:
            idx = find_callback()
            if isinstance(idx, int) and 0 <= idx < len(items):
                current_page = idx // page_size
        elif key == 'c' and chat_callback:
            chat_callback()
            # After chat, continue showing papers
        elif current_page >= total_pages - 1:
            break


def shorten_affiliation(affiliation: str, max_len: int = 18) -> str:
    """Shorten institution name to fit display."""
    if not affiliation:
        return ''
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

    padding = ' ' * max(0, 20 - name_display_len)
    name_formatted += padding

    affil = shorten_affiliation(author.affiliation or '', 10)
    date = author.latest_paper[0][:7] if author.latest_paper[0] else '       '
    title = author.latest_paper[1][:28] + '..' if len(author.latest_paper[1]) > 30 else author.latest_paper[1]

    wp_str = str(author.wp_count) if author.wp_count > 0 else '-'

    if author.name in HIGHLIGHTED_AUTHORS:
        line = f"{rank:>4} {author.paper_count:>4} {GRAY}{wp_str:>4}{RESET} {BLUE}{author.citations:>6}{RESET}  {BLUE}{author.name:<20}{RESET} {BLUE}{affil:<10}{RESET} {BLUE}{date}{RESET} {title}"
    else:
        line = f"{rank:>4} {author.paper_count:>4} {GRAY}{wp_str:>4}{RESET} {GRAY}{author.citations:>6}{RESET}  {name_formatted} {GRAY}{affil:<10}{RESET} {GRAY}{date}{RESET} {title}"

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


def _shorten_topic(name: str) -> str:
    """Shorten a single topic name using abbreviations."""
    words = []
    for w in name.split():
        wl = w.lower()
        if wl in TOPIC_FILLER_WORDS:
            continue
        if wl in TOPIC_ABBREVIATIONS:
            words.append(TOPIC_ABBREVIATIONS[wl])
        else:
            words.append(w[:4])
    return ' '.join(words[:4])


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
    if paper.queried_author:
        queried_surname = paper.queried_author.split()[-1] if paper.queried_author.split() else paper.queried_author
        authors = f"[{queried_surname}] {authors}"
    journal = f"[{paper.journal.upper()}] " if paper.journal else ''
    pub_date = paper.pub_date or 'N/A'

    # Line 1: index, read indicator, journal, date, citations, title
    read_set = load_read_set()
    read_mark = "\033[32mx\033[0m" if paper.openalex_id and paper.openalex_id in read_set else "o"
    title = (paper.title or 'Untitled')[:50]
    line1 = f"{index:3}.{read_mark} {journal}{pub_date} ({paper.citations}) {title}"

    # Line 2: authors and topics
    base_line2 = f"     {authors}"
    line2 = base_line2
    if paper.topics:
        topic_names = []
        for t in paper.topics:
            name = t.get('name', '') if isinstance(t, dict) else str(t)
            if name:
                topic_names.append(_shorten_topic(name))

        if topic_names:
            available = term_width - len(base_line2) - 5
            if available > 10:
                topics_str = ''
                included = 0
                for i, topic in enumerate(topic_names):
                    test = topic if i == 0 else topics_str + '; ' + topic
                    reserve = 2 if i < len(topic_names) - 1 else 0
                    if len(test) <= available - reserve:
                        topics_str = test
                        included += 1
                    else:
                        break
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
                   offer_chat: bool = True, print_mode: bool = False,
                   next_callback=None, next_label: str = "next"):
    """Display papers with pagination and optional chat.

    Can either pass papers directly, or pass search parameters to fetch them.
    Set print_mode=True to print all papers to stdout without pagination.
    """
    if papers is None:
        papers = search_papers(author=author, title=title_search, journals=journals,
                              years=years, topic=topic, limit=limit)

    if not papers:
        search_term = author or title_search or "search"
        print(f"\nNo papers found for '{search_term}'")
        if offer_chat and not print_mode:
            input("Press Enter to return...")
        return

    if context_desc is None:
        parts = []
        if author:
            parts.append(f"author: {author}")
        if title_search:
            parts.append(f"title: {title_search}")
        if topic:
            parts.append(f"topic: {topic}")
        context_desc = ', '.join(parts) if parts else "papers search"

    save_paper_context(papers, context_desc)

    if title is None:
        if author:
            title = f"Papers by {author}"
        elif topic:
            title = f"Papers on '{topic}'"
        else:
            title = "Papers"

    header = f"{'=' * 60}\n{title} ({len(papers)} found)\n{'=' * 60}\n"

    if print_mode:
        print(header)
        for i, paper in enumerate(papers, 1):
            print(format_paper(paper, i))
            print()
        return

    terminal_lines = shutil.get_terminal_size().lines
    lines_per_paper = 3
    papers_per_page = max(3, (terminal_lines - 6) // lines_per_paper)

    indexed_papers = list(enumerate(papers, 1))
    chat_cb = (lambda: chat_with_papers(papers, context_desc)) if offer_chat else None

    def read_toggle_fzf():
        """Open fzf to select a paper and toggle its read status."""
        import subprocess as sp
        read_set = load_read_set()
        options = []
        for i, p in enumerate(papers):
            mark = "x" if p.openalex_id and p.openalex_id in read_set else "o"
            authors = ', '.join(a.split()[-1] for a in p.authors[:2]) if p.authors else ''
            options.append(f"[{mark}] {i+1}. {authors} ({p.pub_date or '?'}) {(p.title or '')[:60]}")
        try:
            result = sp.run(
                ['fzf', '--header=Toggle read status', '--reverse', '--multi'],
                input='\n'.join(options), capture_output=True, text=True,
            )
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    # Extract index from "[x] 3. ..." or "[o] 3. ..."
                    parts = line.split('. ', 1)
                    idx_part = parts[0].split('] ')[-1].strip()
                    try:
                        idx = int(idx_part) - 1
                        if 0 <= idx < len(papers) and papers[idx].openalex_id:
                            toggle_read(papers[idx].openalex_id)
                    except ValueError:
                        pass
        except FileNotFoundError:
            pass

    def find_paper_fzf():
        """Open fzf to fuzzy-find a paper; return its index in `papers` or None."""
        import subprocess as sp
        read_set = load_read_set()
        options = []
        for i, p in enumerate(papers):
            mark = "x" if p.openalex_id and p.openalex_id in read_set else " "
            authors = ', '.join(a.split()[-1] for a in p.authors[:3]) if p.authors else ''
            j = getattr(p, 'journal', '') or ''
            options.append(f"{i+1:>4}. [{mark}] {(j or '?'):<5} {p.pub_date or '?':<10} {authors} — {(p.title or '')}")
        try:
            result = sp.run(
                ['fzf', '--header=Find paper (Enter to jump, Esc to cancel)',
                 '--reverse', '--no-multi'],
                input='\n'.join(options), capture_output=True, text=True,
            )
        except FileNotFoundError:
            print("fzf not installed — install it to use '/' find.")
            input("Press Enter to continue...")
            return None
        if result.returncode != 0 or not result.stdout.strip():
            return None
        line = result.stdout.strip().split('\n')[0]
        try:
            return int(line.split('.', 1)[0].strip()) - 1
        except (ValueError, IndexError):
            return None

    paginate(indexed_papers, page_size=papers_per_page,
             formatter=lambda item: format_paper(item[1], item[0]),
             header=header, chat_callback=chat_cb,
             next_callback=next_callback, next_label=next_label,
             read_callback=read_toggle_fzf,
             find_callback=find_paper_fzf)


def _display_author_working_papers(author_name: str, years: list = None):
    """Display working papers by an author, optionally filtered by year."""
    papers = search_papers(author=author_name, source='working-papers', years=years)
    if not papers:
        return
    title = f"Working Papers by {author_name}"
    if years:
        title += f" ({min(years)}-{max(years)})" if len(years) > 1 else f" ({years[0]})"
    context_desc = f"working papers: {author_name}"
    display_papers(papers=papers, title=title, context_desc=context_desc, offer_chat=True)


def _find_author_match(query: str, authors: list) -> str:
    """Find best matching author name from query."""
    query_lower = query.lower().strip()

    for author in authors:
        if author.name.lower() == query_lower:
            return author.name

    matches = []
    for author in authors:
        if query_lower in author.name.lower():
            matches.append(author.name)

    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
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
    """
    width = min(shutil.get_terminal_size().columns, 120)
    page_size = max(10, shutil.get_terminal_size().lines - 8)
    pubs_label = 'WPs' if working_papers else 'Pubs'

    def print_header():
        print(f"\n{title}\n")
        print(f"{'Rank':>4} {pubs_label:>4} {'WPs':>4} {'Cites':>6}  {'Author':<20} {'Affil':<10} {'Latest Paper'}")
        print("=" * width)

    def print_page(start_idx: int):
        batch = authors[start_idx:start_idx + page_size]
        for j, author in enumerate(batch, start_idx + 1):
            print(format_author_row(author, j, width))

    def handle_author_input(name_query):
        """Handle author name input - show their papers."""
        match = _find_author_match(name_query.strip(), authors)
        if match:
            if working_papers:
                _display_author_working_papers(match, years)
            else:
                # Show published papers; on last page Enter continues to working papers
                wp_cb = lambda: _display_author_working_papers(match, years)
                display_papers(author=match, journals=journals, years=years, topic=topic,
                               next_callback=wp_cb, next_label="working papers")
            return True
        else:
            print(f"No author found matching '{name_query}'")
            input("Press Enter...")
            return True

    def prompt_for_author():
        try:
            user_input = input("Enter author name (or press Enter to exit): ").strip()
            if user_input:
                return handle_author_input(user_input)
        except EOFError:
            pass
        return False

    print_header()

    if not paginated or len(authors) <= page_size:
        for i, author in enumerate(authors, 1):
            print(format_author_row(author, i, width))
        print("=" * width)
        print(f"Total: {len(authors)} authors")
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

            is_last_page = i + page_size >= len(authors)
            if is_last_page:
                print("=" * width)
                print(f"Total: {len(authors)} authors")

            try:
                prev = "\\=prev, " if i > 0 else ""
                if is_last_page:
                    result = _pagination_input(f"{prev}Enter/name: ", i > 0)
                else:
                    result = _pagination_input(
                        f"[{min(i + page_size, len(authors))}/{len(authors)}] {prev}Enter, q=quit, or name: ",
                        i > 0
                    )

                if result == 'quit':
                    break
                elif result == 'prev':
                    i = max(0, i - page_size)
                elif result == 'next':
                    if is_last_page:
                        break
                    i += page_size
                else:
                    handle_author_input(result)
            except (EOFError, KeyboardInterrupt):
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
    stats = []

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

    if os.environ.get('FP_NON_INTERACTIVE'):
        if total_new > 0:
            print(f"\n{total_new} new papers added.")
    elif total_new > 0:
        new_papers = get_papers_added_since(
            pre_update_timestamp or '1970-01-01',
            expanded,
            years
        )
        display_papers(papers=new_papers, title="New Papers Added", offer_chat=False)
    else:
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
    key = os.environ.get('ANTHROPIC_API_KEY')
    if key:
        return key

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

    if ENV_FILE.exists():
        with open(ENV_FILE, 'r') as f:
            lines = f.readlines()

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

    api_key = get_anthropic_api_key()
    if not api_key:
        print("Anthropic API key not found.")
        api_key = input("Enter your Anthropic API key: ").strip()
        if not api_key:
            print("No API key provided. Exiting.")
            return
        save_anthropic_api_key(api_key)
        print(f"API key saved to {ENV_FILE}")

    if papers is None:
        papers, query_description = load_paper_context()

    if not papers:
        print("No papers in context. Run a papers query first.")
        print("Example: finance-papers papers -a 'Fama'")
        print("         finance-papers topic 'Asset Pricing'")
        return

    paper_context = format_papers_for_llm(papers)

    system_prompt = f"""You are a helpful research assistant specializing in academic finance and economics.
You have been given a set of academic papers to discuss. Answer questions about these papers,
summarize them, compare their findings, or help the user understand the research.

{paper_context}

When discussing papers, refer to them by author names and year. Be precise and academic in tone.
If asked about something not covered in the papers, say so clearly."""

    client = anthropic.Anthropic(api_key=api_key)

    print(f"\n{'='*60}")
    print(f"Chat about {len(papers)} papers")
    if query_description:
        print(f"Query: {query_description}")
    print(f"{'='*60}")
    print("Type 'quit' or 'exit' to end the chat.")
    print("Type 'papers' to list the papers in context.")
    print()

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

        messages.append({"role": "user", "content": user_input})

        try:
            print("\nThinking...", end='', flush=True)

            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2048,
                system=system_prompt,
                messages=messages
            )

            print('\r' + ' ' * 12 + '\r', end='')

            assistant_message = response.content[0].text
            messages.append({"role": "assistant", "content": assistant_message})

            print(f"Claude: {assistant_message}\n")

        except anthropic.APIError as e:
            print(f"\nAPI Error: {e}\n")
            messages.pop()
