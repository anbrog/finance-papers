# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CLI tool for analyzing academic papers from top finance journals (JF, RFS, JFE) using the OpenAlex API. Fetches paper metadata, stores in SQLite databases, ranks authors by publications, and tracks working papers.

## Installation & Running

```bash
pip install -e .              # Install package (creates 'finance-papers' command)
finance-papers                 # Interactive mode
finance-papers update articles # Update journal data
finance-papers rank -n 100     # Top 100 authors
finance-papers papers -a "Fama"
streamlit run streamlit_app.py # Web dashboard
```

## Architecture

The codebase was recently refactored from standalone scripts to a proper package:

**Current structure** (`finance_papers/`):
- `core.py` - All functionality: API calls, database ops, author ranking, working papers
- `cli.py` - Command-line interface with subcommands (update, rank, papers, topic)
- `__init__.py` - Public API exports

**Legacy scripts** (`src/archive/`):
- Old standalone scripts before refactoring (kept for reference)

**Key data types** (in `core.py`):
- `Author` dataclass: name, openalex_id, paper_count, citations, affiliation
- `Paper` dataclass: title, authors, year, citations, abstract, doi, topics

## Database Schema

All databases stored in `out/data/`:

**Journal articles**: `openalex_{journal}_{year}.db`
```sql
openalex_articles (
    openalex_id TEXT UNIQUE,  -- Primary key for deduplication
    title, publication_date, doi,
    cited_by_count INTEGER,
    abstract TEXT,
    authors_json TEXT,        -- JSON array of author objects
    topics_json TEXT          -- JSON array of topic objects
)
```

**Working papers**: `working_papers.db`
```sql
working_papers (
    openalex_id TEXT UNIQUE,
    title, publication_date, doi,
    author_name TEXT,
    type TEXT,                -- e.g., "preprint", "posted-content"
    cited_by_count INTEGER
)
```

## OpenAlex API

Base URL: `https://api.openalex.org/works`

Journal source IDs (in `JOURNALS` dict):
- JF: S5353659, RFS: S170137484, JFE: S149240962
- Also includes top 5 econ journals (QJE, AER, Econometrica, JPE, REStud)

Key implementation details:
- Cursor-based pagination with 200 items per page
- Rate limiting: 200ms minimum between requests
- Abstracts reconstructed from inverted index format via `reconstruct_abstract()`
- Retry logic with exponential backoff on 429 responses

## CLI Subcommands

```bash
finance-papers update articles -y 2024       # Fetch 2024 articles
finance-papers update articles --force       # Refresh citation counts
finance-papers update working-papers         # Fetch WPs for top authors
finance-papers rank -n 250 --citations       # Rank by citations
finance-papers rank --working-papers         # Rank by WP count
finance-papers rank -o authors.csv           # Export to CSV
finance-papers papers -a "Cochrane" -y 2024  # Search by author
finance-papers topic "Asset Pricing"         # Find papers by topic
```

Year parsing accepts: `2024`, `2023-2025`, `2023,2024,2025`

## Key Functions (core.py)

Data fetching:
- `fetch_journal_articles(journal, year)` - Get articles from OpenAlex
- `fetch_author_works(author_id, from_year)` - Get working papers for author

Database:
- `save_articles(articles, journal, year, force)` - Store articles
- `iter_articles(db_files)` - Iterator over all articles in DBs
- `get_db_files(journals, years)` - Find matching database files

Author operations:
- `rank_authors(journals, years, top_n, by_citations)` - Rank by pubs/citations
- `export_author_csv(authors, ...)` - Export author list with OpenAlex IDs
- `normalize_name(name)` - Apply name fixes from `AUTHOR_NAME_FIXES` dict

Working papers:
- `update_working_papers(authors, year, max_authors, clean)` - Parallel fetch
- `rank_by_working_papers(top_n, year)` - Rank by WP count

## Configuration

Environment variables:
- `OPENALEX_MAILTO` - Email for polite API usage (optional but recommended)

Constants in `core.py`:
- `JOURNAL_GROUPS`: top3, econ5, alltop
- `AUTHOR_NAME_FIXES`: Name normalization mapping
- `HIGHLIGHTED_AUTHORS`: Names to highlight in output (blue)
