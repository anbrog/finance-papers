"""Finance papers - fetch and analyze top journal publications."""

from finance_papers.core import (
    # Config
    JOURNALS,
    JOURNAL_GROUPS,
    DB_DIR,
    HIGHLIGHTED_AUTHORS,
    # Data types
    Author,
    Paper,
    # Database
    get_db_files,
    iter_articles,
    save_articles,
    db_connection,
    # API
    fetch_journal_articles,
    fetch_author_works,
    reconstruct_abstract,
    # Authors
    rank_authors,
    normalize_name,
    export_author_csv,
    get_topic_counts,
    # Papers
    search_papers,
    get_author_papers,
    get_recent_papers,
    get_papers_from_last_update,
    # Working papers
    update_working_papers,
    rank_by_working_papers,
    # Unified database
    build_unified_db,
    iter_unified_papers,
    # High-level
    update_articles,
)

__version__ = "0.2.0"
