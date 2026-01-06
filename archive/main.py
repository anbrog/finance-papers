#!/usr/bin/env python3
"""
Main script to update papers, rank authors, and fetch working papers.
Orchestrates the entire workflow using other scripts.
"""
import subprocess
import sys
import os
import sqlite3
from datetime import datetime
import glob

# Get DB_DIR relative to project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
DB_DIR = os.path.join(project_root, 'out', 'data')

import json

def get_authors_by_topic_from_db(topic_name="Financial Markets and Investment Strategies", min_papers=1, max_authors=250):
    """Get authors from local database who have papers on a specific topic

    Args:
        topic_name: Name of the topic to search for (case-insensitive partial match)
        min_papers: Minimum number of papers an author must have on this topic
        max_authors: Maximum number of authors to return

    Returns:
        List of dicts with author info
    """
    # Get all journal database files
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    db_files = glob.glob(pattern)

    if not db_files:
        return []

    # Count papers per author on the topic
    author_topic_counts = {}  # {author_name: {'count': N, 'total_citations': N, 'author_id': '', 'papers': [...]}}

    topic_lower = topic_name.lower()

    for db_file in db_files:
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Check if topics_json column exists
            cursor.execute("PRAGMA table_info(openalex_articles)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'topics_json' not in columns:
                conn.close()
                continue

            cursor.execute('SELECT authors_json, topics_json, cited_by_count, title FROM openalex_articles')

            for row in cursor.fetchall():
                authors_json, topics_json, citations, title = row

                if not topics_json:
                    continue

                # Check if this paper has the topic
                try:
                    topics = json.loads(topics_json)
                    has_topic = False
                    for topic in topics:
                        topic_display = topic.get('name', '') or topic.get('display_name', '')
                        if topic_lower in topic_display.lower():
                            has_topic = True
                            break

                    if not has_topic:
                        continue

                    # Paper has the topic - count for each author
                    authors = json.loads(authors_json)
                    for author in authors:
                        name = author.get('name')
                        if not name:
                            continue

                        if name not in author_topic_counts:
                            author_topic_counts[name] = {
                                'count': 0,
                                'total_citations': 0,
                                'author_id': author.get('id', ''),
                                'papers': []
                            }

                        author_topic_counts[name]['count'] += 1
                        author_topic_counts[name]['total_citations'] += citations or 0
                        if title:
                            author_topic_counts[name]['papers'].append(title[:50])

                except (json.JSONDecodeError, TypeError):
                    continue

            conn.close()

        except sqlite3.Error:
            continue

    # Filter by minimum papers and convert to list
    authors = []
    for name, data in author_topic_counts.items():
        if data['count'] >= min_papers:
            authors.append({
                'name': name,
                'author_id': data['author_id'],
                'topic_papers': data['count'],
                'total_citations': data['total_citations'],
                'sample_papers': data['papers'][:3]
            })

    # Sort by topic paper count (descending)
    authors.sort(key=lambda x: (x['topic_papers'], x['total_citations']), reverse=True)

    return authors[:max_authors]

def get_papers_by_author_on_topic(author_name, topic_name):
    """Get all papers by a specific author on a specific topic.

    Args:
        author_name: Name of the author to search for
        topic_name: Name of the topic (case-insensitive partial match)

    Returns:
        List of dicts with paper info (title, year, journal, citations, doi)
    """
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    db_files = glob.glob(pattern)

    if not db_files:
        return []

    papers = []
    topic_lower = topic_name.lower()
    author_lower = author_name.lower()

    for db_file in db_files:
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Check if topics_json column exists
            cursor.execute("PRAGMA table_info(openalex_articles)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'topics_json' not in columns:
                conn.close()
                continue

            # Extract journal and year from filename (openalex_jf_2024.db -> JF, 2024)
            db_basename = os.path.basename(db_file)
            parts = db_basename.replace('.db', '').split('_')
            journal = parts[1].upper() if len(parts) >= 2 else '?'
            year = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else None

            cursor.execute('''
                SELECT title, authors_json, topics_json, cited_by_count, doi
                FROM openalex_articles
            ''')

            for row in cursor.fetchall():
                title, authors_json, topics_json, citations, doi = row

                if not topics_json or not authors_json:
                    continue

                try:
                    # Check if paper has the topic
                    topics = json.loads(topics_json)
                    has_topic = False
                    for topic in topics:
                        topic_display = topic.get('name', '') or topic.get('display_name', '')
                        if topic_lower in topic_display.lower():
                            has_topic = True
                            break

                    if not has_topic:
                        continue

                    # Check if author is on this paper
                    authors = json.loads(authors_json)
                    has_author = False
                    for author in authors:
                        name = author.get('name', '')
                        if name.lower() == author_lower:
                            has_author = True
                            break

                    if not has_author:
                        continue

                    papers.append({
                        'title': title,
                        'year': year,
                        'journal': journal,
                        'citations': citations or 0,
                        'doi': doi
                    })

                except (json.JSONDecodeError, TypeError):
                    continue

            conn.close()

        except sqlite3.Error:
            continue

    # Sort by year (descending), then citations
    papers.sort(key=lambda x: (x['year'] or 0, x['citations']), reverse=True)

    return papers


def get_popular_topics_from_db(limit=20):
    """Get the most popular topics from the database based on paper count.

    Args:
        limit: Maximum number of topics to return

    Returns:
        List of dicts with topic name and paper count, sorted by count descending
    """
    # Get all journal database files
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    db_files = glob.glob(pattern)

    if not db_files:
        return []

    # Count papers per topic
    topic_counts = {}  # {topic_name: count}

    for db_file in db_files:
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Check if topics_json column exists
            cursor.execute("PRAGMA table_info(openalex_articles)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'topics_json' not in columns:
                conn.close()
                continue

            cursor.execute('SELECT topics_json FROM openalex_articles WHERE topics_json IS NOT NULL')

            for row in cursor.fetchall():
                topics_json = row[0]

                if not topics_json:
                    continue

                try:
                    topics = json.loads(topics_json)
                    for topic in topics:
                        topic_name = topic.get('name', '') or topic.get('display_name', '')
                        if topic_name:
                            topic_counts[topic_name] = topic_counts.get(topic_name, 0) + 1
                except (json.JSONDecodeError, TypeError):
                    continue

            conn.close()

        except sqlite3.Error:
            continue

    # Convert to list and sort by count
    topics = [{'name': name, 'count': count} for name, count in topic_counts.items()]
    topics.sort(key=lambda x: x['count'], reverse=True)

    return topics[:limit]

def get_papers_by_citations(year_filter=None, limit=1000):
    """Get papers from the database ranked by citation count.

    Args:
        year_filter: Optional year or year range (e.g., 2024 or "2022-2025")
        limit: Maximum number of papers to return

    Returns:
        List of dicts with paper info, sorted by citations descending
    """
    # Get all journal database files
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    db_files = glob.glob(pattern)

    if not db_files:
        return []

    # Determine year range (default: last 4 years)
    current_year = datetime.now().year
    if year_filter:
        if isinstance(year_filter, str) and '-' in year_filter:
            start, end = year_filter.split('-')
            years = list(range(int(start), int(end) + 1))
        elif isinstance(year_filter, (int, str)):
            years = [int(year_filter)]
        else:
            years = list(range(current_year - 3, current_year + 1))
    else:
        years = list(range(current_year - 3, current_year + 1))

    all_papers = []

    for db_file in db_files:
        # Extract year from filename (e.g., openalex_jf_2024.db)
        try:
            filename = os.path.basename(db_file)
            db_year = int(filename.split('_')[-1].replace('.db', ''))
            if db_year not in years:
                continue
        except (ValueError, IndexError):
            continue

        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT title, authors_json, publication_date, cited_by_count,
                       doi, openalex_id, abstract
                FROM openalex_articles
                ORDER BY cited_by_count DESC
            ''')

            for row in cursor.fetchall():
                title, authors_json, pub_date, citations, doi, openalex_id, abstract = row

                # Parse authors
                try:
                    authors = json.loads(authors_json) if authors_json else []
                    author_names = [a.get('name', '') for a in authors if a.get('name')]
                except (json.JSONDecodeError, TypeError):
                    author_names = []

                # Extract journal from filename
                journal = filename.split('_')[1].upper()

                all_papers.append({
                    'title': title or 'Untitled',
                    'authors': author_names,
                    'authors_str': ', '.join(author_names[:3]) + (' et al.' if len(author_names) > 3 else ''),
                    'publication_date': pub_date,
                    'year': db_year,
                    'citations': citations or 0,
                    'doi': doi,
                    'openalex_id': openalex_id,
                    'abstract': abstract,
                    'journal': journal
                })

            conn.close()

        except sqlite3.Error:
            continue

    # Sort by citations descending
    all_papers.sort(key=lambda x: x['citations'], reverse=True)

    return all_papers[:limit]

def display_paper_details(paper):
    """Display detailed information about a paper."""
    import shutil
    terminal_width = max(78, min(shutil.get_terminal_size().columns, 100))

    print(f"\n{'='*terminal_width}")
    print("PAPER DETAILS")
    print(f"{'='*terminal_width}\n")

    # Title
    print(f"Title: {paper['title']}")
    print()

    # Authors
    if paper['authors']:
        print(f"Authors: {', '.join(paper['authors'])}")
    print()

    # Publication info
    print(f"Journal: {paper['journal']}")
    print(f"Publication Date: {paper['publication_date'] or 'N/A'}")
    print(f"Citations: {paper['citations']}")
    print()

    # Links
    if paper['doi']:
        print(f"DOI: https://doi.org/{paper['doi']}")
    if paper['openalex_id']:
        print(f"OpenAlex: {paper['openalex_id']}")
    print()

    # Abstract
    if paper['abstract']:
        print(f"Abstract:")
        print(f"{'─'*terminal_width}")
        # Word wrap the abstract
        words = paper['abstract'].split()
        line = ""
        for word in words:
            if len(line) + len(word) + 1 <= terminal_width - 2:
                line += (" " if line else "") + word
            else:
                print(f"  {line}")
                line = word
        if line:
            print(f"  {line}")
        print(f"{'─'*terminal_width}")
    else:
        print("Abstract: Not available")

    print(f"\n{'='*terminal_width}")

def run_command(cmd, description):
    """Run a shell command and handle errors"""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Running: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))
    
    if result.returncode != 0:
        print(f"\n❌ Error: Command failed with exit code {result.returncode}")
        return result.returncode
    
    return result.returncode

def run_git_command(cmd, description):
    """Run a git command from project root"""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Running: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, cwd=project_root)
    
    if result.returncode != 0:
        print(f"\n❌ Error: Command failed with exit code {result.returncode}")
        return result.returncode
    
    return result.returncode

def get_current_year():
    """Get current year"""
    return datetime.now().year

def get_years_to_update(force=False):
    """Get list of years to update"""
    current_year = get_current_year()
    years = [2023, 2024, 2025]
    
    # Add future years up to current year
    for year in range(2026, current_year + 1):
        years.append(year)
    
    if force:
        return years
    else:
        # Only return the latest year
        return [years[-1]]

def parse_year_input(year_input):
    """Parse year input string into list of years
    
    Examples:
        "" or "all" -> None (all years)
        "2024" -> [2024]
        "2023-2025" -> [2023, 2024, 2025]
        "2023,2024,2025" -> [2023, 2024, 2025]
    """
    if not year_input or year_input.lower() in ['all', 'a']:
        return None  # All years
    
    years = []
    
    # Handle comma-separated years
    if ',' in year_input:
        parts = year_input.split(',')
        for part in parts:
            part = part.strip()
            if '-' in part:
                # Range within comma list
                start, end = part.split('-')
                years.extend(range(int(start), int(end) + 1))
            else:
                years.append(int(part))
    # Handle year range
    elif '-' in year_input:
        start, end = year_input.split('-')
        years = list(range(int(start.strip()), int(end.strip()) + 1))
    # Single year
    else:
        years = [int(year_input.strip())]
    
    return years

def count_new_papers_added(journals, years):
    """Count papers added in current session and last session"""
    from datetime import datetime, timedelta

    total_current = 0
    total_last_scrape = 0
    last_scrape_date = None
    cutoff_time = (datetime.now() - timedelta(minutes=5)).isoformat()

    # First, count papers from current session (last 5 minutes)
    for journal in journals:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue

            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Count papers added in last 5 minutes (current session)
            cursor.execute('''
                SELECT COUNT(*) FROM openalex_articles
                WHERE scraped_at >= ?
            ''', (cutoff_time,))
            count = cursor.fetchone()[0]
            total_current += count

            if count > 0:
                print(f"  {journal.upper()} {year}: {count} new papers")

            conn.close()

    # If no papers in current session, count from most recent scrape
    if total_current == 0:
        for journal in journals:
            for year in years:
                db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
                if not os.path.exists(db_file):
                    continue

                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()

                # Get the most recent scrape date
                cursor.execute('SELECT MAX(scraped_at) FROM openalex_articles')
                latest_scrape = cursor.fetchone()[0]

                if latest_scrape:
                    latest_date = latest_scrape.split('T')[0]
                    if not last_scrape_date:
                        last_scrape_date = latest_date

                    # Count papers from that scrape date
                    cursor.execute('''
                        SELECT COUNT(*) FROM openalex_articles
                        WHERE DATE(scraped_at) = ?
                    ''', (latest_date,))
                    count = cursor.fetchone()[0]
                    total_last_scrape += count

                conn.close()

    return total_current, total_last_scrape, last_scrape_date

def display_recent_papers(journals, years, limit=20):
    """Display recently added papers (current session or most recent scrape)"""
    from datetime import datetime, timedelta

    all_papers = []
    display_label = "Current Session"
    cutoff_time = (datetime.now() - timedelta(minutes=5)).isoformat()

    # First, try to get papers from the last 5 minutes (current session)
    for journal in journals:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue

            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT title, authors_json, publication_date, cited_by_count, scraped_at, doi, openalex_id
                FROM openalex_articles
                WHERE scraped_at >= ?
                ORDER BY scraped_at DESC
                LIMIT ?
            ''', (cutoff_time, limit))

            papers = cursor.fetchall()
            for paper in papers:
                all_papers.append((journal, year, *paper))

            conn.close()

    # If no papers in current session, get papers from most recent scrape
    total_from_scrape = 0
    if not all_papers:
        for journal in journals:
            for year in years:
                db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
                if not os.path.exists(db_file):
                    continue

                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()

                # Get the most recent scrape date
                cursor.execute('SELECT MAX(scraped_at) FROM openalex_articles')
                latest_scrape = cursor.fetchone()[0]

                if latest_scrape:
                    # Get the date part (YYYY-MM-DD) from the latest scrape timestamp
                    latest_date = latest_scrape.split('T')[0]

                    # Count all papers from that scrape date
                    cursor.execute('''
                        SELECT COUNT(*) FROM openalex_articles
                        WHERE DATE(scraped_at) = ?
                    ''', (latest_date,))
                    total_from_scrape += cursor.fetchone()[0]

                    # Get all papers from that scrape date (no limit yet)
                    cursor.execute('''
                        SELECT title, authors_json, publication_date, cited_by_count, scraped_at, doi, openalex_id
                        FROM openalex_articles
                        WHERE DATE(scraped_at) = ?
                        ORDER BY scraped_at DESC
                    ''', (latest_date,))

                    papers = cursor.fetchall()
                    for paper in papers:
                        all_papers.append((journal, year, *paper))

                conn.close()

        # Set display label with count
        if all_papers and total_from_scrape > 0:
            latest_date = all_papers[0][6].split('T')[0] if all_papers[0][6] else 'Unknown'
            display_label = f"Most Recent Scrape ({latest_date}) ({total_from_scrape} added)"

    if not all_papers:
        print("\nNo papers found.")
        return

    # Sort by publication_date (most recent first) and limit
    all_papers.sort(key=lambda x: x[4] or '', reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal dimensions
    import shutil
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    # Calculate batch size - each paper takes 2 lines (paper + link)
    # Reserve space for header (4 lines) and footer
    batch_size = max(3, (terminal_height - 8) // 2)

    # Fixed column widths
    journal_width = 4
    date_width = 11
    cites_width = 6
    fixed_width = journal_width + date_width + cites_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 4  # 4 for spacing

    # Allocate remaining space: 40% to authors, 60% to title
    authors_width = max(20, int(remaining * 0.40))
    title_width = remaining - authors_width

    print(f"\n{'='*terminal_width}")
    # Combine count info smartly
    if "added)" in display_label:
        header = f"Recently Added Papers - {display_label[:-1]}, showing {len(all_papers)})"
    else:
        header = f"Recently Added Papers - {display_label} (showing {len(all_papers)})"
    print(header)
    print(f"{'='*terminal_width}")
    print(f"{'J':<{journal_width}} {'Date':<{date_width}} {'Cites':<{cites_width}} {'Authors':<{authors_width}} {'Title':<{title_width}}")
    print("-"*terminal_width)

    import json
    for idx, (journal, year, title, authors_json, pub_date, citations, scraped_at, doi, openalex_id) in enumerate(all_papers, 1):
        # Parse authors
        try:
            authors = json.loads(authors_json) if authors_json else []
            author_names = [a.get('name', '') for a in authors if a.get('name')]
            if author_names:
                # Show first 2 authors
                if len(author_names) > 2:
                    authors_str = ', '.join(author_names[:2]) + ' et al.'
                else:
                    authors_str = ', '.join(author_names)
            else:
                authors_str = 'N/A'
        except (json.JSONDecodeError, TypeError):
            authors_str = 'N/A'

        # Truncate for display (title gets extra -1 for safety)
        authors_short = (authors_str[:authors_width-3] + '...') if len(authors_str) > authors_width else authors_str
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{journal.upper():<{journal_width}} {pub_date or 'N/A':<{date_width}} {citations or 0:<{cites_width}} {authors_short:<{authors_width}} {title_short:<{title_width}}")

        # Print link on next line (in gray)
        gray = "\033[90m"
        reset = "\033[0m"
        if doi:
            print(f"{gray}  → https://doi.org/{doi}{reset}")
        elif openalex_id:
            print(f"{gray}  → {openalex_id}{reset}")
        else:
            print(f"{gray}  → (No link available){reset}")

        # Pagination
        if idx % batch_size == 0 and idx < len(all_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(all_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'J':<{journal_width}} {'Date':<{date_width}} {'Cites':<{cites_width}} {'Authors':<{authors_width}} {'Title':<{title_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def count_new_working_papers():
    """Count working papers added in current session and last session"""
    from datetime import datetime, timedelta

    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)

    total_current = 0
    total_last_scrape = 0
    last_scrape_date = None
    cutoff_time = (datetime.now() - timedelta(minutes=5)).isoformat()

    # First, count papers from current session (last 5 minutes)
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT COUNT(*) FROM working_papers
            WHERE scraped_at >= ?
        ''', (cutoff_time,))
        count = cursor.fetchone()[0]
        total_current += count

        conn.close()

    # If no papers in current session, count from most recent scrape
    if total_current == 0:
        for db_file in db_files:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Get the most recent scrape date
            cursor.execute('SELECT MAX(scraped_at) FROM working_papers')
            latest_scrape = cursor.fetchone()[0]

            if latest_scrape:
                latest_date = latest_scrape.split('T')[0]
                if not last_scrape_date:
                    last_scrape_date = latest_date

                # Count papers from that scrape date
                cursor.execute('''
                    SELECT COUNT(*) FROM working_papers
                    WHERE DATE(scraped_at) = ?
                ''', (latest_date,))
                count = cursor.fetchone()[0]
                total_last_scrape += count

            conn.close()

    return total_current, total_last_scrape, last_scrape_date

def display_recent_working_papers(limit=30):
    """Display most recently added working papers (current session or most recent scrape)"""
    from datetime import datetime, timedelta

    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)

    all_papers = []
    display_label = "Current Session"
    cutoff_time = (datetime.now() - timedelta(minutes=5)).isoformat()

    # First, try to get papers from the last 5 minutes (current session)
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, author_name, author_affiliation, publication_date,
                   primary_location, cited_by_count, scraped_at, doi, type
            FROM working_papers
            WHERE scraped_at >= ?
            ORDER BY scraped_at DESC
            LIMIT ?
        ''', (cutoff_time, limit))

        papers = cursor.fetchall()
        all_papers.extend(papers)

        conn.close()

    # If no papers in current session, get papers from most recent scrape
    total_from_scrape = 0
    if not all_papers:
        for db_file in db_files:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Get the most recent scrape date
            cursor.execute('SELECT MAX(scraped_at) FROM working_papers')
            latest_scrape = cursor.fetchone()[0]

            if latest_scrape:
                # Get the date part (YYYY-MM-DD) from the latest scrape timestamp
                latest_date = latest_scrape.split('T')[0]

                # Count all papers from that scrape date
                cursor.execute('''
                    SELECT COUNT(*) FROM working_papers
                    WHERE DATE(scraped_at) = ?
                ''', (latest_date,))
                total_from_scrape += cursor.fetchone()[0]

                # Get all papers from that scrape date (no limit yet)
                cursor.execute('''
                    SELECT title, author_name, author_affiliation, publication_date,
                           primary_location, cited_by_count, scraped_at, doi, type
                    FROM working_papers
                    WHERE DATE(scraped_at) = ?
                    ORDER BY scraped_at DESC
                ''', (latest_date,))

                papers = cursor.fetchall()
                all_papers.extend(papers)

            conn.close()

        # Set display label with count
        if all_papers and total_from_scrape > 0:
            latest_date = all_papers[0][6].split('T')[0] if all_papers[0][6] else 'Unknown'
            display_label = f"Most Recent Scrape ({latest_date}) ({total_from_scrape} added)"

    if not all_papers:
        print("\nNo working papers found.")
        return

    # Sort by publication_date (most recent first) and limit
    all_papers.sort(key=lambda x: x[3] or '', reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal dimensions
    import shutil
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    # Calculate batch size - 2 lines per paper (paper + link)
    # Reserve space for header (4 lines) and footer
    batch_size = max(5, (terminal_height - 8) // 2)

    # Helper to abbreviate source names
    def abbrev_source(loc, paper_type=None):
        if loc:
            loc_lower = loc.lower()
            if 'ssrn' in loc_lower:
                return 'SSRN'
            elif 'nber' in loc_lower:
                return 'NBER'
            elif 'arxiv' in loc_lower:
                return 'arXiv'
            elif 'repec' in loc_lower or 'ideas' in loc_lower:
                return 'RePEc'
            elif 'cepr' in loc_lower:
                return 'CEPR'
            elif 'imf' in loc_lower:
                return 'IMF'
            elif 'world bank' in loc_lower or 'worldbank' in loc_lower:
                return 'WB'
            elif 'fed' in loc_lower or 'federal reserve' in loc_lower:
                return 'Fed'
            elif 'econstor' in loc_lower:
                return 'Econ'
            elif 'dataverse' in loc_lower:
                return 'Data'
            else:
                # Take first 5 chars
                return loc[:5]
        # Fallback to type if no location
        if paper_type:
            type_map = {
                'preprint': 'WP',
                'report': 'Rept',
                'dissertation': 'Diss',
                'book': 'Book',
                'book-chapter': 'Chap',
                'dataset': 'Data',
            }
            return type_map.get(paper_type, paper_type[:4].title())
        return '?'

    # Fixed column widths
    num_width = 4
    source_width = 5
    date_width = 11
    citations_width = 6
    fixed_width = num_width + source_width + date_width + citations_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 5  # 5 spaces between 6 columns

    # Allocate remaining space: 25% author, 75% title
    author_width = max(15, int(remaining * 0.25))
    title_width = remaining - author_width

    # ANSI color codes
    gray = "\033[90m"
    reset = "\033[0m"

    print(f"\n{'='*terminal_width}")
    # Combine count info smartly
    if "added)" in display_label:
        header = f"Recently Added Working Papers - {display_label[:-1]}, showing {len(all_papers)})"
    else:
        header = f"Recently Added Working Papers - {display_label} (showing {len(all_papers)})"
    print(header)
    print(f"{'='*terminal_width}")
    print(f"{'#':<{num_width}} {'Src':<{source_width}} {'Date':<{date_width}} {'Cites':<{citations_width}} {'Author':<{author_width}} {'Title':<{title_width}}")
    print("-"*terminal_width)

    for idx, (title, author, affiliation, pub_date, location, citations, scraped_at, doi, paper_type) in enumerate(all_papers, 1):
        source_short = abbrev_source(location, paper_type)
        author_short = (author[:author_width-3] + '...') if author and len(author) > author_width else (author or 'N/A')
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{idx:<{num_width}} {source_short:<{source_width}} {pub_date or 'N/A':<{date_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {title_short:<{title_width}}")

        # Display link below
        if doi:
            # Remove https://doi.org/ prefix if present
            doi_clean = doi.replace('https://doi.org/', '') if doi.startswith('https://') else doi
            print(f"{gray}  → https://doi.org/{doi_clean}{reset}")
        else:
            print(f"{gray}  → (no link available){reset}")

        # Pagination
        if idx % batch_size == 0 and idx < len(all_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(all_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'#':<{num_width}} {'Src':<{source_width}} {'Date':<{date_width}} {'Cites':<{citations_width}} {'Author':<{author_width}} {'Title':<{title_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def display_newly_added_working_papers(limit=50):
    """Display working papers that were just added (current session or most recent scrape)"""
    from datetime import datetime, timedelta

    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)

    all_papers = []
    cutoff_time = (datetime.now() - timedelta(minutes=5)).isoformat()
    display_label = "Current Session"

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, author_name, publication_date, cited_by_count, scraped_at, doi, primary_location, type
            FROM working_papers
            WHERE scraped_at >= ?
            ORDER BY scraped_at DESC
            LIMIT ?
        ''', (cutoff_time, limit))

        papers = cursor.fetchall()
        all_papers.extend(papers)

        conn.close()

    # If no papers in current session, get papers from most recent scrape
    if not all_papers:
        for db_file in db_files:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Get the most recent scrape date
            cursor.execute('SELECT MAX(scraped_at) FROM working_papers')
            latest_scrape = cursor.fetchone()[0]

            if latest_scrape:
                # Get the date part (YYYY-MM-DD) from the latest scrape timestamp
                latest_date = latest_scrape.split('T')[0]
                display_label = f"Most Recent Scrape ({latest_date})"

                # Get all papers from that scrape date
                cursor.execute('''
                    SELECT title, author_name, publication_date, cited_by_count, scraped_at, doi, primary_location, type
                    FROM working_papers
                    WHERE DATE(scraped_at) = ?
                    ORDER BY scraped_at DESC
                    LIMIT ?
                ''', (latest_date, limit))

                papers = cursor.fetchall()
                all_papers.extend(papers)

            conn.close()

    if not all_papers:
        print("\nNo working papers found.")
        return

    # Sort by publication_date (most recent first), then by citations (most cited first)
    all_papers.sort(key=lambda x: (x[2] or '', x[3] or 0), reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal width and calculate column sizes
    import shutil
    terminal_width = shutil.get_terminal_size().columns
    terminal_width = max(78, min(terminal_width, 160))

    # Helper to abbreviate source names
    def abbrev_source(loc, paper_type=None):
        if loc:
            loc_lower = loc.lower()
            if 'ssrn' in loc_lower:
                return 'SSRN'
            elif 'nber' in loc_lower:
                return 'NBER'
            elif 'arxiv' in loc_lower:
                return 'arXiv'
            elif 'repec' in loc_lower or 'ideas' in loc_lower:
                return 'RePEc'
            elif 'cepr' in loc_lower:
                return 'CEPR'
            elif 'imf' in loc_lower:
                return 'IMF'
            elif 'world bank' in loc_lower or 'worldbank' in loc_lower:
                return 'WB'
            elif 'fed' in loc_lower or 'federal reserve' in loc_lower:
                return 'Fed'
            elif 'econstor' in loc_lower:
                return 'Econ'
            elif 'dataverse' in loc_lower:
                return 'Data'
            else:
                return loc[:5]
        # Fallback to type if no location
        if paper_type:
            type_map = {
                'preprint': 'WP',
                'report': 'Rept',
                'dissertation': 'Diss',
                'book': 'Book',
                'book-chapter': 'Chap',
                'dataset': 'Data',
            }
            return type_map.get(paper_type, paper_type[:4].title())
        return '?'

    # Fixed column widths
    num_width = 4
    source_width = 5
    date_width = 11
    citations_width = 6
    fixed_width = num_width + source_width + date_width + citations_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 5  # 5 spaces between 6 columns

    # Allocate remaining space: 25% author, 75% title
    author_width = max(15, int(remaining * 0.25))
    title_width = remaining - author_width

    # ANSI color codes
    gray = "\033[90m"
    reset = "\033[0m"

    print(f"\n{'='*terminal_width}")
    print(f"Newly Added Working Papers - {display_label} (showing {len(all_papers)})")
    print(f"{'='*terminal_width}")
    print(f"{'#':<{num_width}} {'Src':<{source_width}} {'Date':<{date_width}} {'Cites':<{citations_width}} {'Author':<{author_width}} {'Title':<{title_width}}")
    print("-"*terminal_width)

    for idx, (title, author, pub_date, citations, scraped_at, doi, location, paper_type) in enumerate(all_papers, 1):
        source_short = abbrev_source(location, paper_type)
        author_short = (author[:author_width-3] + '...') if author and len(author) > author_width else (author or 'N/A')
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{idx:<{num_width}} {source_short:<{source_width}} {pub_date or 'N/A':<{date_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {title_short:<{title_width}}")

        # Display link below
        if doi:
            doi_clean = doi.replace('https://doi.org/', '') if doi.startswith('https://') else doi
            print(f"{gray}  → https://doi.org/{doi_clean}{reset}")
        else:
            print(f"{gray}  → (no link available){reset}")

    print("="*terminal_width)


def get_wp_topics(year=None):
    """Get all topics from working papers database with paper counts.

    Args:
        year: Optional year filter (str like "2024" or "2023-2025")

    Returns:
        List of dicts: [{'name': 'Topic Name', 'count': N}, ...]
        Sorted by count descending.
    """
    db_path = os.path.join(DB_DIR, 'working_papers.db')
    if not os.path.exists(db_path):
        return []

    # Build year filter
    year_filter = ""
    if year:
        if '-' in str(year):
            start_year, end_year = str(year).split('-')
            year_filter = f"AND CAST(SUBSTR(publication_date, 1, 4) AS INTEGER) BETWEEN {start_year} AND {end_year}"
        else:
            year_filter = f"AND SUBSTR(publication_date, 1, 4) = '{year}'"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f'''
        SELECT topics_json FROM working_papers
        WHERE topics_json IS NOT NULL {year_filter}
    ''')

    topic_counts = {}
    for row in cursor.fetchall():
        topics_json = row[0]
        if topics_json:
            try:
                topics = json.loads(topics_json)
                for topic in topics:
                    topic_name = topic.get('name', '') or ''
                    if topic_name:
                        topic_counts[topic_name] = topic_counts.get(topic_name, 0) + 1
            except (json.JSONDecodeError, TypeError):
                pass

    conn.close()

    # Sort by count descending
    sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
    return [{'name': t[0], 'count': t[1]} for t in sorted_topics]


def rank_authors_by_wp_topic(topic_name, year=None, top_n=250, mincite=None):
    """Rank authors by their number of working papers in a specific topic.

    Args:
        topic_name: Topic to filter by (case-insensitive partial match)
        year: Optional year filter (str like "2024" or "2023-2025")
        top_n: Number of top authors to display
        mincite: Minimum citations filter
    """
    import shutil

    db_path = os.path.join(DB_DIR, 'working_papers.db')
    if not os.path.exists(db_path):
        print(f"No working papers database found.")
        return

    # Build year filter
    year_filter = ""
    year_desc = ""
    if year:
        if '-' in str(year):
            start_year, end_year = str(year).split('-')
            year_filter = f"AND CAST(SUBSTR(publication_date, 1, 4) AS INTEGER) BETWEEN {start_year} AND {end_year}"
            year_desc = f" ({start_year}-{end_year})"
        else:
            year_filter = f"AND SUBSTR(publication_date, 1, 4) = '{year}'"
            year_desc = f" ({year})"

    cite_filter = ""
    if mincite is not None:
        cite_filter = f"AND cited_by_count >= {mincite}"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all working papers with topics, then filter by topic in Python
    cursor.execute(f'''
        SELECT author_name, author_affiliation, title, publication_date, cited_by_count, topics_json
        FROM working_papers
        WHERE topics_json IS NOT NULL {year_filter} {cite_filter}
    ''')

    all_papers = cursor.fetchall()
    conn.close()

    # Filter papers by topic and count per author
    topic_lower = topic_name.lower()
    author_stats = {}  # {author_name: {'affiliation': '', 'papers': set(), 'citations': 0, 'latest_date': '', 'latest_title': ''}}

    for author_name, affiliation, title, pub_date, citations, topics_json in all_papers:
        if not topics_json:
            continue
        try:
            topics = json.loads(topics_json)
            # Check if any topic matches
            has_topic = any(topic_lower in (t.get('name', '') or '').lower() for t in topics)
            if not has_topic:
                continue

            if author_name not in author_stats:
                author_stats[author_name] = {
                    'affiliation': affiliation,
                    'papers': set(),
                    'citations': 0,
                    'latest_date': '',
                    'latest_title': ''
                }

            # Use title as unique paper identifier
            if title not in author_stats[author_name]['papers']:
                author_stats[author_name]['papers'].add(title)
                author_stats[author_name]['citations'] += citations or 0

            # Track latest paper
            if pub_date and (not author_stats[author_name]['latest_date'] or pub_date > author_stats[author_name]['latest_date']):
                author_stats[author_name]['latest_date'] = pub_date
                author_stats[author_name]['latest_title'] = title
        except (json.JSONDecodeError, TypeError):
            pass

    if not author_stats:
        print(f"\nNo working papers found for topic '{topic_name}'{year_desc}")
        return

    # Convert to list and sort by paper count
    ranked = []
    for author_name, stats in author_stats.items():
        ranked.append((
            author_name,
            stats['affiliation'],
            len(stats['papers']),
            stats['citations'],
            stats['latest_date'],
            stats['latest_title']
        ))

    ranked.sort(key=lambda x: (x[2], x[3]), reverse=True)  # Sort by paper count, then citations

    # Display
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines
    batch_size = max(10, terminal_height - 6)

    # Fixed column widths
    rank_width = 4
    papers_width = 4
    citations_width = 5
    date_width = 11
    fixed_width = rank_width + papers_width + citations_width + date_width

    # Calculate variable column widths
    remaining = terminal_width - fixed_width - 6
    author_width = max(20, int(remaining * 0.30))
    affiliation_width = max(12, int(remaining * 0.15))
    title_width = remaining - author_width - affiliation_width

    # Truncate topic name for display
    topic_display = (topic_name[:40] + '...') if len(topic_name) > 43 else topic_name

    print(f"\n{'='*terminal_width}")
    print(f"Author Rankings by Working Papers on '{topic_display}'{year_desc}")
    print(f"(Total: {len(ranked)} authors with {sum(r[2] for r in ranked)} papers)")
    print(f"{'='*terminal_width}")

    def print_header():
        print(f"{'Rank':<{rank_width}} {'#':<{papers_width}} {'Cites':<{citations_width}} {'Author':<{author_width}} {'Affiliation':<{affiliation_width}} {'Date':<{date_width}} {'Latest Title':<{title_width}}")
        print("-"*terminal_width)

    print_header()

    # Track Andreas Brøgger
    andreas_rank = None
    andreas_row = None
    for i, row in enumerate(ranked):
        if row[0] == 'Andreas Brøgger':
            andreas_rank = i + 1
            andreas_row = row
            break

    # Display loop
    display_count = 0
    for rank, (author_name, affiliation, wp_count, citations, latest_date, latest_title) in enumerate(ranked[:top_n], 1):
        author_short = (author_name[:author_width-3] + '...') if len(author_name) > author_width else author_name
        affil_short = (affiliation[:affiliation_width-3] + '...') if affiliation and len(affiliation) > affiliation_width else (affiliation or '')
        title_short = (latest_title[:title_width-4] + '...') if latest_title and len(latest_title) > (title_width-1) else (latest_title or 'N/A')

        # Highlight Andreas Brøgger
        if author_name == 'Andreas Brøgger':
            print(f"\033[94m{rank:<{rank_width}} {wp_count:<{papers_width}} {citations:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}\033[0m")
        else:
            print(f"{rank:<{rank_width}} {wp_count:<{papers_width}} {citations:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}")

        display_count += 1

        # Pagination
        if display_count % batch_size == 0 and rank < min(len(ranked), top_n):
            print("\n" + "-"*terminal_width)
            user_input = input(f"[{rank}/{min(len(ranked), top_n)}] Enter to continue, or type author name: ").strip()
            if user_input:
                # Import from query_wp_db
                from query_wp_db import show_author_working_papers
                show_author_working_papers(user_input, year=year, mincite=mincite)
            print("-"*terminal_width + "\n")
            print_header()

    # Show remaining count
    remaining_count = len(ranked) - top_n
    if remaining_count > 0:
        andreas_in_top = andreas_rank and andreas_rank <= top_n
        if not andreas_in_top and andreas_rank:
            print(f"\n... and {remaining_count - 1} more authors")
        else:
            print(f"\n... and {remaining_count} more authors")

    # Show Andreas Brøgger if not in top N
    if andreas_rank and andreas_rank > top_n and andreas_row:
        print()
        author_name, affiliation, wp_count, citations, latest_date, latest_title = andreas_row
        affil_short = (affiliation[:affiliation_width-3] + '...') if affiliation and len(affiliation) > affiliation_width else (affiliation or '')
        title_short = (latest_title[:title_width-4] + '...') if latest_title and len(latest_title) > (title_width-1) else (latest_title or 'N/A')
        print(f"\033[94m{andreas_rank:<{rank_width}} {wp_count:<{papers_width}} {citations:<{citations_width}} {'Andreas Brøgger':<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}\033[0m")

    print("="*terminal_width)


def select_wp_topic_fzf(year=None):
    """Show fzf menu to select a topic from working papers.

    Args:
        year: Optional year filter (str like "2024" or "2023-2025")

    Returns:
        Tuple (topic_name, year) if selected, (None, None) if cancelled
    """
    topics = get_wp_topics(year=year)

    if not topics:
        year_desc = f" for {year}" if year else ""
        print(f"\nNo topics found in working papers{year_desc}.")
        return None, None

    # Create fzf input: "topic_name (N papers)"
    fzf_lines = [f"{t['name']} ({t['count']} papers)" for t in topics]
    fzf_input = "\n".join(fzf_lines)

    year_desc = f" ({year})" if year else ""
    try:
        result = subprocess.run(
            ['fzf', '--height=50%', '--reverse', '--prompt=Topic: ',
             f'--header=Select topic to rank authors{year_desc} ({len(topics)} topics)'],
            input=fzf_input,
            capture_output=True,
            text=True
        )

        if result.returncode != 0 or not result.stdout.strip():
            return None, None  # User cancelled

        # Extract topic name from "Topic Name (N papers)"
        selected = result.stdout.strip()
        paren_idx = selected.rfind(' (')
        topic_name = selected[:paren_idx] if paren_idx > 0 else selected

        return topic_name, year

    except FileNotFoundError:
        print("fzf not found. Install with: brew install fzf")
        return None, None


def display_most_recent_papers_by_date(journals, years, limit=50):
    """Display the 50 most recent journal papers by publication date"""
    all_papers = []

    for journal in journals:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue

            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT title, authors_json, publication_date, cited_by_count, doi, openalex_id
                FROM openalex_articles
                ORDER BY publication_date DESC
                LIMIT ?
            ''', (limit,))

            papers = cursor.fetchall()
            for paper in papers:
                all_papers.append((journal, year, *paper))

            conn.close()

    if not all_papers:
        print("\nNo papers found.")
        return

    # Sort by publication_date (most recent first) and limit
    all_papers.sort(key=lambda x: x[4] or '', reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal dimensions
    import shutil
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    # Calculate batch size - each paper takes 2 lines (paper + link)
    batch_size = max(3, (terminal_height - 8) // 2)

    # Fixed column widths
    journal_width = 6
    year_width = 5
    date_width = 11
    fixed_width = journal_width + year_width + date_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 4  # 4 for spacing

    # Allocate remaining space: 40% to authors, 60% to title
    authors_width = max(20, int(remaining * 0.40))
    title_width = remaining - authors_width

    print(f"\n{'='*terminal_width}")
    print(f"Most Recent Papers by Publication Date (showing {len(all_papers)})")
    print(f"{'='*terminal_width}")
    print(f"{'J':<{journal_width}} {'Year':<{year_width}} {'Date':<{date_width}} {'Authors':<{authors_width}} {'Title':<{title_width}}")
    print("-"*terminal_width)

    import json
    for idx, (journal, year, title, authors_json, pub_date, citations, doi, openalex_id) in enumerate(all_papers, 1):
        # Parse authors
        try:
            authors = json.loads(authors_json) if authors_json else []
            author_names = [a.get('name', '') for a in authors if a.get('name')]
            if author_names:
                # Show first 2 authors
                if len(author_names) > 2:
                    authors_str = ', '.join(author_names[:2]) + ' et al.'
                else:
                    authors_str = ', '.join(author_names)
            else:
                authors_str = 'N/A'
        except (json.JSONDecodeError, TypeError):
            authors_str = 'N/A'

        # Truncate for display
        authors_short = (authors_str[:authors_width-3] + '...') if len(authors_str) > authors_width else authors_str
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{journal.upper():<{journal_width}} {year:<{year_width}} {pub_date or 'N/A':<{date_width}} {authors_short:<{authors_width}} {title_short:<{title_width}}")

        # Print link on next line (in gray)
        gray = "\033[90m"
        reset = "\033[0m"
        if doi:
            print(f"{gray}  → https://doi.org/{doi}{reset}")
        elif openalex_id:
            print(f"{gray}  → {openalex_id}{reset}")
        else:
            print(f"{gray}  → (No link available){reset}")

        # Pagination
        if idx % batch_size == 0 and idx < len(all_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(all_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'J':<{journal_width}} {'Year':<{year_width}} {'Date':<{date_width}} {'Authors':<{authors_width}} {'Title':<{title_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def display_recent_author_papers(author_name, source='all', limit=20):
    """Display recent papers by a specific author

    Args:
        author_name: Name of the author to search for
        source: 'top3' (journals), 'wp' (working papers), or 'all' (both)
        limit: Number of papers to show
    """
    import shutil
    all_papers = []

    # Handle "brogger" or "broegger" -> also search for "brøgger"
    alt_author_name = None
    if 'brogger' in author_name.lower():
        alt_author_name = author_name.lower().replace('brogger', 'brøgger')
    elif 'broegger' in author_name.lower():
        alt_author_name = author_name.lower().replace('broegger', 'brøgger')

    # Search journal papers if source is 'top3' or 'all'
    if source in ['top3', 'all']:
        journals = ['jf', 'rfs', 'jfe']
        years = range(2023, datetime.now().year + 1)

        for journal in journals:
            for year in years:
                db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
                if not os.path.exists(db_file):
                    continue

                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()

                if alt_author_name:
                    cursor.execute('''
                        SELECT title, authors_json, publication_date, cited_by_count, doi, openalex_id
                        FROM openalex_articles
                        WHERE authors_json LIKE ? OR authors_json LIKE ?
                        ORDER BY publication_date DESC
                    ''', (f'%{author_name}%', f'%{alt_author_name}%'))
                else:
                    cursor.execute('''
                        SELECT title, authors_json, publication_date, cited_by_count, doi, openalex_id
                        FROM openalex_articles
                        WHERE authors_json LIKE ?
                        ORDER BY publication_date DESC
                    ''', (f'%{author_name}%',))

                papers = cursor.fetchall()
                for paper in papers:
                    all_papers.append(('journal', journal.upper(), year, *paper))

                conn.close()

    # Search working papers if source is 'wp' or 'all'
    if source in ['wp', 'all']:
        pattern = os.path.join(DB_DIR, 'working_papers*.db')
        db_files = glob.glob(pattern)

        for db_file in db_files:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            if alt_author_name:
                cursor.execute('''
                    SELECT title, author_name, publication_date, cited_by_count, primary_location, doi, openalex_id
                    FROM working_papers
                    WHERE author_name LIKE ? OR author_name LIKE ?
                    ORDER BY publication_date DESC
                ''', (f'%{author_name}%', f'%{alt_author_name}%'))
            else:
                cursor.execute('''
                    SELECT title, author_name, publication_date, cited_by_count, primary_location, doi, openalex_id
                    FROM working_papers
                    WHERE author_name LIKE ?
                    ORDER BY publication_date DESC
                ''', (f'%{author_name}%',))

            papers = cursor.fetchall()
            for paper in papers:
                all_papers.append(('wp', 'WP', '', *paper))

            conn.close()

    if not all_papers:
        print(f"\n⚠️  No papers found for '{author_name}'")
        return

    # Sort by publication date and limit
    all_papers.sort(key=lambda x: x[5] or '', reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal dimensions
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    # Calculate batch size - each paper takes 2 lines (paper + link)
    batch_size = max(3, (terminal_height - 8) // 2)

    # Fixed column widths
    type_width = 6
    journal_width = 5
    date_width = 11
    citations_width = 7
    fixed_width = type_width + journal_width + date_width + citations_width

    # Calculate variable column widths
    remaining = terminal_width - fixed_width - 5
    author_width = max(15, int(remaining * 0.25))
    title_width = remaining - author_width

    print(f"\n{'='*terminal_width}")
    print(f"Recent Papers by '{author_name}' (found {len(all_papers)})")
    print(f"{'='*terminal_width}")
    print(f"{'Type':<{type_width}} {'J':<{journal_width}} {'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
    print("-"*terminal_width)

    import json
    for idx, paper in enumerate(all_papers, 1):
        paper_type = paper[0]

        if paper_type == 'journal':
            _, journal, year, title, authors_json, pub_date, citations, doi, openalex_id = paper

            # Parse authors
            try:
                authors = json.loads(authors_json) if authors_json else []
                author_names = [a.get('name', '') for a in authors if a.get('name')]
                if len(author_names) > 2:
                    authors_str = ', '.join(author_names[:2]) + ' et al.'
                else:
                    authors_str = ', '.join(author_names) if author_names else 'N/A'
            except (json.JSONDecodeError, TypeError):
                authors_str = 'N/A'

            location = journal

        else:  # working paper
            _, journal, year_placeholder, title, author_name_wp, pub_date, citations, location, doi, openalex_id = paper
            authors_str = author_name_wp or 'N/A'
            year = ''

        # Truncate for display
        authors_short = (authors_str[:author_width-3] + '...') if len(authors_str) > author_width else authors_str
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')

        print(f"{paper_type.upper():<{type_width}} {journal:<{journal_width}} {pub_date or 'N/A':<{date_width}} {authors_short:<{author_width}} {title_short:<{title_width}} {citations or 0:<{citations_width}}")

        # Print link on next line (flush right, gray)
        if doi:
            link = f"https://doi.org/{doi}"
        elif openalex_id:
            link = openalex_id
        else:
            link = "(No link available)"

        print(f"\033[90m{link:>{terminal_width}}\033[0m")

        # Pagination
        if idx % batch_size == 0 and idx < len(all_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(all_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'Type':<{type_width}} {'J':<{journal_width}} {'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def display_most_recent_working_papers_by_date(limit=50):
    """Display the 50 most recent working papers by publication date"""
    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)

    all_papers = []

    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT title, author_name, author_affiliation, publication_date,
                   primary_location, cited_by_count
            FROM working_papers
            ORDER BY publication_date DESC
            LIMIT ?
        ''', (limit,))

        papers = cursor.fetchall()
        all_papers.extend(papers)

        conn.close()

    if not all_papers:
        print("\nNo working papers found.")
        return

    # Sort by publication_date (most recent first) and limit
    all_papers.sort(key=lambda x: x[3] or '', reverse=True)
    all_papers = all_papers[:limit]

    # Get terminal dimensions
    import shutil
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    # Calculate batch size - working papers don't have links, so 1 line each
    batch_size = max(5, terminal_height - 8)

    # Fixed column widths
    date_width = 11
    citations_width = 7
    fixed_width = date_width + citations_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 3  # 3 spaces between 4 columns

    # Allocate remaining space: 30% author, 70% title
    author_width = max(20, int(remaining * 0.30))
    title_width = remaining - author_width

    print(f"\n{'='*terminal_width}")
    print(f"Most Recent Working Papers by Publication Date (showing {len(all_papers)})")
    print(f"{'='*terminal_width}")
    print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
    print("-"*terminal_width)

    for idx, (title, author, affiliation, pub_date, location, citations) in enumerate(all_papers, 1):
        author_short = (author[:author_width-3] + '...') if author and len(author) > author_width else (author or 'N/A')
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{pub_date or 'N/A':<{date_width}} {author_short:<{author_width}} {title_short:<{title_width}} {citations or 0:<{citations_width}}")

        # Pagination
        if idx % batch_size == 0 and idx < len(all_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(all_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def display_working_papers_by_authors(author_names, limit=50):
    """Display working papers filtered by a list of author names"""
    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)

    if not db_files:
        print("\nNo working papers database found.")
        return

    all_papers = []

    # Build SQL WHERE clause for author matching
    # Use LIKE for partial matching
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        for author_name in author_names:
            cursor.execute('''
                SELECT title, author_name, author_affiliation, publication_date,
                       primary_location, cited_by_count
                FROM working_papers
                WHERE author_name LIKE ?
                ORDER BY publication_date DESC
            ''', (f'%{author_name}%',))

            papers = cursor.fetchall()
            all_papers.extend(papers)

        conn.close()

    if not all_papers:
        print(f"\nNo working papers found for the selected {len(author_names)} authors.")
        return

    # Remove duplicates (same title)
    seen_titles = set()
    unique_papers = []
    for paper in all_papers:
        if paper[0] not in seen_titles:
            seen_titles.add(paper[0])
            unique_papers.append(paper)

    # Sort by publication date and limit
    unique_papers.sort(key=lambda x: x[3] or '', reverse=True)
    unique_papers = unique_papers[:limit]

    # Get terminal dimensions
    import shutil
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines

    batch_size = max(5, terminal_height - 8)

    # Column widths
    date_width = 11
    citations_width = 7
    fixed_width = date_width + citations_width
    remaining = terminal_width - fixed_width - 3
    author_width = max(20, int(remaining * 0.30))
    title_width = remaining - author_width

    print(f"\n{'='*terminal_width}")
    print(f"Working Papers from Topic-Based Authors (showing {len(unique_papers)} of {len(seen_titles)} unique)")
    print(f"{'='*terminal_width}")
    print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
    print("-"*terminal_width)

    for idx, (title, author, affiliation, pub_date, location, citations) in enumerate(unique_papers, 1):
        author_short = (author[:author_width-3] + '...') if author and len(author) > author_width else (author or 'N/A')
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')
        print(f"{pub_date or 'N/A':<{date_width}} {author_short:<{author_width}} {title_short:<{title_width}} {citations or 0:<{citations_width}}")

        if idx % batch_size == 0 and idx < len(unique_papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(unique_papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
            print("-"*terminal_width)

    print("="*terminal_width)

def main():
    # Check for --help flag first
    if '--help' in sys.argv or '-h' in sys.argv:
        # If it's for a subcommand, let the subcommand handle it
        if 'prolific-authors' not in sys.argv and 'wp' not in sys.argv and 'workingpapers' not in sys.argv and 'topic' not in sys.argv and 'papers' not in sys.argv:
            print("Usage: finance-papers [command] [options]")
            print()
            print("Commands:")
            print("  (default)          Interactive mode - update papers, rank authors, fetch working papers")
            print("  papers             Browse papers ranked by citations (with fzf search)")
            print("  prolific-authors   Show authors with multiple publications in top 3 journals")
            print("  topic TOPIC        Find authors by topic and fetch their working papers")
            print("  wp [year]          Skip to working papers section (alias: workingpapers)")
            print()
            print("Options:")
            print("  --force            Force re-scrape all years (updates citation counts)")
            print("  --alltop           Include economics journals (QJE, AER, Econometrica, JPE, ReStud)")
            print("  --help, -h         Show this help message")
            print()
            print("Journal Groups:")
            print("  top3     Top 3 finance journals (JF, RFS, JFE) - default")
            print("  econ5    Top 5 economics journals (QJE, AER, Econometrica, JPE, ReStud)")
            print("  alltop   All top journals (finance + economics)")
            print()
            print("Examples:")
            print("  finance-papers                       # Interactive mode (top 3 finance)")
            print("  finance-papers --alltop              # Include economics journals")
            print("  finance-papers --force               # Force update all data")
            print("  finance-papers papers                # Browse papers with fzf")
            print("  finance-papers papers 2025           # Papers from 2025 only")
            print("  finance-papers papers --year=2024    # Papers from 2024 only")
            print("  finance-papers prolific-authors      # Show prolific authors")
            print("  finance-papers prolific-authors -h   # Help for prolific-authors")
            print("  finance-papers topic \"Asset Pricing\" # Find authors by topic")
            print("  finance-papers topic Corporate -n3   # Topic with min 3 papers")
            print("  finance-papers wp                    # Skip to working papers section")
            print("  finance-papers workingpapers 2024    # WP section with year filter")
            return

    # Check for prolific-authors subcommand first
    if 'prolific-authors' in sys.argv:
        # Check for --help flag
        if '--help' in sys.argv or '-h' in sys.argv:
            print("Usage: finance-papers prolific-authors [options]")
            print()
            print("Options:")
            print("  --min=N, -n=N, -nN   Minimum papers to qualify (default: 2)")
            print("  --max=N              Maximum authors to display")
            print("  --start=YYYY         Start year (default: 2022)")
            print("  --end=YYYY           End year (default: 2025)")
            print("  --topic TOPIC        Only count papers with this topic")
            print("  --topicany TOPIC     Authors with any paper in this topic")
            print("  --topicmain TOPIC    Authors whose main topic matches")
            print("  --update             Update journal articles before display")
            print("  --no-pager           Disable pagination")
            print("  --help, -h           Show this help message")
            print()
            print("Examples:")
            print("  finance-papers prolific-authors                          # 2+ papers (2022-2025)")
            print("  finance-papers prolific-authors -n3                      # 3+ papers")
            print("  finance-papers prolific-authors --max=50                 # Show top 50 only")
            print("  finance-papers prolific-authors --topic \"Asset Pricing\"  # Only Asset Pricing papers")
            print("  finance-papers prolific-authors --topicany Corporate     # Any paper in Corporate")
            print("  finance-papers prolific-authors --topicmain Corporate    # Main topic is Corporate")
            print("  finance-papers prolific-authors --update                 # Update data first")
            return

        # Try relative import first (when run as module), then absolute (when run directly)
        try:
            from src.query_openalex_db import prolific_authors
        except ImportError:
            from query_openalex_db import prolific_authors

        # Parse --min=N, --max=N, --start=YYYY, --end=YYYY, --topic, --topicany, --topicmain, --no-pager, --update flags
        min_papers = 2
        max_authors = None  # None means show all
        start_year = 2022
        end_year = 2025
        no_pager = False
        do_update = False
        topic_filter = None      # --topic: only show papers with this topic
        topic_any_filter = None  # --topicany: authors with any paper in this topic
        topic_main_filter = None # --topicmain: authors whose main topic matches

        args = sys.argv[1:]
        i = 0
        while i < len(args):
            arg = args[i]
            if arg.startswith('--min='):
                try:
                    min_papers = int(arg[6:])
                except ValueError:
                    print(f"Invalid --min flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('-n='):
                try:
                    min_papers = int(arg[3:])
                except ValueError:
                    print(f"Invalid -n flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('-n') and len(arg) > 2 and arg[2:].isdigit():
                # Handle -n3 format (no equals sign)
                try:
                    min_papers = int(arg[2:])
                except ValueError:
                    print(f"Invalid -n flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('--max='):
                try:
                    max_authors = int(arg[6:])
                except ValueError:
                    print(f"Invalid --max flag: {arg}")
                    sys.exit(1)
            elif arg == '--topic':
                # Next argument is the topic value - filter papers by topic
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    topic_filter = args[i + 1]
                    i += 1
                else:
                    print("--topic requires a value")
                    sys.exit(1)
            elif arg == '--topicany':
                # Next argument is the topic value - authors with any paper in topic
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    topic_any_filter = args[i + 1]
                    i += 1
                else:
                    print("--topicany requires a value")
                    sys.exit(1)
            elif arg == '--topicmain':
                # Next argument is the topic value - authors whose main topic matches
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    topic_main_filter = args[i + 1]
                    i += 1
                else:
                    print("--topicmain requires a value")
                    sys.exit(1)
            elif arg.startswith('--start='):
                try:
                    start_year = int(arg[8:])
                except ValueError:
                    print(f"Invalid --start flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('--end='):
                try:
                    end_year = int(arg[6:])
                except ValueError:
                    print(f"Invalid --end flag: {arg}")
                    sys.exit(1)
            elif arg == '--no-pager':
                no_pager = True
            elif arg == '--update':
                do_update = True
            elif arg.startswith('-'):
                print(f"Unknown flag: {arg}")
                print("Valid flags: --min=N, -n=N, -nN, --max=N, --topic TOPIC, --topicany TOPIC, --topicmain TOPIC, --start=YYYY, --end=YYYY, --no-pager, --update")
                sys.exit(1)
            i += 1

        # Update journal articles if --update flag is set
        if do_update:
            journals = ['jf', 'rfs', 'jfe']
            years = list(range(start_year, end_year + 1))

            print(f"\n🔄 Updating journal articles for years {start_year}-{end_year}...")

            # Pass journal group to script for parallel fetching
            for year in years:
                script_path = os.path.join(script_dir, 'getpapers_openalex.py')
                cmd = [sys.executable, script_path, 'top3', str(year)]
                run_command(cmd, f"Updating TOP3 {year}")

            # Count and display new papers
            print(f"\n{'='*80}")
            print("Summary: Papers Added")
            print(f"{'='*80}")

            current_count, last_count, last_date = count_new_papers_added(journals, years)

            if current_count > 0:
                print(f"\n✅ Total new papers added this session: {current_count}")
            else:
                print(f"\n📋 No new papers added this session.")
                if last_count > 0 and last_date:
                    print(f"   Last scrape ({last_date}): {last_count} papers were added")

            print()

        prolific_authors(min_papers, start_year, end_year, no_pager, max_authors, topic_filter, topic_any_filter, topic_main_filter)
        return

    # Check for papers subcommand
    if 'papers' in sys.argv:
        # Check for --help flag
        if '--help' in sys.argv or '-h' in sys.argv:
            print("Usage: finance-papers papers [YEAR] [options]")
            print()
            print("Browse papers from top finance journals ranked by citations.")
            print("Uses fzf for interactive fuzzy searching through all papers.")
            print()
            print("Arguments:")
            print("  YEAR                 Filter by specific year (e.g., 2025)")
            print()
            print("Options:")
            print("  --year=YYYY          Filter by specific year (alternative syntax)")
            print("  --years=YYYY-YYYY    Filter by year range (default: last 4 years)")
            print("  --limit=N            Maximum papers to load (default: 1000)")
            print("  --no-fzf             Show ranked list without fzf")
            print("  --help, -h           Show this help message")
            print()
            print("Examples:")
            print("  finance-papers papers                # Browse all papers with fzf")
            print("  finance-papers papers 2025          # Papers from 2025 only")
            print("  finance-papers papers --year=2024   # Papers from 2024 only")
            print("  finance-papers papers --years=2023-2025  # Papers from 2023-2025")
            print("  finance-papers papers --no-fzf      # Show ranked list without fzf")
            return

        # Parse options
        year_filter = None
        paper_limit = 1000
        use_fzf = True

        args = sys.argv[1:]
        for arg in args:
            if arg.startswith('--year='):
                year_filter = arg[7:]
            elif arg.startswith('--years='):
                year_filter = arg[8:]
            elif arg.startswith('--limit='):
                try:
                    paper_limit = int(arg[8:])
                except ValueError:
                    print(f"Invalid --limit flag: {arg}")
                    sys.exit(1)
            elif arg == '--no-fzf':
                use_fzf = False
            elif arg != 'papers' and arg.isdigit() and len(arg) == 4:
                # Positional year argument (e.g., "finance-papers papers 2025")
                year_filter = arg

        # Load papers
        print(f"\n{'='*80}")
        if year_filter:
            print(f"Loading papers from {year_filter}...")
        else:
            current_year = datetime.now().year
            print(f"Loading papers from {current_year-3}-{current_year}...")
        print(f"{'='*80}\n")

        papers = get_papers_by_citations(year_filter=year_filter, limit=paper_limit)

        if not papers:
            print("No papers found in database.")
            print("Run 'finance-papers' to update journal articles first.")
            sys.exit(0)

        print(f"Loaded {len(papers)} papers ranked by citations\n")

        if use_fzf:
            # Create fzf input: "citations | journal | year | authors | title"
            fzf_lines = []
            for i, p in enumerate(papers, 1):  # Start from 1
                # Format: rank | citations | journal | year | first author | title
                first_author = p['authors'][0] if p['authors'] else 'Unknown'
                if len(first_author) > 20:
                    first_author = first_author[:17] + '...'
                title = p['title']
                if len(title) > 60:
                    title = title[:57] + '...'
                line = f"{i:4d} | {p['citations']:4d} cites | {p['journal']:3s} | {p['year']} | {first_author:<20s} | {title}"
                fzf_lines.append(line)

            fzf_input = "\n".join(fzf_lines)

            # Run fzf in a loop to allow browsing multiple papers
            while True:
                try:
                    result = subprocess.run(
                        ['fzf', '--height=80%', '--reverse',
                         '--prompt=Search papers (ESC to quit): ',
                         '--header=Rank | Citations   | J   | Year | Author               | Title',
                         '--preview-window=hidden'],
                        input=fzf_input,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode == 0 and result.stdout.strip():
                        # Extract rank from the selected line (1-indexed)
                        selected = result.stdout.strip()
                        try:
                            rank = int(selected.split('|')[0].strip())
                            paper = papers[rank - 1]  # Convert to 0-indexed
                            display_paper_details(paper)

                            # Ask if user wants to continue browsing
                            cont = input("\nPress Enter to search again, or 'q' to quit: ").strip().lower()
                            if cont == 'q':
                                break
                        except (ValueError, IndexError):
                            print("Error parsing selection")
                            break
                    else:
                        # User pressed ESC or cancelled
                        break

                except FileNotFoundError:
                    print("fzf not found. Install with: brew install fzf")
                    print("Falling back to ranked list...\n")
                    use_fzf = False
                    break

        if not use_fzf:
            # Display ranked list without fzf
            import shutil
            terminal_size = shutil.get_terminal_size()
            terminal_width = max(78, min(terminal_size.columns, 160))
            terminal_height = terminal_size.lines
            batch_size = max(5, terminal_height - 10)

            print(f"{'─'*terminal_width}")
            print(f"{'Rank':<6} {'Cites':<7} {'J':<4} {'Year':<6} {'Authors':<25} {'Title':<50}")
            print(f"{'─'*terminal_width}")

            for idx, paper in enumerate(papers[:100], 1):  # Show top 100
                authors = paper['authors_str']
                if len(authors) > 23:
                    authors = authors[:20] + '...'
                title = paper['title']
                if len(title) > 48:
                    title = title[:45] + '...'

                print(f"{idx:<6} {paper['citations']:<7} {paper['journal']:<4} {paper['year']:<6} {authors:<25} {title:<50}")

                if idx % batch_size == 0 and idx < min(100, len(papers)):
                    print(f"\n{'─'*terminal_width}")
                    user_input = input(f"Showing {idx}/100. Press Enter to continue (q to quit, number to view details): ").strip()
                    if user_input.lower() == 'q':
                        break
                    elif user_input.isdigit():
                        paper_idx = int(user_input) - 1
                        if 0 <= paper_idx < len(papers):
                            display_paper_details(papers[paper_idx])
                            input("\nPress Enter to continue...")
                    print(f"{'─'*terminal_width}")
                    print(f"{'Rank':<6} {'Cites':<7} {'J':<4} {'Year':<6} {'Authors':<25} {'Title':<50}")
                    print(f"{'─'*terminal_width}")

            print(f"{'─'*terminal_width}")

        return

    # Check for topic subcommand
    if 'topic' in sys.argv:
        # Check for --help flag
        if '--help' in sys.argv or '-h' in sys.argv:
            print("Usage: finance-papers topic [TOPIC] [options]")
            print()
            print("Find authors who have published papers on a specific topic and optionally")
            print("fetch their working papers.")
            print()
            print("If TOPIC is omitted, shows an interactive selection of popular topics.")
            print("Press 'f' to fuzzy search all topics with fzf.")
            print()
            print("Arguments:")
            print("  TOPIC              Topic name to search for (case-insensitive partial match)")
            print("                     If omitted, shows selection menu (use 'f' for fzf search)")
            print()
            print("Options:")
            print("  --min=N, -n=N, -nN   Minimum papers on topic to qualify (default: 1)")
            print("  --max=N              Maximum authors to display/process (default: 250)")
            print("  --wp                 Fetch working papers for found authors")
            print("  --wp-clean           Fetch working papers (clean mode - replace all)")
            print("  --no-pager           Disable pagination")
            print("  --help, -h           Show this help message")
            print()
            print("Examples:")
            print("  finance-papers topic                         # Interactive topic selection")
            print("  finance-papers topic \"Asset Pricing\"         # Find authors on Asset Pricing")
            print("  finance-papers topic Corporate -n3           # Min 3 papers on topic")
            print("  finance-papers topic \"Climate\" --wp          # Find and fetch working papers")
            print("  finance-papers topic Banking --max=50        # Limit to 50 authors")
            print("  finance-papers topic \"Market Microstructure\" --wp-clean  # Replace WP data")
            return

        # Parse topic name and options
        topic_name = None
        min_topic_papers = 1
        max_topic_authors = 250
        fetch_wp = False
        wp_clean = False
        no_pager = False

        args = sys.argv[1:]
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == 'topic':
                # Next non-flag argument is the topic name
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    topic_name = args[i + 1]
                    i += 1
            elif arg.startswith('--min='):
                try:
                    min_topic_papers = int(arg[6:])
                except ValueError:
                    print(f"Invalid --min flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('-n='):
                try:
                    min_topic_papers = int(arg[3:])
                except ValueError:
                    print(f"Invalid -n flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('-n') and len(arg) > 2 and arg[2:].isdigit():
                try:
                    min_topic_papers = int(arg[2:])
                except ValueError:
                    print(f"Invalid -n flag: {arg}")
                    sys.exit(1)
            elif arg.startswith('--max='):
                try:
                    max_topic_authors = int(arg[6:])
                except ValueError:
                    print(f"Invalid --max flag: {arg}")
                    sys.exit(1)
            elif arg == '--wp':
                fetch_wp = True
            elif arg == '--wp-clean':
                fetch_wp = True
                wp_clean = True
            elif arg == '--no-pager':
                no_pager = True
            elif arg.startswith('-') and arg not in ['-h', '--help']:
                # Unknown flag - but it might be a topic starting with -
                pass
            i += 1

        if not topic_name:
            # Launch fzf directly with all topics
            print("Loading topics...")
            all_topics = get_popular_topics_from_db(limit=500)

            if not all_topics:
                print("No topics found in database. Run journal article updates first.")
                sys.exit(1)

            # Create fzf input: "topic_name (N papers)"
            fzf_input = "\n".join([f"{t['name']} ({t['count']} papers)" for t in all_topics])

            while not topic_name:
                try:
                    result = subprocess.run(
                        ['fzf', '--height=40%', '--reverse', '--prompt=Topic: '],
                        input=fzf_input,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode == 0 and result.stdout.strip():
                        # Extract topic name (remove the " (N papers)" suffix)
                        selected = result.stdout.strip()
                        # Find the last occurrence of " (" to handle topics with parentheses
                        paren_idx = selected.rfind(' (')
                        if paren_idx > 0:
                            topic_name = selected[:paren_idx]
                        else:
                            topic_name = selected
                    else:
                        print("No topic selected. Press Enter to try again, or type a topic name:")
                        manual = input().strip()
                        if manual:
                            topic_name = manual
                except FileNotFoundError:
                    print("fzf not found. Install with: brew install fzf")
                    topic_name = input("Enter topic name: ").strip()
                    if not topic_name:
                        sys.exit(1)

        # Get terminal dimensions for display
        import shutil
        terminal_size = shutil.get_terminal_size()
        terminal_width = max(78, min(terminal_size.columns, 160))

        # Loop to allow filtering and author selection
        while True:
            # Find authors by topic
            print(f"\n{'='*80}")
            print(f"Authors with papers on '{topic_name}' (min {min_topic_papers} paper{'s' if min_topic_papers != 1 else ''}):")
            print(f"{'='*80}\n")

            topic_authors = get_authors_by_topic_from_db(
                topic_name=topic_name,
                min_papers=min_topic_papers,
                max_authors=max_topic_authors
            )

            if not topic_authors:
                print(f"No authors found with {min_topic_papers}+ papers on '{topic_name}'")
                if min_topic_papers > 1:
                    print("Try a lower number to see more authors.")
                    user_input = input("\n[Enter] Exit  [Number] Filter by min papers: ").strip()
                    if user_input.isdigit() and int(user_input) >= 1:
                        min_topic_papers = int(user_input)
                        continue
                sys.exit(0)

            # Display ranking
            print(f"{'Rank':<6} {'Author':<42} {'Papers':<8} {'Citations'}")
            print(f"{'─'*terminal_width}")
            for idx, author in enumerate(topic_authors[:50], 1):  # Show top 50
                print(f"{idx:<6} {author['name']:<42} {author['topic_papers']:<8} {author['total_citations']}")
            if len(topic_authors) > 50:
                print(f"... and {len(topic_authors) - 50} more authors")
            print(f"{'─'*terminal_width}")
            print(f"Total: {len(topic_authors)} authors\n")

            # Prompt for action (single key press)
            print("[Enter] Select  [1-9] Filter  [n] Saved WPs  [w] Fetch WPs  [q] Quit", end='', flush=True)

            import tty
            import termios
            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(fd)
                ch = sys.stdin.read(1)
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            print()  # Newline after keypress

            if ch.lower() == 'q' or ord(ch) == 3:  # q or Ctrl-C
                sys.exit(0)
            elif ord(ch) == 27:  # Esc
                sys.exit(0)
            elif ch.isdigit() and int(ch) >= 1:
                min_topic_papers = int(ch)
                continue  # Re-display with new filter
            elif ch.lower() == 'n':
                # Display saved working papers with topic selection via fzf
                wp_db_path = os.path.join(DB_DIR, 'working_papers.db')
                if not os.path.exists(wp_db_path):
                    print("\nWorking papers database not found.")
                    print("Press 'w' to fetch working papers from OpenAlex.")
                    print("[Any key] Back", end='', flush=True)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        tty.setraw(fd)
                        sys.stdin.read(1)
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    print()
                    continue

                import sqlite3 as sqlite3_wp
                conn = sqlite3_wp.connect(wp_db_path)
                cursor = conn.cursor()

                # Get author names for lookup
                author_names = [a['name'] for a in topic_authors]

                # Find working papers by these authors (include topics_json and type)
                placeholders = ','.join(['?' for _ in author_names])
                cursor.execute(f'''
                    SELECT author_name, title, primary_location, publication_date, doi, topics_json, type
                    FROM working_papers
                    WHERE author_name IN ({placeholders})
                    ORDER BY publication_date DESC, author_name
                ''', author_names)

                all_wp_results = cursor.fetchall()
                conn.close()

                if not all_wp_results:
                    print("\nNo working papers found for these authors.")
                    print("Press 'w' to fetch working papers from OpenAlex.")
                    print("[Any key] Back", end='', flush=True)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        tty.setraw(fd)
                        sys.stdin.read(1)
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    print()
                    continue

                # Count topics across all working papers
                topic_counts = {}
                for row in all_wp_results:
                    topics_json = row[5]
                    if topics_json:
                        try:
                            topics = json.loads(topics_json)
                            for topic in topics:
                                topic_name_wp = topic.get('name', '') or ''
                                if topic_name_wp:
                                    topic_counts[topic_name_wp] = topic_counts.get(topic_name_wp, 0) + 1
                        except (json.JSONDecodeError, TypeError):
                            pass

                if not topic_counts:
                    print("\nNo topic data found in working papers.")
                    print("Working papers may need to be re-fetched to include topic data.")
                    print("[Any key] Back", end='', flush=True)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        tty.setraw(fd)
                        sys.stdin.read(1)
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    print()
                    continue

                # Sort topics by count (descending)
                sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)

                # Create fzf input with "All" option at top
                fzf_topic_lines = [f"[All papers] ({len(all_wp_results)} papers)"]
                fzf_topic_lines.extend([f"{t[0]} ({t[1]} papers)" for t in sorted_topics])
                fzf_topic_input = "\n".join(fzf_topic_lines)

                # Show fzf menu to select topic
                try:
                    result = subprocess.run(
                        ['fzf', '--height=50%', '--reverse', '--prompt=Topic: ',
                         f'--header=Select topic to filter {len(all_wp_results)} working papers'],
                        input=fzf_topic_input,
                        capture_output=True,
                        text=True
                    )

                    if result.returncode != 0 or not result.stdout.strip():
                        continue  # User cancelled

                    # Extract topic name from "Topic Name (N papers)"
                    selected = result.stdout.strip()
                    paren_idx = selected.rfind(' (')
                    selected_wp_topic = selected[:paren_idx] if paren_idx > 0 else selected

                except FileNotFoundError:
                    print("fzf not found. Install with: brew install fzf")
                    continue

                # Filter papers by selected topic (or show all)
                # Row format: (author_name, title, primary_location, publication_date, doi, topics_json, type)
                if selected_wp_topic == '[All papers]':
                    wp_results = [row[:5] + (row[6],) for row in all_wp_results]  # Include type, exclude topics_json
                    selected_wp_topic = 'All'
                else:
                    selected_topic_lower = selected_wp_topic.lower()
                    wp_results = []
                    for row in all_wp_results:
                        topics_json = row[5]
                        if topics_json:
                            try:
                                topics = json.loads(topics_json)
                                for topic in topics:
                                    topic_display = topic.get('name', '') or ''
                                    if selected_topic_lower in topic_display.lower():
                                        wp_results.append(row[:5] + (row[6],))  # Include type, exclude topics_json
                                        break
                            except (json.JSONDecodeError, TypeError):
                                pass

                print(f"\n{'='*80}")
                print(f"Working papers on '{selected_wp_topic}':")
                print(f"{'='*80}\n")

                if wp_results:
                    # Paginate results
                    terminal_height = terminal_size.lines - 6
                    lines_per_paper = 2
                    papers_per_page = max(1, terminal_height // lines_per_paper)
                    total_papers = len(wp_results)
                    total_pages = (total_papers + papers_per_page - 1) // papers_per_page
                    current_page = 0

                    while True:
                        print(f"{'Date':<12} {'Source':<12} {'Author':<25} {'Title'}")
                        print(f"{'─'*terminal_width}")

                        start_idx = current_page * papers_per_page
                        end_idx = min(start_idx + papers_per_page, total_papers)

                        for author_name, title, location, pub_date, doi, wp_type in wp_results[start_idx:end_idx]:
                            date_display = pub_date[:10] if pub_date else '?'
                            author_short = (author_name[:22] + '...') if len(author_name) > 25 else author_name
                            source = location or wp_type or '?'
                            source_short = (source[:9] + '...') if len(source) > 12 else source
                            max_title_len = terminal_width - 53
                            title_display = (title[:max_title_len-3] + '...') if title and len(title) > max_title_len else (title or 'Untitled')
                            print(f"{date_display:<12} {source_short:<12} {author_short:<25} {title_display}")
                            if doi:
                                print(f"             \033[90m{doi}\033[0m")

                        print(f"{'─'*terminal_width}")
                        print(f"Page {current_page + 1}/{total_pages} | {total_papers} papers from {len(set(r[0] for r in wp_results))} authors")

                        if total_pages == 1:
                            print("[Any key] Back", end='', flush=True)
                        else:
                            print("[Space/n] Next  [p] Prev  [q] Back", end='', flush=True)

                        fd = sys.stdin.fileno()
                        old_settings = termios.tcgetattr(fd)
                        try:
                            tty.setraw(fd)
                            nav_ch = sys.stdin.read(1)
                        finally:
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                        print()

                        if total_pages == 1:
                            break
                        elif nav_ch.lower() == 'q' or ord(nav_ch) == 27 or ord(nav_ch) == 3:
                            break
                        elif nav_ch.lower() == 'n' or nav_ch == ' ' or ord(nav_ch) == 13:
                            if current_page < total_pages - 1:
                                current_page += 1
                            else:
                                break
                        elif nav_ch.lower() == 'p':
                            if current_page > 0:
                                current_page -= 1
                else:
                    print(f"No working papers found on topic '{selected_wp_topic}'")

                continue

            elif ch.lower() == 'w':
                # Fetch and display working papers for these authors
                import tempfile
                import csv as csv_module

                print(f"\n{'='*80}")
                print(f"Fetching working papers for {len(topic_authors)} authors on '{topic_name}'...")
                print(f"{'='*80}\n")

                # Create temporary CSV with author data
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                    writer = csv_module.DictWriter(f, fieldnames=['Rank', 'Author Name', 'Author ID', 'Paper Count'])
                    writer.writeheader()
                    for i, author in enumerate(topic_authors, 1):
                        writer.writerow({
                            'Rank': i,
                            'Author Name': author['name'],
                            'Author ID': author.get('author_id', ''),
                            'Paper Count': author['topic_papers']
                        })
                    temp_csv = f.name

                # Fetch working papers
                script_path = os.path.join(script_dir, 'get_wp.py')
                cmd = [sys.executable, script_path, temp_csv]
                run_command(cmd, f"Fetching Working Papers for {len(topic_authors)} Authors")

                # Clean up temp file
                try:
                    os.unlink(temp_csv)
                except:
                    pass

                # Display working papers for these authors, filtered by topic
                print(f"\n{'='*80}")
                print(f"Working papers on '{topic_name}':")
                print(f"{'='*80}\n")

                topic_lower = topic_name.lower()

                # Query working papers database for these authors
                wp_db_path = os.path.join(DB_DIR, 'working_papers.db')
                if os.path.exists(wp_db_path):
                    import sqlite3
                    conn = sqlite3.connect(wp_db_path)
                    cursor = conn.cursor()

                    # Get author names for lookup
                    author_names = [a['name'] for a in topic_authors]

                    # Find working papers by these authors (include topics_json and type)
                    placeholders = ','.join(['?' for _ in author_names])
                    cursor.execute(f'''
                        SELECT author_name, title, primary_location, publication_date, doi, topics_json, type
                        FROM working_papers
                        WHERE author_name IN ({placeholders})
                        ORDER BY publication_date DESC, author_name
                    ''', author_names)

                    all_wp_results = cursor.fetchall()
                    conn.close()

                    # Filter by topic (same logic as journal articles)
                    # Row format: (author_name, title, primary_location, publication_date, doi, topics_json, type)
                    wp_results = []
                    for row in all_wp_results:
                        topics_json = row[5]
                        if topics_json:
                            try:
                                topics = json.loads(topics_json)
                                for topic in topics:
                                    topic_display = topic.get('name', '') or ''
                                    if topic_lower in topic_display.lower():
                                        wp_results.append(row[:5] + (row[6],))  # Include type, exclude topics_json
                                        break
                            except (json.JSONDecodeError, TypeError):
                                pass

                    if wp_results:
                        # Paginate results
                        terminal_height = terminal_size.lines - 6  # Leave room for header/footer
                        lines_per_paper = 2  # Title line + DOI line
                        papers_per_page = max(1, terminal_height // lines_per_paper)
                        total_papers = len(wp_results)
                        total_pages = (total_papers + papers_per_page - 1) // papers_per_page
                        current_page = 0

                        while True:
                            # Table header
                            print(f"{'Date':<12} {'Source':<12} {'Author':<25} {'Title'}")
                            print(f"{'─'*terminal_width}")

                            # Show current page of papers
                            start_idx = current_page * papers_per_page
                            end_idx = min(start_idx + papers_per_page, total_papers)

                            for author_name, title, location, pub_date, doi, wp_type in wp_results[start_idx:end_idx]:
                                date_display = pub_date[:10] if pub_date else '?'
                                author_short = (author_name[:22] + '...') if len(author_name) > 25 else author_name
                                source = location or wp_type or '?'
                                source_short = (source[:9] + '...') if len(source) > 12 else source
                                max_title_len = terminal_width - 53
                                title_display = (title[:max_title_len-3] + '...') if title and len(title) > max_title_len else (title or 'Untitled')
                                print(f"{date_display:<12} {source_short:<12} {author_short:<25} {title_display}")
                                if doi:
                                    print(f"             \033[90m{doi}\033[0m")

                            print(f"{'─'*terminal_width}")
                            print(f"Page {current_page + 1}/{total_pages} | {total_papers} papers from {len(set(r[0] for r in wp_results))} authors")

                            # Navigation prompt
                            if total_pages == 1:
                                print("[Any key] Back", end='', flush=True)
                            else:
                                print("[Space/n] Next  [p] Prev  [q] Back", end='', flush=True)

                            fd = sys.stdin.fileno()
                            old_settings = termios.tcgetattr(fd)
                            try:
                                tty.setraw(fd)
                                ch = sys.stdin.read(1)
                            finally:
                                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                            print()

                            if total_pages == 1:
                                break
                            elif ch.lower() == 'q' or ord(ch) == 27 or ord(ch) == 3:  # q, Esc, Ctrl-C
                                break
                            elif ch.lower() == 'n' or ch == ' ' or ord(ch) == 13:  # n, Space, Enter
                                if current_page < total_pages - 1:
                                    current_page += 1
                                else:
                                    break  # Exit on last page
                            elif ch.lower() == 'p':
                                if current_page > 0:
                                    current_page -= 1
                    else:
                        print(f"No working papers found on topic '{topic_name}'")
                        print(f"({len(all_wp_results)} total papers by these authors)")
                        print("[Any key] Back", end='', flush=True)
                        fd = sys.stdin.fileno()
                        old_settings = termios.tcgetattr(fd)
                        try:
                            tty.setraw(fd)
                            sys.stdin.read(1)
                        finally:
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                        print()
                else:
                    print("Working papers database not found. Run the fetch first.")
                    print("[Any key] Back", end='', flush=True)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        tty.setraw(fd)
                        sys.stdin.read(1)
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    print()

                continue
            else:
                # Proceed to fzf selection
                break

        # Create fzf input: "Rank. Author Name (N papers, M citations)"
        fzf_lines = []
        for idx, author in enumerate(topic_authors, 1):
            fzf_lines.append(f"{idx:>3}. {author['name']:<40} ({author['topic_papers']} papers, {author['total_citations']} citations)")

        fzf_input = "\n".join(fzf_lines)

        # Loop to allow multiple author selections
        while True:
            try:
                result = subprocess.run(
                    ['fzf', '--height=50%', '--reverse', '--prompt=Author (ESC to exit): ',
                     '--header=Select an author to view their papers on this topic'],
                    input=fzf_input,
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0 or not result.stdout.strip():
                    # User pressed ESC or cancelled
                    break

                selected = result.stdout.strip()
                # Extract author name from "Rank. Author Name (N papers, M citations)"
                # Find first ". " and last " ("
                dot_idx = selected.find('. ')
                paren_idx = selected.rfind(' (')
                if dot_idx > 0 and paren_idx > dot_idx:
                    selected_author = selected[dot_idx + 2:paren_idx].strip()

                    # Get papers by this author on this topic
                    print(f"\n{'='*terminal_width}")
                    print(f"Papers by {selected_author} on '{topic_name}':")
                    print(f"{'='*terminal_width}\n")

                    author_papers = get_papers_by_author_on_topic(selected_author, topic_name)

                    if author_papers:
                        for paper in author_papers:
                            title = paper['title'] or 'Untitled'
                            # Truncate title to fit terminal
                            max_title_len = terminal_width - 20
                            if len(title) > max_title_len:
                                title = title[:max_title_len - 3] + '...'
                            print(f"{paper['year'] or '?':<6} {paper['journal']:<5} {paper['citations']:<7} {title}")
                            # Show DOI link on next line (in gray)
                            if paper.get('doi'):
                                print(f"\033[90m       https://doi.org/{paper['doi']}\033[0m")
                            print()
                        print(f"{'─'*terminal_width}")
                        print(f"Total: {len(author_papers)} papers")
                    else:
                        print(f"No papers found for {selected_author} on '{topic_name}'")

                    # Wait for user input: Enter to continue, Esc to exit
                    print("\n[Enter] Back to author list  [Esc] Exit")
                    import tty
                    import termios
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        tty.setraw(fd)
                        ch = sys.stdin.read(1)
                        # Check for Esc (27) or Ctrl-C (3)
                        if ord(ch) == 27 or ord(ch) == 3:
                            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                            print()  # Newline after raw mode
                            break
                    finally:
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    print()  # Blank line before next fzf

            except FileNotFoundError:
                print("fzf not found. Install with: brew install fzf")
                break

        # Fetch working papers if requested
        if fetch_wp:
            import tempfile
            import csv as csv_module

            print(f"\n{'='*80}")
            print(f"Fetching working papers for {len(topic_authors)} authors...")
            print(f"{'='*80}\n")

            # Create temporary CSV with author data
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                writer = csv_module.DictWriter(f, fieldnames=['Rank', 'Author Name', 'Author ID', 'Paper Count'])
                writer.writeheader()
                for i, author in enumerate(topic_authors, 1):
                    writer.writerow({
                        'Rank': i,
                        'Author Name': author['name'],
                        'Author ID': author.get('author_id', ''),
                        'Paper Count': author['topic_papers']
                    })
                temp_csv = f.name

            # Fetch working papers
            script_path = os.path.join(script_dir, 'get_wp.py')
            cmd = [sys.executable, script_path, temp_csv]

            if wp_clean:
                cmd.append('--clean')

            run_command(cmd, f"Fetching Working Papers for {len(topic_authors)} Topic-Based Authors")

            # Clean up temp file
            try:
                os.unlink(temp_csv)
            except:
                pass

            # Display summary
            current_wp_count, last_wp_count, last_wp_date = count_new_working_papers()

            print(f"\n{'='*80}")
            print("Summary: Working Papers Added")
            print(f"{'='*80}")

            if current_wp_count > 0:
                print(f"\n✅ Total new working papers added: {current_wp_count}")
            else:
                print(f"\n📋 No new working papers added this session.")
                if last_wp_count > 0 and last_wp_date:
                    print(f"   Last scrape ({last_wp_date}): {last_wp_count} working papers were added")

            # Display recent working papers
            display_newly_added_working_papers(limit=50)

        return

    # Parse arguments
    force = '--force' in sys.argv
    alltop = '--alltop' in sys.argv
    wp_only = 'wp' in sys.argv or 'workingpapers' in sys.argv
    wp_update_flag = None  # -n, -y, or --clean for wp mode
    wp_mincite = None  # --mincite N for minimum citations filter
    if wp_only:
        if '-n' in sys.argv:
            wp_update_flag = 'n'
        elif '-y' in sys.argv:
            wp_update_flag = 'y'
        elif '--clean' in sys.argv:
            wp_update_flag = 'clean'
        # Parse --mincite N
        for i, arg in enumerate(sys.argv):
            if arg == '--mincite' and i + 1 < len(sys.argv):
                try:
                    wp_mincite = int(sys.argv[i + 1])
                except ValueError:
                    pass
    wp_year_arg = None

    # Define journal groups
    JOURNAL_GROUPS = {
        'top3': ['jf', 'rfs', 'jfe'],
        'econ5': ['qje', 'aer', 'ecma', 'jpe', 'restud'],
        'alltop': ['jf', 'rfs', 'jfe', 'qje', 'aer', 'ecma', 'jpe', 'restud'],
    }

    # Check if year range provided with wp (e.g., "finance-papers wp 2023-2025")
    if wp_only:
        try:
            # Find the wp/workingpapers argument
            wp_idx = sys.argv.index('wp') if 'wp' in sys.argv else sys.argv.index('workingpapers')
            # Check if there's an argument after 'wp'
            if wp_idx + 1 < len(sys.argv):
                potential_year = sys.argv[wp_idx + 1]
                # Try to parse as year
                if potential_year and not potential_year.startswith('--'):
                    try:
                        parsed_years = parse_year_input(potential_year)
                        if parsed_years:
                            # Convert to year range format
                            if len(parsed_years) == 1:
                                wp_year_arg = str(parsed_years[0])
                            else:
                                wp_year_arg = f"{min(parsed_years)}-{max(parsed_years)}"
                    except (ValueError, AttributeError):
                        pass
        except ValueError:
            pass

    if wp_only:
        if wp_year_arg:
            print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     Finance Papers - Working Papers Only                     ║
║                              Year Filter: {wp_year_arg:^14}                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        else:
            print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     Finance Papers - Working Papers Only                     ║
║                                                                              ║
║  This script will:                                                          ║
║  1. Display recent working papers                                           ║
║  2. Show working papers rankings                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
    else:
        if alltop:
            print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                Finance & Economics Papers Update & Analysis                  ║
║                                                                              ║
║  This script will:                                                          ║
║  1. Update journal articles from top finance + economics journals           ║
║     (JF, RFS, JFE, QJE, AER, Econometrica, JPE, ReStud)                     ║
║  2. Rank authors and generate top 250 list                                  ║
║  3. Fetch working papers for top authors                                    ║
║  4. Display results and rankings                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
        else:
            print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     Finance Papers Update & Analysis                         ║
║                                                                              ║
║  This script will:                                                          ║
║  1. Update journal articles from top finance journals                       ║
║  2. Rank authors and generate top 250 list                                  ║
║  3. Fetch working papers for top authors                                    ║
║  4. Display results and rankings                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

        if force:
            print("⚠️  FORCE MODE: Will update all years (2023-present)")
        else:
            print("📅 INCREMENTAL MODE: Will update only the latest year")
            print("   (Use --force to update all years)")

        if alltop:
            print("📚 ALL TOP MODE: Including economics journals")

    # Skip journal articles if wp_only mode
    new_count = 0
    wp_count = 0

    if not wp_only:
        # Determine years and journals to work with
        if alltop:
            journals = JOURNAL_GROUPS['alltop']
        else:
            journals = JOURNAL_GROUPS['top3']
        years = get_years_to_update(force)

        # Ask if user wants to update papers
        print("\n" + "="*80)
        journal_mode = 'alltop' if alltop else 'top3'
        print(f"Update journal articles? (current: {journal_mode})")
        print("  • Type 'y' to update (using incremental/force mode years)")
        print("  • Type 'all top' or 'alltop' to include economics journals")
        print("  • Type 'force' to re-scrape all years and update author IDs")
        print("  • Type 'force 2024' to re-scrape specific years with author IDs")
        print("  • Type 'n' to skip update")
        print("  • Type year(s) to update specific years: 2024 or 2023-2025")
        print()

        while True:
            update_papers = input("Choice [y/n/force/alltop/years]: ").strip().lower()
            if update_papers == 'n':
                break
            elif update_papers == 'y':
                break
            elif update_papers in ['all top', 'alltop']:
                alltop = True
                journals = JOURNAL_GROUPS['alltop']
                update_papers = 'y'
                print(f"✅ ALL TOP MODE: Including economics journals ({', '.join(j.upper() for j in journals)})")
                break
            elif update_papers == 'force' or update_papers == 'f':
                force = True
                years = get_years_to_update(force)
                update_papers = 'y'
                print(f"✅ FORCE MODE: Will re-scrape all years ({', '.join(map(str, years))})")
                break
            else:
                # Try to parse as years (possibly with force or alltop prefix)
                input_parts = update_papers.split()
                force_prefix = False
                alltop_prefix = False
                year_input = update_papers

                # Check for "force 2024", "alltop 2024", or "alltop force 2024" patterns
                while len(input_parts) > 0:
                    if input_parts[0] in ['force', 'f']:
                        force_prefix = True
                        input_parts = input_parts[1:]
                    elif input_parts[0] in ['alltop', 'all']:
                        alltop_prefix = True
                        input_parts = input_parts[1:]
                        # Handle "all top" as two words
                        if len(input_parts) > 0 and input_parts[0] == 'top':
                            input_parts = input_parts[1:]
                    else:
                        break

                year_input = ' '.join(input_parts) if input_parts else ''

                # Handle case where only alltop/force was specified without years
                if alltop_prefix and not year_input:
                    alltop = True
                    journals = JOURNAL_GROUPS['alltop']
                    update_papers = 'y'
                    if force_prefix:
                        force = True
                        years = get_years_to_update(force)
                        print(f"✅ FORCE + ALL TOP MODE: Will re-scrape all years ({', '.join(map(str, years))}) with all journals")
                    else:
                        print(f"✅ ALL TOP MODE: Including economics journals ({', '.join(j.upper() for j in journals)})")
                    break

                try:
                    custom_years = parse_year_input(year_input) if year_input else None
                    if custom_years is not None:
                        years = custom_years
                        update_papers = 'y'
                        if alltop_prefix:
                            alltop = True
                            journals = JOURNAL_GROUPS['alltop']
                        if force_prefix:
                            force = True
                            print(f"✅ FORCE MODE: Will re-scrape years: {', '.join(map(str, years))}" + (" (all top journals)" if alltop else ""))
                        else:
                            print(f"✅ Will update years: {', '.join(map(str, years))}" + (" (all top journals)" if alltop else ""))
                        break
                    else:
                        print("⚠️  Invalid input. Please enter 'y', 'n', 'force', 'alltop', or specify years (e.g., 2024 or 2023-2025)")
                except (ValueError, AttributeError):
                    print("⚠️  Invalid input. Please enter 'y', 'n', 'force', 'alltop', or specify years (e.g., 2024 or 2023-2025)")

        if update_papers == 'y':
            # Step 1: Update journal articles
            if force:
                print(f"\n🔄 FORCE MODE: Re-scraping {', '.join(map(str, years))} (this will update author IDs)")
            else:
                print(f"\n🔄 Years to update: {', '.join(map(str, years))}")

            # Pass journal group to script for parallel fetching
            journal_group = 'alltop' if alltop else 'top3'
            for year in years:
                script_path = os.path.join(script_dir, 'getpapers_openalex.py')
                cmd = [sys.executable, script_path, journal_group, str(year)]
                if force:
                    cmd.append('--force')

                run_command(cmd, f"Updating {journal_group.upper()} {year}")

            # Count and display new papers
            print(f"\n{'='*80}")
            print("Summary: Papers Added")
            print(f"{'='*80}")

            current_count, last_count, last_date = count_new_papers_added(journals, years)

            if current_count > 0:
                print(f"\n✅ Total new papers added this session: {current_count}")
                new_count = current_count
            else:
                print(f"\n📋 No papers added this session.")
                if last_count > 0 and last_date:
                    print(f"   Last scrape ({last_date}): {last_count} papers were added")
                new_count = 0
        else:
            print("\n⏩ Skipping journal articles update...")
            new_count = 0

        # Always display recent papers (even if update was skipped)
        display_recent_papers(journals, years, limit=20)

        # Wait for user and get ranking year selection
        print("\n" + "="*80)

        ranking_years = None
        while True:
            user_input = input("📋 Choice [Enter=rankings (all years) | recent/recent N=show papers | author [name] [top3/wp/all] [N]=author search | year(s)=rankings filtered]: ").strip()
            if user_input.lower().startswith('recent'):
                # Parse limit if provided (e.g., "recent 200")
                parts = user_input.split()
                limit = 50  # default
                if len(parts) > 1:
                    try:
                        limit = int(parts[1])
                    except ValueError:
                        print(f"⚠️  Invalid limit. Use: 'recent' or 'recent N' (e.g., 'recent 200')")
                        continue
                display_most_recent_papers_by_date(journals, years, limit=limit)
                continue  # Show menu again after displaying papers
            elif user_input.lower().startswith('author '):
                # Parse author search: "author Name top3/wp/all N"
                parts = user_input.split()
                if len(parts) < 2:
                    print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                    continue

                # Extract author name (everything between "author" and source/N)
                # Find source keyword (top3, wp, all) or number
                source_keywords = ['top3', 'wp', 'all']
                author_parts = []
                source = 'all'  # default
                limit = 20  # default

                i = 1  # Skip "author"
                while i < len(parts):
                    if parts[i] in source_keywords:
                        source = parts[i]
                        # Check if next part is a number
                        if i + 1 < len(parts):
                            try:
                                limit = int(parts[i + 1])
                            except ValueError:
                                pass
                        break
                    else:
                        # Try to parse as number (limit without source)
                        try:
                            limit = int(parts[i])
                            break
                        except ValueError:
                            # It's part of the author name
                            author_parts.append(parts[i])
                    i += 1

                if not author_parts:
                    print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                    continue

                author_name = ' '.join(author_parts)
                display_recent_author_papers(author_name, source=source, limit=limit)
                continue  # Show menu again after displaying papers
            elif user_input == '':
                # Enter pressed - use all years
                ranking_years = None
                break
            else:
                # Try to parse as year input
                try:
                    ranking_years = parse_year_input(user_input)
                    break
                except (ValueError, AttributeError) as e:
                    print(f"⚠️  Invalid input. Use: Enter (all years), 'recent', 'author <name> <source> <N>', or year format (2024, 2023-2025, etc.)")

        # Step 2: Rank authors and create top 250 list

        # Determine which journals/years to rank
        journal_arg = 'alltop' if alltop else 'top3'

        if ranking_years is None:
            year_arg = None  # All years
            print("✅ Using ALL available years for ranking")
        else:
            # For simplicity, if multiple years specified, we'll use the range
            # query_openalex_db.py doesn't support multiple specific years,
            # so we'll call it without year arg (all years) or with single year
            if len(ranking_years) == 1:
                year_arg = str(ranking_years[0])
                print(f"✅ Using year: {year_arg}")
            else:
                # For multiple years, use all years mode
                year_arg = None
                print(f"✅ Using years: {', '.join(map(str, ranking_years))} (querying all years)")

        # First run rank-authors to see the rankings
        script_path = os.path.join(script_dir, 'query_openalex_db.py')
        cmd = [sys.executable, script_path, 'rank-authors', journal_arg]
        if year_arg:
            cmd.append(year_arg)
        cmd.append('--250')

        run_command(cmd, "Ranking Top 250 Authors")

        # Only create new author list CSV if papers were updated
        if update_papers == 'y':
            # Then create the author list CSV
            cmd = [sys.executable, script_path, 'make-author-list', journal_arg]
            if year_arg:
                cmd.append(year_arg)
            cmd.append('--250')

            run_command(cmd, "Generating Top 250 Author List CSV")

        # Wait for user - select working papers source
        print("\n" + "="*80)
        use_topic = False
        topic_authors = None
        topic_name = "Financial Markets and Investment Strategies"

        while True:
            user_input = input("📄 [Enter=working papers (top 250) | topic [TOPIC]=by topic | author [name]=search]: ").strip()
            if user_input == '':
                # Default: use top 250 from CSV
                use_topic = False
                break
            elif user_input.lower() == 'topic' or user_input.lower().startswith('topic '):
                # Topic-based selection
                use_topic = True
                # Parse optional topic name
                parts = user_input.split(maxsplit=1)
                if len(parts) > 1:
                    topic_name = parts[1].strip()

                # Ask for minimum papers
                min_input = input(f"Minimum papers on topic [2]: ").strip()
                min_topic_papers = 1
                if min_input:
                    try:
                        min_topic_papers = int(min_input)
                    except ValueError:
                        print(f"⚠️  Invalid number, using default: 2")

                # Find authors by topic
                print(f"\n🔍 Finding authors with papers on '{topic_name}' (min {min_topic_papers} papers)...")
                topic_authors = get_authors_by_topic_from_db(
                    topic_name=topic_name,
                    min_papers=min_topic_papers,
                    max_authors=250
                )

                if not topic_authors:
                    print(f"❌ No authors found with papers on '{topic_name}'")
                    print("   Try a different topic or lower the minimum papers threshold.")
                    use_topic = False
                    continue
                else:
                    print(f"✅ Found {len(topic_authors)} authors with {min_topic_papers}+ papers on '{topic_name}'")

                    # Show preview of top authors
                    print(f"\n{'─'*60}")
                    print(f"Top 15 authors on '{topic_name}':")
                    print(f"{'─'*60}")
                    for i, author in enumerate(topic_authors[:15], 1):
                        print(f"  {i:2}. {author['name']:<35} ({author['topic_papers']} papers)")
                    if len(topic_authors) > 15:
                        print(f"  ... and {len(topic_authors) - 15} more")
                    print(f"{'─'*60}")
                    break

            elif user_input.lower().startswith('author '):
                # Parse author search: "author Name top3/wp/all N"
                parts = user_input.split()
                if len(parts) < 2:
                    print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                    continue

                # Extract author name (everything between "author" and source/N)
                source_keywords = ['top3', 'wp', 'all']
                author_parts = []
                source = 'all'  # default
                limit = 20  # default

                i = 1  # Skip "author"
                while i < len(parts):
                    if parts[i] in source_keywords:
                        source = parts[i]
                        # Check if next part is a number
                        if i + 1 < len(parts):
                            try:
                                limit = int(parts[i + 1])
                            except ValueError:
                                pass
                        break
                    else:
                        # Try to parse as number (limit without source)
                        try:
                            limit = int(parts[i])
                            break
                        except ValueError:
                            # It's part of the author name
                            author_parts.append(parts[i])
                    i += 1

                if not author_parts:
                    print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                    continue

                author_name = ' '.join(author_parts)
                display_recent_author_papers(author_name, source=source, limit=limit)
                print("\n" + "="*80)
                continue  # Show menu again
            else:
                print(f"⚠️  Invalid input. Press Enter for top 250, 'topic [name]' for topic-based, or 'author <name>'")
    
    # Step 3: Update working papers
    # Initialize variables if wp_only mode skipped the topic selection
    if wp_only:
        use_topic = False
        topic_authors = None
        journal_arg = 'top3'  # Default for wp_only mode

    # If wp_year_arg is provided, skip update and go directly to rankings
    if wp_only and wp_year_arg:
        # Skip all updates, go directly to rankings with year filter
        wp_ranking_year = wp_year_arg
        wp_topic_selected = None
        wp_count = 0
    else:
        # Ask if user wants to update working papers
        print("\n" + "="*80)
        if use_topic and topic_authors:
            print(f"📝 Update working papers for {len(topic_authors)} topic-based authors? (y/n/clean)")
        else:
            print("📝 Update working papers for Top 250 authors? (y/n/clean)")

        # Use flag if provided via command line
        if wp_only and wp_update_flag:
            update_wp = wp_update_flag
            print(f"Choice [y/n/clean]: {update_wp} (from command line)")
        else:
            while True:
                update_wp = input("Choice [y/n/clean]: ").strip().lower()
                if update_wp in ['y', 'n', 'clean']:
                    break
                print("⚠️  Please enter 'y' (update), 'n' (skip), or 'clean' (replace all)")

        wp_count = 0

        if update_wp in ['y', 'clean']:
            if use_topic and topic_authors:
                # Topic-based: use the authors we already found
                import tempfile
                import csv as csv_module

                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                    writer = csv_module.DictWriter(f, fieldnames=['Rank', 'Author Name', 'Author ID', 'Paper Count'])
                    writer.writeheader()
                    for i, author in enumerate(topic_authors, 1):
                        writer.writerow({
                            'Rank': i,
                            'Author Name': author['name'],
                            'Author ID': author.get('author_id', ''),
                            'Paper Count': author['topic_papers']
                        })
                    temp_csv = f.name

                # Fetch working papers
                script_path = os.path.join(script_dir, 'get_wp.py')
                cmd = [sys.executable, script_path, temp_csv]

                if update_wp == 'clean':
                    cmd.append('--clean')

                run_command(cmd, f"Fetching Working Papers for {len(topic_authors)} Topic-Based Authors")

                # Clean up temp file
                try:
                    os.unlink(temp_csv)
                except:
                    pass
            else:
                # Original behavior: use author list CSV
                # Look for matching journal group CSV (alltop or top3)
                csv_pattern = f'author_list_{journal_arg}_*.csv'
                pattern = os.path.join(DB_DIR, csv_pattern)
                csv_files = glob.glob(pattern)

                # If no matching files for alltop, try top3 as fallback
                if not csv_files and alltop:
                    pattern = os.path.join(DB_DIR, 'author_list_top3_*.csv')
                    csv_files = glob.glob(pattern)
                    if csv_files:
                        print(f"📋 Note: Using top3 author list (no alltop list found)")

                if not csv_files:
                    print(f"❌ Error: No author list CSV found matching {csv_pattern}!")
                    sys.exit(1)

                latest_csv = max(csv_files, key=os.path.getmtime)
                print(f"📋 Using author list: {os.path.basename(latest_csv)}")

                # Use absolute path for the CSV file
                csv_abs_path = os.path.abspath(latest_csv)

                # Fetch working papers (without year filter - get all years)
                script_path = os.path.join(script_dir, 'get_wp.py')
                cmd = [sys.executable, script_path, csv_abs_path]

                # Add --clean flag if user chose 'clean'
                if update_wp == 'clean':
                    cmd.append('--clean')

                run_command(cmd, "Fetching Working Papers for Top 250 Authors")

            # Step 4: Display new working papers
            print(f"\n{'='*80}")
            print("Summary: Working Papers Added")
            print(f"{'='*80}")

            current_wp_count, last_wp_count, last_wp_date = count_new_working_papers()

            if current_wp_count > 0:
                print(f"\n✅ Total new working papers added this session: {current_wp_count}")
                wp_count = current_wp_count
            else:
                print(f"\n📋 No working papers added this session.")
                if last_wp_count > 0 and last_wp_date:
                    print(f"   Last scrape ({last_wp_date}): {last_wp_count} working papers were added")
                wp_count = 0

            # Display the newly added papers in a table (or most recent scrape if none added)
            display_newly_added_working_papers(limit=50)

            # Wait for user and get year selection for working papers rankings
            print("\n" + "="*80)

            wp_ranking_year = None
            wp_topic_selected = None  # Track if topic ranking was shown
            while True:
                user_input = input("📊 Choice [Enter=rankings | topic=select topic | YEAR topic | recent N | author NAME | year(s)]: ").strip()
                if user_input.lower().startswith('recent'):
                    # Parse limit if provided (e.g., "recent 200")
                    parts = user_input.split()
                    limit = 50  # default
                    if len(parts) > 1:
                        try:
                            limit = int(parts[1])
                        except ValueError:
                            print(f"⚠️  Invalid limit. Use: 'recent' or 'recent N' (e.g., 'recent 200')")
                            continue
                    display_most_recent_working_papers_by_date(limit=limit)
                    continue  # Show menu again
                elif user_input.lower().startswith('author '):
                    # Parse author search: "author Name top3/wp/all N"
                    parts = user_input.split()
                    if len(parts) < 2:
                        print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                        continue

                    # Extract author name (everything between "author" and source/N)
                    # Find source keyword (top3, wp, all) or number
                    source_keywords = ['top3', 'wp', 'all']
                    author_parts = []
                    source = 'all'  # default
                    limit = 20  # default

                    i = 1  # Skip "author"
                    while i < len(parts):
                        if parts[i] in source_keywords:
                            source = parts[i]
                            # Check if next part is a number
                            if i + 1 < len(parts):
                                try:
                                    limit = int(parts[i + 1])
                                except ValueError:
                                    pass
                            break
                        else:
                            # Try to parse as number (limit without source)
                            try:
                                limit = int(parts[i])
                                break
                            except ValueError:
                                # It's part of the author name
                                author_parts.append(parts[i])
                        i += 1

                    if not author_parts:
                        print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                        continue

                    author_name = ' '.join(author_parts)
                    display_recent_author_papers(author_name, source=source, limit=limit)
                    continue  # Show menu again after displaying papers
                elif user_input == '':
                    # Enter pressed - use all years
                    wp_ranking_year = None
                    break
                elif user_input.lower() == 'topic' or user_input.lower().endswith(' topic'):
                    # Handle "topic" or "YEAR topic" patterns
                    parts = user_input.lower().split()
                    topic_year = None
                    if len(parts) > 1 and parts[-1] == 'topic':
                        # Try to parse year(s) before "topic"
                        year_part = ' '.join(parts[:-1])
                        try:
                            years_parsed = parse_year_input(year_part)
                            if years_parsed:
                                if len(years_parsed) == 1:
                                    topic_year = str(years_parsed[0])
                                else:
                                    topic_year = f"{min(years_parsed)}-{max(years_parsed)}"
                        except (ValueError, AttributeError):
                            pass

                    # Show fzf topic selection
                    selected_topic, _ = select_wp_topic_fzf(year=topic_year)
                    if selected_topic:
                        # Show topic-based ranking
                        rank_authors_by_wp_topic(selected_topic, year=topic_year, top_n=250, mincite=wp_mincite)
                        wp_topic_selected = selected_topic
                        break  # Exit loop after showing topic ranking
                    continue  # User cancelled, show menu again
                else:
                    # Try to parse as year (single year or range)
                    try:
                        years_parsed = parse_year_input(user_input)
                        if years_parsed and len(years_parsed) == 1:
                            wp_ranking_year = str(years_parsed[0])
                            break
                        elif years_parsed and len(years_parsed) > 1:
                            # Range like 2023-2025
                            wp_ranking_year = f"{min(years_parsed)}-{max(years_parsed)}"
                            break
                        else:
                            print(f"⚠️  Invalid input. Use: Enter, 'topic', 'YEAR topic', 'recent N', 'author NAME', or year(s)")
                    except (ValueError, AttributeError) as e:
                        print(f"⚠️  Invalid input. Use: Enter, 'topic', 'YEAR topic', 'recent N', 'author NAME', or year(s)")
        else:
            print("\n⏩ Skipping working papers update...")
            wp_count = 0

            # If -n flag, skip directly to rankings
            if wp_only and wp_update_flag == 'n':
                wp_ranking_year = None
                wp_topic_selected = None
            else:
                # Display working papers - filtered by topic authors if applicable
                if use_topic and topic_authors:
                    author_names = [a['name'] for a in topic_authors]
                    display_working_papers_by_authors(author_names, limit=50)
                else:
                    display_recent_working_papers(limit=20)

                # Wait for user and get year selection for working papers rankings
                print("\n" + "="*80)

                wp_ranking_year = None
                wp_topic_selected = None
                while True:
                    user_input = input("📊 Choice [Enter=rankings | topic=select topic | YEAR topic | recent N | author NAME | year(s)]: ").strip()
                    if user_input.lower().startswith('recent'):
                        # Parse limit if provided (e.g., "recent 200")
                        parts = user_input.split()
                        limit = 50  # default
                        if len(parts) > 1:
                            try:
                                limit = int(parts[1])
                            except ValueError:
                                print(f"⚠️  Invalid limit. Use: 'recent' or 'recent N' (e.g., 'recent 200')")
                                continue
                        display_most_recent_working_papers_by_date(limit=limit)
                        continue  # Show menu again
                    elif user_input.lower().startswith('author '):
                        # Parse author search: "author Name top3/wp/all N"
                        parts = user_input.split()
                        if len(parts) < 2:
                            print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                            continue

                        # Extract author name (everything between "author" and source/N)
                        # Find source keyword (top3, wp, all) or number
                        source_keywords = ['top3', 'wp', 'all']
                        author_parts = []
                        source = 'all'  # default
                        limit = 20  # default

                        i = 1  # Skip "author"
                        while i < len(parts):
                            if parts[i] in source_keywords:
                                source = parts[i]
                                # Check if next part is a number
                                if i + 1 < len(parts):
                                    try:
                                        limit = int(parts[i + 1])
                                    except ValueError:
                                        pass
                                break
                            else:
                                # Try to parse as number (limit without source)
                                try:
                                    limit = int(parts[i])
                                    break
                                except ValueError:
                                    # It's part of the author name
                                    author_parts.append(parts[i])
                            i += 1

                        if not author_parts:
                            print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                            continue

                        author_name = ' '.join(author_parts)
                        display_recent_author_papers(author_name, source=source, limit=limit)
                        continue  # Show menu again after displaying papers
                    elif user_input == '':
                        # Enter pressed - use all years
                        wp_ranking_year = None
                        break
                    elif user_input.lower() == 'topic' or user_input.lower().endswith(' topic'):
                        # Handle "topic" or "YEAR topic" patterns
                        parts = user_input.lower().split()
                        topic_year = None
                        if len(parts) > 1 and parts[-1] == 'topic':
                            # Try to parse year(s) before "topic"
                            year_part = ' '.join(parts[:-1])
                            try:
                                years_parsed = parse_year_input(year_part)
                                if years_parsed:
                                    if len(years_parsed) == 1:
                                        topic_year = str(years_parsed[0])
                                    else:
                                        topic_year = f"{min(years_parsed)}-{max(years_parsed)}"
                            except (ValueError, AttributeError):
                                pass

                        # Show fzf topic selection
                        selected_topic, _ = select_wp_topic_fzf(year=topic_year)
                        if selected_topic:
                            # Show topic-based ranking
                            rank_authors_by_wp_topic(selected_topic, year=topic_year, top_n=250, mincite=wp_mincite)
                            wp_topic_selected = selected_topic
                            break  # Exit loop after showing topic ranking
                        continue  # User cancelled, show menu again
                    else:
                        # Try to parse as year (single year or range)
                        try:
                            years_parsed = parse_year_input(user_input)
                            if years_parsed and len(years_parsed) == 1:
                                wp_ranking_year = str(years_parsed[0])
                                break
                            elif years_parsed and len(years_parsed) > 1:
                                # Range like 2023-2025
                                wp_ranking_year = f"{min(years_parsed)}-{max(years_parsed)}"
                                break
                            else:
                                print(f"⚠️  Invalid input. Use: Enter, 'topic', 'YEAR topic', 'recent N', 'author NAME', or year(s)")
                        except (ValueError, AttributeError) as e:
                            print(f"⚠️  Invalid input. Use: Enter, 'topic', 'YEAR topic', 'recent N', 'author NAME', or year(s)")

    # Always display recent working papers table (if database exists) - skip if -n flag or topic ranking shown
    wp_db = os.path.join(DB_DIR, 'working_papers.db')
    if os.path.exists(wp_db) and not (wp_only and wp_update_flag == 'n') and not wp_topic_selected:
        display_recent_working_papers(limit=20)

    # Display working papers ranking (always use main database, filter by year in SQL)
    # Skip if topic ranking was already shown
    wp_db = os.path.join(DB_DIR, 'working_papers.db')

    if wp_topic_selected:
        # Topic ranking already shown, skip normal ranking
        pass
    elif os.path.exists(wp_db):
        if wp_ranking_year:
            rank_desc = f"Author Rankings by Working Papers ({wp_ranking_year})"
        else:
            rank_desc = "Author Rankings by Working Papers (All Years)"
        if wp_mincite:
            rank_desc += f" [min {wp_mincite} cites]"

        script_path = os.path.join(script_dir, 'query_wp_db.py')
        cmd = [sys.executable, script_path, 'rank']
        if wp_ranking_year:
            cmd.append(wp_ranking_year)
        cmd.append('--250')
        if wp_mincite:
            cmd.append(f'--mincite={wp_mincite}')

        run_command(cmd, rank_desc)
    else:
        print(f"\n⚠️  No working papers database found. Skipping rankings.")

    # Read input (may have been typed while table was scrolling)
    print("\n" + "="*80)
    while True:
        user_input = input("📄 [Enter=exit | recent N | author NAME | YEAR]: ").strip()
        if user_input == '':
            break
        elif user_input.lower().startswith('recent'):
            # Parse limit if provided (e.g., "recent 200")
            parts = user_input.split()
            limit = 50  # default
            if len(parts) > 1:
                try:
                    limit = int(parts[1])
                except ValueError:
                    print(f"⚠️  Invalid limit. Use: 'recent' or 'recent N' (e.g., 'recent 200')")
                    continue
            display_recent_working_papers(limit=limit)
            print("\n" + "="*80)
            continue  # Show menu again
        elif user_input.lower().startswith('author '):
            # Parse author search: "author Name top3/wp/all N"
            parts = user_input.split()
            if len(parts) < 2:
                print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                continue

            # Extract author name (everything between "author" and source/N)
            source_keywords = ['top3', 'wp', 'all']
            author_parts = []
            source = 'all'  # default
            limit = 20  # default

            i = 1  # Skip "author"
            while i < len(parts):
                if parts[i] in source_keywords:
                    source = parts[i]
                    # Check if next part is a number
                    if i + 1 < len(parts):
                        try:
                            limit = int(parts[i + 1])
                        except ValueError:
                            pass
                    break
                else:
                    # Try to parse as number (limit without source)
                    try:
                        limit = int(parts[i])
                        break
                    except ValueError:
                        # It's part of the author name
                        author_parts.append(parts[i])
                i += 1

            if not author_parts:
                print(f"⚠️  Invalid format. Use: 'author <name> <source> <N>' (e.g., 'author Brøgger wp 20')")
                continue

            author_name = ' '.join(author_parts)
            display_recent_author_papers(author_name, source=source, limit=limit)
            print("\n" + "="*80)
            continue  # Show menu again
        else:
            # Try to parse as year input for re-ranking
            try:
                new_years = parse_year_input(user_input)
                if len(new_years) == 1:
                    wp_ranking_year = str(new_years[0])
                else:
                    wp_ranking_year = f"{min(new_years)}-{max(new_years)}"

                # Re-run WP ranking with new year filter
                if os.path.exists(wp_db):
                    rank_desc = f"Author Rankings by Working Papers ({wp_ranking_year})"
                    if wp_mincite:
                        rank_desc += f" [min {wp_mincite} cites]"
                    cmd = [sys.executable, script_path, 'rank', wp_ranking_year, '--250']
                    if wp_mincite:
                        cmd.append(f'--mincite={wp_mincite}')
                    run_command(cmd, rank_desc)
                print("\n" + "="*80)
                continue
            except (ValueError, AttributeError):
                # Not a year - treat as author name search
                author_name = user_input.strip()
                if author_name:
                    display_recent_author_papers(author_name, source='wp', limit=20)
                    print("\n" + "="*80)
                    continue

    # Final summary
    print(f"""
{'='*80}
✅ Update Complete!
{'='*80}

Summary:
  • Journal papers added: {new_count}
  • Working papers added: {wp_count}
  • Top authors analyzed: 250

Next steps:
  • Run 'python3 query_openalex_db.py rank-authors top3 --250' to view journal rankings
  • Run 'python3 query_wp_db.py rank --250' to view working paper rankings
  • Run 'python3 query_wp_db.py list' to browse working papers
  • Run 'python3 extract_research_agendas.py 250 --display' to view research agendas

{'='*80}
""")
    
    # Ask about updating static website
    print("\n" + "="*80)
    update_web = ""
    while update_web not in ['y', 'n']:
        update_web = input("Update static website data (GitHub Pages)? (y/n): ").lower().strip()
        if not update_web:
            print("Please enter 'y' or 'n'")
    
    if update_web == 'y':
        print("\n" + "="*80)
        print("Updating static website data...")
        print("="*80 + "\n")
        
        # Export rankings to JSON (use absolute path from project root)
        export_script = os.path.join(project_root, 'src', 'export_rankings.py')
        export_cmd = [sys.executable, export_script]
        result = run_command(export_cmd, "Exporting rankings to JSON")
        
        if result == 0:
            # Check if there are any changes to commit
            print("\nChecking for changes...")
            check_result = run_git_command(['git', 'diff', '--quiet', 'docs/data/rankings.json'], "Checking for changes")
            
            if check_result != 0:  # Non-zero means there are changes
                # Git commands
                print("\nCommitting and pushing to GitHub...")
                git_commands = [
                    (['git', 'add', 'docs/data/rankings.json'], "Staging changes"),
                    (['git', 'commit', '-m', 'Update rankings data'], "Committing changes"),
                    (['git', 'push'], "Pushing to GitHub")
                ]
                
                for cmd, desc in git_commands:
                    result = run_git_command(cmd, desc)
                    if result != 0:
                        print(f"⚠️  Git command failed: {desc}")
                        break
                else:
                    print("\n✅ Static website data updated successfully!")
                    print("   View at: https://anbrog.github.io/finance-papers/")
            else:
                print("ℹ️  No changes detected in rankings data - skipping commit")
        else:
            print("⚠️  Export failed, skipping git push")

if __name__ == "__main__":
    main()
