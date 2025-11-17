#!/usr/bin/env python3
"""Query the OpenAlex articles database"""
import sqlite3
import json
import sys
import os

# Map database file patterns
DB_DIR = '../out/data'

def get_db_path(journal=None, year=None):
    """Get database path based on journal and year"""
    if journal and year:
        # Specific journal and year
        db_file = f'openalex_{journal}_{year}.db'
    elif journal:
        # Specific journal, any year - need to find available years
        # For now, just use the pattern
        db_file = f'openalex_{journal}_*.db'
    elif year:
        # All journals, specific year
        db_file = f'openalex_*_{year}.db'
    else:
        # Default: openalex_articles.db
        db_file = 'openalex_articles.db'
    
    return os.path.join(DB_DIR, db_file)

def get_all_matching_dbs(pattern):
    """Get all database files matching the pattern"""
    import glob
    db_path = os.path.join(DB_DIR, pattern)
    matches = glob.glob(db_path)
    return matches if matches else None

DB_PATH = '../out/data/openalex_articles.db'

def connect_db(db_path=None):
    """
    Connect to the SQLite database.
    
    Args:
        db_path (str, optional): Path to database file. Defaults to DB_PATH.
    
    Returns:
        sqlite3.Connection: Database connection object
    
    Exits:
        If database file not found
    """
    path = db_path or DB_PATH
    if not os.path.exists(path):
        print(f"Error: Database not found at {path}")
        sys.exit(1)
    return sqlite3.connect(path)

def list_all_articles():
    """
    List all articles in the database with basic information.
    
    Args:
        None
    
    Returns:
        None (prints to console)
    
    Output:
        Displays table with ID, Date, Title, and Authors for each article
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, title, publication_date, doi, authors_json
        FROM openalex_articles
        ORDER BY publication_date DESC
    ''')
    
    articles = cursor.fetchall()
    conn.close()
    
    print(f"\n{'ID':<5} {'Date':<12} {'Title':<60} {'Authors':<40}")
    print("=" * 120)
    
    for article_id, title, pub_date, doi, authors_json in articles:
        title = title or 'N/A'
        title_short = (title[:57] + '...') if len(title) > 60 else title
        
        # Parse and format authors
        authors = json.loads(authors_json)
        if authors:
            author_names = [a['name'] for a in authors if a['name']]
            if author_names:
                authors_str = ', '.join(author_names)
            else:
                authors_str = 'N/A'
        else:
            authors_str = 'N/A'
        
        authors_short = (authors_str[:37] + '...') if len(authors_str) > 40 else authors_str
        
        print(f"{article_id:<5} {pub_date or 'N/A':<12} {title_short:<60} {authors_short:<40}")
    
    print(f"\nTotal: {len(articles)} articles")

def get_article(article_id):
    """
    Get detailed information about a specific article by ID.
    
    Args:
        article_id (int): Database ID of the article
    
    Returns:
        None (prints to console)
    
    Output:
        Displays full article details including title, date, DOI, scraped timestamp, and all authors with ORCIDs
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT openalex_id, title, publication_date, doi, authors_json, scraped_at
        FROM openalex_articles
        WHERE id = ?
    ''', (article_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print(f"Article with ID {article_id} not found")
        return
    
    openalex_id, title, pub_date, doi, authors_json, scraped_at = result
    authors = json.loads(authors_json)
    
    print(f"\n{'='*80}")
    print(f"ID: {article_id}")
    print(f"OpenAlex ID: {openalex_id}")
    print(f"Title: {title}")
    print(f"Publication Date: {pub_date or 'N/A'}")
    print(f"DOI: {doi or 'N/A'}")
    print(f"Scraped At: {scraped_at or 'N/A'}")
    print(f"\nAuthors ({len(authors)}):")
    for i, author in enumerate(authors, 1):
        institutions = ', '.join(author['institutions']) if author['institutions'] else 'N/A'
        orcid = author['orcid'] or 'N/A'
        print(f"  {i}. {author['name']}")
        print(f"     ORCID: {orcid}")
        print(f"     Institutions: {institutions}")
    print(f"{'='*80}\n")

def search_by_title(keyword):
    """
    Search for articles by title keyword (case-insensitive).
    
    Args:
        keyword (str): Search term to find in article titles
    
    Returns:
        None (prints to console)
    
    Output:
        Displays table of matching articles with ID, Date, Title, and Authors
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, title, publication_date, doi
        FROM openalex_articles
        WHERE title LIKE ?
        ORDER BY publication_date DESC
    ''', (f'%{keyword}%',))
    
    articles = cursor.fetchall()
    conn.close()
    
    if not articles:
        print(f"No articles found with keyword '{keyword}' in title")
        return
    
    print(f"\nFound {len(articles)} article(s) matching '{keyword}':\n")
    print(f"{'ID':<5} {'Date':<12} {'Title':<100}")
    print("=" * 120)
    
    for article_id, title, pub_date, doi in articles:
        print(f"{article_id:<5} {pub_date or 'N/A':<12} {title}")

def count_articles():
    """
    Count total number of articles in the database.
    
    Args:
        None
    
    Returns:
        None (prints to console)
    
    Output:
        Displays total article count
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM openalex_articles')
    count = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT MIN(publication_date), MAX(publication_date)
        FROM openalex_articles
    ''')
    min_date, max_date = cursor.fetchone()
    
    conn.close()
    
    print(f"\nDatabase Statistics:")
    print(f"  Total articles: {count}")
    print(f"  Date range: {min_date or 'N/A'} to {max_date or 'N/A'}")

def search_by_author(author_name):
    """
    Search for articles by author name (case-insensitive).
    
    Args:
        author_name (str): Author name or partial name to search for
    
    Returns:
        None (prints to console)
    
    Output:
        Displays table of matching articles with ID, Date, Title, and Authors
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, title, publication_date, doi, authors_json
        FROM openalex_articles
    ''')
    
    articles = cursor.fetchall()
    conn.close()
    
    matches = []
    for article_id, title, pub_date, doi, authors_json in articles:
        authors = json.loads(authors_json)
        # Check if any author matches
        for author in authors:
            if author['name'] and author_name.lower() in author['name'].lower():
                matches.append((article_id, title, pub_date, authors_json))
                break
    
    if not matches:
        print(f"No articles found with author matching '{author_name}'")
        return
    
    print(f"\nFound {len(matches)} article(s) with author matching '{author_name}':\n")
    print(f"{'ID':<5} {'Date':<12} {'Title':<60} {'Authors':<40}")
    print("=" * 120)
    
    for article_id, title, pub_date, authors_json in matches:
        title = title or 'N/A'
        title_short = (title[:57] + '...') if len(title) > 60 else title
        
        authors = json.loads(authors_json)
        author_names = [a['name'] for a in authors if a['name']]
        authors_str = ', '.join(author_names) if author_names else 'N/A'
        authors_short = (authors_str[:37] + '...') if len(authors_str) > 40 else authors_str
        
        print(f"{article_id:<5} {pub_date or 'N/A':<12} {title_short:<60} {authors_short:<40}")

def rank_authors(journals=None, year=None, top_n=50):
    """
    Rank authors by number of publications in specified journals/years.
    
    Args:
        journals (str, optional): Journal code ('jf', 'rfs', 'jfe', 'top3') or None for all
        year (str, optional): Year filter (e.g., '2024') or None for all years
        top_n (int): Number of top authors to display (default: 50)
    
    Returns:
        None (prints to console)
    
    Output:
        Displays ranked list of top N authors with publication counts and totals
    """
    import glob
    
    # Determine which databases to query
    if journals == 'top3':
        journal_codes = ['jf', 'rfs', 'jfe']
    elif journals:
        journal_codes = [journals]
    else:
        journal_codes = None
    
    # Collect all matching database files
    db_files = []
    if journal_codes and year:
        for jcode in journal_codes:
            db_file = os.path.join(DB_DIR, f'openalex_{jcode}_{year}.db')
            if os.path.exists(db_file):
                db_files.append(db_file)
    elif journal_codes:
        for jcode in journal_codes:
            pattern = os.path.join(DB_DIR, f'openalex_{jcode}_*.db')
            db_files.extend(glob.glob(pattern))
    elif year:
        pattern = os.path.join(DB_DIR, f'openalex_*_{year}.db')
        db_files.extend(glob.glob(pattern))
    else:
        # Default database
        default_db = os.path.join(DB_DIR, 'openalex_articles.db')
        if os.path.exists(default_db):
            db_files.append(default_db)
    
    if not db_files:
        print("Error: No matching database files found")
        print(f"Journals: {journals}, Year: {year}")
        sys.exit(1)
    
    print(f"Querying {len(db_files)} database(s):")
    for db in db_files:
        print(f"  - {os.path.basename(db)}")
    
    # Count publications and citations per author across all databases
    author_counts = {}
    author_citations = {}
    author_latest_paper = {}
    total_articles = 0
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('SELECT authors_json, cited_by_count, publication_date, title FROM openalex_articles')
        articles = cursor.fetchall()
        total_articles += len(articles)
        
        for (authors_json, cited_by_count, pub_date, title) in articles:
            authors = json.loads(authors_json)
            citations = cited_by_count or 0
            for author in authors:
                name = author.get('name')
                if name:
                    if name not in author_counts:
                        author_counts[name] = 0
                        author_citations[name] = 0
                        author_latest_paper[name] = (pub_date or '', title or '')
                    author_counts[name] += 1
                    author_citations[name] += citations
                    # Update latest paper if this one is more recent
                    if pub_date and pub_date > author_latest_paper[name][0]:
                        author_latest_paper[name] = (pub_date, title or '')
        
        conn.close()
    
    # Sort by count (descending), then by citations
    ranked = sorted(author_counts.items(), key=lambda x: (x[1], author_citations[x[0]]), reverse=True)
    
    # Build label
    journal_label = f"{journals}" if journals else "all journals"
    year_label = f" ({year})" if year else " (all years)"
    
    print(f"\nAuthor Rankings - {journal_label}{year_label} (Total: {len(ranked)} authors)\n")
    print(f"{'Rank':<6} {'Papers':<8} {'Citations':<11} {'Author Name':<45} {'Latest Paper':<50}")
    print("=" * 125)
    
    for rank, (author_name, count) in enumerate(ranked, 1):
        citations = author_citations[author_name]
        latest_date, latest_title = author_latest_paper.get(author_name, ('', ''))
        
        # Truncate author name and title for display
        author_short = (author_name[:42] + '...') if len(author_name) > 45 else author_name
        title_short = (latest_title[:47] + '...') if len(latest_title) > 50 else latest_title
        latest_paper_str = f"{latest_date}: {title_short}" if latest_date else 'N/A'
        
        print(f"{rank:<6} {count:<8} {citations:<11} {author_short:<45} {latest_paper_str}")
        if rank >= top_n:
            print(f"\n... and {len(ranked) - top_n} more authors")
            break
    
    print(f"\nTotal articles: {total_articles}")
    print(f"Total unique authors: {len(ranked)}")

def papers_by_author(author_name, journals=None, year=None):
    """
    Show all papers by a specific author with optional journal/year filters.
    
    Args:
        author_name (str): Author name to search for
        journals (str, optional): Journal code ('jf', 'rfs', 'jfe', 'top3') or None
        year (str, optional): Year filter or None
    
    Returns:
        None (prints to console)
    
    Output:
        Displays table of papers with Database, ID, Date, Title, and Authors
    """
    import glob
    
    # Determine which databases to query
    if journals == 'top3':
        journal_codes = ['jf', 'rfs', 'jfe']
    elif journals:
        journal_codes = [journals]
    else:
        journal_codes = None
    
    # Collect all matching database files
    db_files = []
    if journal_codes and year:
        for jcode in journal_codes:
            db_file = os.path.join(DB_DIR, f'openalex_{jcode}_{year}.db')
            if os.path.exists(db_file):
                db_files.append(db_file)
    elif journal_codes:
        for jcode in journal_codes:
            pattern = os.path.join(DB_DIR, f'openalex_{jcode}_*.db')
            db_files.extend(glob.glob(pattern))
    elif year:
        pattern = os.path.join(DB_DIR, f'openalex_*_{year}.db')
        db_files.extend(glob.glob(pattern))
    else:
        # Default database
        default_db = os.path.join(DB_DIR, 'openalex_articles.db')
        if os.path.exists(default_db):
            db_files.append(default_db)
    
    if not db_files:
        print("Error: No matching database files found")
        sys.exit(1)
    
    # Search for articles by author across all databases
    matches = []
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, title, publication_date, doi, authors_json
            FROM openalex_articles
        ''')
        
        articles = cursor.fetchall()
        conn.close()
        
        for article_id, title, pub_date, doi, authors_json in articles:
            authors = json.loads(authors_json)
            # Check if any author matches
            for author in authors:
                if author['name'] and author_name.lower() in author['name'].lower():
                    matches.append((article_id, title, pub_date, authors_json, os.path.basename(db_file)))
                    break
    
    if not matches:
        print(f"No articles found with author matching '{author_name}'")
        journal_label = f" in {journals}" if journals else ""
        year_label = f" ({year})" if year else ""
        print(f"Search scope: {journal_label}{year_label}")
        return
    
    # Build label
    journal_label = f"{journals}" if journals else "all journals"
    year_label = f" ({year})" if year else " (all years)"
    
    print(f"\nFound {len(matches)} paper(s) by '{author_name}' - {journal_label}{year_label}\n")
    print(f"{'ID':<5} {'Date':<12} {'Source':<20} {'Title':<50} {'Authors':<40}")
    print("=" * 130)
    
    for article_id, title, pub_date, authors_json, db_file in matches:
        title = title or 'N/A'
        title_short = (title[:47] + '...') if len(title) > 50 else title
        source = db_file.replace('openalex_', '').replace('.db', '')
        
        # Format authors
        authors = json.loads(authors_json)
        author_names = [a['name'] for a in authors if a['name']]
        authors_str = ', '.join(author_names) if author_names else 'N/A'
        authors_short = (authors_str[:37] + '...') if len(authors_str) > 40 else authors_str
        
        print(f"{article_id:<5} {pub_date or 'N/A':<12} {source:<20} {title_short:<50} {authors_short:<40}")

def make_author_list(journals=None, year=None, top_n=250):
    """
    Create a CSV file with top ranked authors by publication count.
    
    Args:
        journals (str, optional): Journal code or 'top3'
        year (str, optional): Year filter
        top_n (int): Number of top authors to include (default: 250)
    
    Returns:
        None (saves CSV file and prints summary to console)
    
    Output:
        - CSV file: ../out/data/author_list_{journals}_{year}_top{top_n}_{timestamp}.csv
        - Console: File path, author counts, top 10 preview
    """
    import glob
    import csv
    from datetime import datetime
    
    # Determine which databases to query
    if journals == 'top3':
        journal_codes = ['jf', 'rfs', 'jfe']
    elif journals:
        journal_codes = [journals]
    else:
        journal_codes = None
    
    # Collect all matching database files
    db_files = []
    if journal_codes and year:
        for jcode in journal_codes:
            db_file = os.path.join(DB_DIR, f'openalex_{jcode}_{year}.db')
            if os.path.exists(db_file):
                db_files.append(db_file)
    elif journal_codes:
        for jcode in journal_codes:
            pattern = os.path.join(DB_DIR, f'openalex_{jcode}_*.db')
            db_files.extend(glob.glob(pattern))
    elif year:
        pattern = os.path.join(DB_DIR, f'openalex_*_{year}.db')
        db_files.extend(glob.glob(pattern))
    else:
        # Default database
        default_db = os.path.join(DB_DIR, 'openalex_articles.db')
        if os.path.exists(default_db):
            db_files.append(default_db)
    
    if not db_files:
        print("Error: No matching database files found")
        sys.exit(1)
    
    print(f"Querying {len(db_files)} database(s) for author rankings...")
    
    # Count publications per author across all databases
    author_counts = {}
    total_articles = 0
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('SELECT authors_json FROM openalex_articles')
        articles = cursor.fetchall()
        total_articles += len(articles)
        
        for (authors_json,) in articles:
            authors = json.loads(authors_json)
            for author in authors:
                name = author.get('name')
                if name:
                    if name not in author_counts:
                        author_counts[name] = 0
                    author_counts[name] += 1
        
        conn.close()
    
    # Sort by count (descending)
    ranked = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Take top N
    top_authors = ranked[:top_n]
    
    # Create filename
    journal_label = f"{journals}" if journals else "all"
    year_label = f"_{year}" if year else "_all"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"author_list_{journal_label}{year_label}_top{top_n}_{timestamp}.csv"
    filepath = os.path.join(DB_DIR, filename)
    
    # Write to CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Rank', 'Author Name', 'Paper Count'])
        
        for rank, (author_name, count) in enumerate(top_authors, 1):
            writer.writerow([rank, author_name, count])
    
    print(f"\n✅ Author list saved to: {filepath}")
    print(f"   Total authors in list: {len(top_authors)}")
    print(f"   Total unique authors: {len(ranked)}")
    print(f"   Total articles: {total_articles}")
    print(f"\nTop 10 authors:")
    for rank, (author_name, count) in enumerate(top_authors[:10], 1):
        print(f"   {rank}. {author_name} ({count} papers)")

def view_wp_new(year=None):
    """
    View working papers from the most recent scrape day.
    
    Args:
        year (str, optional): Year filter for database selection (e.g., '2024')
                             Uses working_papers_{year}.db or working_papers.db
    
    Returns:
        None (prints to console)
    
    Output:
        Displays table of papers from latest scrape with ID, Author, Date, Title, Source
    """
    import glob
    from datetime import datetime, timedelta
    
    # Determine which working papers database to use
    if year:
        db_file = os.path.join(DB_DIR, f'working_papers_{year}.db')
    else:
        db_file = os.path.join(DB_DIR, 'working_papers.db')
    
    if not os.path.exists(db_file):
        print(f"Error: Database not found: {db_file}")
        print("Run get_wp.py first to fetch working papers.")
        sys.exit(1)
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Get the most recent scrape timestamp
    cursor.execute('SELECT MAX(scraped_at) FROM working_papers')
    latest_scrape = cursor.fetchone()[0]
    
    if not latest_scrape:
        print("No working papers found in database")
        conn.close()
        return
    
    # Get the date part (YYYY-MM-DD) from the latest scrape timestamp
    latest_date = latest_scrape.split('T')[0]
    
    # Get all papers from the latest day
    cursor.execute('''
        SELECT id, title, author_name, publication_date, doi, primary_location, scraped_at
        FROM working_papers
        WHERE DATE(scraped_at) = ?
        ORDER BY author_name, publication_date DESC
    ''', (latest_date,))
    
    papers = cursor.fetchall()
    conn.close()
    
    if not papers:
        print("No working papers found from the latest scrape")
        return
    
    db_label = f" from working_papers_{year}.db" if year else " (all years)"
    print(f"\nWorking Papers from Latest Scrape{db_label}")
    print(f"Scraped at: {latest_scrape}")
    print(f"Total papers: {len(papers)}\n")
    print(f"{'ID':<5} {'Author':<30} {'Date':<12} {'Title':<60} {'Source':<30}")
    print("=" * 140)
    
    for paper_id, title, author_name, pub_date, doi, source, scraped_at in papers:
        title = title or 'N/A'
        title_short = (title[:57] + '...') if len(title) > 60 else title
        
        author_short = (author_name[:27] + '...') if len(author_name) > 30 else author_name
        source_short = (source[:27] + '...') if source and len(source) > 30 else (source or 'N/A')
        
        print(f"{paper_id:<5} {author_short:<30} {pub_date or 'N/A':<12} {title_short:<60} {source_short:<30}")

def view_wp_mine(year=None):
    """
    View working papers by Andreas Brøgger (fetches and displays).
    
    Args:
        year (str, optional): Year filter for working papers
    
    Returns:
        None (prints to console)
    
    Process:
        1. Fetches working papers using get_wp.py with author IDs A5011626190 and A5118977207
        2. Searches database for papers by Andreas Brøgger
        3. Displays results with ID, Date, Title, Source
    
    Output:
        Working papers by Andreas Brøgger with full details
    """
    import glob
    import subprocess
    import tempfile
    
    # Andreas Brøgger's OpenAlex IDs (multiple profiles)
    author_ids = ["A5011626190", "A5118977207"]
    
    # First, fetch working papers for Andreas Brøgger using author IDs
    print(f"Fetching working papers for Andreas Brøgger (IDs: {', '.join(author_ids)})...")
    
    # Create a temporary CSV with both author IDs
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
        import csv
        writer = csv.writer(f)
        writer.writerow(['Rank', 'Author Name', 'Paper Count'])
        for idx, author_id in enumerate(author_ids, 1):
            writer.writerow([idx, f'Andreas Brøgger|{author_id}', 0])  # Include ID in name field
        temp_csv = f.name
    
    try:
        # Run get_wp.py with the temporary CSV
        cmd = ['python3', 'src/get_wp.py', temp_csv]
        if year:
            cmd.append(str(year))
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error running get_wp.py: {result.stderr}")
            return
    finally:
        # Clean up temp file
        import os as os_module
        os_module.unlink(temp_csv)
    
    print()  # Add blank line after fetch
    
    # Determine which working papers database to use
    if year:
        db_file = os.path.join(DB_DIR, f'working_papers_{year}.db')
    else:
        db_file = os.path.join(DB_DIR, 'working_papers.db')
    
    if not os.path.exists(db_file):
        print(f"Error: Database not found: {db_file}")
        sys.exit(1)
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Search for papers by Andreas Brøgger (with variations)
    cursor.execute('''
        SELECT id, title, author_name, publication_date, doi, primary_location, scraped_at
        FROM working_papers
        WHERE author_name LIKE '%Andreas%Brøgger%' 
           OR author_name LIKE '%Andreas%Brogger%'
           OR author_name LIKE '%Brøgger%Andreas%'
           OR author_name LIKE '%Brogger%Andreas%'
        ORDER BY publication_date DESC
    ''')
    
    papers = cursor.fetchall()
    conn.close()
    
    if not papers:
        db_label = f" in working_papers_{year}.db" if year else " (all years)"
        print(f"No working papers found for Andreas Brøgger{db_label}")
        return
    
    db_label = f" from working_papers_{year}.db" if year else " (all years)"
    print(f"\nWorking Papers by Andreas Brøgger{db_label}")
    print(f"Total papers: {len(papers)}\n")
    print(f"{'ID':<5} {'Date':<12} {'Title':<80} {'Source':<30}")
    print("=" * 130)
    
    for paper_id, title, author_name, pub_date, doi, source, scraped_at in papers:
        title = title or 'N/A'
        title_short = (title[:77] + '...') if len(title) > 80 else title
        source_short = (source[:27] + '...') if source and len(source) > 30 else (source or 'N/A')
        
        print(f"{paper_id:<5} {pub_date or 'N/A':<12} {title_short:<80} {source_short:<30}")

def view_wp_year(year, limit=30):
    """
    View working papers from a specific year, sorted by date (newest first).
    
    Args:
        year (str): Year to filter papers (e.g., '2025')
        limit (int): Maximum number of papers to display (default: 30)
    
    Returns:
        None (prints to console)
    
    Output:
        - Table with Author, Title, Date, Source
        - Full clickable DOI/OpenAlex links below each entry
        - Shows N of total count for the year
    """
    
    # Use the all-years database
    db_file = os.path.join(DB_DIR, 'working_papers.db')
    
    if not os.path.exists(db_file):
        print(f"Error: Database not found: {db_file}")
        print("Run get_wp.py first to fetch working papers.")
        sys.exit(1)
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Get papers from the specified year
    cursor.execute('''
        SELECT id, title, author_name, publication_date, doi, primary_location, openalex_id, cited_by_count
        FROM working_papers
        WHERE publication_date LIKE ?
        ORDER BY publication_date DESC
        LIMIT ?
    ''', (f'{year}%', limit))
    
    papers = cursor.fetchall()
    
    # Get total count for the year
    cursor.execute('SELECT COUNT(*) FROM working_papers WHERE publication_date LIKE ?', (f'{year}%',))
    total_count = cursor.fetchone()[0]
    
    conn.close()
    
    if not papers:
        print(f"No working papers found from {year}")
        return
    
    print(f"\nWorking Papers from {year}")
    print(f"Showing {len(papers)} of {total_count} total papers\n")
    print(f"{'Author':<22} {'Title':<58} {'Date':<12} {'Cites':<7} {'Source':<28}")
    print("=" * 130)
    
    for paper_id, title, author_name, pub_date, doi, source, openalex_id, cited_by_count in papers:
        title = title or 'N/A'
        title_short = (title[:55] + '...') if len(title) > 58 else title
        
        author_short = (author_name[:19] + '...') if len(author_name) > 22 else author_name
        
        # Prefer DOI, fall back to OpenAlex ID
        link = doi if doi else (openalex_id if openalex_id else 'N/A')
        
        source_short = (source[:25] + '...') if source and len(source) > 28 else (source or 'N/A')
        
        cites = str(cited_by_count or 0)
        
        print(f"{author_short:<22} {title_short:<58} {pub_date or 'N/A':<12} {cites:<7} {source_short:<28}")
        if link != 'N/A':
            print(f"  Link: {link}")
        print()

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 query_openalex_db.py list              # List all articles")
        print("  python3 query_openalex_db.py get <id>          # Get article details")
        print("  python3 query_openalex_db.py search <keyword>  # Search by title")
        print("  python3 query_openalex_db.py author <name>     # Search by author")
        print("  python3 query_openalex_db.py rank-authors [journal] [year] [--N] # Rank authors")
        print("    Examples:")
        print("      rank-authors                  # All journals, all years, top 50")
        print("      rank-authors jf               # Journal of Finance, all years")
        print("      rank-authors top3 2024        # All 3 journals, 2024 only")
        print("      rank-authors jfe 2023         # JFE, 2023 only")
        print("      rank-authors top3 --250       # All 3 journals, top 250 authors")
        print("  python3 query_openalex_db.py papers-by-author <name> [journal] [year]")
        print("    Examples:")
        print("      papers-by-author Kelly")
        print("      papers-by-author Kelly jf 2024")
        print("      papers-by-author Kelly top3 2024")
        print("  python3 query_openalex_db.py make-author-list [journal] [year]")
        print("    Examples:")
        print("      make-author-list top3 2024    # Save top 250 authors to CSV")
        print("      make-author-list jf 2024")
        print("  python3 query_openalex_db.py view-wp-new [year]")
        print("    Examples:")
        print("      view-wp-new                   # View working papers from latest scrape")
        print("      view-wp-new 2024              # View 2024 working papers from latest scrape")
        print("  python3 query_openalex_db.py view-wp-mine [year]")
        print("    Examples:")
        print("      view-wp-mine                  # View Andreas Brøgger's working papers")
        print("      view-wp-mine 2024             # View Andreas Brøgger's 2024 working papers")
        print("  python3 query_openalex_db.py view-wp-year <year> [limit]")
        print("    Examples:")
        print("      view-wp-year 2025             # View 2025 working papers (default 30)")
        print("      view-wp-year 2025 50          # View 50 most recent from 2025")
        print("  python3 query_openalex_db.py count             # Count articles")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'list':
        list_all_articles()
    elif command == 'get' and len(sys.argv) > 2:
        get_article(int(sys.argv[2]))
    elif command == 'search' and len(sys.argv) > 2:
        search_by_title(' '.join(sys.argv[2:]))
    elif command == 'author' and len(sys.argv) > 2:
        search_by_author(' '.join(sys.argv[2:]))
    elif command == 'rank-authors':
        # Parse arguments and --N flag
        journals = None
        year = None
        top_n = 50
        
        for arg in sys.argv[2:]:
            if arg.startswith('--'):
                try:
                    top_n = int(arg[2:])
                except ValueError:
                    print(f"Invalid --N flag: {arg}")
                    sys.exit(1)
            elif journals is None:
                journals = arg
            elif year is None:
                year = arg
        
        rank_authors(journals, year, top_n)
    elif command == 'papers-by-author' and len(sys.argv) > 2:
        author_name = sys.argv[2]
        journals = sys.argv[3] if len(sys.argv) > 3 else None
        year = sys.argv[4] if len(sys.argv) > 4 else None
        papers_by_author(author_name, journals, year)
    elif command == 'make-author-list':
        journals = sys.argv[2] if len(sys.argv) > 2 else None
        year = sys.argv[3] if len(sys.argv) > 3 else None
        make_author_list(journals, year)
    elif command == 'view-wp-new':
        year = sys.argv[2] if len(sys.argv) > 2 else None
        view_wp_new(year)
    elif command == 'view-wp-mine':
        year = sys.argv[2] if len(sys.argv) > 2 else None
        view_wp_mine(year)
    elif command == 'view-wp-year' and len(sys.argv) > 2:
        year = sys.argv[2]
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 30
        view_wp_year(year, limit)
    elif command == 'count':
        count_articles()
    else:
        print("Invalid command or missing arguments")
        sys.exit(1)

if __name__ == "__main__":
    main()
