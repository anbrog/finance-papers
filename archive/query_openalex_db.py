#!/usr/bin/env python3
"""Query the OpenAlex articles database"""
import sqlite3
import json
import sys
import os
import shutil

# Get DB_DIR relative to project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
DB_DIR = os.path.join(project_root, 'out', 'data')

# Journal group definitions (must match getpapers_openalex.py)
JOURNAL_GROUPS = {
    'top3': ['jf', 'rfs', 'jfe'],  # Top 3 Finance journals
    'econ5': ['qje', 'aer', 'ecma', 'jpe', 'restud'],  # Top 5 Economics journals
    'alltop': ['jf', 'rfs', 'jfe', 'qje', 'aer', 'ecma', 'jpe', 'restud'],  # All top journals
}

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

def rank_authors(journals=None, year=None, top_n=50, by_citations=False):
    """
    Rank authors by number of publications or citations in specified journals/years.

    Args:
        journals (str, optional): Journal code ('jf', 'rfs', 'jfe') or group ('top3', 'econ5', 'alltop') or None for all
        year (str, optional): Year filter (e.g., '2024') or None for all years
        top_n (int): Number of top authors to display (default: 50)
        by_citations (bool): If True, rank by total citations instead of paper count

    Returns:
        None (prints to console)

    Output:
        Displays ranked list of top N authors with publication counts and totals
    """
    import glob

    # Determine which databases to query
    if journals in JOURNAL_GROUPS:
        journal_codes = JOURNAL_GROUPS[journals]
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

    # Author name normalization mapping (variants -> canonical name)
    author_name_mapping = {
        'Jules H. van Binsbergen': 'Jules van Binsbergen',
        # Add more mappings here as needed
    }

    # Build reverse mapping (canonical name -> list of all variants including canonical)
    canonical_to_variants = {}
    for variant, canonical in author_name_mapping.items():
        if canonical not in canonical_to_variants:
            canonical_to_variants[canonical] = [canonical]
        canonical_to_variants[canonical].append(variant)

    def normalize_author_name(name):
        """Normalize author name to canonical form"""
        return author_name_mapping.get(name, name)

    def get_all_name_variants(canonical_name):
        """Get all name variants for a canonical author name"""
        return canonical_to_variants.get(canonical_name, [canonical_name])

    # Count publications and citations per author across all databases
    author_counts = {}
    author_citations = {}
    author_latest_paper = {}
    total_articles = 0

    # Initialize Andreas Brøgger with 0 (will be updated if he has papers)
    author_counts['Andreas Brøgger'] = 0
    author_citations['Andreas Brøgger'] = 0
    author_latest_paper['Andreas Brøgger'] = ('', '')

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
                    # Normalize author name to handle variants
                    name = normalize_author_name(name)

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
    
    # Sort by citations or count
    if by_citations:
        ranked = sorted(author_counts.items(), key=lambda x: (author_citations[x[0]], x[1]), reverse=True)
        sort_label = "by Citations"
    else:
        ranked = sorted(author_counts.items(), key=lambda x: (x[1], author_citations[x[0]]), reverse=True)
        sort_label = "by Papers"
    
    # Build label
    journal_label = f"{journals}" if journals else "all journals"
    year_label = f" ({year})" if year else " (all years)"

    # Get terminal dimensions
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines
    batch_size = max(10, terminal_height - 6)

    # Fixed column widths
    rank_width = 5
    papers_width = 7
    citations_width = 9
    fixed_width = rank_width + papers_width + citations_width

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 4  # 4 for spacing

    # Allocate remaining space: 35% to author name, 65% to latest paper
    author_width = max(20, int(remaining * 0.35))
    paper_width = remaining - author_width

    print(f"\nAuthor Rankings {sort_label} - {journal_label}{year_label} (Total: {len(ranked)} authors)\n")

    def print_header():
        print(f"{'Rank':<{rank_width}} {'Papers':<{papers_width}} {'Cites':<{citations_width}} {'Author Name':<{author_width}} {'Latest Paper':<{paper_width}}")
        print("=" * terminal_width)

    def show_author_papers_from_db(search_term, limit=20):
        """Search for an author and display their papers from the databases"""
        # Parse optional limit from search term (e.g., "Fama 10")
        parts = search_term.strip().split()
        if len(parts) > 1:
            try:
                limit = int(parts[-1])
                search_term = ' '.join(parts[:-1])
            except ValueError:
                pass

        # Find matching authors (case-insensitive partial match)
        matching_authors = [(name, cnt) for name, cnt in ranked if search_term.lower() in name.lower()]

        if not matching_authors:
            print(f"\nNo authors found matching '{search_term}'")
            return

        # If multiple matches, show them and let user pick
        if len(matching_authors) > 1:
            print(f"\nFound {len(matching_authors)} matching authors:")
            for i, (name, cnt) in enumerate(matching_authors[:10], 1):
                print(f"  {i}. {name} ({cnt} papers)")
            if len(matching_authors) > 10:
                print(f"  ... and {len(matching_authors) - 10} more")

            pick = input("Enter number to select (or press Enter to cancel): ").strip()
            if not pick:
                return
            try:
                idx = int(pick) - 1
                if 0 <= idx < len(matching_authors):
                    matching_authors = [matching_authors[idx]]
                else:
                    print("Invalid selection")
                    return
            except ValueError:
                print("Invalid selection")
                return

        # Get the selected author
        selected_author, paper_count = matching_authors[0]

        # Get all name variants for this author (handles normalized names)
        name_variants = get_all_name_variants(selected_author)

        # Query databases for this author's papers (search all name variants)
        all_papers = []
        seen_titles = set()  # Avoid duplicates
        for db_file in db_files:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # Search for all name variants
            for name_variant in name_variants:
                cursor.execute('''
                    SELECT title, authors_json, publication_date, cited_by_count, doi, openalex_id, topics_json
                    FROM openalex_articles
                    WHERE authors_json LIKE ?
                ''', (f'%{name_variant}%',))

                papers = cursor.fetchall()
                # Extract journal from filename
                db_basename = os.path.basename(db_file)
                journal = db_basename.split('_')[1].upper() if '_' in db_basename else 'UNK'

                for paper in papers:
                    # Avoid duplicates (same title)
                    if paper[0] not in seen_titles:
                        seen_titles.add(paper[0])
                        all_papers.append((journal, *paper))

            conn.close()

        if not all_papers:
            print(f"\nNo papers found for '{selected_author}'")
            return

        # Sort by publication date (most recent first)
        all_papers.sort(key=lambda x: x[3] or '', reverse=True)
        all_papers = all_papers[:limit]

        # Display
        print(f"\n{'='*terminal_width}")
        print(f"Papers by {selected_author} ({paper_count} papers, {author_citations.get(selected_author, 0)} total citations)")
        print(f"{'='*terminal_width}\n")

        for i, row in enumerate(all_papers, 1):
            journal, title, authors_json, pub_date, citations, doi, openalex_id = row[:7]
            topics_json = row[7] if len(row) > 7 else None

            # Parse topic
            topic = None
            if topics_json:
                try:
                    topics_data = json.loads(topics_json)
                    if topics_data:
                        topic = topics_data[0].get('name')
                except (json.JSONDecodeError, TypeError):
                    pass

            print(f"{i}. [{journal}] {pub_date or 'N/A'} - {title} ({citations or 0} cites)")
            if topic:
                print(f"   Topic: {topic}")
            if doi:
                print(f"   https://doi.org/{doi}")
            elif openalex_id:
                print(f"   {openalex_id}")
            print()

        print(f"{'='*terminal_width}")

    is_interactive = sys.stdin.isatty() and sys.stdout.isatty()

    # Track if Andreas Brøgger appears in top N (scan all ranked authors first)
    andreas_brogger_in_top_n = False
    andreas_brogger_rank = None
    andreas_brogger_count = None
    andreas_brogger_citations = None
    andreas_brogger_paper = None

    for rank, (author_name, count) in enumerate(ranked, 1):
        if author_name == 'Andreas Brøgger':
            andreas_brogger_rank = rank
            andreas_brogger_count = count
            andreas_brogger_citations = author_citations[author_name]
            andreas_brogger_paper = author_latest_paper.get(author_name, ('', ''))
            if rank <= top_n:
                andreas_brogger_in_top_n = True
            break

    # Display rankings in batches with while loop for proper pagination control
    print_header()
    display_rank = 0
    while display_rank < min(len(ranked), top_n):
        # Calculate batch boundaries
        batch_start = display_rank
        batch_end = min(display_rank + batch_size, top_n, len(ranked))

        # Print current batch
        for i in range(batch_start, batch_end):
            author_name, count = ranked[i]
            rank = i + 1
            citations = author_citations[author_name]
            latest_date, latest_title = author_latest_paper.get(author_name, ('', ''))

            # Truncate for display (paper field gets extra -1 for safety)
            author_short = (author_name[:author_width-3] + '...') if len(author_name) > author_width else author_name
            title_short = (latest_title[:paper_width-15] + '...') if len(latest_title) > (paper_width - 15) else latest_title
            latest_paper_str = f"{latest_date}: {title_short}" if latest_date else 'N/A'

            # Highlight Andreas Brøgger in light blue
            if author_name == 'Andreas Brøgger':
                print(f"\033[94m{rank:<{rank_width}} {count:<{papers_width}} {citations:<{citations_width}} {author_short:<{author_width}} {latest_paper_str}\033[0m")
            else:
                print(f"{rank:<{rank_width}} {count:<{papers_width}} {citations:<{citations_width}} {author_short:<{author_width}} {latest_paper_str}")

        display_rank = batch_end

        # Wait for user after each batch (except the last batch)
        if display_rank < min(len(ranked), top_n) and is_interactive:
            print("\n" + "-"*terminal_width)
            user_input = input(f"[{display_rank}/{min(len(ranked), top_n)}] Enter to continue, or type author name [N]: ").strip()

            if user_input:
                show_author_papers_from_db(user_input)
                input("\nPress Enter to return to ranking...")
                # Go back to repeat current batch
                display_rank = batch_start

            print("-"*terminal_width + "\n")
            # Repeat table heading
            print_header()

    # Show how many more authors exist
    remaining = len(ranked) - top_n
    if remaining > 0 and not andreas_brogger_in_top_n:
        print(f"\n... and {remaining - 1} more authors")
    elif remaining > 0:
        print(f"\n... and {remaining} more authors")

    # Always display Andreas Brøgger at the end if not in top N
    if not andreas_brogger_in_top_n and andreas_brogger_rank:
        print()  # Blank line before Andreas Brøgger
        latest_date, latest_title = andreas_brogger_paper
        author_short = 'Andreas Brøgger'
        title_short = (latest_title[:paper_width-15] + '...') if len(latest_title) > (paper_width - 15) else latest_title
        latest_paper_str = f"{latest_date}: {title_short}" if latest_date else 'N/A'

        print(f"\033[94m{andreas_brogger_rank:<{rank_width}} {andreas_brogger_count:<{papers_width}} {andreas_brogger_citations:<{citations_width}} {author_short:<{author_width}} {latest_paper_str}\033[0m")

    print("=" * terminal_width)

    print(f"\nTotal articles: {total_articles}")
    print(f"Total unique authors: {len(ranked)}")

    # Interactive author lookup loop (only if interactive)
    if is_interactive:
        while True:
            print("\n" + "-"*terminal_width)
            user_input = input("Enter author name [N] to see papers (or press Enter to exit): ").strip()

            if not user_input:
                break

            show_author_papers_from_db(user_input)

def papers_by_author(author_name, journals=None, year=None):
    """
    Show all papers by a specific author with optional journal/year filters.

    Args:
        author_name (str): Author name to search for
        journals (str, optional): Journal code or group ('top3', 'econ5', 'alltop') or None
        year (str, optional): Year filter or None

    Returns:
        None (prints to console)

    Output:
        Displays table of papers with Database, ID, Date, Title, and Authors
    """
    import glob

    # Determine which databases to query
    if journals in JOURNAL_GROUPS:
        journal_codes = JOURNAL_GROUPS[journals]
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
        journals (str, optional): Journal code or group ('top3', 'econ5', 'alltop')
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
    if journals in JOURNAL_GROUPS:
        journal_codes = JOURNAL_GROUPS[journals]
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

    # Author name normalization mapping (variants -> canonical name)
    author_name_mapping = {
        'Jules H. van Binsbergen': 'Jules van Binsbergen',
        # Add more mappings here as needed
    }

    def normalize_author_name(name):
        """Normalize author name to canonical form"""
        return author_name_mapping.get(name, name)

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
                author_id = author.get('author_id')
                if name:
                    # Normalize author name to handle variants
                    name = normalize_author_name(name)

                    # Use author_id as key if available, otherwise fall back to name
                    key = author_id if author_id else name
                    if key not in author_counts:
                        author_counts[key] = {
                            'name': name,
                            'author_id': author_id,
                            'count': 0
                        }
                    author_counts[key]['count'] += 1

        conn.close()
    
    # Sort by count (descending)
    ranked = sorted(author_counts.items(), key=lambda x: x[1]['count'], reverse=True)

    # Take top N
    top_authors = ranked[:top_n]

    # Find Andreas Brøgger in the full list
    andreas_brogger_entry = None
    andreas_brogger_rank = None
    for rank, (key, data) in enumerate(ranked, 1):
        if data['name'] == 'Andreas Brøgger':
            andreas_brogger_entry = (key, data)
            andreas_brogger_rank = rank
            break

    # Always add Andreas Brøgger after top N if not in top N
    # Check if already in top authors
    already_in_top = any(data['name'] == 'Andreas Brøgger' for _, data in top_authors)

    if not already_in_top:
        if andreas_brogger_entry:
            # He exists but outside top 250
            top_authors.append(andreas_brogger_entry)
            print(f"\n📝 Note: Added Andreas Brøgger at position {len(top_authors)} (actual rank: {andreas_brogger_rank})")
        else:
            # He doesn't exist in rankings at all (0 papers) - add him with both OpenAlex IDs
            # Use comma-separated IDs so working papers lookup will check both profiles
            andreas_ids = 'A5011626190,A5118977207'  # Both OpenAlex IDs
            andreas_entry = (andreas_ids, {
                'name': 'Andreas Brøgger',
                'author_id': andreas_ids,
                'count': 0
            })
            top_authors.append(andreas_entry)
            print(f"\n📝 Note: Added Andreas Brøgger at position {len(top_authors)} (0 papers in dataset, will search both OpenAlex profiles)")

    # Create filename
    journal_label = f"{journals}" if journals else "all"
    year_label = f"_{year}" if year else "_all"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"author_list_{journal_label}{year_label}_top{top_n}_{timestamp}.csv"
    filepath = os.path.join(DB_DIR, filename)
    
    # Write to CSV
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Rank', 'Author Name', 'Author ID', 'Paper Count'])
        
        for rank, (key, data) in enumerate(top_authors, 1):
            writer.writerow([rank, data['name'], data['author_id'] or '', data['count']])
    
    print(f"\n✅ Author list saved to: {filepath}")
    print(f"   Total authors in list: {len(top_authors)}")
    print(f"   Total unique authors: {len(ranked)}")
    print(f"   Total articles: {total_articles}")
    print(f"\nTop 10 authors:")
    for rank, (key, data) in enumerate(top_authors[:10], 1):
        print(f"   {rank}. {data['name']} ({data['count']} papers)")

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

def prolific_authors(min_papers=2, start_year=2022, end_year=2025, no_pager=False, max_authors=None, topic_filter=None, topic_any_filter=None, topic_main_filter=None):
    """
    Show authors with N or more publications in top 3 journals (JF, RFS, JFE) within a year range.

    Args:
        min_papers (int): Minimum number of papers to qualify (default: 2)
        start_year (int): Start year (default: 2022)
        end_year (int): End year (default: 2025)
        no_pager (bool): If True, disable pagination (default: False)
        max_authors (int): Maximum number of authors to display (default: None = all)
        topic_filter (str): Only count papers with this topic (case-insensitive partial match)
        topic_any_filter (str): Filter to authors with any paper in this topic
        topic_main_filter (str): Filter to authors whose main topic matches

    Returns:
        None (prints to console)

    Output:
        Displays ranked list of qualifying authors with publication counts per journal
    """
    import glob

    journal_codes = ['jf', 'rfs', 'jfe']
    years = list(range(start_year, end_year + 1))

    # Collect all matching database files
    db_files = []
    for jcode in journal_codes:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{jcode}_{year}.db')
            if os.path.exists(db_file):
                db_files.append((db_file, jcode, year))

    if not db_files:
        print("Error: No matching database files found")
        sys.exit(1)

    print(f"Querying {len(db_files)} database(s) for years {start_year}-{end_year}...")

    # Author name normalization mapping
    author_name_mapping = {
        'Jules H. van Binsbergen': 'Jules van Binsbergen',
        'ANTOINETTE SCHOAR': 'Antoinette Schoar',
    }

    # Manual affiliation overrides (when OpenAlex data is incorrect/inconsistent)
    affiliation_overrides = {
    }

    def normalize_author_name(name):
        return author_name_mapping.get(name, name)

    # Count publications per author, per journal
    # Use author_id as primary key when available for better deduplication
    author_data = {}  # {name: {'total': N, 'jf': N, 'rfs': N, 'jfe': N, 'citations': N, 'affiliations': [...], 'papers': [...]}}
    author_id_to_name = {}  # Track author_id -> canonical name mapping
    author_id_affiliations = {}  # Track affiliations by author_id for more reliable matching
    total_articles = 0

    for db_file, jcode, year in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('SELECT authors_json, title, publication_date, cited_by_count, topics_json FROM openalex_articles')
        articles = cursor.fetchall()
        total_articles += len(articles)

        for row in articles:
            authors_json, title, pub_date, cited_by_count = row[:4]
            topics_json = row[4] if len(row) > 4 else None
            authors = json.loads(authors_json)
            citations = cited_by_count or 0

            # Parse topics if available
            paper_topics = []
            if topics_json:
                try:
                    topics_data = json.loads(topics_json)
                    paper_topics = [t.get('name') for t in topics_data if t.get('name')]
                except (json.JSONDecodeError, TypeError):
                    pass

            # If --topic filter is set, skip papers that don't match
            if topic_filter:
                topic_filter_lower = topic_filter.lower()
                if not any(topic_filter_lower in t.lower() for t in paper_topics):
                    continue  # Skip this paper

            for author in authors:
                name = author.get('name')
                author_id = author.get('author_id')
                if name:
                    name = normalize_author_name(name)

                    # Track author_id to name mapping
                    if author_id:
                        if author_id not in author_id_to_name:
                            author_id_to_name[author_id] = name
                            author_id_affiliations[author_id] = []

                    if name not in author_data:
                        author_data[name] = {
                            'total': 0,
                            'jf': 0, 'rfs': 0, 'jfe': 0,
                            'citations': 0,
                            'author_id': author_id,
                            'affiliations': [],  # Collect all affiliations
                            'topics': [],  # Collect all topics
                            'papers': []
                        }
                    # Update author_id if we find one
                    if author_id and not author_data[name].get('author_id'):
                        author_data[name]['author_id'] = author_id

                    author_data[name]['total'] += 1
                    author_data[name][jcode] += 1
                    author_data[name]['citations'] += citations
                    author_data[name]['papers'].append({
                        'title': title,
                        'date': pub_date,
                        'journal': jcode.upper()
                    })

                    # Collect topics for this author
                    author_data[name]['topics'].extend(paper_topics)

                    # Collect institutions - track by both name and author_id
                    institutions = author.get('institutions', [])
                    if institutions:
                        for inst in institutions:
                            if inst:  # Skip empty strings
                                author_data[name]['affiliations'].append(inst)
                                if author_id:
                                    author_id_affiliations[author_id].append(inst)

        conn.close()

    # Determine most common affiliation and topic for each author
    # Use name-based affiliations which consolidate all author_id variants
    from collections import Counter
    for name, data in author_data.items():
        # Check for manual override first
        if name in affiliation_overrides:
            data['affiliation'] = affiliation_overrides[name]
        elif data['affiliations']:
            # Use name-based affiliations (consolidated from all author_id variants)
            affil_counts = Counter(data['affiliations'])
            data['affiliation'] = affil_counts.most_common(1)[0][0]
        else:
            data['affiliation'] = None

        # Determine most common topic
        if data.get('topics'):
            topic_counts = Counter(data['topics'])
            data['top_topic'] = topic_counts.most_common(1)[0][0]
        else:
            data['top_topic'] = None

    # Filter to authors with min_papers or more
    qualifying = {name: data for name, data in author_data.items() if data['total'] >= min_papers}

    # Apply --topicany filter: authors with any paper in this topic
    if topic_any_filter:
        topic_any_lower = topic_any_filter.lower()
        qualifying = {
            name: data for name, data in qualifying.items()
            if any(topic_any_lower in t.lower() for t in data.get('topics', []))
        }

    # Apply --topicmain filter: authors whose main topic matches
    if topic_main_filter:
        topic_main_lower = topic_main_filter.lower()
        qualifying = {
            name: data for name, data in qualifying.items()
            if data.get('top_topic') and topic_main_lower in data['top_topic'].lower()
        }

    # Sort by total count (descending), then by citations (descending)
    ranked = sorted(qualifying.items(), key=lambda x: (x[1]['total'], x[1]['citations']), reverse=True)

    # Get terminal dimensions
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(100, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines
    batch_size = max(10, terminal_height - 6)

    # Determine display limit
    display_limit = min(max_authors, len(ranked)) if max_authors else len(ranked)

    topic_msg = ""
    if topic_filter:
        topic_msg = f" [Papers in: {topic_filter}]"
    elif topic_any_filter:
        topic_msg = f" [Any paper in: {topic_any_filter}]"
    elif topic_main_filter:
        topic_msg = f" [Main topic: {topic_main_filter}]"
    print(f"\nAuthors with {min_papers}+ Publications in Top 3 Journals ({start_year}-{end_year}){topic_msg}")
    if max_authors and max_authors < len(ranked):
        print(f"Showing top {display_limit} of {len(ranked)} qualifying authors (out of {len(author_data)} unique authors)\n")
    else:
        print(f"Total qualifying authors: {len(ranked)} (out of {len(author_data)} unique authors)\n")

    def show_author_papers(search_term, qualifying_authors, db_files_list, term_width):
        """Helper function to display papers for an author"""
        # Find matching authors (case-insensitive partial match)
        matches = [(name, data) for name, data in qualifying_authors.items()
                  if search_term.lower() in name.lower()]

        if not matches:
            print(f"No authors found matching '{search_term}'")
            return

        # If multiple matches, show them and let user pick
        if len(matches) > 1:
            print(f"\nFound {len(matches)} matching authors:")
            for i, (name, data) in enumerate(matches, 1):
                print(f"  {i}. {name} ({data['total']} papers)")

            pick = input("Enter number to select (or press Enter to cancel): ").strip()
            if not pick:
                return
            try:
                idx = int(pick) - 1
                if 0 <= idx < len(matches):
                    matches = [matches[idx]]
                else:
                    print("Invalid selection")
                    return
            except ValueError:
                print("Invalid selection")
                return

        # Show papers for the selected author
        author_name, author_info = matches[0]
        papers = author_info['papers']

        print(f"\n{'='*term_width}")
        print(f"Papers by {author_name} ({author_info['total']} papers, {author_info['citations']} citations)")
        affil = author_info.get('affiliation') or 'Unknown'
        print(f"Affiliation: {affil}")
        print(f"JF: {author_info['jf']} | RFS: {author_info['rfs']} | JFE: {author_info['jfe']}")
        print(f"{'='*term_width}\n")

        # Sort papers by date (most recent first)
        papers_sorted = sorted(papers, key=lambda x: x['date'] or '', reverse=True)

        # Get DOIs and topics for these papers - query the databases again
        paper_details = []
        for paper in papers_sorted:
            for db_file, jcode, yr in db_files_list:
                if jcode.upper() != paper['journal']:
                    continue
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT doi, openalex_id, cited_by_count, topics_json
                    FROM openalex_articles
                    WHERE title = ? AND publication_date = ?
                ''', (paper['title'], paper['date']))
                result = cursor.fetchone()
                conn.close()
                if result:
                    # Parse topic from topics_json
                    topic = None
                    if result[3]:
                        try:
                            topics_data = json.loads(result[3])
                            if topics_data:
                                topic = topics_data[0].get('name')
                        except (json.JSONDecodeError, TypeError):
                            pass
                    paper_details.append({
                        'title': paper['title'],
                        'date': paper['date'],
                        'journal': paper['journal'],
                        'doi': result[0],
                        'openalex_id': result[1],
                        'citations': result[2] or 0,
                        'topic': topic
                    })
                    break

        for i, paper in enumerate(paper_details, 1):
            print(f"{i}. [{paper['journal']}] {paper['date'] or 'N/A'} - {paper['title']} ({paper['citations']} cites)")
            if paper.get('topic'):
                print(f"   Topic: {paper['topic']}")
            if paper['doi']:
                print(f"   https://doi.org/{paper['doi']}")
            elif paper['openalex_id']:
                print(f"   {paper['openalex_id']}")
            print()

        print(f"{'='*term_width}")

    # Calculate column widths based on terminal
    # Fixed columns: Rank(5) + Author(22) + Total(4) + JF(4) + RFS(4) + JFE(4) + Cites(7) = 50
    remaining_width = terminal_width - 52
    affil_width = max(14, int(remaining_width * 0.38))
    topic_width = max(16, int(remaining_width * 0.38))

    # ANSI color codes (using 256-color for lighter gray)
    GRAY = '\033[38;5;245m'
    RESET = '\033[0m'

    def print_header():
        print(f"{'Rank':<5} {'Author Name':<22} {GRAY}{'Affiliation':<{affil_width}} {'Topic':<{topic_width}}{RESET} {'Tot':<4} {'JF':<4} {'RFS':<4} {'JFE':<4} {'Cites':<7}")
        print("=" * terminal_width)

    print_header()

    is_interactive = sys.stdin.isatty() and sys.stdout.isatty()

    rank = 0
    while rank < display_limit:
        # Calculate batch start and end
        batch_start = rank
        batch_end = min(rank + batch_size, display_limit)

        # Print current batch
        for i in range(batch_start, batch_end):
            name, data = ranked[i]
            author_short = (name[:19] + '...') if len(name) > 22 else name
            affil = data.get('affiliation') or ''
            affil_short = (affil[:affil_width-3] + '...') if len(affil) > affil_width else affil
            topic = data.get('top_topic') or ''
            topic_short = (topic[:topic_width-3] + '...') if len(topic) > topic_width else topic
            print(f"{i+1:<5} {author_short:<22} {GRAY}{affil_short:<{affil_width}} {topic_short:<{topic_width}}{RESET} {data['total']:<4} {data['jf']:<4} {data['rfs']:<4} {data['jfe']:<4} {data['citations']:<7}")

        rank = batch_end

        # Wait for user after each batch (only if interactive and pager enabled)
        if rank < display_limit and is_interactive and not no_pager:
            print("\n" + "-"*terminal_width)
            user_input = input(f"[{rank}/{display_limit}] Enter to continue, or type author name: ").strip()

            if user_input:
                show_author_papers(user_input, qualifying, db_files, terminal_width)
                input("\nPress Enter to return to ranking...")
                # Go back to repeat current batch
                rank = batch_start

            print("-"*terminal_width + "\n")
            print_header()

    print("=" * terminal_width)
    print(f"\nTotal articles scanned: {total_articles}")
    print(f"Unique authors: {len(author_data)}")
    print(f"Authors with {min_papers}+ papers: {len(ranked)}")

    # Interactive author lookup loop (only if interactive)
    if is_interactive and not no_pager:
        while True:
            print("\n" + "-"*terminal_width)
            user_input = input("Enter author name to see papers (or press Enter to exit): ").strip()

            if not user_input:
                break

            show_author_papers(user_input, qualifying, db_files, terminal_width)


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 query_openalex_db.py list              # List all articles")
        print("  python3 query_openalex_db.py get <id>          # Get article details")
        print("  python3 query_openalex_db.py search <keyword>  # Search by title")
        print("  python3 query_openalex_db.py author <name>     # Search by author")
        print("  python3 query_openalex_db.py rank-authors [journal] [year] [--N] # Rank authors")
        print("  python3 query_openalex_db.py prolific-authors [options]")
        print("    Options:")
        print("      --min=N, -n=N, -nN   Minimum papers to qualify (default: 2)")
        print("      --max=N              Maximum authors to display")
        print("      --start=YYYY         Start year (default: 2022)")
        print("      --end=YYYY           End year (default: 2025)")
        print("      --topic TOPIC        Only count papers with this topic")
        print("      --topicany TOPIC     Authors with any paper in this topic")
        print("      --topicmain TOPIC    Authors whose main topic matches")
        print("      --update             Update journal articles before display")
        print("      --no-pager           Disable pagination")
        print("    Examples:")
        print("      prolific-authors                          # 2+ papers in top3 (2022-2025)")
        print("      prolific-authors -n3                      # 3+ papers")
        print("      prolific-authors --topic \"Asset Pricing\"  # Only Asset Pricing papers")
        print("      prolific-authors --topicmain Corporate    # Authors focused on Corporate")
        print("      prolific-authors --update                 # Update data first")
        print("    Examples:")
        print("      rank-authors                  # All journals, all years, top 50")
        print("      rank-authors jf               # Journal of Finance, all years")
        print("      rank-authors top3 2024        # Top 3 finance journals, 2024 only")
        print("      rank-authors alltop 2024      # All top journals (finance + econ), 2024")
        print("      rank-authors jfe 2023         # JFE, 2023 only")
        print("      rank-authors top3 --250       # Top 3 finance journals, top 250 authors")
        print("      rank-authors alltop --250     # All top journals, top 250 authors")
        print("  python3 query_openalex_db.py papers-by-author <name> [journal] [year]")
        print("    Examples:")
        print("      papers-by-author Kelly")
        print("      papers-by-author Kelly jf 2024")
        print("      papers-by-author Kelly alltop 2024  # All top journals")
        print("  python3 query_openalex_db.py make-author-list [journal] [year]")
        print("    Examples:")
        print("      make-author-list top3 2024    # Top 3 finance journals")
        print("      make-author-list alltop 2024  # All top journals (finance + econ)")
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
        # Parse arguments and --N flag and --citations flag
        journals = None
        year = None
        top_n = 50
        by_citations = False
        
        for arg in sys.argv[2:]:
            if arg == '--citations':
                by_citations = True
            elif arg.startswith('--'):
                try:
                    top_n = int(arg[2:])
                except ValueError:
                    print(f"Invalid --N flag: {arg}")
                    sys.exit(1)
            elif journals is None:
                journals = arg
            elif year is None:
                year = arg
        
        rank_authors(journals, year, top_n, by_citations)
    elif command == 'papers-by-author' and len(sys.argv) > 2:
        author_name = sys.argv[2]
        journals = sys.argv[3] if len(sys.argv) > 3 else None
        year = sys.argv[4] if len(sys.argv) > 4 else None
        papers_by_author(author_name, journals, year)
    elif command == 'make-author-list':
        # Parse arguments and --N flag
        journals = None
        year = None
        top_n = 250
        
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
        
        make_author_list(journals, year, top_n)
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
    elif command == 'prolific-authors':
        # Parse --min=N, --start=YYYY, --end=YYYY, --no-pager flags
        min_papers = 2
        start_year = 2022
        end_year = 2025
        no_pager = False

        for arg in sys.argv[2:]:
            if arg.startswith('--min='):
                try:
                    min_papers = int(arg[6:])
                except ValueError:
                    print(f"Invalid --min flag: {arg}")
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

        prolific_authors(min_papers, start_year, end_year, no_pager)
    elif command == 'count':
        count_articles()
    else:
        print("Invalid command or missing arguments")
        sys.exit(1)

if __name__ == "__main__":
    main()
