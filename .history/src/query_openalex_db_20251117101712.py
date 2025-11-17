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
    """Connect to the database"""
    path = db_path or DB_PATH
    if not os.path.exists(path):
        print(f"Error: Database not found at {path}")
        sys.exit(1)
    return sqlite3.connect(path)

def list_all_articles():
    """List all articles with basic info"""
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
    """Get detailed info for a specific article"""
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
    """Search articles by title keyword"""
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
    """Count total articles in database"""
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
    """Search articles by author name"""
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

def rank_authors(journals=None, year=None):
    """Rank authors by number of publications"""
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
    
    # Build label
    journal_label = f"{journals}" if journals else "all journals"
    year_label = f" ({year})" if year else " (all years)"
    
    print(f"\nAuthor Rankings - {journal_label}{year_label} (Total: {len(ranked)} authors)\n")
    print(f"{'Rank':<6} {'Papers':<8} {'Author Name':<60}")
    print("=" * 80)
    
    for rank, (author_name, count) in enumerate(ranked, 1):
        print(f"{rank:<6} {count:<8} {author_name}")
        if rank >= 50:  # Limit to top 50
            print(f"\n... and {len(ranked) - 50} more authors")
            break
    
    print(f"\nTotal articles: {total_articles}")
    print(f"Total unique authors: {len(ranked)}")

def papers_by_author(author_name, journals=None, year=None):
    """Show all papers by a specific author"""
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
    """Create a CSV file with top ranked authors"""
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
    """View working papers from the most recent scrape"""
    import glob
    from datetime import datetime
    
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
    
    # Get all papers from the latest scrape
    cursor.execute('''
        SELECT id, title, author_name, publication_date, doi, primary_location, scraped_at
        FROM working_papers
        WHERE scraped_at = ?
        ORDER BY author_name, publication_date DESC
    ''', (latest_scrape,))
    
    papers = cursor.fetchall()
    conn.close()
    
    if not papers:
        print("No working papers found from the latest scrape")
        return
    
    year_label = f" ({year})" if year else ""
    print(f"\nWorking Papers from Latest Scrape{year_label}")
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

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 query_openalex_db.py list              # List all articles")
        print("  python3 query_openalex_db.py get <id>          # Get article details")
        print("  python3 query_openalex_db.py search <keyword>  # Search by title")
        print("  python3 query_openalex_db.py author <name>     # Search by author")
        print("  python3 query_openalex_db.py rank-authors [journal] [year] # Rank authors")
        print("    Examples:")
        print("      rank-authors                  # All journals, all years")
        print("      rank-authors jf               # Journal of Finance, all years")
        print("      rank-authors top3 2024        # All 3 journals, 2024 only")
        print("      rank-authors jfe 2023         # JFE, 2023 only")
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
        journals = sys.argv[2] if len(sys.argv) > 2 else None
        year = sys.argv[3] if len(sys.argv) > 3 else None
        rank_authors(journals, year)
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
    elif command == 'count':
        count_articles()
    else:
        print("Invalid command or missing arguments")
        sys.exit(1)

if __name__ == "__main__":
    main()
