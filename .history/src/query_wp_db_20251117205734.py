#!/usr/bin/env python3
"""Query the working papers database"""
import sqlite3
import sys
import os
import shutil

DB_DIR = '../out/data'

def list_working_papers(year=None, author=None, limit=50):
    """
    List working papers from database.
    
    Args:
        year (str, optional): Year filter
        author (str, optional): Author name filter (partial match)
        limit (int): Maximum number of papers to display
    """
    db_filename = f"working_papers_{year}.db" if year else "working_papers.db"
    db_path = os.path.join(DB_DIR, db_filename)
    
    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build query
    query = '''
        SELECT title, author_name, author_affiliation, publication_date, 
               primary_location, type, cited_by_count
        FROM working_papers
    '''
    params = []
    
    if author:
        query += " WHERE author_name LIKE ?"
        params.append(f"%{author}%")
    
    query += " ORDER BY publication_date DESC"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query, params)
    papers = cursor.fetchall()
    
    if not papers:
        print("No working papers found.")
        conn.close()
        return
    
    year_label = f" ({year})" if year else ""
    author_label = f" for '{author}'" if author else ""
    print(f"\nWorking Papers{year_label}{author_label} (Total: {len(papers)})\n")
    
    # Get terminal height for pagination
    terminal_height = shutil.get_terminal_size().lines
    batch_size = max(5, terminal_height - 8)
    
    print("="*150)
    print(f"{'Date':<12} {'Author':<35} {'Affiliation':<30} {'Title':<50} {'Citations':<10}")
    print("="*150)
    
    for idx, (title, author_name, affiliation, pub_date, location, wp_type, citations) in enumerate(papers, 1):
        # Truncate for display
        author_short = (author_name[:32] + '...') if len(author_name) > 35 else author_name
        affiliation_short = (affiliation[:27] + '...') if affiliation and len(affiliation) > 30 else (affiliation or '')
        title_short = (title[:47] + '...') if title and len(title) > 50 else (title or 'N/A')
        
        print(f"{pub_date or 'N/A':<12} {author_short:<35} {affiliation_short:<30} {title_short:<50} {citations or 0:<10}")
        
        # Pagination
        if idx % batch_size == 0 and idx < len(papers):
            print("\n" + "-"*150)
            input(f"Showing {idx}/{len(papers)} papers. Press Enter to continue...")
            print("-"*150 + "\n")
            print(f"{'Date':<12} {'Author':<35} {'Affiliation':<30} {'Title':<50} {'Citations':<10}")
            print("="*150)
    
    print("="*150)
    conn.close()

def rank_authors_by_wp(year=None, top_n=50):
    """
    Rank authors by number of working papers.
    
    Args:
        year (str, optional): Year filter
        top_n (int): Number of top authors to display
    """
    db_filename = f"working_papers_{year}.db" if year else "working_papers.db"
    db_path = os.path.join(DB_DIR, db_filename)
    
    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT author_name, author_affiliation, COUNT(*) as wp_count, 
               SUM(cited_by_count) as total_citations,
               MAX(publication_date) as latest_date
        FROM working_papers
        GROUP BY author_name
        ORDER BY wp_count DESC, total_citations DESC
    ''')
    
    ranked = cursor.fetchall()
    conn.close()
    
    if not ranked:
        print("No working papers found.")
        return
    
    year_label = f" ({year})" if year else ""
    print(f"\nAuthor Rankings by Working Papers{year_label} (Total: {len(ranked)} authors)\n")
    
    # Get terminal height for pagination
    terminal_height = shutil.get_terminal_size().lines
    batch_size = max(10, terminal_height - 6)
    
    print("="*140)
    print(f"{'Rank':<6} {'Papers':<8} {'Citations':<11} {'Author Name':<40} {'Affiliation':<30} {'Latest WP':<12}")
    print("="*140)
    
    for rank, (author_name, affiliation, wp_count, citations, latest_date) in enumerate(ranked[:top_n], 1):
        author_short = (author_name[:37] + '...') if len(author_name) > 40 else author_name
        affiliation_short = (affiliation[:27] + '...') if affiliation and len(affiliation) > 30 else (affiliation or '')
        
        print(f"{rank:<6} {wp_count:<8} {citations or 0:<11} {author_short:<40} {affiliation_short:<30} {latest_date or 'N/A':<12}")
        
        # Pagination
        if rank % batch_size == 0 and rank < min(top_n, len(ranked)):
            print("\n" + "-"*140)
            input(f"Showing {rank}/{min(top_n, len(ranked))} authors. Press Enter to continue...")
            print("-"*140 + "\n")
            print(f"{'Rank':<6} {'Papers':<8} {'Citations':<11} {'Author Name':<40} {'Affiliation':<30} {'Latest WP':<12}")
            print("="*140)
    
    print("="*140)
    
    if len(ranked) > top_n:
        print(f"\n... and {len(ranked) - top_n} more authors")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 query_wp_db.py <command> [options]")
        print("\nCommands:")
        print("  list [year] [--author=name] [--N]  - List working papers")
        print("  rank [year] [--N]                   - Rank authors by working papers")
        print("\nExamples:")
        print("  python3 query_wp_db.py list 2024")
        print("  python3 query_wp_db.py list 2024 --author='Kelly' --100")
        print("  python3 query_wp_db.py rank 2024 --50")
        sys.exit(1)
    
    command = sys.argv[1]
    
    # Parse arguments
    year = None
    author = None
    limit = 50
    
    for arg in sys.argv[2:]:
        if arg.startswith('--author='):
            author = arg.split('=', 1)[1].strip("'\"")
        elif arg.startswith('--'):
            try:
                limit = int(arg[2:])
            except ValueError:
                print(f"Invalid --N flag: {arg}")
                sys.exit(1)
        elif year is None:
            year = arg
    
    if command == 'list':
        list_working_papers(year=year, author=author, limit=limit)
    elif command == 'rank':
        rank_authors_by_wp(year=year, top_n=limit)
    else:
        print(f"Unknown command: {command}")
        print("Available commands: list, rank")
        sys.exit(1)

if __name__ == "__main__":
    main()
