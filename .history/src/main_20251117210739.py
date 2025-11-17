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

DB_DIR = '../out/data'

def run_command(cmd, description):
    """Run a shell command and handle errors"""
    print(f"\n{'='*80}")
    print(f"{description}")
    print(f"{'='*80}")
    print(f"Running: {' '.join(cmd)}\n")
    
    result = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))
    
    if result.returncode != 0:
        print(f"\n❌ Error: Command failed with exit code {result.returncode}")
        sys.exit(1)
    
    return result

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

def count_new_papers_added(journals, years):
    """Count papers added in the last scrape"""
    total_new = 0
    
    for journal in journals:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue
            
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Count papers added in last hour (approximate "just added")
            cursor.execute('''
                SELECT COUNT(*) FROM openalex_articles 
                WHERE datetime(scraped_at) > datetime('now', '-1 hour')
            ''')
            count = cursor.fetchone()[0]
            total_new += count
            
            if count > 0:
                print(f"  {journal.upper()} {year}: {count} new papers")
            
            conn.close()
    
    return total_new

def display_recent_papers(journals, years, limit=20):
    """Display recently added papers"""
    all_papers = []
    
    for journal in journals:
        for year in years:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue
            
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT title, authors_json, publication_date, cited_by_count, scraped_at
                FROM openalex_articles 
                WHERE datetime(scraped_at) > datetime('now', '-1 hour')
                ORDER BY scraped_at DESC
                LIMIT ?
            ''', (limit,))
            
            papers = cursor.fetchall()
            for paper in papers:
                all_papers.append((journal, year, *paper))
            
            conn.close()
    
    if not all_papers:
        print("\nNo new papers added in this run.")
        return
    
    # Sort by scraped_at and limit
    all_papers.sort(key=lambda x: x[6], reverse=True)
    all_papers = all_papers[:limit]
    
    print(f"\n{'='*120}")
    print(f"Recently Added Papers (showing {len(all_papers)})")
    print(f"{'='*120}")
    print(f"{'Journal':<8} {'Year':<6} {'Date':<12} {'Cites':<7} {'Title':<70}")
    print("-"*120)
    
    import json
    for journal, year, title, authors_json, pub_date, citations, scraped_at in all_papers:
        title_short = (title[:67] + '...') if title and len(title) > 70 else (title or 'N/A')
        print(f"{journal.upper():<8} {year:<6} {pub_date or 'N/A':<12} {citations or 0:<7} {title_short:<70}")
    
    print("="*120)

def count_new_working_papers():
    """Count working papers added in the last scrape"""
    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)
    
    total_new = 0
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM working_papers 
            WHERE datetime(scraped_at) > datetime('now', '-1 hour')
        ''')
        count = cursor.fetchone()[0]
        total_new += count
        
        conn.close()
    
    return total_new

def display_recent_working_papers(limit=30):
    """Display recently added working papers"""
    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)
    
    all_papers = []
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT title, author_name, author_affiliation, publication_date, 
                   primary_location, cited_by_count, scraped_at
            FROM working_papers 
            WHERE datetime(scraped_at) > datetime('now', '-1 hour')
            ORDER BY scraped_at DESC
            LIMIT ?
        ''', (limit,))
        
        papers = cursor.fetchall()
        all_papers.extend(papers)
        
        conn.close()
    
    if not all_papers:
        print("\nNo new working papers added in this run.")
        return
    
    # Sort by scraped_at and limit
    all_papers.sort(key=lambda x: x[6], reverse=True)
    all_papers = all_papers[:limit]
    
    print(f"\n{'='*140}")
    print(f"Recently Added Working Papers (showing {len(all_papers)})")
    print(f"{'='*140}")
    print(f"{'Date':<12} {'Author':<30} {'Affiliation':<25} {'Title':<50} {'Cites':<7}")
    print("-"*140)
    
    for title, author, affiliation, pub_date, location, citations, scraped_at in all_papers:
        author_short = (author[:27] + '...') if author and len(author) > 30 else (author or 'N/A')
        affiliation_short = (affiliation[:22] + '...') if affiliation and len(affiliation) > 25 else (affiliation or '')
        title_short = (title[:47] + '...') if title and len(title) > 50 else (title or 'N/A')
        print(f"{pub_date or 'N/A':<12} {author_short:<30} {affiliation_short:<25} {title_short:<50} {citations or 0:<7}")
    
    print("="*140)

def main():
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
    
    # Parse arguments
    force = '--force' in sys.argv
    
    if force:
        print("⚠️  FORCE MODE: Will update all years (2023-present)")
    else:
        print("📅 INCREMENTAL MODE: Will update only the latest year")
        print("   (Use --force to update all years)")
    
    # Determine years to work with
    journals = ['jf', 'rfs', 'jfe']
    years = get_years_to_update(force)
    
    # Ask if user wants to update papers
    print("\n" + "="*80)
    update_papers = input("📰 Update journal articles? (y/n): ").strip().lower()
    
    if update_papers == 'y':
        # Step 1: Update journal articles
        print(f"\n🔄 Years to update: {', '.join(map(str, years))}")
        
        for journal in journals:
            for year in years:
                cmd = ['python3', 'getpapers-openalex.py', journal, str(year)]
                if force:
                    cmd.append('--force')
                
                run_command(cmd, f"Updating {journal.upper()} {year}")
        
        # Count and display new papers
        print(f"\n{'='*80}")
        print("Summary: Papers Added")
        print(f"{'='*80}")
        
        new_count = count_new_papers_added(journals, years)
        print(f"\n✅ Total new papers added: {new_count}")
        
        if new_count > 0:
            display_recent_papers(journals, years)
        
        # Wait for user
        print("\n" + "="*80)
        input("📋 Press Enter to continue to author ranking...")
    else:
        print("\n⏩ Skipping journal articles update...")
        new_count = 0
    
    # Step 2: Rank authors and create top 250 list
    # Determine which journals/years to rank
    if force:
        # Use all years for ranking
        journal_arg = 'top3'
        year_arg = None  # All years
    else:
        # Use latest year only
        journal_arg = 'top3'
        year_arg = str(years[-1])
    
    # First run rank-authors to see the rankings
    cmd = ['python3', 'query_openalex_db.py', 'rank-authors', journal_arg]
    if year_arg:
        cmd.append(year_arg)
    cmd.append('--250')
    
    run_command(cmd, "Ranking Top 250 Authors")
    
    # Only create new author list CSV if papers were updated
    if update_papers == 'y':
        # Then create the author list CSV
        cmd = ['python3', 'query_openalex_db.py', 'make-author-list', journal_arg]
        if year_arg:
            cmd.append(year_arg)
        cmd.append('--250')
        
        run_command(cmd, "Generating Top 250 Author List CSV")
    
    # Wait for user
    print("\n" + "="*80)
    input("📄 Press Enter to continue to working papers update...")
    
    # Step 3: Update working papers for top 250 authors
    # Ask if user wants to update working papers
    print("\n" + "="*80)
    update_wp = input("📝 Update working papers? (y/n): ").strip().lower()
    
    wp_count = 0
    
    if update_wp == 'y':
        # Find the latest author list CSV
        pattern = os.path.join(DB_DIR, 'author_list_top3_*.csv')
        csv_files = glob.glob(pattern)
        
        if not csv_files:
            print("❌ Error: No author list CSV found!")
            sys.exit(1)
        
        latest_csv = max(csv_files, key=os.path.getmtime)
        print(f"📋 Using author list: {os.path.basename(latest_csv)}")
        
        # Use absolute path for the CSV file
        csv_abs_path = os.path.abspath(latest_csv)
        
        # Fetch working papers
        cmd = ['python3', 'get_wp.py', csv_abs_path]
        if year_arg:
            cmd.append(year_arg)
        
        run_command(cmd, "Fetching Working Papers for Top 250 Authors")
        
        # Step 4: Display new working papers
        print(f"\n{'='*80}")
        print("Summary: Working Papers Added")
        print(f"{'='*80}")
        
        wp_count = count_new_working_papers()
        print(f"\n✅ Total new working papers added: {wp_count}")
        
        if wp_count > 0:
            display_recent_working_papers()
        
        # Wait for user
        print("\n" + "="*80)
        input("📊 Press Enter to view working papers rankings...")
    else:
        print("\n⏩ Skipping working papers update...")
    
    # Display working papers ranking (if database exists)
    # Check if working papers database exists for the specified year
    if year_arg:
        wp_db = os.path.join(DB_DIR, f'working_papers_{year_arg}.db')
        if not os.path.exists(wp_db):
            # Fall back to default database
            wp_db = os.path.join(DB_DIR, 'working_papers.db')
            if os.path.exists(wp_db):
                year_arg = None  # Use default database without year
            else:
                print(f"\n⚠️  No working papers database found. Skipping rankings.")
                year_arg = None
    
    if year_arg is not None or os.path.exists(os.path.join(DB_DIR, 'working_papers.db')):
        cmd = ['python3', 'query_wp_db.py', 'rank']
        if year_arg:
            cmd.append(year_arg)
        cmd.append('--250')
        
        run_command(cmd, "Author Rankings by Working Papers")
    
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

if __name__ == "__main__":
    main()
