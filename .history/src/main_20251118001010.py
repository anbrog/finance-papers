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
    print("Update journal articles?")
    print("  • Type 'y' to update (using incremental/force mode years)")
    print("  • Type 'n' to skip update")
    print("  • Type year(s) to update specific years: 2024 or 2023-2025")
    print()
    
    while True:
        update_papers = input("Choice [y/n/years]: ").strip().lower()
        if update_papers == 'n':
            break
        elif update_papers == 'y':
            break
        else:
            # Try to parse as years
            try:
                custom_years = parse_year_input(update_papers)
                if custom_years is not None:
                    years = custom_years
                    update_papers = 'y'
                    print(f"✅ Will update years: {', '.join(map(str, years))}")
                    break
                else:
                    print("⚠️  Invalid input. Please enter 'y', 'n', or specify years (e.g., 2024 or 2023-2025)")
            except (ValueError, AttributeError):
                print("⚠️  Invalid input. Please enter 'y', 'n', or specify years (e.g., 2024 or 2023-2025)")
    
    if update_papers == 'y':
        # Step 1: Update journal articles
        print(f"\n🔄 Years to update: {', '.join(map(str, years))}")
        
        for journal in journals:
            for year in years:
                cmd = ['python3', 'getpapers_openalex.py', journal, str(year)]
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
    # Ask which years to use for ranking
    print("\n" + "="*80)
    print("📊 Author Ranking Year Selection")
    print("="*80)
    print("Choose years for author ranking:")
    print("  • Press Enter for ALL available years (default, recommended)")
    print("  • Type a single year: 2024")
    print("  • Type a range: 2023-2025")
    print("  • Type comma-separated: 2023,2024,2025")
    print()
    
    while True:
        year_input = input("Years [Enter for all]: ").strip()
        try:
            ranking_years = parse_year_input(year_input)
            break
        except (ValueError, AttributeError) as e:
            print(f"⚠️  Invalid input. Please use format: 2024 or 2023-2025 or 2023,2024")
    
    # Determine which journals/years to rank
    journal_arg = 'top3'
    
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
    while True:
        update_wp = input("📝 Update working papers? (y/n): ").strip().lower()
        if update_wp in ['y', 'n']:
            break
        print("⚠️  Please enter 'y' or 'n'")
    
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
        
        # Fetch working papers (without year filter - get all years)
        cmd = ['python3', 'get_wp.py', csv_abs_path]
        
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
    
    # Display working papers ranking (use default database for all years)
    wp_db = os.path.join(DB_DIR, 'working_papers.db')
    
    if os.path.exists(wp_db):
        cmd = ['python3', 'query_wp_db.py', 'rank', '--250']
        
        run_command(cmd, "Author Rankings by Working Papers")
    else:
        print(f"\n⚠️  No working papers database found. Skipping rankings.")
    
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
            # Git commands
            print("\nCommitting and pushing to GitHub...")
            git_commands = [
                (['git', 'add', 'docs/data/rankings.json'], "Staging changes"),
                (['git', 'commit', '-m', 'Update rankings data'], "Committing changes"),
                (['git', 'push'], "Pushing to GitHub")
            ]
            
            for cmd, desc in git_commands:
                result = run_command(cmd, desc)
                if result != 0:
                    print(f"⚠️  Git command failed: {desc}")
                    break
            else:
                print("\n✅ Static website data updated successfully!")
                print("   View at: https://anbrog.github.io/finance-papers/")
        else:
            print("⚠️  Export failed, skipping git push")

if __name__ == "__main__":
    main()
