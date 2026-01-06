#!/usr/bin/env python3
"""Query the working papers database"""
import sqlite3
import sys
import os
import shutil

DB_DIR = '../out/data'

def getch():
    """Read a single character without waiting for Enter"""
    try:
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
    except Exception:
        # Fallback to regular input
        return input()[0] if input() else '\n'

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

    # Get terminal dimensions
    terminal_size = shutil.get_terminal_size()
    terminal_width = max(78, min(terminal_size.columns, 160))
    terminal_height = terminal_size.lines
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

    print("="*terminal_width)
    print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
    print("="*terminal_width)

    for idx, (title, author_name, affiliation, pub_date, location, wp_type, citations) in enumerate(papers, 1):
        # Truncate for display (title gets extra -1 for safety)
        author_short = (author_name[:author_width-3] + '...') if len(author_name) > author_width else author_name
        title_short = (title[:title_width-4] + '...') if title and len(title) > (title_width-1) else (title or 'N/A')

        print(f"{pub_date or 'N/A':<{date_width}} {author_short:<{author_width}} {title_short:<{title_width}} {citations or 0:<{citations_width}}")

        # Pagination
        if idx % batch_size == 0 and idx < len(papers):
            print("\n" + "-"*terminal_width)
            input(f"Showing {idx}/{len(papers)} papers. Press Enter to continue...")
            print("-"*terminal_width + "\n")
            print(f"{'Date':<{date_width}} {'Author':<{author_width}} {'Title':<{title_width}} {'Cites':<{citations_width}}")
            print("="*terminal_width)

    print("="*terminal_width)
    conn.close()

def show_author_working_papers(author_query, limit=None, year=None, mincite=None):
    """Show working papers for a specific author

    Args:
        author_query: Author name to search for
        limit: Maximum number of papers to show
        year: Year filter (single year like "2024" or range like "2023-2025")
        mincite: Minimum citations filter
    """
    db_path = os.path.join(DB_DIR, 'working_papers.db')

    if not os.path.exists(db_path):
        print(f"No working papers database found.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

    # Build citation filter
    cite_filter = ""
    cite_desc = ""
    if mincite is not None:
        cite_filter = f"AND cited_by_count >= {mincite}"
        cite_desc = f" [min {mincite} cites]"

    # Search for author (case-insensitive partial match)
    # Handle "brogger" or "broegger" -> also search for "brøgger" and "Brøgger"
    search_variants = [author_query.lower()]
    if 'brogger' in author_query.lower():
        search_variants.append(author_query.lower().replace('brogger', 'brøgger'))
        search_variants.append(author_query.lower().replace('brogger', 'Brøgger'))
    elif 'broegger' in author_query.lower():
        search_variants.append(author_query.lower().replace('broegger', 'brøgger'))
        search_variants.append(author_query.lower().replace('broegger', 'Brøgger'))

    # Build WHERE clause for all variants - search both author_name and authors_json
    author_clauses = []
    like_params = []
    for v in search_variants:
        author_clauses.append('author_name LIKE ?')
        like_params.append(f'%{v}%')
        # Also search in authors_json (contains JSON like [{"name": "...", ...}])
        author_clauses.append('authors_json LIKE ?')
        like_params.append(f'%{v}%')

    like_clauses = ' OR '.join(author_clauses)

    limit_clause = f"LIMIT {limit}" if limit else ""
    cursor.execute(f'''
        SELECT title, author_name, publication_date, primary_location, cited_by_count, doi, authors_json
        FROM working_papers
        WHERE ({like_clauses})
        {year_filter} {cite_filter}
        ORDER BY publication_date DESC
        {limit_clause}
    ''', like_params)

    papers = cursor.fetchall()
    conn.close()

    if not papers:
        print(f"\nNo working papers found for '{author_query}'{year_desc}{cite_desc}")
        print("Press any key to continue...", end='', flush=True)
        getch()
        print()
        return

    # Count unique titles
    unique_titles = len(set(p[0] for p in papers))

    # Get terminal width
    terminal_width = max(78, min(shutil.get_terminal_size().columns, 160))

    # Get unique author names found
    authors_found = list(set(p[1] for p in papers))

    print(f"\n{'='*terminal_width}")
    if len(authors_found) == 1:
        print(f"Working Papers by {authors_found[0]}{year_desc}{cite_desc} ({unique_titles} papers)")
    else:
        print(f"Working Papers matching '{author_query}'{year_desc}{cite_desc} ({unique_titles} papers)")
    print(f"{'='*terminal_width}")

    # ANSI color codes
    gray = "\033[90m"
    reset = "\033[0m"

    # Pagination - each paper takes ~2 lines (title + doi link)
    terminal_height = shutil.get_terminal_size().lines
    batch_size = max(5, (terminal_height - 6) // 2)

    # Display with pagination (space/n=next, p=prev, q=back)
    page = 0
    total_pages = (len(papers) + batch_size - 1) // batch_size

    while True:
        start_idx = page * batch_size
        end_idx = min(start_idx + batch_size, len(papers))

        # Display current page
        paper_num = 0
        prev_title = None
        # Count unique titles up to start_idx
        for i in range(start_idx):
            if papers[i][0] != prev_title:
                paper_num += 1
            prev_title = papers[i][0]

        prev_title = papers[start_idx - 1][0] if start_idx > 0 else None
        for idx in range(start_idx, end_idx):
            title, author_name, pub_date, location, citations, doi, authors_json = papers[idx]

            # Only increment paper number if title is different from previous
            is_repeat = (title == prev_title)
            if not is_repeat:
                paper_num += 1
            prev_title = title

            cites_str = f"({citations} cites) " if citations else ""
            prefix_len = 5 + 13 + len(cites_str)
            max_title_len = terminal_width - prefix_len - 3
            title_short = (title[:max_title_len] + '...') if title and len(title) > max_title_len else (title or 'N/A')

            if is_repeat:
                print(f"{gray}{paper_num:>3}. [{pub_date or 'N/A'}] {cites_str}{title_short}{reset}")
            else:
                print(f"{paper_num:>3}. [{pub_date or 'N/A'}] {cites_str}{title_short}")
            if doi:
                doi_clean = doi.replace('https://doi.org/', '') if doi.startswith('https://') else doi
                print(f"{gray}     → https://doi.org/{doi_clean}{reset}")

        # Check if we're done
        if end_idx >= len(papers) and page == 0:
            # All papers fit on one page - still wait for keypress
            print(f"\n[{end_idx}/{len(papers)}] Press any key to go back ", end='', flush=True)
            getch()
            print()
            break

        # Pagination prompt
        print(f"\n[{end_idx}/{len(papers)}] space/n=next, p=prev, q=back ", end='', flush=True)
        ch = getch()
        print()

        if ch.lower() == 'q':
            print(f"{'='*terminal_width}")
            return
        elif ch.lower() == 'p' and page > 0:
            page -= 1
            # Clear and reprint header
            print(f"\n{'='*terminal_width}")
            if len(authors_found) == 1:
                print(f"Working Papers by {authors_found[0]}{year_desc} ({unique_titles} papers)")
            else:
                print(f"Working Papers matching '{author_query}'{year_desc} ({unique_titles} papers)")
            print(f"{'='*terminal_width}")
        elif ch in (' ', 'n', 'N', '\r', '\n') and end_idx < len(papers):
            page += 1
        elif end_idx >= len(papers):
            # At the end, any key except p goes back
            break

    print(f"{'='*terminal_width}")

def rank_authors_by_wp(year=None, top_n=50, mincite=None):
    """
    Rank authors by number of working papers.

    Args:
        year (str, optional): Year filter (single year like "2024" or range like "2023-2025")
        top_n (int): Number of top authors to display
        mincite (int, optional): Minimum citations filter for papers
    """
    # Always use main database
    db_path = os.path.join(DB_DIR, 'working_papers.db')

    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        sys.exit(1)

    # Parse year range if provided
    year_filter = ""
    year_desc = ""
    cite_filter = ""
    if mincite is not None:
        cite_filter = f"AND cited_by_count >= {mincite}"
    if year:
        if '-' in year:
            # Range like "2023-2025"
            start_year, end_year = year.split('-')
            year_filter = f"AND CAST(SUBSTR(publication_date, 1, 4) AS INTEGER) BETWEEN {start_year} AND {end_year}"
            year_desc = f" ({start_year}-{end_year})"
        else:
            # Single year
            year_filter = f"AND SUBSTR(publication_date, 1, 4) = '{year}'"
            year_desc = f" ({year})"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # First check if Andreas Brøgger exists in database
    # Count distinct titles to avoid counting duplicate papers
    cursor.execute(f'''
        SELECT author_name, author_affiliation, COUNT(DISTINCT title) as wp_count,
               SUM(cited_by_count) as total_citations,
               MAX(publication_date) as latest_date
        FROM working_papers
        WHERE author_name = 'Andreas Brøgger'
        {year_filter} {cite_filter}
        GROUP BY author_name
    ''')

    andreas_result = cursor.fetchone()

    # Get latest title for Andreas if he exists
    andreas_brogger_data = None
    if andreas_result:
        cursor.execute(f'''
            SELECT title
            FROM working_papers
            WHERE author_name = 'Andreas Brøgger'
            {year_filter} {cite_filter}
            ORDER BY publication_date DESC
            LIMIT 1
        ''')
        title_result = cursor.fetchone()
        andreas_brogger_data = andreas_result + (title_result[0] if title_result else '',)

    cursor.execute(f'''
        SELECT counts.author_name,
               latest_paper.author_affiliation,
               counts.wp_count,
               counts.total_citations,
               latest_paper.publication_date as latest_date,
               latest_paper.title as latest_title
        FROM (
            SELECT author_name,
                   COUNT(DISTINCT title) as wp_count,
                   SUM(cited_by_count) as total_citations
            FROM working_papers
            WHERE 1=1 {year_filter} {cite_filter}
            GROUP BY author_name
        ) counts
        INNER JOIN (
            SELECT author_name, author_affiliation, publication_date, title
            FROM (
                SELECT wp.author_name, wp.author_affiliation, wp.publication_date, wp.title,
                       ROW_NUMBER() OVER (PARTITION BY wp.author_name ORDER BY wp.publication_date DESC, wp.openalex_id) as rn
                FROM working_papers wp
                WHERE 1=1 {year_filter} {cite_filter}
            )
            WHERE rn = 1
        ) latest_paper ON counts.author_name = latest_paper.author_name
        ORDER BY counts.wp_count DESC, counts.total_citations DESC
    ''')

    ranked = cursor.fetchall()
    conn.close()

    # If Andreas Brøgger not in results but exists in DB, add him
    # If he doesn't exist at all, add him with 0
    andreas_in_ranked = any(row[0] == 'Andreas Brøgger' for row in ranked)
    if not andreas_in_ranked:
        if andreas_brogger_data:
            ranked = list(ranked) + [andreas_brogger_data]
        else:
            # Add Andreas with 0 papers
            ranked = list(ranked) + [('Andreas Brøgger', None, 0, 0, None, 'N/A')]
    
    if not ranked:
        print("No working papers found.")
        return

    print(f"\nAuthor Rankings by Working Papers{year_desc} (Total: {len(ranked)} authors)")
    print("(Type author name at pagination prompt to see their papers)\n")

    # Get terminal dimensions
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

    # Calculate variable column widths based on remaining space
    remaining = terminal_width - fixed_width - 6  # 6 spaces between 7 columns

    # Allocate remaining space: 30% author, 15% affiliation, 55% title
    author_width = max(20, int(remaining * 0.30))
    affiliation_width = max(12, int(remaining * 0.15))
    title_width = remaining - author_width - affiliation_width

    def print_header():
        print("="*terminal_width)
        print(f"{'Rank':<{rank_width}} {'#':<{papers_width}} {'Cites':<{citations_width}} {'Author':<{author_width}} {'Affiliation':<{affiliation_width}} {'Date':<{date_width}} {'Latest Title':<{title_width}}")
        print("="*terminal_width)

    print_header()

    # Track if Andreas Brøgger appears in top N
    andreas_brogger_in_top_n = False
    andreas_brogger_rank = None
    andreas_brogger_row = None

    # First pass to find Andreas Brøgger
    for rank, row in enumerate(ranked, 1):
        author_name = row[0]
        if author_name == 'Andreas Brøgger':
            andreas_brogger_rank = rank
            andreas_brogger_row = row
            if rank <= top_n:
                andreas_brogger_in_top_n = True
            break

    # Display loop with ability to go back
    display_rank = 0
    batch_start = 0
    while display_rank < min(len(ranked), top_n):
        # Track batch start for returning after author lookup
        if display_rank % batch_size == 0:
            batch_start = display_rank

        rank = display_rank + 1
        row = ranked[display_rank]
        author_name, affiliation, wp_count, citations, latest_date, latest_title = row

        author_short = (author_name[:author_width-3] + '...') if len(author_name) > author_width else author_name
        affil_short = (affiliation[:affiliation_width-3] + '...') if affiliation and len(affiliation) > affiliation_width else (affiliation or '')
        title_short = (latest_title[:title_width-4] + '...') if latest_title and len(latest_title) > (title_width-1) else (latest_title or 'N/A')

        # Highlight Andreas Brøgger in light blue
        if author_name == 'Andreas Brøgger':
            print(f"\033[94m{rank:<{rank_width}} {wp_count:<{papers_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}\033[0m")
        else:
            print(f"{rank:<{rank_width}} {wp_count:<{papers_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}")

        display_rank += 1

        # Pagination with author lookup
        if display_rank % batch_size == 0 and display_rank < min(len(ranked), top_n):
            print("\n" + "-"*terminal_width)
            user_input = input(f"[{display_rank}/{min(len(ranked), top_n)}] Enter to continue, or type author name: ").strip()

            if user_input:
                show_author_working_papers(user_input, year=year, mincite=mincite)
                # Go back to start of current batch
                display_rank = batch_start

            print("-"*terminal_width + "\n")
            print_header()

    # Show how many more authors exist
    remaining = len(ranked) - top_n
    if remaining > 0 and not andreas_brogger_in_top_n:
        print(f"\n... and {remaining - 1} more authors")
    elif remaining > 0:
        print(f"\n... and {remaining} more authors")

    # Always display Andreas Brøgger at the end if not in top N
    if not andreas_brogger_in_top_n and andreas_brogger_rank and andreas_brogger_row:
        print()  # Blank line before Andreas Brøgger
        author_name, affiliation, wp_count, citations, latest_date, latest_title = andreas_brogger_row
        author_short = 'Andreas Brøgger'
        affil_short = (affiliation[:affiliation_width-3] + '...') if affiliation and len(affiliation) > affiliation_width else (affiliation or '')
        title_short = (latest_title[:title_width-4] + '...') if latest_title and len(latest_title) > (title_width-1) else (latest_title or 'N/A')

        print(f"\033[94m{andreas_brogger_rank:<{rank_width}} {wp_count:<{papers_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}\033[0m")

    print("="*terminal_width)

    # Allow author lookup at the end
    while True:
        print("\n" + "-"*terminal_width)
        user_input = input("Type author name to see papers (or Enter to exit): ").strip()
        if not user_input:
            break
        show_author_working_papers(user_input, year=year, mincite=mincite)
        print_header()
        # Re-display last batch
        start = max(0, min(len(ranked), top_n) - batch_size)
        for i in range(start, min(len(ranked), top_n)):
            row = ranked[i]
            author_name, affiliation, wp_count, citations, latest_date, latest_title = row
            author_short = (author_name[:author_width-3] + '...') if len(author_name) > author_width else author_name
            affil_short = (affiliation[:affiliation_width-3] + '...') if affiliation and len(affiliation) > affiliation_width else (affiliation or '')
            title_short = (latest_title[:title_width-4] + '...') if latest_title and len(latest_title) > (title_width-1) else (latest_title or 'N/A')
            rank = i + 1
            if author_name == 'Andreas Brøgger':
                print(f"\033[94m{rank:<{rank_width}} {wp_count:<{papers_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}\033[0m")
            else:
                print(f"{rank:<{rank_width}} {wp_count:<{papers_width}} {citations or 0:<{citations_width}} {author_short:<{author_width}} {affil_short:<{affiliation_width}} {latest_date or 'N/A':<{date_width}} {title_short:<{title_width}}")
        print("="*terminal_width)

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
    mincite = None

    for arg in sys.argv[2:]:
        if arg.startswith('--author='):
            author = arg.split('=', 1)[1].strip("'\"")
        elif arg.startswith('--mincite='):
            try:
                mincite = int(arg.split('=', 1)[1])
            except ValueError:
                print(f"Invalid --mincite value: {arg}")
                sys.exit(1)
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
        rank_authors_by_wp(year=year, top_n=limit, mincite=mincite)
    else:
        print(f"Unknown command: {command}")
        print("Available commands: list, rank")
        sys.exit(1)

if __name__ == "__main__":
    main()
