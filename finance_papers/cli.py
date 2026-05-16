#!/usr/bin/env python3
"""CLI for finance-papers.

Usage:
    finance-papers                    # Papers from last update
    finance-papers -N                 # Peek at new papers (no save)
    finance-papers -a "Fama"          # Search papers by author
    finance-papers -t                 # Browse by topic (fzf)
    finance-papers -r -n 20           # 20 most recent papers
    finance-papers -w                 # Working papers from last update
    finance-papers update             # Update articles (current year)
    finance-papers rank               # Rank authors
    finance-papers chat               # Chat with Claude about papers
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

import shutil

from finance_papers.core import (
    # Config
    JOURNALS, JOURNAL_GROUPS, DB_DIR,
    # Operations
    update_articles, rank_authors, search_papers,
    update_working_papers, export_author_csv, read_author_csv,
    rank_by_working_papers, get_topic_counts,
    get_recent_papers, get_papers_from_last_update,
    get_last_update_date, get_previous_update_date,
    peek_new_articles, peek_new_working_papers,
    save_peek_cache, load_peek_cache, peek_cache_age, peek_cache_age_minutes,
    notify_ntfy, notify_ntfy_heartbeat,
    # Chat
    save_paper_context, load_paper_context, clear_paper_context,
    chat_with_papers, export_papers_to_file,
    # Display
    print_author_table, paginate, format_paper, display_papers,
)


def select_topic_fzf(journals: list = None, years: list = None,
                     author: str = None, title: str = None) -> str:
    """Select a topic using fzf from available topics sorted by prevalence.

    Topic counts are filtered by any provided author/title to show relevant topics.
    """
    import subprocess

    topic_counts, total_papers = get_topic_counts(journals, years, author=author, title=title)
    if not topic_counts:
        print("No topics found matching filters")
        return None

    # Format: "count | topic name" for fzf, with "all" option first
    lines = [f"{total_papers:>4} | all (no topic filter)"]
    lines.extend([f"{count:>4} | {name}" for name, count in topic_counts.items()])
    input_text = "\n".join(lines)

    # Build header showing active filters
    header = "Select topic (sorted by prevalence)"
    if author or title:
        filters = []
        if author:
            filters.append(f"author: {author}")
        if title:
            filters.append(f"title: {title}")
        header += f" - filtered by {', '.join(filters)}"

    try:
        result = subprocess.run(
            ['fzf', f'--header={header}', '--reverse'],
            input=input_text,
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            selected = result.stdout.strip()
            # "all" option returns empty string (no filter, but not cancelled)
            if selected.startswith('all') or 'no topic filter' in selected:
                return ''
            # Parse selection: "count | topic name" -> "topic name"
            return selected.split(' | ', 1)[1] if ' | ' in selected else selected
    except FileNotFoundError:
        print("fzf not found. Install with: brew install fzf")
        return None

    return None  # User cancelled


def parse_years(value: str) -> list:
    """Parse year specification: '2024', '2023-2025', '2023,2024,2025'."""
    if not value:
        return None

    years = []
    for part in value.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            years.extend(range(int(start), int(end) + 1))
        else:
            years.append(int(part))
    return years


def fzf_select(options: list, header: str = "Select an option") -> str:
    """Select from options using fzf dropdown. Returns None if cancelled."""
    import subprocess

    try:
        result = subprocess.run(
            ['fzf', '--header=' + header, '--reverse', '--height=~10', '--no-info'],
            input='\n'.join(options),
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except FileNotFoundError:
        # fzf not installed - fallback to numbered menu
        print(f"\n{header}:")
        for i, opt in enumerate(options, 1):
            print(f"  {i}. {opt}")
        try:
            choice = input(f"\nEnter number [1]: ").strip()
            if not choice:
                return options[0]
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except (ValueError, EOFError):
            pass
        return options[0]
    return None


def select_update_source() -> str:
    """Select update source interactively using fzf."""
    options = ['articles', 'working-papers', 'all']
    result = fzf_select(options, "Select what to update")
    return result if result else 'articles'


def select_journals() -> list:
    """Select journals interactively using fzf."""
    options = [
        'top3 (JF, RFS, JFE)',
        'econ5 (QJE, AER, Ecma, JPE, REStud)',
        'alltop (all 8 journals)',
        'jf',
        'rfs',
        'jfe',
    ]
    result = fzf_select(options, "Select journals")
    if result:
        # Extract the key before any parenthetical
        return [result.split()[0]]
    return ['top3']


def select_years() -> list:
    """Select years interactively using fzf."""
    current = datetime.now().year
    options = [str(y) for y in range(current, current - 21, -1)]
    result = fzf_select(options, "Select year")
    if result:
        return [int(result)]
    return [current]


def select_rank_source() -> str:
    """Select rank source: articles or working-papers."""
    options = ['articles', 'working-papers']
    result = fzf_select(options, "Rank by")
    return result if result else 'articles'


def select_rank_years() -> list:
    """Select year range for ranking."""
    current = datetime.now().year
    options = [
        f"{current}-{current}",      # Just this year
        f"{current-1}-{current}",    # Last 2 years
        f"{current-2}-{current}",    # Last 3 years
        f"{current-3}-{current}",    # Last 4 years
        f"{current-4}-{current}",    # Last 5 years
        f"{current-5}-{current}",    # Last 6 years
        f"{current-6}-{current}",    # Last 7 years
        f"{current-7}-{current}",    # 2018-current
        "all",
    ]
    result = fzf_select(options, "Select years")
    if result and result != 'all':
        start, end = result.split('-')
        return list(range(int(start), int(end) + 1))
    return None  # None means all years


def select_rank_topic(journals: list = None, years: list = None) -> str:
    """Select topic for ranking with 'all' option."""
    topic_counts, total_papers = get_topic_counts(journals, years)
    if not topic_counts:
        return None

    options = [f"{total_papers:>4} | all (no filter)"]
    options.extend([f"{count:>4} | {name}" for name, count in list(topic_counts.items())[:100]])

    result = fzf_select(options, "Select topic")
    if result and 'all (no filter)' not in result:
        return result.split(' | ', 1)[1] if ' | ' in result else result
    return None


def cmd_update(args):
    """Handle update command."""
    current_year = datetime.now().year

    if args.dropdown:
        source = select_update_source()
        if source in ('articles', 'all'):
            years = select_years()
            journals = select_journals()
        else:
            years = [current_year]
            journals = ['top3']
        update_wp = source in ('working-papers', 'all')
        update_articles_flag = source in ('articles', 'all')
    else:
        years = parse_years(args.years) if args.years else [current_year]
        journals = args.journals.split(',') if args.journals else ['top3']
        update_wp = args.working_papers
        update_articles_flag = not args.working_papers

        if update_articles_flag and not args.years and args.journals == 'top3':
            print(f"\033[90mDefaults: journals=top3, years={current_year}\033[0m")

    # Update articles
    if update_articles_flag:
        print("Updating journal articles...")
        update_articles(journals=journals, years=years, force=args.force)

    # Update working papers
    if update_wp:
        last_wp_date = get_last_update_date(source='working-papers')
        if last_wp_date:
            print(f"\n\033[90mLast working papers update: {last_wp_date}\033[0m")
        print("Updating working papers...")
        import glob
        pattern = str(DB_DIR / 'author_list_*.csv')
        csv_files = glob.glob(pattern)
        if csv_files:
            csv_file = max(csv_files, key=lambda x: Path(x).stat().st_mtime)
            authors = read_author_csv(Path(csv_file))
            update_working_papers(authors, year=years[0] if years else None,
                                 max_authors=args.limit, clean=args.clean)
            # Show working papers from this update
            wp_papers = get_papers_from_last_update(source='working-papers')
            if wp_papers:
                display_papers(papers=wp_papers, title="Working Papers from Update",
                             offer_chat=False)
        else:
            print("No author list found. Run 'finance-papers rank -o' first.")


def cmd_rank(args):
    """Handle rank command."""
    current_year = datetime.now().year

    if args.dropdown:
        source = select_rank_source()
        working_papers = (source == 'working-papers')

        if working_papers:
            journals = None
            years = None
            topic = None
        else:
            years = select_rank_years()
            journals = select_journals()
            topic = select_rank_topic(journals, years)
    else:
        journals = args.journals.split(',') if args.journals else None
        years = parse_years(args.years) if args.years else None
        working_papers = args.working_papers

        topic = None
        if hasattr(args, 'topic') and args.topic is not None:
            if args.topic == '':
                topic = select_topic_fzf(journals, years)
                if not topic:
                    return
            else:
                topic = args.topic

        if not args.journals and not args.years and not working_papers and topic is None:
            print(f"\033[90mDefaults: journals=top3, years=all, topic=all (use -d for dropdown selection)\033[0m")

    if working_papers:
        authors = rank_by_working_papers(top_n=args.top, years=years, topic=topic)
        title = "Author Rankings by Working Papers"
        if years:
            title += f" ({min(years)}-{max(years)})"
        if topic:
            title += f" (Topic: {topic})"
    else:
        all_authors = rank_authors(journals=journals, years=years, top_n=10000,
                                   by_citations=args.citations, topic=topic)

        if all_authors:
            max_papers = max(a.paper_count for a in all_authors)
            options = []
            for n in range(1, min(max_papers + 1, 20)):
                count = len([a for a in all_authors if a.paper_count >= n])
                if count > 0:
                    options.append(f"{n} ({count} authors)")

            selected = fzf_select(options, "Minimum papers")
            if selected:
                min_papers = int(selected.split()[0])
            else:
                min_papers = 1

            authors = [a for a in all_authors if a.paper_count >= min_papers]
        else:
            authors = all_authors

        sort_by = "Citations" if args.citations else "Papers"
        title = f"Author Rankings by {sort_by}"
        if years:
            title += f" ({min(years)}-{max(years)})"
        if topic:
            title += f" (Topic: {topic})"

    if args.output:
        export_author_csv(authors, Path(args.output), journals=args.journals or 'top3',
                         years=args.years, top_n=args.top)
    else:
        print_author_table(authors, title, journals=journals, years=years,
                          working_papers=working_papers, topic=topic)

        if authors and not working_papers:
            try:
                response = input(f"\nSave author list ({len(authors)} authors)? (y/n) [n]: ").strip().lower()
                if response == 'y':
                    journals_str = args.journals or 'top3'
                    export_author_csv(authors, journals=journals_str,
                                     years=args.years, top_n=len(authors))
            except (EOFError, KeyboardInterrupt):
                pass


def cmd_papers(args):
    """Handle papers command."""
    journals = args.journals.split(',') if args.journals else None
    years = parse_years(args.years) if args.years else None
    source = 'working-papers' if args.working_papers else 'articles'

    topic = None
    if hasattr(args, 'topic') and args.topic is not None:
        if args.topic == '':
            topic = select_topic_fzf(journals, years, author=args.author, title=args.title)
            if topic is None:
                return
            if topic == '':
                topic = None
        else:
            topic = args.topic

    papers = search_papers(
        author=args.author,
        title=args.title,
        journals=journals,
        years=years,
        topic=topic,
        limit=args.top,
        source=source
    )

    label = "Working Papers" if args.working_papers else "Papers"
    last_date = get_last_update_date(source=source)
    prev_date = get_previous_update_date(source=source)
    if last_date and prev_date:
        date_suffix = f" [{last_date} ← {prev_date}]"
    elif last_date:
        date_suffix = f" [{last_date}]"
    else:
        date_suffix = ""
    title = f"{label}{date_suffix}"
    if args.author:
        title += f" by {args.author}"
    elif topic:
        title += f" on '{topic}'"

    context_desc = f"{label.lower()}: {args.author or args.title or topic or 'all'}"
    print_mode = getattr(args, 'print_output', False)
    display_papers(papers=papers, title=title, context_desc=context_desc, offer_chat=True,
                   print_mode=print_mode)


def cmd_chat(args):
    """Handle chat command - start chat or export papers."""
    if args.clear:
        clear_paper_context()
        print("Paper context cleared.")
        return

    if args.export:
        output_path = Path(args.export) if args.export != 'auto' else None
        result = export_papers_to_file(output_path=output_path)
        if result:
            print(f"Papers exported to: {result}")
        else:
            print("No papers in context to export.")
        return

    if args.show:
        papers, query = load_paper_context()
        if not papers:
            print("No papers in context.")
            return
        print(f"\nPapers in context ({len(papers)}):")
        if query:
            print(f"Query: {query}\n")
        for i, p in enumerate(papers, 1):
            authors = p.authors[0] if p.authors else 'Unknown'
            if p.authors and len(p.authors) > 1:
                authors += f' et al.'
            year = p.year or (p.pub_date[:4] if p.pub_date else '')
            print(f"  {i}. {authors} ({year}): {p.title[:50]}...")
        return

    source = 'working-papers' if args.working_papers else 'articles'
    label = "working papers" if args.working_papers else "articles"

    if args.recent:
        limit = args.top or 40
        papers = get_recent_papers(source=source, limit=limit)
        context_desc = f"recent {label}"
        msg = f"Loaded {len(papers)} most recent {label}"
    else:
        papers = get_papers_from_last_update(source=source)
        if args.top:
            papers = papers[:args.top]
        context_desc = f"{label} from last update"
        msg = f"Loaded {len(papers)} {label} from last update"

    if not papers:
        print(f"No {label} in database. Run 'finance-papers update' first.")
        return

    save_paper_context(papers, context_desc)
    print(msg)

    chat_with_papers(papers, context_desc)


def cmd_notify(args):
    """Run notify_on.sh / notify_off.sh shipped alongside the package."""
    import subprocess

    script_name = f"notify_{args.action}.sh"
    # Scripts live at the repo root (one level up from the package dir).
    repo_root = Path(__file__).resolve().parent.parent
    script = repo_root / script_name
    if not script.exists():
        print(f"Error: {script} not found.", file=sys.stderr)
        sys.exit(1)
    cmd = ['bash', str(script)]
    if args.action == 'on':
        cmd.append('working-papers' if args.working_papers else 'articles')
    elif args.working_papers:
        print("Note: -w is ignored with 'notify off' (both agents are removed).",
              file=sys.stderr)
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def interactive_mode():
    """Run full workflow: update articles -> rank authors -> update working papers."""
    import glob

    current_year = datetime.now().year

    print("\n" + "=" * 60)
    print("Finance Papers - Full Workflow")
    print("=" * 60)

    # Step 1: Update journal articles
    print("\n[Step 1/4] Update Journal Articles")
    print("-" * 40)
    response = input(f"Update articles? (y/n/years) [y={current_year}]: ").strip().lower()

    if response == 'n':
        print("Skipping article update.")
    else:
        years = parse_years(response) if response and response != 'y' else [current_year]
        force = input("Force update existing? (y/n) [n]: ").strip().lower() == 'y'
        update_articles(journals=['top3'], years=years, force=force)

    # Step 2: Rank authors
    print("\n[Step 2/4] Rank Authors")
    print("-" * 40)
    top_n_str = input("Top N authors [250]: ").strip()
    top_n = int(top_n_str) if top_n_str else 250

    authors = rank_authors(top_n=top_n)
    if not authors:
        print("No authors found. Make sure articles are loaded.")
        return

    print_author_table(authors, f"Top {len(authors)} Authors")

    # Step 3: Export author list
    print("\n[Step 3/4] Export Author List")
    print("-" * 40)
    response = input("Update author list? (y/n) [y]: ").strip().lower()

    if response == 'n':
        print("Skipping author list export.")
        csv_path = None
    else:
        all_authors = rank_authors(top_n=10000)

        max_papers = max(a.paper_count for a in all_authors) if all_authors else 1
        options = []
        for n in range(1, min(max_papers + 1, 20)):
            count = len([a for a in all_authors if a.paper_count >= n])
            if count > 0:
                options.append(f"{n} ({count} authors)")

        selected = fzf_select(options, "Minimum papers")
        if selected:
            min_papers = int(selected.split()[0])
        else:
            min_papers = 1

        filtered_authors = [a for a in all_authors if a.paper_count >= min_papers]
        print(f"{len(filtered_authors)} authors have at least {min_papers} paper(s)")

        export_n_str = input(f"How many authors? [all]: ").strip().lower()
        if export_n_str == '' or export_n_str == 'all':
            export_authors = filtered_authors
        else:
            export_n = int(export_n_str)
            export_authors = filtered_authors[:export_n]

        csv_path = export_author_csv(export_authors, top_n=len(export_authors))

    # Step 4: Update working papers
    print("\n[Step 4/4] Update Working Papers")
    print("-" * 40)

    pattern = str(DB_DIR / 'author_list_*.csv')
    csv_files = glob.glob(pattern)
    if csv_files:
        latest_csv = max(csv_files, key=lambda x: Path(x).stat().st_mtime)
        wp_authors = read_author_csv(Path(latest_csv))
        print(f"Using: {Path(latest_csv).name} ({len(wp_authors)} authors)")
    else:
        print("No author list found. Using authors from Step 2.")
        wp_authors = authors

    response = input("Update working papers? (y/n/clean) [y]: ").strip().lower()

    if response == 'n':
        print("Skipping working papers update.")
    else:
        clean = response == 'clean'
        limit_str = input(f"Max authors to fetch [{len(wp_authors)} total]: ").strip()
        limit = int(limit_str) if limit_str else len(wp_authors)

        update_working_papers(wp_authors[:limit], clean=clean)

    # Final summary
    print("\n" + "=" * 60)
    print("Workflow Complete")
    print("=" * 60)

    wp_authors = rank_by_working_papers(top_n=top_n)
    if wp_authors:
        print_author_table(wp_authors, title="Ranking by Working Papers",
                          working_papers=True)

    print(f"\nData saved to: {DB_DIR}")
    print("Run 'finance-papers rank' or 'finance-papers papers -a <name>' for queries.")


def main():
    parser = argparse.ArgumentParser(
        description='Finance papers analysis tool. Without arguments, shows papers from the last update.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Journals (-j):
  Individual: jf, rfs, jfe, qje, aer, ecma, jpe, restud
  Groups:     top3 (jf,rfs,jfe), econ5, alltop

Examples:
  finance-papers                           # Papers from last update
  finance-papers -N                        # Peek at new papers (no save)
  finance-papers -r                        # 40 most recent papers
  finance-papers -r -n 20                  # 20 most recent papers
  finance-papers -w                        # Working papers from last update
  finance-papers -wr                       # Recent working papers
  finance-papers -a "Fama"                 # Papers by Fama
  finance-papers -t                        # Browse by topic (fzf)
  finance-papers -t "Asset Pricing"        # Papers on Asset Pricing
  finance-papers -a "Fama" -t              # Papers by Fama, select topic
  finance-papers -i                        # Interactive workflow mode
  finance-papers update                    # Update articles (current year)
  finance-papers update -w                 # Update working papers
  finance-papers rank -n 100               # Top 100 authors
  finance-papers rank -t                   # Rank authors, select topic
"""
    )

    # Main parser options (paper search - default mode)
    parser.add_argument('-i', '--interactive', action='store_true',
                       help='Run interactive workflow (update -> rank -> export)')
    parser.add_argument('-a', '--author', help='Filter by author name')
    parser.add_argument('--title', help='Filter by title keyword')
    parser.add_argument('-j', '--journals', help='Filter by journals')
    parser.add_argument('-y', '--years', help='Filter by years')
    parser.add_argument('-n', '--top', type=int, help='Limit results')
    parser.add_argument('-t', '--topic', nargs='?', const='', default=None,
                       help='Filter by topic (omit value for fzf selection)')
    parser.add_argument('-w', '--working-papers', action='store_true',
                       help='Search working papers instead of articles')
    parser.add_argument('-r', '--recent', action='store_true',
                       help='Show most recent papers (use -n to limit, default 40)')
    parser.add_argument('-N', '--new', action='store_true',
                       help='Peek at new papers since last update (fetches from API but does not save)')
    parser.add_argument('-q', '--quiet', action='store_true',
                       help='With -N: fetch silently and refresh cache, no display. With -N -p: print only papers not yet in the cache; cache is rewritten only if missing or >=30 min old.')
    parser.add_argument('-p', '--print', action='store_true', dest='print_output',
                       help='Print all results to stdout without pagination')
    parser.add_argument('--wet', action='store_true',
                       help='With -Nqp(w): send a heartbeat ntfy notification even when no new papers are found')

    subparsers = parser.add_subparsers(dest='command', metavar='COMMAND')

    # update
    p_update = subparsers.add_parser('update',
        help='Update articles (default) or working papers [-w] [-j JOURNALS] [-y YEARS]')
    p_update.add_argument('-w', '--working-papers', action='store_true',
                         help='Update working papers instead of articles')
    p_update.add_argument('-j', '--journals', default='top3',
                         help='Journals: jf,rfs,jfe,qje,aer,ecma,jpe,restud or groups: top3,econ5,alltop')
    p_update.add_argument('-y', '--years', help='Year(s): 2024 or 2023-2025 or 2023,2024')
    p_update.add_argument('-d', '--dropdown', action='store_true', help='Interactive dropdown selection')
    p_update.add_argument('--force', action='store_true', help='Update existing records')
    p_update.add_argument('--clean', action='store_true', help='Remove old working papers')
    p_update.add_argument('-n', '--limit', type=int, help='Limit number of authors (for working papers)')

    # rank
    p_rank = subparsers.add_parser('rank',
        help='Rank authors [-n TOP] [-j JOURNALS] [-y YEARS] [-o FILE] [--citations] [--working-papers] [-t TOPIC]')
    p_rank.add_argument('-n', '--top', type=int, default=250, help='Number of authors')
    p_rank.add_argument('-j', '--journals', help='Journals: jf,rfs,jfe,qje,aer,ecma,jpe,restud or groups: top3,econ5,alltop')
    p_rank.add_argument('-y', '--years', help='Filter by years')
    p_rank.add_argument('-o', '--output', help='Export to CSV file')
    p_rank.add_argument('-d', '--dropdown', action='store_true', help='Interactive dropdown selection')
    p_rank.add_argument('--citations', action='store_true', help='Rank by citations')
    p_rank.add_argument('-w', '--working-papers', action='store_true', help='Rank by working papers')
    p_rank.add_argument('-t', '--topic', nargs='?', const='', default=None,
                        help='Filter by topic (omit value for fzf selection)')

    # chat
    p_chat = subparsers.add_parser('chat',
        help='Chat about papers [-w working papers] [-r recent] [-n limit] [--export] [--show] [--clear]')
    p_chat.add_argument('-w', '--working-papers', action='store_true',
                        help='Chat about working papers instead of articles')
    p_chat.add_argument('-r', '--recent', action='store_true',
                        help='Chat about most recent papers (by scraped_at)')
    p_chat.add_argument('-n', '--top', type=int,
                        help='Limit number of papers (default 40 for -r)')
    p_chat.add_argument('--export', '-e', nargs='?', const='auto', metavar='FILE',
                        help='Export papers to markdown file instead of chat')
    p_chat.add_argument('--show', '-s', action='store_true', help='Show papers in context')
    p_chat.add_argument('--clear', '-c', action='store_true', help='Clear paper context')

    # notify
    p_notify = subparsers.add_parser('notify',
        help='Manage the hourly LaunchAgent that runs `finance-papers -Nqp[w]`',
        description=(
            "Install or remove macOS LaunchAgents that run finance-papers hourly "
            "and append output to ~/logs/finance-papers.log. "
            "'notify on' installs the articles agent (-Nqp). "
            "'notify on -w' installs the working-papers agent (-Nqpw) as a second, "
            "independent agent. 'notify off' removes both. All operations are idempotent."
        ))
    p_notify.add_argument('action', choices=['on', 'off'],
                          help="'on' = install LaunchAgent; 'off' = remove all LaunchAgents")
    p_notify.add_argument('-w', '--working-papers', action='store_true',
                          help="With 'on': install the working-papers agent (-Nqpw) instead of articles")

    args = parser.parse_args()

    # If subcommand specified, dispatch to handler
    if args.command is not None:
        handlers = {
            'update': cmd_update,
            'rank': cmd_rank,
            'chat': cmd_chat,
            'notify': cmd_notify,
        }
        handlers[args.command](args)
        return

    # Interactive workflow mode
    if args.interactive:
        interactive_mode()
        return

    # Peek at new papers (fetch from API, don't save)
    if args.new:
        source = 'working-papers' if args.working_papers else 'articles'
        label = "Working Papers" if args.working_papers else "Papers"
        last_date = get_last_update_date(source=source)
        since = f" [Since {last_date}]" if last_date else ""

        # Try cache first (unless --quiet forces a fresh fetch)
        papers = None
        if not args.quiet:
            papers = load_peek_cache(source)
            if papers is not None:
                age = peek_cache_age(source) or "unknown"
                title = f"New {label}{since} (cached {age})"
                if not papers:
                    print(f"No new {label.lower()} found{since} (cached {age}).")
                    return
                display_papers(papers=papers, title=title,
                               context_desc=f"new {label.lower()} (cached)",
                               offer_chat=True, print_mode=args.print_output)
                return

        # Snapshot prior cache ids (any age) before fetching, so -q -p can diff.
        prior_ids = set()
        if args.quiet and args.print_output:
            prior = load_peek_cache(source, max_age_minutes=None) or []
            prior_ids = {p.openalex_id for p in prior if p.openalex_id}

        # Fetch fresh
        if not (args.quiet and args.print_output):
            print(f"Checking for new {label.lower()}{since} (read-only, nothing saved)...")
        if args.working_papers:
            papers = peek_new_working_papers(max_authors=args.top)
        else:
            journals = args.journals.split(',') if args.journals else None
            years = parse_years(args.years) if args.years else None
            papers = peek_new_articles(journals=journals, years=years)

        # Cache write rule:
        #   - default -N / -N -q : always refresh cache (existing behaviour)
        #   - -N -q -p           : only refresh if cache is missing or >= 30 min old
        skip_cache_write = False
        if args.quiet and args.print_output:
            age = peek_cache_age_minutes(source)
            if age is not None and age < 30:
                skip_cache_write = True
        if not skip_cache_write:
            save_peek_cache(papers, source)

        if args.quiet and args.print_output:
            new_papers = [p for p in papers if not p.openalex_id or p.openalex_id not in prior_ids]
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cache_note = "cache kept" if skip_cache_write else "cache refreshed"
            print(f"[{ts}] {len(new_papers)} new {label.lower()} since last cache ({len(papers)} total fetched{since}, {cache_note}).")
            if not new_papers:
                if args.wet:
                    sent = notify_ntfy_heartbeat(label=label, since=since,
                                                 total_fetched=len(papers),
                                                 working_papers=args.working_papers)
                    print(f"[{ts}] ntfy heartbeat {'sent' if sent else 'skipped/failed'}.")
                return
            sent = notify_ntfy(new_papers, since=since,
                               working_papers=args.working_papers)
            print(f"[{ts}] ntfy: sent {sent} notifications for {len(new_papers)} new papers.")
            title = f"New {label}{since} (delta vs cache)"
            display_papers(papers=new_papers, title=title,
                           context_desc=f"new {label.lower()} (delta)",
                           offer_chat=False, print_mode=True)
            return

        if args.quiet:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            count = len(papers)
            if count:
                print(f"[{ts}] Fetched {count} new {label.lower()}{since}.")
            else:
                print(f"[{ts}] Fetched 0 new {label.lower()}{since}.")
            return

        if not papers:
            print(f"No new {label.lower()} found{since}.")
            return
        title = f"New {label}{since} (peek, not saved)"
        display_papers(papers=papers, title=title, context_desc=f"new {label.lower()} (peek)",
                       offer_chat=True, print_mode=args.print_output)
        return

    # Default: paper search mode
    has_filters = args.author or args.title or args.topic is not None
    if not has_filters:
        source = 'working-papers' if args.working_papers else 'articles'
        label = "Working Papers" if args.working_papers else "Papers"

        last_date = get_last_update_date(source=source)
        prev_date = get_previous_update_date(source=source)
        if last_date and prev_date:
            date_suffix = f" [{last_date} ← {prev_date}]"
        elif last_date:
            date_suffix = f" [{last_date}]"
        else:
            date_suffix = ""

        if args.recent:
            limit = args.top or 40
            papers = get_recent_papers(source=source, limit=limit)
            context_desc = f"recent {label.lower()}"
            title = f"Most Recent {label}{date_suffix}"
        else:
            papers = get_papers_from_last_update(source=source)
            context_desc = f"{label.lower()} from last update"
            title = f"{label} from Last Update{date_suffix}"

        if not papers:
            print(f"No {source} in database. Run 'finance-papers update' first.")
            return

        display_papers(papers=papers, title=title, context_desc=context_desc, offer_chat=True,
                       print_mode=args.print_output)
        return

    # Run paper search with the args
    cmd_papers(args)


if __name__ == '__main__':
    main()
