#!/usr/bin/env python3
"""
Extract research agendas for top 250 authors using LLM.
For each author, uses their papers to identify their primary research agenda.
"""

import sqlite3
import json
import sys
import os
import shutil
from collections import defaultdict
from datetime import datetime
import time

DB_DIR = '../out/data'

def get_author_papers(author_name, journals=None, year=None):
    """
    Get all papers for a specific author from databases.
    
    Args:
        author_name (str): Author name to search for
        journals (str, optional): Journal code or 'top3'
        year (str, optional): Year filter
    
    Returns:
        list: Papers with title, abstract, publication_date, cited_by_count
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
        default_db = os.path.join(DB_DIR, 'openalex_articles.db')
        if os.path.exists(default_db):
            db_files.append(default_db)
    
    papers = []
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT title, abstract, publication_date, cited_by_count, authors_json
            FROM openalex_articles
        ''')
        
        articles = cursor.fetchall()
        conn.close()
        
        for title, abstract, pub_date, citations, authors_json in articles:
            authors = json.loads(authors_json)
            # Check if author is in this paper
            for author in authors:
                if author.get('name') and author_name.lower() in author['name'].lower():
                    papers.append({
                        'title': title or '',
                        'abstract': abstract or '',
                        'publication_date': pub_date or '',
                        'cited_by_count': citations or 0
                    })
                    break
    
    return papers

def get_top_authors(journals='top3', year=None, top_n=250):
    """
    Get list of top N authors by publication count.
    
    Args:
        journals (str): Journal filter
        year (str, optional): Year filter
        top_n (int): Number of authors to return
    
    Returns:
        list: Tuples of (author_name, paper_count, total_citations)
    """
    import glob
    
    if journals == 'top3':
        journal_codes = ['jf', 'rfs', 'jfe']
    elif journals:
        journal_codes = [journals]
    else:
        journal_codes = None
    
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
    
    author_counts = {}
    author_citations = {}
    
    for db_file in db_files:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        cursor.execute('SELECT authors_json, cited_by_count FROM openalex_articles')
        articles = cursor.fetchall()
        
        for (authors_json, cited_by_count) in articles:
            authors = json.loads(authors_json)
            citations = cited_by_count or 0
            for author in authors:
                name = author.get('name')
                if name:
                    if name not in author_counts:
                        author_counts[name] = 0
                        author_citations[name] = 0
                    author_counts[name] += 1
                    author_citations[name] += citations
        
        conn.close()
    
    # Sort by count (descending), then by citations
    ranked = sorted(author_counts.items(), key=lambda x: (x[1], author_citations[x[0]]), reverse=True)
    
    return [(name, count, author_citations[name]) for name, count in ranked[:top_n]]

def call_llm_for_research_agenda(author_name, papers):
    """
    Call LLM to determine research agenda from papers.
    
    Args:
        author_name (str): Author name
        papers (list): List of paper dictionaries
    
    Returns:
        str: Research agenda description
    """
    # Prepare paper summaries for LLM
    paper_texts = []
    for i, paper in enumerate(papers[:10], 1):  # Use up to 10 most recent papers
        title = paper['title']
        abstract = paper['abstract'][:500] if paper['abstract'] else "No abstract available"
        citations = paper['cited_by_count']
        paper_texts.append(f"{i}. Title: {title}\n   Abstract: {abstract}\n   Citations: {citations}")
    
    papers_summary = "\n\n".join(paper_texts)
    
    # Create prompt for LLM
    prompt = f"""Based on the following research papers by {author_name}, identify their primary research agenda in 3-5 words.

The research agenda should be a concise description of their main research focus area in finance.

Examples of good research agendas:
- "Market Anomalies' influence on Asset Pricing"
- "Corporate Finance related to Investment"
- "Credit Markets interactions with Banking"
- "ESG and Climate Finance"
- "Digital Assets important for Fintech"
- "Behavior important for Household Finance"
- "Trading's effects on Market Microstructure"

Be specific on how the fields or areas interact.

Papers:
{papers_summary}

Primary Research Agenda (3-5 words):"""

    try:
        # Try to use OpenAI API
        import openai
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("\nWarning: OPENAI_API_KEY not found in environment")
            print("To set it, use: export OPENAI_API_KEY='your-api-key-here'")
            print("Falling back to keyword-based classification")
            return fallback_research_agenda(papers)
        
        client = openai.OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # or "gpt-3.5-turbo" for faster/cheaper
            messages=[
                {"role": "system", "content": "You are a finance research expert who identifies research agendas from academic papers. Respond with only the research agenda in 3-5 words, nothing else."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=20
        )
        
        agenda = response.choices[0].message.content.strip()
        # Remove quotes if present
        agenda = agenda.strip('"').strip("'")
        return agenda
        
    except ImportError:
        print("\nWarning: openai package not installed")
        print("Install with: pip install openai")
        print("Falling back to keyword-based classification")
        return fallback_research_agenda(papers)
    except Exception as e:
        print(f"\nWarning: LLM API call failed: {e}")
        print("Falling back to keyword-based classification")
        return fallback_research_agenda(papers)

def fallback_research_agenda(papers):
    """
    Fallback method using keyword matching if LLM unavailable.
    
    Args:
        papers (list): List of paper dictionaries
    
    Returns:
        str: Research agenda description
    """
    # Combine all titles and abstracts
    text = ' '.join([p['title'] + ' ' + p['abstract'] for p in papers]).lower()
    
    # Define agenda patterns
    agendas = {
        'Asset Pricing and Returns': ['return', 'risk premium', 'asset pricing', 'equity premium', 'expected return', 'factor'],
        'Corporate Finance and Investment': ['corporate', 'firm', 'investment', 'capital structure', 'dividend', 'financing'],
        'Banking and Credit Markets': ['bank', 'credit', 'loan', 'lending', 'financial institution'],
        'ESG and Climate Finance': ['esg', 'climate', 'green', 'sustainable', 'environmental', 'carbon'],
        'Market Microstructure and Trading': ['liquidity', 'trading', 'market microstructure', 'bid', 'spread', 'high frequency'],
        'Behavioral Finance': ['investor sentiment', 'behavioral', 'bias', 'retail investor', 'attention'],
        'Fintech and Innovation': ['fintech', 'technology', 'digital', 'blockchain', 'crypto', 'innovation'],
        'International Finance': ['international', 'exchange rate', 'currency', 'global', 'cross country'],
        'Household Finance': ['household', 'consumer', 'mortgage', 'retirement', 'saving', 'personal finance'],
        'Derivatives and Options': ['option', 'derivative', 'volatility', 'futures', 'hedging'],
        'Monetary Policy and Macro': ['monetary policy', 'federal reserve', 'interest rate', 'inflation', 'central bank'],
        'Private Equity and VC': ['private equity', 'venture capital', 'startup', 'ipo', 'vc'],
        'Real Estate Finance': ['real estate', 'housing', 'property', 'mortgage market'],
        'Risk Management': ['risk management', 'systemic risk', 'financial stability', 'regulation']
    }
    
    # Score each agenda
    scores = {}
    for agenda, keywords in agendas.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scores[agenda] = score
    
    if scores:
        return max(scores.items(), key=lambda x: x[1])[0]
    else:
        return "Finance Research"

def extract_research_agendas(top_n=250, journals='top3', year=None, use_llm=True):
    """
    Extract research agendas for top N authors.
    
    Args:
        top_n (int): Number of top authors to analyze
        journals (str): Journal filter
        year (str, optional): Year filter
        use_llm (bool): Whether to use LLM (requires OpenAI API key)
    
    Returns:
        dict: Mapping of author names to research agendas
    """
    print(f"\n{'='*80}")
    print(f"Extracting Research Agendas for Top {top_n} Authors")
    if use_llm:
        print("Using LLM (OpenAI GPT) for agenda identification")
    else:
        print("Using keyword-based classification")
    print(f"{'='*80}\n")
    
    # Get top authors
    print("Step 1: Fetching top authors...")
    top_authors = get_top_authors(journals=journals, year=year, top_n=top_n)
    print(f"Found {len(top_authors)} authors")
    
    # Extract agenda for each author
    print(f"\nStep 2: Extracting research agendas...")
    author_agendas = {}
    
    for i, (author_name, paper_count, total_citations) in enumerate(top_authors, 1):
        print(f"[{i}/{len(top_authors)}] {author_name}...", end=' ', flush=True)
        
        papers = get_author_papers(author_name, journals=journals, year=year)
        
        if papers:
            # Sort by date (most recent first)
            papers.sort(key=lambda p: p['publication_date'], reverse=True)
            
            if use_llm:
                agenda = call_llm_for_research_agenda(author_name, papers)
            else:
                agenda = fallback_research_agenda(papers)
            
            # Get latest paper title
            latest_paper = papers[0]['title'] if papers else ''
            
            author_agendas[author_name] = {
                'research_agenda': agenda,
                'paper_count': paper_count,
                'total_citations': total_citations,
                'papers_with_abstracts': sum(1 for p in papers if p['abstract']),
                'latest_paper': latest_paper
            }
            
            print(f"{agenda}")
            
            # Rate limit for API calls
            if use_llm and i < len(top_authors):
                time.sleep(0.5)  # Small delay to avoid rate limits
        else:
            print("No papers found")
    
    # Group by research agenda
    print("\n" + "="*80)
    print("Research Agendas Summary")
    print("="*80)
    
    agenda_groups = defaultdict(list)
    for author, info in author_agendas.items():
        agenda_groups[info['research_agenda']].append((author, info))
    
    # Sort agendas by total citations
    sorted_agendas = sorted(agenda_groups.items(), 
                           key=lambda x: sum(info['total_citations'] for _, info in x[1]), 
                           reverse=True)
    
    for agenda, authors in sorted_agendas:
        total_cites = sum(info['total_citations'] for _, info in authors)
        total_papers = sum(info['paper_count'] for _, info in authors)
        print(f"\n{agenda} ({len(authors)} authors, {total_papers} papers, {total_cites} citations)")
        print("-" * 80)
        
        # Sort authors by paper count
        authors.sort(key=lambda x: x[1]['paper_count'], reverse=True)
        
        for author, info in authors[:5]:  # Show top 5 per agenda
            print(f"  {author:45} Papers: {info['paper_count']:2d}  Citations: {info['total_citations']:4.0f}")
    
    print("\n" + "="*80)
    
    return author_agendas

def display_saved_results(top_n=250, journals='top3', year=None, use_llm=True):
    """
    Display previously saved research agenda results.
    
    Args:
        top_n (int): Number of top authors
        journals (str): Journal filter
        year (str, optional): Year filter
        use_llm (bool): Whether LLM was used
    
    Returns:
        dict: Loaded author agendas or None if not found
    """
    output_dir = '../out/data'
    year_suffix = f"_{year}" if year else ""
    llm_suffix = "_llm" if use_llm else "_keywords"
    output_file = os.path.join(output_dir, f'author_research_agendas_{journals}{year_suffix}_top{top_n}{llm_suffix}.json')
    
    if not os.path.exists(output_file):
        print(f"Error: Results file not found: {output_file}")
        print("\nRun without --display flag to compute results first.")
        return None
    
    print(f"\n{'='*80}")
    print(f"Loading Saved Research Agendas")
    print(f"File: {os.path.basename(output_file)}")
    print(f"{'='*80}\n")
    
    with open(output_file, 'r') as f:
        author_agendas = json.load(f)
    
    print(f"Loaded results for {len(author_agendas)} authors\n")
    
    # Display as table
    print("="*140)
    print(f"{'Rank':<6} {'Author':<40} {'Papers':<8} {'Citations':<11} {'Research Agenda':<70}")
    print("="*140)
    
    # Sort by paper count
    sorted_authors = sorted(author_agendas.items(), 
                           key=lambda x: x[1]['paper_count'], 
                           reverse=True)
    
    # Get terminal height (subtract 6 for header, footer, prompt, and margin)
    terminal_height = shutil.get_terminal_size().lines
    batch_size = max(10, terminal_height - 6)
    
    # Display in batches that fit the terminal
    for i, (rank, (author, info)) in enumerate(enumerate(sorted_authors, 1), 1):
        author_short = (author[:37] + '...') if len(author) > 40 else author
        agenda = info['research_agenda']
        agenda_short = (agenda[:67] + '...') if len(agenda) > 70 else agenda
        
        print(f"{rank:<6} {author_short:<40} {info['paper_count']:<8} {info['total_citations']:<11.0f} {agenda_short:<70}")
        
        # Wait for user after each batch (except the last batch)
        if i % batch_size == 0 and i < len(sorted_authors):
            print("\n" + "-"*140)
            input(f"Showing {i}/{len(sorted_authors)} authors. Press Enter to continue...")
            print("-"*140 + "\n")
            # Repeat table heading
            print("="*140)
            print(f"{'Rank':<6} {'Author':<40} {'Papers':<8} {'Citations':<11} {'Research Agenda':<70}")
            print("="*140)
    
    print("="*140)
    
    # Group by research agenda
    print("\n" + "="*80)
    print("Research Agendas Summary")
    print("="*80)
    
    agenda_groups = defaultdict(list)
    for author, info in author_agendas.items():
        agenda_groups[info['research_agenda']].append((author, info))
    
    # Sort agendas by total citations
    sorted_agendas = sorted(agenda_groups.items(), 
                           key=lambda x: sum(info['total_citations'] for _, info in x[1]), 
                           reverse=True)
    
    for agenda, authors in sorted_agendas:
        total_cites = sum(info['total_citations'] for _, info in authors)
        total_papers = sum(info['paper_count'] for _, info in authors)
        print(f"\n{agenda} ({len(authors)} authors, {total_papers} papers, {total_cites} citations)")
        print("-" * 80)
        
        # Sort authors by paper count
        authors.sort(key=lambda x: x[1]['paper_count'], reverse=True)
        
        for author, info in authors[:5]:  # Show top 5 per agenda
            print(f"  {author:45} Papers: {info['paper_count']:2d}  Citations: {info['total_citations']:4.0f}")
    
    print("\n" + "="*80)
    
    return author_agendas

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 extract_research_agendas.py <top_n> [journal] [year] [--no-llm] [--display]")
        print("\nExamples:")
        print("  python3 extract_research_agendas.py 250")
        print("  python3 extract_research_agendas.py 250 top3 2024")
        print("  python3 extract_research_agendas.py 250 top3 2024 --no-llm")
        print("  python3 extract_research_agendas.py 250 --display        # Show saved results")
        print("\nRequires OPENAI_API_KEY environment variable for LLM mode")
        print("Use --no-llm flag to use keyword-based classification instead")
        print("Use --display flag to show previously saved results without recomputing")
        sys.exit(1)
    
    top_n = int(sys.argv[1])
    journals = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else 'top3'
    year = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else None
    use_llm = '--no-llm' not in sys.argv
    display_only = '--display' in sys.argv
    
    if display_only:
        # Just display saved results
        author_agendas = display_saved_results(top_n=top_n, journals=journals, year=year, use_llm=use_llm)
        if author_agendas is None:
            sys.exit(1)
        return
    
    # Compute new results
    author_agendas = extract_research_agendas(
        top_n=top_n,
        journals=journals,
        year=year,
        use_llm=use_llm
    )
    
    # Save results to JSON
    output_dir = '../out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    year_suffix = f"_{year}" if year else ""
    llm_suffix = "_llm" if use_llm else "_keywords"
    output_file = os.path.join(output_dir, f'author_research_agendas_{journals}{year_suffix}_top{top_n}{llm_suffix}.json')
    
    with open(output_file, 'w') as f:
        json.dump(author_agendas, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_file}")
    
    # Also save as CSV for easy viewing
    import csv
    csv_file = output_file.replace('.json', '.csv')
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Rank', 'Author', 'Research Agenda', 'Papers', 'Citations'])
        for i, (author, info) in enumerate(sorted(author_agendas.items(), 
                                                   key=lambda x: x[1]['paper_count'], 
                                                   reverse=True), 1):
            writer.writerow([
                i,
                author,
                info['research_agenda'],
                info['paper_count'],
                info['total_citations']
            ])
    
    print(f"✅ CSV saved to: {csv_file}")

if __name__ == "__main__":
    main()
