#!/usr/bin/env python3
"""
Classify the research agenda of a specific author using machine learning.
Uses paper titles and abstracts to identify their primary research themes.
"""

import sqlite3
import json
import sys
import os
from collections import Counter

# Try to import ML libraries
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import LatentDirichletAllocation
    import numpy as np
except ImportError:
    print("Error: scikit-learn not installed")
    print("Install with: pip install scikit-learn numpy")
    sys.exit(1)

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

def extract_keywords(text, n_keywords=20):
    """
    Extract top keywords from text using TF-IDF.
    
    Args:
        text (str): Combined text from papers
        n_keywords (int): Number of keywords to extract
    
    Returns:
        list: Top keywords
    """
    if not text.strip():
        return []
    
    vectorizer = TfidfVectorizer(
        max_features=n_keywords,
        stop_words='english',
        ngram_range=(1, 3),
        min_df=1
    )
    
    try:
        tfidf_matrix = vectorizer.fit_transform([text])
        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf_matrix.toarray()[0]
        
        # Sort by score
        keyword_scores = [(feature_names[i], scores[i]) for i in range(len(feature_names))]
        keyword_scores.sort(key=lambda x: x[1], reverse=True)
        
        return [kw for kw, score in keyword_scores]
    except:
        return []

def classify_research_agenda(author_name, journals='top3', year=None):
    """
    Classify the research agenda for a specific author.
    
    Args:
        author_name (str): Author name to classify
        journals (str): Journal filter
        year (str, optional): Year filter
    
    Returns:
        dict: Research agenda classification with keywords and themes
    """
    print(f"\n{'='*80}")
    print(f"Classifying Research Agenda: {author_name}")
    print(f"{'='*80}\n")
    
    # Get papers
    print("Step 1: Fetching papers...")
    papers = get_author_papers(author_name, journals=journals, year=year)
    
    if not papers:
        print(f"No papers found for {author_name}")
        return None
    
    print(f"Found {len(papers)} papers")
    
    papers_with_abstracts = sum(1 for p in papers if p['abstract'])
    print(f"Papers with abstracts: {papers_with_abstracts}/{len(papers)} ({100*papers_with_abstracts/len(papers):.1f}%)")
    
    # Calculate citation metrics
    total_citations = sum(p['cited_by_count'] for p in papers)
    avg_citations = total_citations / len(papers) if papers else 0
    max_citations = max((p['cited_by_count'] for p in papers), default=0)
    
    print(f"Total citations: {total_citations}")
    print(f"Average citations per paper: {avg_citations:.1f}")
    print(f"Most cited paper: {max_citations} citations")
    
    # Combine text
    print("\nStep 2: Analyzing paper content...")
    titles_text = ' '.join([p['title'] for p in papers if p['title']])
    abstracts_text = ' '.join([p['abstract'] for p in papers if p['abstract']])
    
    # Weight abstracts more heavily
    combined_text = titles_text + ' ' + abstracts_text + ' ' + abstracts_text
    
    # Extract keywords
    print("\nStep 3: Extracting key research themes...")
    keywords = extract_keywords(combined_text, n_keywords=30)
    
    if not keywords:
        print("Could not extract keywords from papers")
        return None
    
    # Infer research themes
    themes = infer_research_themes(keywords)
    
    # Print results
    print("\n" + "="*80)
    print("Research Agenda Classification")
    print("="*80)
    
    print(f"\nAuthor: {author_name}")
    print(f"Papers: {len(papers)}")
    print(f"Total Citations: {total_citations}")
    print(f"Average Citations: {avg_citations:.1f}")
    
    print(f"\nPrimary Research Themes:")
    for i, theme in enumerate(themes[:3], 1):
        print(f"  {i}. {theme}")
    
    print(f"\nTop Keywords:")
    for i, keyword in enumerate(keywords[:15], 1):
        print(f"  {i:2d}. {keyword}")
    
    print(f"\nRecent Papers:")
    # Sort by date and show top 5
    recent_papers = sorted(papers, key=lambda p: p['publication_date'], reverse=True)[:5]
    for paper in recent_papers:
        date = paper['publication_date'][:10] if paper['publication_date'] else 'N/A'
        citations = paper['cited_by_count']
        title = paper['title'][:70] + '...' if len(paper['title']) > 70 else paper['title']
        print(f"  [{date}] {title} ({citations} cites)")
    
    print("\n" + "="*80)
    
    return {
        'author': author_name,
        'paper_count': len(papers),
        'papers_with_abstracts': papers_with_abstracts,
        'total_citations': total_citations,
        'avg_citations': avg_citations,
        'max_citations': max_citations,
        'themes': themes,
        'keywords': keywords,
        'recent_papers': [
            {
                'title': p['title'],
                'date': p['publication_date'],
                'citations': p['cited_by_count']
            }
            for p in recent_papers
        ]
    }

def infer_research_themes(keywords):
    """
    Infer research themes from keywords.
    
    Args:
        keywords (list): List of keywords
    
    Returns:
        list: Inferred research themes
    """
    keywords_lower = [k.lower() for k in keywords]
    
    # Define theme patterns with scores
    theme_patterns = {
        'Asset Pricing & Equity Markets': [
            'return', 'risk', 'premium', 'price', 'asset', 'equity', 'stock', 
            'portfolio', 'factor', 'expected return', 'market return', 'anomaly'
        ],
        'Corporate Finance & Investment': [
            'firm', 'corporate', 'investment', 'dividend', 'capital structure', 
            'leverage', 'financing', 'cash flow', 'corporate investment', 'capital'
        ],
        'Banking & Financial Institutions': [
            'bank', 'credit', 'loan', 'lending', 'financial institution', 
            'commercial bank', 'deposit', 'banking'
        ],
        'Market Microstructure & Trading': [
            'liquidity', 'trading', 'market', 'bid', 'spread', 'order', 
            'high frequency', 'market maker', 'transaction'
        ],
        'Behavioral Finance & Investor Psychology': [
            'investor', 'sentiment', 'bias', 'behavioral', 'retail', 'attention',
            'investor sentiment', 'behavioral bias', 'perception'
        ],
        'ESG & Climate Finance': [
            'esg', 'climate', 'green', 'sustainable', 'environmental', 'carbon',
            'climate risk', 'sustainability', 'social'
        ],
        'Fintech & Financial Innovation': [
            'fintech', 'technology', 'digital', 'blockchain', 'crypto', 
            'innovation', 'cryptocurrency', 'algorithmic'
        ],
        'International Finance & Exchange Rates': [
            'international', 'global', 'exchange rate', 'country', 'foreign',
            'currency', 'cross country', 'emerging market'
        ],
        'Household Finance & Consumer Behavior': [
            'household', 'consumer', 'mortgage', 'saving', 'retirement', 
            'financial advice', 'personal finance', 'wealth'
        ],
        'Derivatives & Options': [
            'option', 'derivative', 'futures', 'volatility', 'hedging',
            'option pricing', 'swap', 'implied volatility'
        ],
        'Monetary Policy & Macroeconomics': [
            'monetary', 'policy', 'federal reserve', 'interest rate', 'inflation',
            'macro', 'central bank', 'monetary policy', 'bond'
        ],
        'Private Equity & Venture Capital': [
            'private equity', 'venture capital', 'vc', 'startup', 'ipo',
            'private market', 'buyout'
        ],
        'Real Estate Finance': [
            'real estate', 'housing', 'mortgage', 'property', 'housing market',
            'home', 'residential'
        ],
        'Risk Management & Regulation': [
            'risk management', 'regulation', 'regulatory', 'compliance',
            'systemic risk', 'financial stability', 'stress test'
        ]
    }
    
    # Score each theme
    theme_scores = {}
    for theme, patterns in theme_patterns.items():
        score = 0
        matched_keywords = []
        for kw in keywords_lower:
            for pattern in patterns:
                if pattern in kw:
                    score += 1
                    matched_keywords.append(kw)
                    break
        
        if score > 0:
            theme_scores[theme] = (score, matched_keywords)
    
    # Sort by score
    sorted_themes = sorted(theme_scores.items(), key=lambda x: x[1][0], reverse=True)
    
    # Return top themes
    result = []
    for theme, (score, matched) in sorted_themes:
        result.append(theme)
    
    # If no matches, create generic theme from top keywords
    if not result:
        result.append(' & '.join(keywords[:2]).title() + ' Research')
    
    return result

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 classify_author_research_agenda.py <author_name> [journal] [year]")
        print("\nExamples:")
        print("  python3 classify_author_research_agenda.py 'Bryan Kelly'")
        print("  python3 classify_author_research_agenda.py 'Bryan Kelly' top3")
        print("  python3 classify_author_research_agenda.py 'Bryan Kelly' top3 2024")
        print("\nThis will classify the research agenda for the specified author")
        sys.exit(1)
    
    author_name = sys.argv[1]
    journals = sys.argv[2] if len(sys.argv) > 2 else 'top3'
    year = sys.argv[3] if len(sys.argv) > 3 else None
    
    result = classify_research_agenda(author_name, journals=journals, year=year)
    
    if result:
        # Save to JSON
        output_dir = '../out/data'
        os.makedirs(output_dir, exist_ok=True)
        
        safe_name = author_name.replace(' ', '_').replace('.', '')
        year_suffix = f"_{year}" if year else ""
        output_file = os.path.join(output_dir, f'research_agenda_{safe_name}{year_suffix}.json')
        
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"\n✅ Results saved to: {output_file}")

if __name__ == "__main__":
    main()
