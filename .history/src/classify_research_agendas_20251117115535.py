#!/usr/bin/env python3
"""
Classify research agendas of top finance authors using machine learning.
Uses paper titles, abstracts, and citation patterns to identify research themes.
"""

import sqlite3
import json
import sys
import os
from collections import defaultdict
import numpy as np

# Try to import ML libraries
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.decomposition import LatentDirichletAllocation, NMF
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
except ImportError:
    print("Error: scikit-learn not installed")
    print("Install with: pip install scikit-learn")
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

def classify_research_agendas(top_n=250, journals='top3', year=None, n_topics=10):
    """
    Classify research agendas for top N authors using topic modeling.
    
    Args:
        top_n (int): Number of top authors to analyze
        journals (str): Journal filter
        year (str, optional): Year filter
        n_topics (int): Number of research topics to identify
    
    Returns:
        dict: Mapping of author names to research agenda classifications
    """
    print(f"\n{'='*80}")
    print(f"Classifying Research Agendas for Top {top_n} Authors")
    print(f"{'='*80}\n")
    
    # Get top authors
    print("Step 1: Fetching top authors...")
    top_authors = get_top_authors(journals=journals, year=year, top_n=top_n)
    print(f"Found {len(top_authors)} authors")
    
    # Collect papers for each author
    print("\nStep 2: Collecting papers for each author...")
    author_papers = {}
    author_features = {}
    
    for i, (author_name, paper_count, total_citations) in enumerate(top_authors, 1):
        if i % 50 == 0:
            print(f"  Processing author {i}/{len(top_authors)}...")
        
        papers = get_author_papers(author_name, journals=journals, year=year)
        
        if papers:
            # Combine all paper titles and abstracts into one text corpus
            combined_titles = ' '.join([p['title'] for p in papers if p['title']])
            combined_abstracts = ' '.join([p['abstract'] for p in papers if p['abstract']])
            
            # Weight abstracts more heavily by including them twice
            combined_text = combined_titles + ' ' + combined_abstracts + ' ' + combined_abstracts
            
            # Calculate features
            avg_citations = np.mean([p['cited_by_count'] for p in papers])
            max_citations = max([p['cited_by_count'] for p in papers])
            
            # Count papers with abstracts
            papers_with_abstracts = sum(1 for p in papers if p['abstract'])
            
            author_papers[author_name] = papers
            author_features[author_name] = {
                'text': combined_text,
                'paper_count': len(papers),
                'papers_with_abstracts': papers_with_abstracts,
                'avg_citations': avg_citations,
                'max_citations': max_citations,
                'total_citations': total_citations
            }
    
    print(f"\nCollected papers for {len(author_papers)} authors")
    
    # Count total abstracts
    total_abstracts = sum(f['papers_with_abstracts'] for f in author_features.values())
    total_papers = sum(f['paper_count'] for f in author_features.values())
    print(f"Papers with abstracts: {total_abstracts}/{total_papers} ({100*total_abstracts/total_papers:.1f}%)")
    
    # Step 3: Topic modeling on paper titles
    print(f"\nStep 3: Performing topic modeling with {n_topics} topics...")
    
    author_names = list(author_features.keys())
    texts = [author_features[name]['text'] for name in author_names]
    
    # TF-IDF vectorization
    vectorizer = TfidfVectorizer(
        max_features=500,
        stop_words='english',
        ngram_range=(1, 2),
        min_df=2
    )
    
    tfidf_matrix = vectorizer.fit_transform(texts)
    feature_names = vectorizer.get_feature_names_out()
    
    # Topic modeling using LDA
    lda = LatentDirichletAllocation(
        n_components=n_topics,
        random_state=42,
        max_iter=20
    )
    
    topic_distributions = lda.fit_transform(tfidf_matrix)
    
    # Define topic labels based on top words
    print("\nStep 4: Identifying topic themes...")
    topic_labels = []
    
    for topic_idx, topic in enumerate(lda.components_):
        top_word_indices = topic.argsort()[-5:][::-1]
        top_words = [feature_names[i] for i in top_word_indices]
        topic_label = infer_topic_label(top_words)
        topic_labels.append(topic_label)
        print(f"  Topic {topic_idx+1}: {topic_label}")
        print(f"    Keywords: {', '.join(top_words)}")
    
    # Step 5: Assign research agendas to authors
    print("\nStep 5: Classifying authors...")
    
    author_agendas = {}
    for i, author_name in enumerate(author_names):
        # Get primary and secondary topics
        topic_dist = topic_distributions[i]
        top_topics = topic_dist.argsort()[-2:][::-1]
        
        primary_topic = topic_labels[top_topics[0]]
        primary_weight = topic_dist[top_topics[0]]
        
        secondary_topic = topic_labels[top_topics[1]] if primary_weight < 0.6 else None
        
        author_agendas[author_name] = {
            'primary_topic': primary_topic,
            'primary_weight': float(primary_weight),
            'secondary_topic': secondary_topic,
            'paper_count': author_features[author_name]['paper_count'],
            'avg_citations': author_features[author_name]['avg_citations'],
            'total_citations': author_features[author_name]['total_citations']
        }
    
    # Print results
    print("\n" + "="*80)
    print("Research Agenda Classification Results")
    print("="*80)
    
    # Group by primary topic
    topic_groups = defaultdict(list)
    for author, info in author_agendas.items():
        topic_groups[info['primary_topic']].append((author, info))
    
    for topic in sorted(topic_groups.keys()):
        authors = topic_groups[topic]
        print(f"\n{topic} ({len(authors)} authors):")
        print("-" * 80)
        
        # Sort by paper count
        authors.sort(key=lambda x: x[1]['paper_count'], reverse=True)
        
        for author, info in authors[:10]:  # Show top 10 per topic
            secondary = f" + {info['secondary_topic']}" if info['secondary_topic'] else ""
            print(f"  {author:45} Papers: {info['paper_count']:2d}  Citations: {info['total_citations']:4.0f}{secondary}")
    
    print("\n" + "="*80)
    
    return author_agendas

def infer_topic_label(keywords):
    """
    Infer a human-readable topic label from top keywords.
    
    Args:
        keywords (list): Top keywords for the topic
    
    Returns:
        str: Topic label
    """
    keywords_lower = [k.lower() for k in keywords]
    
    # Define topic patterns
    patterns = {
        'Asset Pricing': ['return', 'risk', 'premium', 'price', 'asset', 'equity', 'stock', 'portfolio'],
        'Corporate Finance': ['firm', 'corporate', 'investment', 'dividend', 'capital structure', 'leverage'],
        'Banking & Financial Institutions': ['bank', 'credit', 'loan', 'lending', 'financial institution'],
        'Market Microstructure': ['liquidity', 'trading', 'market', 'bid', 'spread', 'order'],
        'Behavioral Finance': ['investor', 'sentiment', 'bias', 'behavioral', 'retail', 'attention'],
        'ESG & Climate Finance': ['esg', 'climate', 'green', 'sustainable', 'environmental', 'carbon'],
        'Fintech & Innovation': ['fintech', 'technology', 'digital', 'blockchain', 'crypto', 'innovation'],
        'International Finance': ['international', 'global', 'exchange rate', 'country', 'foreign'],
        'Household Finance': ['household', 'consumer', 'mortgage', 'saving', 'retirement', 'financial advice'],
        'Derivatives & Options': ['option', 'derivative', 'futures', 'volatility', 'hedging'],
        'Monetary Policy & Macro': ['monetary', 'policy', 'federal reserve', 'interest rate', 'inflation', 'macro'],
        'Private Markets': ['private equity', 'venture capital', 'vc', 'startup', 'ipo']
    }
    
    # Score each pattern
    scores = {}
    for label, pattern_words in patterns.items():
        score = sum(1 for kw in keywords_lower for pw in pattern_words if pw in kw)
        if score > 0:
            scores[label] = score
    
    if scores:
        return max(scores.items(), key=lambda x: x[1])[0]
    else:
        # Default: use first two keywords
        return ' & '.join(keywords[:2]).title()

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 classify_research_agendas.py <top_n> [journal] [year] [--topics=N]")
        print("\nExamples:")
        print("  python3 classify_research_agendas.py 250")
        print("  python3 classify_research_agendas.py 250 top3")
        print("  python3 classify_research_agendas.py 250 top3 2024")
        print("  python3 classify_research_agendas.py 250 top3 2024 --topics=15")
        print("\nThis will classify research agendas using machine learning on paper titles")
        sys.exit(1)
    
    top_n = int(sys.argv[1])
    journals = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else 'top3'
    year = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else None
    
    # Parse --topics flag
    n_topics = 10
    for arg in sys.argv:
        if arg.startswith('--topics='):
            n_topics = int(arg.split('=')[1])
    
    author_agendas = classify_research_agendas(
        top_n=top_n,
        journals=journals,
        year=year,
        n_topics=n_topics
    )
    
    # Save results to JSON
    output_dir = '../out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    year_suffix = f"_{year}" if year else ""
    output_file = os.path.join(output_dir, f'research_agendas_{journals}{year_suffix}_top{top_n}.json')
    
    with open(output_file, 'w') as f:
        json.dump(author_agendas, f, indent=2)
    
    print(f"\n✅ Results saved to: {output_file}")

if __name__ == "__main__":
    main()
