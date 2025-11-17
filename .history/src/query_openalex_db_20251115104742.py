#!/usr/bin/env python3
"""Query the OpenAlex articles database"""
import sqlite3
import json
import sys
import os

DB_PATH = '../out/data/openalex_articles.db'

def connect_db():
    """Connect to the database"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)

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
        SELECT openalex_id, title, publication_date, doi, authors_json
        FROM openalex_articles
        WHERE id = ?
    ''', (article_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print(f"Article with ID {article_id} not found")
        return
    
    openalex_id, title, pub_date, doi, authors_json = result
    authors = json.loads(authors_json)
    
    print(f"\n{'='*80}")
    print(f"ID: {article_id}")
    print(f"OpenAlex ID: {openalex_id}")
    print(f"Title: {title}")
    print(f"Publication Date: {pub_date or 'N/A'}")
    print(f"DOI: {doi or 'N/A'}")
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

def rank_authors(year=None):
    """Rank authors by number of publications"""
    conn = connect_db()
    cursor = conn.cursor()
    
    if year:
        # Filter by year
        cursor.execute('''
            SELECT authors_json FROM openalex_articles
            WHERE publication_date LIKE ?
        ''', (f'{year}%',))
        year_label = f" ({year})"
    else:
        cursor.execute('SELECT authors_json FROM openalex_articles')
        year_label = " (all years)"
    
    articles = cursor.fetchall()
    conn.close()
    
    # Count publications per author
    author_counts = {}
    for (authors_json,) in articles:
        authors = json.loads(authors_json)
        for author in authors:
            name = author.get('name')
            if name:
                if name not in author_counts:
                    author_counts[name] = 0
                author_counts[name] += 1
    
    # Sort by count (descending)
    ranked = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\nAuthor Rankings{year_label} (Total: {len(ranked)} authors)\n")
    print(f"{'Rank':<6} {'Papers':<8} {'Author Name':<60}")
    print("=" * 80)
    
    for rank, (author_name, count) in enumerate(ranked, 1):
        print(f"{rank:<6} {count:<8} {author_name}")
        if rank >= 50:  # Limit to top 50
            print(f"\n... and {len(ranked) - 50} more authors")
            break
    
    print(f"\nTotal articles: {len(articles)}")
    print(f"Total unique authors: {len(ranked)}")

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 query_openalex_db.py list              # List all articles")
        print("  python3 query_openalex_db.py get <id>          # Get article details")
        print("  python3 query_openalex_db.py search <keyword>  # Search by title")
        print("  python3 query_openalex_db.py author <name>     # Search by author")
        print("  python3 query_openalex_db.py rank-authors      # Rank authors by publications")
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
        rank_authors()
    elif command == 'count':
        count_articles()
    else:
        print("Invalid command or missing arguments")
        sys.exit(1)

if __name__ == "__main__":
    main()
