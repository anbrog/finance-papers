# Database module for saving articles from both JF and AER to a unified database
import sqlite3
import json
import os
from datetime import datetime

def save_articles_to_db(articles_data, journal, volume, issue, db_filename='articles.db'):
    """
    Save articles to a unified SQLite database for both JF and AER
    
    Args:
        articles_data: List of article dictionaries
        journal: 'jf' or 'aer' to identify the journal
        volume: Volume number or issue ID
        issue: Issue number or 'forthcoming'
        db_filename: Database filename (default: 'articles.db')
    
    Returns:
        tuple: (number_of_new_articles, number_of_duplicates)
    """
    
    # Create output directory structure
    output_dir = 'out/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # Full path to the database file
    db_filepath = os.path.join(output_dir, db_filename)
    
    # Connect to SQLite database (creates file if it doesn't exist)
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Create unified table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journal TEXT NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            authors TEXT,
            abstract TEXT,
            volume TEXT,
            issue TEXT,
            article_link TEXT,
            all_links TEXT,
            paragraph_count INTEGER,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create indexes for faster duplicate checking and queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_journal ON articles(journal)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_article_link ON articles(article_link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_title_journal ON articles(title, journal)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_volume_issue_journal ON articles(volume, issue, journal)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scraped_at ON articles(scraped_at)')
    
    new_articles = []
    duplicate_count = 0
    
    for article in articles_data:
        # Get the appropriate link field based on journal
        if journal.lower() == 'jf':
            article_link = article.get('jofi_link')
        else:  # AER
            article_link = article.get('aer_link')
        
        title = article.get('title')
        
        # Check for duplicates based on link or title within the same journal
        is_duplicate = False
        
        if article_link:
            cursor.execute(
                'SELECT id FROM articles WHERE article_link = ? AND journal = ?', 
                (article_link, journal.lower())
            )
            if cursor.fetchone():
                is_duplicate = True
        
        if not is_duplicate and title:
            cursor.execute(
                'SELECT id FROM articles WHERE title = ? AND journal = ?', 
                (title, journal.lower())
            )
            if cursor.fetchone():
                is_duplicate = True
        
        if is_duplicate:
            duplicate_count += 1
            print(f"DB Duplicate found: {article.get('title', 'Unknown Title')}")
            continue
        
        # Prepare data for insertion
        insert_data = {
            'journal': journal.lower(),
            'title': article.get('title', ''),
            'date': article.get('date', ''),
            'authors': article.get('authors', ''),
            'abstract': article.get('abstract', ''),
            'volume': str(volume),
            'issue': str(issue),
            'article_link': article_link,
            'all_links': json.dumps(article.get('all_links', [])),
            'paragraph_count': article.get('paragraph_count', 0) if journal.lower() == 'jf' else None,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Insert article into database
        cursor.execute('''
            INSERT INTO articles (journal, title, date, authors, abstract, volume, issue, article_link, all_links, paragraph_count, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            insert_data['journal'],
            insert_data['title'],
            insert_data['date'],
            insert_data['authors'],
            insert_data['abstract'],
            insert_data['volume'],
            insert_data['issue'],
            insert_data['article_link'],
            insert_data['all_links'],
            insert_data['paragraph_count'],
            insert_data['scraped_at']
        ))
        
        new_articles.append(article)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    # Display results
    journal_name = "JF" if journal.lower() == 'jf' else "AER"
    
    if new_articles:
        print(f"\n💾 Saved {len(new_articles)} new {journal_name} articles to database {db_filepath}")
    else:
        print(f"\n📝 No new {journal_name} articles to save to database {db_filepath}")
    
    if duplicate_count > 0:
        print(f"🔄 DB: Skipped {duplicate_count} duplicate {journal_name} articles")
    
    return len(new_articles), duplicate_count

def get_article_count(journal=None, db_filename='articles.db'):
    """
    Get the total count of articles in the database
    
    Args:
        journal: Optional filter by journal ('jf' or 'aer'). If None, returns total count.
        db_filename: Database filename (default: 'articles.db')
    
    Returns:
        int: Number of articles
    """
    output_dir = 'out/data'
    db_filepath = os.path.join(output_dir, db_filename)
    
    if not os.path.exists(db_filepath):
        return 0
    
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    if journal:
        cursor.execute('SELECT COUNT(*) FROM articles WHERE journal = ?', (journal.lower(),))
    else:
        cursor.execute('SELECT COUNT(*) FROM articles')
    
    count = cursor.fetchone()[0]
    conn.close()
    
    return count

def get_latest_articles(journal=None, limit=10, db_filename='articles.db'):
    """
    Get the most recently scraped articles
    
    Args:
        journal: Optional filter by journal ('jf' or 'aer'). If None, returns from both.
        limit: Maximum number of articles to return
        db_filename: Database filename (default: 'articles.db')
    
    Returns:
        list: List of article dictionaries
    """
    output_dir = 'out/data'
    db_filepath = os.path.join(output_dir, db_filename)
    
    if not os.path.exists(db_filepath):
        return []
    
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    if journal:
        cursor.execute('''
            SELECT journal, title, authors, volume, issue, article_link, scraped_at 
            FROM articles 
            WHERE journal = ? 
            ORDER BY scraped_at DESC 
            LIMIT ?
        ''', (journal.lower(), limit))
    else:
        cursor.execute('''
            SELECT journal, title, authors, volume, issue, article_link, scraped_at 
            FROM articles 
            ORDER BY scraped_at DESC 
            LIMIT ?
        ''', (limit,))
    
    articles = []
    for row in cursor.fetchall():
        articles.append({
            'journal': row[0],
            'title': row[1],
            'authors': row[2],
            'volume': row[3],
            'issue': row[4],
            'article_link': row[5],
            'scraped_at': row[6]
        })
    
    conn.close()
    return articles

def print_forthcoming_articles(journal=None, db_filename='articles.db'):
    """
    Print all forthcoming articles from the database
    
    Args:
        journal: Optional filter by journal ('jf' or 'aer'). If None, shows from both.
        db_filename: Database filename (default: 'articles.db')
    """
    output_dir = 'out/data'
    db_filepath = os.path.join(output_dir, db_filename)
    
    if not os.path.exists(db_filepath):
        print("❌ No database found")
        return
    
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()
    
    # Query for forthcoming articles
    if journal:
        cursor.execute('''
            SELECT journal, title, authors, volume, issue, article_link, abstract, scraped_at 
            FROM articles 
            WHERE journal = ? AND (issue = 'forthcoming' OR volume = 'forthcoming')
            ORDER BY journal, scraped_at DESC
        ''', (journal.lower(),))
        journal_filter = f" {journal.upper()}"
    else:
        cursor.execute('''
            SELECT journal, title, authors, volume, issue, article_link, abstract, scraped_at 
            FROM articles 
            WHERE issue = 'forthcoming' OR volume = 'forthcoming'
            ORDER BY journal, scraped_at DESC
        ''')
        journal_filter = ""
    
    articles = cursor.fetchall()
    conn.close()
    
    if not articles:
        print(f"📰 No{journal_filter} forthcoming articles found in database")
        return
    
    print(f"📰 {len(articles)}{journal_filter} Forthcoming Articles:")
    print("=" * 80)
    
    current_journal = None
    for i, row in enumerate(articles, 1):
        journal_name, title, authors, volume, issue, article_link, abstract, scraped_at = row
        
        # Add journal header if it changes
        if current_journal != journal_name.upper():
            if current_journal is not None:
                print()  # Add spacing between journals
            current_journal = journal_name.upper()
            print(f"\n🏛️  {current_journal} FORTHCOMING ARTICLES:")
            print("-" * 50)
        
        print(f"\n=== Article {i} ===")
        print(f"Title: {title}")
        if authors:
            print(f"Authors: {authors}")
        if article_link:
            print(f"Link: {article_link}")
        if abstract and abstract.strip():
            # Truncate long abstracts
            abstract_preview = abstract[:200] + "..." if len(abstract) > 200 else abstract
            print(f"Abstract: {abstract_preview}")
        print(f"Scraped: {scraped_at}")

if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "forthcoming":
            # Handle journal filter argument
            journal_filter = sys.argv[2] if len(sys.argv) > 2 else None
            if journal_filter and journal_filter.lower() not in ['jf', 'aer']:
                print("❌ Journal filter must be 'jf' or 'aer'")
                sys.exit(1)
            print_forthcoming_articles(journal_filter)
        
        elif command == "stats":
            # Display database statistics
            print("📊 Database Statistics:")
            print(f"Total articles: {get_article_count()}")
            print(f"JF articles: {get_article_count('jf')}")
            print(f"AER articles: {get_article_count('aer')}")
        
        elif command == "latest":
            # Show latest articles
            limit = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 5
            journal_filter = sys.argv[3] if len(sys.argv) > 3 else None
            
            print(f"\n📰 Latest {limit} articles:")
            latest = get_latest_articles(journal=journal_filter, limit=limit)
            for i, article in enumerate(latest, 1):
                print(f"{i}. [{article['journal'].upper()}] {article['title'][:50]}...")
        
        elif command == "help":
            print("📚 Available commands:")
            print("  python save_db.py forthcoming [jf|aer]  - Show forthcoming articles")
            print("  python save_db.py stats                 - Show database statistics")
            print("  python save_db.py latest [N] [jf|aer]   - Show N latest articles")
            print("  python save_db.py help                  - Show this help")
            print("\nExamples:")
            print("  python save_db.py forthcoming           # All forthcoming articles")
            print("  python save_db.py forthcoming jf        # Only JF forthcoming")
            print("  python save_db.py latest 10             # Latest 10 articles")
            print("  python save_db.py latest 5 aer          # Latest 5 AER articles")
        
        else:
            print(f"❌ Unknown command: {command}")
            print("Use 'python save_db.py help' for available commands")
            sys.exit(1)
    
    else:
        # Default behavior - show stats and latest articles
        print("📊 Database Statistics:")
        print(f"Total articles: {get_article_count()}")
        print(f"JF articles: {get_article_count('jf')}")
        print(f"AER articles: {get_article_count('aer')}")
        
        print(f"\n📰 Latest 5 articles:")
        latest = get_latest_articles(limit=5)
        for i, article in enumerate(latest, 1):
            print(f"{i}. [{article['journal'].upper()}] {article['title'][:50]}...")
