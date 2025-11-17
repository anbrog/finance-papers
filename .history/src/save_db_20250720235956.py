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
    
    # Create output directory structure (relative to project root)
    output_dir = '../out/data'
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
        elif journal.lower() == 'aer':
            article_link = article.get('aer_link')
        elif journal.lower() == 'qje':
            article_link = article.get('qje_link')
        else:
            # Fallback: try to find any link field
            article_link = (article.get('jofi_link') or 
                          article.get('aer_link') or 
                          article.get('qje_link') or 
                          article.get('article_link'))
        
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
    journal_name = {
        'jf': 'JF',
        'aer': 'AER', 
        'qje': 'QJE'
    }.get(journal.lower(), journal.upper())
    
    if new_articles:
        print(f"\n💾 Saved {len(new_articles)} new {journal_name} articles to database {db_filepath}")
    else:
        print(f"\n📝 No new {journal_name} articles to save to database {db_filepath}")
    
    if duplicate_count > 0:
        print(f"🔄 DB: Skipped {duplicate_count} duplicate {journal_name} articles")
    
    return len(new_articles), duplicate_count
