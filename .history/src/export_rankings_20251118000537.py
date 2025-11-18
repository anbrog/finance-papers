#!/usr/bin/env python3
"""
Export author rankings to JSON for the static website
"""
import sqlite3
import json
import os
import glob
import sys

# Get DB_DIR relative to project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
DB_DIR = os.path.join(project_root, 'out', 'data')

def get_author_rankings(journals=['aer', 'jf', 'jfe', 'qje', 'rfs'], year=None, top_n=250):
    """Get author rankings from database"""
    authors_data = {}
    
    for journal in journals:
        if year:
            db_file = os.path.join(DB_DIR, f'openalex_{journal}_{year}.db')
            if not os.path.exists(db_file):
                continue
            db_files = [db_file]
        else:
            # All years
            pattern = os.path.join(DB_DIR, f'openalex_{journal}_*.db')
            db_files = glob.glob(pattern)
        
        for db_file in db_files:
            if not os.path.exists(db_file):
                continue
            
            print(f"Processing: {os.path.basename(db_file)}")
                
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            try:
                cursor.execute('SELECT authors_json, publication_date, title, cited_by_count FROM openalex_articles')
                
                for row in cursor.fetchall():
                    authors_json, pub_date, title, citations = row
                    try:
                        authors = json.loads(authors_json)
                    except:
                        continue
                    
                    for author in authors:
                        name = author.get('name')
                        if not name:
                            continue
                        
                        if name not in authors_data:
                            authors_data[name] = {
                                'papers': 0,
                                'citations': 0,
                                'latest_date': '',
                                'latest_title': ''
                            }
                        
                        authors_data[name]['papers'] += 1
                        authors_data[name]['citations'] += citations or 0
                        
                        if pub_date and (not authors_data[name]['latest_date'] or pub_date > authors_data[name]['latest_date']):
                            authors_data[name]['latest_date'] = pub_date
                            authors_data[name]['latest_title'] = title
            except Exception as e:
                print(f"Error processing {db_file}: {e}")
            finally:
                conn.close()
    
    # Convert to list
    data = []
    for author, stats in authors_data.items():
        data.append({
            'Author': author,
            'Papers': stats['papers'],
            'Citations': stats['citations'],
            'Latest Paper': stats['latest_title'][:100] if stats['latest_title'] else '',
            'Latest Date': stats['latest_date']
        })
    
    # Sort by papers then citations
    data.sort(key=lambda x: (x['Papers'], x['Citations']), reverse=True)
    
    # Limit to top N
    data = data[:top_n]
    
    return data

def main():
    print("Generating author rankings from databases...")
    print(f"Database directory: {DB_DIR}")
    
    # Get rankings
    rankings = get_author_rankings(top_n=250)
    
    if not rankings:
        print("No data found in databases!")
        sys.exit(1)
    
    print(f"\nFound {len(rankings)} authors")
    print(f"Total papers: {sum(r['Papers'] for r in rankings)}")
    print(f"Total citations: {sum(r['Citations'] for r in rankings)}")
    
    # Export to JSON
    output_file = os.path.join(project_root, 'docs', 'data', 'rankings.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(rankings, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Exported to: {output_file}")
    print(f"\nTop 10 authors:")
    for i, author in enumerate(rankings[:10], 1):
        print(f"{i}. {author['Author']} - {author['Papers']} papers, {author['Citations']} citations")
    
    sys.exit(0)

if __name__ == "__main__":
    main()
