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
                    
                    # Extract year from publication date
                    paper_year = None
                    if pub_date:
                        try:
                            paper_year = int(pub_date.split('-')[0])
                        except:
                            pass
                    
                    for author in authors:
                        name = author.get('name')
                        if not name:
                            continue
                        
                        if name not in authors_data:
                            authors_data[name] = {
                                'papers': 0,
                                'citations': 0,
                                'latest_date': '',
                                'latest_title': '',
                                'papers_by_year': {}
                            }
                        
                        authors_data[name]['papers'] += 1
                        authors_data[name]['citations'] += citations or 0
                        
                        # Track papers by year
                        if paper_year:
                            if paper_year not in authors_data[name]['papers_by_year']:
                                authors_data[name]['papers_by_year'][paper_year] = 0
                            authors_data[name]['papers_by_year'][paper_year] += 1
                        
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
            'Latest Date': stats['latest_date'],
            'Years': stats['papers_by_year']
        })
    
    # Sort by papers then citations
    data.sort(key=lambda x: (x['Papers'], x['Citations']), reverse=True)
    
    # Limit to top N
    data = data[:top_n]
    
    return data

def get_working_papers_rankings(top_n=250):
    """Get author rankings from working papers database"""
    authors_data = {}
    
    # Check for working papers database
    pattern = os.path.join(DB_DIR, 'working_papers*.db')
    db_files = glob.glob(pattern)
    
    if not db_files:
        print("No working papers database found")
        return []
    
    for db_file in db_files:
        print(f"Processing: {os.path.basename(db_file)}")
        
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT author_name, title, publication_date, cited_by_count, primary_location
                FROM working_papers
            ''')
            
            for row in cursor.fetchall():
                author_name, title, pub_date, citations, location = row
                
                if not author_name:
                    continue
                
                # Extract year from publication date
                paper_year = None
                if pub_date:
                    try:
                        paper_year = int(pub_date.split('-')[0])
                    except:
                        pass
                
                if author_name not in authors_data:
                    authors_data[author_name] = {
                        'papers': 0,
                        'citations': 0,
                        'latest_date': '',
                        'latest_title': '',
                        'latest_location': '',
                        'papers_by_year': {}
                    }
                
                authors_data[author_name]['papers'] += 1
                authors_data[author_name]['citations'] += citations or 0
                
                # Track papers by year
                if paper_year:
                    if paper_year not in authors_data[author_name]['papers_by_year']:
                        authors_data[author_name]['papers_by_year'][paper_year] = 0
                    authors_data[author_name]['papers_by_year'][paper_year] += 1
                
                if pub_date and (not authors_data[author_name]['latest_date'] or pub_date > authors_data[author_name]['latest_date']):
                    authors_data[author_name]['latest_date'] = pub_date
                    authors_data[author_name]['latest_title'] = title
                    authors_data[author_name]['latest_location'] = location
        
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
            'Latest Date': stats['latest_date'],
            'Location': stats['latest_location'][:50] if stats['latest_location'] else '',
            'Years': stats['papers_by_year']
        })
    
    # Sort by papers then citations
    data.sort(key=lambda x: (x['Papers'], x['Citations']), reverse=True)
    
    # Limit to top N
    data = data[:top_n]
    
    return data

def main():
    print("Generating author rankings from databases...")
    print(f"Database directory: {DB_DIR}")
    
    # Get journal rankings
    print("\n📚 Processing journal articles...")
    journal_rankings = get_author_rankings(top_n=250)
    
    if not journal_rankings:
        print("No journal data found in databases!")
    else:
        print(f"Found {len(journal_rankings)} authors")
        print(f"Total papers: {sum(r['Papers'] for r in journal_rankings)}")
        print(f"Total citations: {sum(r['Citations'] for r in journal_rankings)}")
    
    # Get working papers rankings
    print("\n📄 Processing working papers...")
    wp_rankings = get_working_papers_rankings(top_n=250)
    
    if not wp_rankings:
        print("No working papers data found")
    else:
        print(f"Found {len(wp_rankings)} authors")
        print(f"Total working papers: {sum(r['Papers'] for r in wp_rankings)}")
        print(f"Total citations: {sum(r['Citations'] for r in wp_rankings)}")
    
    # Combine data
    export_data = {
        'journals': journal_rankings,
        'working_papers': wp_rankings,
        'last_updated': os.popen('date -u +"%Y-%m-%d %H:%M:%S UTC"').read().strip()
    }
    
    # Export to JSON
    output_file = os.path.join(project_root, 'docs', 'data', 'rankings.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Exported to: {output_file}")
    
    if journal_rankings:
        print(f"\nTop 10 authors (journals):")
        for i, author in enumerate(journal_rankings[:10], 1):
            print(f"{i}. {author['Author']} - {author['Papers']} papers, {author['Citations']} citations")
    
    if wp_rankings:
        print(f"\nTop 10 authors (working papers):")
        for i, author in enumerate(wp_rankings[:10], 1):
            print(f"{i}. {author['Author']} - {author['Papers']} papers, {author['Citations']} citations")
    
    sys.exit(0)

if __name__ == "__main__":
    main()
