#!/usr/bin/env python3
"""
Streamlit web interface for Finance Papers Analysis
"""
import streamlit as st
import sqlite3
import json
import os
import sys
import subprocess
import pandas as pd
from datetime import datetime
import glob

# Page configuration
st.set_page_config(
    page_title="Finance Papers Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_DIR = 'out/data'

@st.cache_data(ttl=300)
def get_available_databases():
    """Get list of available database files"""
    dbs = {}
    
    # Journal databases
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    for db_file in glob.glob(pattern):
        basename = os.path.basename(db_file)
        parts = basename.replace('openalex_', '').replace('.db', '').split('_')
        if len(parts) == 2:
            journal, year = parts
            if journal not in dbs:
                dbs[journal] = []
            dbs[journal].append(int(year))
    
    # Sort years
    for journal in dbs:
        dbs[journal].sort(reverse=True)
    
    # Working papers
    wp_pattern = os.path.join(DB_DIR, 'working_papers*.db')
    wp_files = glob.glob(wp_pattern)
    dbs['working_papers'] = len(wp_files) > 0
    
    return dbs

@st.cache_data(ttl=60)
def get_author_rankings(journals, year=None, top_n=250, by_citations=False):
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
                
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
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
            
            conn.close()
    
    # Convert to dataframe
    data = []
    for author, stats in authors_data.items():
        data.append({
            'Author': author,
            'Papers': stats['papers'],
            'Citations': stats['citations'],
            'Latest Paper': stats['latest_title'][:80] if stats['latest_title'] else '',
            'Latest Date': stats['latest_date']
        })
    
    df = pd.DataFrame(data)
    
    if len(df) > 0:
        # Sort
        sort_by = 'Citations' if by_citations else 'Papers'
        df = df.sort_values(by=[sort_by, 'Citations'], ascending=False)
        df = df.head(top_n)
        df.insert(0, 'Rank', range(1, len(df) + 1))
    
    return df

@st.cache_data(ttl=60)
def get_working_papers(author_filter=None, year=None, top_n=100):
    """Get working papers from database"""
    # Try year-specific database first
    if year:
        db_file = os.path.join(DB_DIR, f'working_papers_{year}.db')
    else:
        db_file = os.path.join(DB_DIR, 'working_papers.db')
    
    if not os.path.exists(db_file):
        # Try all working papers databases
        pattern = os.path.join(DB_DIR, 'working_papers*.db')
        db_files = glob.glob(pattern)
        if not db_files:
            return pd.DataFrame()
        db_file = db_files[0]
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    query = '''
        SELECT title, author_name, author_affiliation, publication_date, 
               primary_location, cited_by_count
        FROM working_papers
    '''
    params = []
    
    if author_filter:
        query += ' WHERE author_name LIKE ?'
        params.append(f'%{author_filter}%')
    
    query += ' ORDER BY publication_date DESC'
    
    if top_n:
        query += f' LIMIT {top_n}'
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        title, author, affiliation, pub_date, location, citations = row
        data.append({
            'Date': pub_date or '',
            'Author': author or '',
            'Affiliation': affiliation or '',
            'Title': title or '',
            'Location': location or '',
            'Citations': citations or 0
        })
    
    return pd.DataFrame(data)

@st.cache_data(ttl=60)
def get_database_stats():
    """Get statistics about the databases"""
    stats = {}
    
    # Journal papers
    pattern = os.path.join(DB_DIR, 'openalex_*.db')
    total_papers = 0
    for db_file in glob.glob(pattern):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM openalex_articles')
        count = cursor.fetchone()[0]
        total_papers += count
        
        basename = os.path.basename(db_file).replace('openalex_', '').replace('.db', '')
        stats[basename] = count
        conn.close()
    
    stats['total_journal_papers'] = total_papers
    
    # Working papers
    wp_pattern = os.path.join(DB_DIR, 'working_papers*.db')
    total_wp = 0
    for db_file in glob.glob(wp_pattern):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM working_papers')
        count = cursor.fetchone()[0]
        total_wp += count
        conn.close()
    
    stats['total_working_papers'] = total_wp
    
    return stats

def main():
    st.title("📊 Finance Papers Analysis Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.header("⚙️ Settings")
    
    # Check database availability
    dbs = get_available_databases()
    
    if not dbs:
        st.error("No databases found! Please run `finance-papers` first to populate the database.")
        st.info("Run: `finance-papers` in your terminal to fetch papers.")
        return
    
    # Tab selection
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Author Rankings", "📄 Working Papers", "💾 Database Stats", "🔄 Update Data"])
    
    # Tab 1: Author Rankings
    with tab1:
        st.header("Top Author Rankings")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            available_journals = [j for j in dbs.keys() if j != 'working_papers']
            journals = st.multiselect(
                "Journals",
                options=available_journals,
                default=available_journals
            )
        
        with col2:
            all_years = set()
            for j in available_journals:
                if j in dbs and isinstance(dbs[j], list):
                    all_years.update(dbs[j])
            year_options = ["All Years"] + sorted(list(all_years), reverse=True)
            
            year_filter = st.selectbox("Year", year_options)
            year = None if year_filter == "All Years" else int(year_filter)
        
        with col3:
            top_n = st.number_input("Top N Authors", min_value=10, max_value=500, value=250, step=10)
        
        with col4:
            sort_by = st.radio("Sort by", ["Papers", "Citations"])
        
        if st.button("🔍 Show Rankings", key="show_rankings"):
            with st.spinner("Loading rankings..."):
                df = get_author_rankings(
                    journals, 
                    year=year, 
                    top_n=top_n,
                    by_citations=(sort_by == "Citations")
                )
                
                if len(df) > 0:
                    st.success(f"Found {len(df)} authors")
                    
                    # Display metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Authors", len(df))
                    with col2:
                        st.metric("Total Papers", df['Papers'].sum())
                    with col3:
                        st.metric("Total Citations", df['Citations'].sum())
                    
                    # Display table
                    st.dataframe(
                        df,
                        use_container_width=True,
                        height=600,
                        hide_index=True
                    )
                    
                    # Download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"author_rankings_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No data found for selected criteria.")
    
    # Tab 2: Working Papers
    with tab2:
        st.header("Working Papers")
        
        if not dbs.get('working_papers'):
            st.warning("No working papers database found. Run working papers update first.")
        else:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                author_search = st.text_input("Search by Author", "")
            
            with col2:
                wp_year_options = ["All Years"] + sorted(list(all_years), reverse=True) if all_years else ["All Years"]
                wp_year = st.selectbox("Year", wp_year_options, key="wp_year")
                wp_year_val = None if wp_year == "All Years" else int(wp_year)
            
            with col3:
                wp_limit = st.number_input("Limit", min_value=10, max_value=500, value=100, step=10)
            
            if st.button("🔍 Show Working Papers", key="show_wp"):
                with st.spinner("Loading working papers..."):
                    wp_df = get_working_papers(
                        author_filter=author_search if author_search else None,
                        year=wp_year_val,
                        top_n=wp_limit
                    )
                    
                    if len(wp_df) > 0:
                        st.success(f"Found {len(wp_df)} working papers")
                        
                        st.dataframe(
                            wp_df,
                            use_container_width=True,
                            height=600
                        )
                        
                        # Download button
                        csv = wp_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download as CSV",
                            data=csv,
                            file_name=f"working_papers_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            key="download_wp"
                        )
                    else:
                        st.warning("No working papers found.")
    
    # Tab 3: Database Stats
    with tab3:
        st.header("Database Statistics")
        
        if st.button("📊 Refresh Stats"):
            with st.spinner("Loading statistics..."):
                stats = get_database_stats()
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Journal Papers", stats.get('total_journal_papers', 0))
                
                with col2:
                    st.metric("Total Working Papers", stats.get('total_working_papers', 0))
                
                st.subheader("Papers by Journal and Year")
                
                journal_stats = {k: v for k, v in stats.items() if k not in ['total_journal_papers', 'total_working_papers']}
                if journal_stats:
                    df_stats = pd.DataFrame([
                        {'Database': k, 'Papers': v} 
                        for k, v in sorted(journal_stats.items())
                    ])
                    st.dataframe(df_stats, use_container_width=True)
    
    # Tab 4: Update Data
    with tab4:
        st.header("Update Data")
        st.warning("⚠️ Updates run the command-line tools. This may take several minutes.")
        
        st.subheader("Update Journal Articles")
        
        col1, col2 = st.columns(2)
        
        with col1:
            update_journals = st.multiselect(
                "Select Journals",
                options=['jf', 'rfs', 'jfe'],
                default=['jf', 'rfs', 'jfe'],
                key="update_journals"
            )
        
        with col2:
            current_year = datetime.now().year
            update_years = st.multiselect(
                "Select Years",
                options=list(range(2023, current_year + 1)),
                default=[current_year],
                key="update_years"
            )
        
        force_update = st.checkbox("Force update (refresh citations)", value=False)
        
        if st.button("🔄 Update Journal Articles", key="update_journals_btn"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_steps = len(update_journals) * len(update_years)
            current_step = 0
            
            for journal in update_journals:
                for year in update_years:
                    current_step += 1
                    progress_bar.progress(current_step / total_steps)
                    status_text.text(f"Updating {journal.upper()} {year}...")
                    
                    cmd = [sys.executable, 'src/getpapers_openalex.py', journal, str(year)]
                    if force_update:
                        cmd.append('--force')
                    
                    try:
                        result = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=300
                        )
                        if result.returncode != 0:
                            st.error(f"Error updating {journal} {year}: {result.stderr}")
                    except subprocess.TimeoutExpired:
                        st.error(f"Timeout updating {journal} {year}")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            
            progress_bar.progress(1.0)
            status_text.text("Update complete!")
            st.success("✅ Journal articles updated!")
            st.cache_data.clear()
        
        st.markdown("---")
        st.subheader("Update Working Papers")
        
        csv_pattern = os.path.join(DB_DIR, 'author_list_*.csv')
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            st.warning("No author list CSV found. Run author rankings first.")
        else:
            csv_file = st.selectbox("Select Author List", [os.path.basename(f) for f in csv_files])
            wp_year_update = st.number_input("Year", min_value=2020, max_value=current_year, value=current_year, key="wp_year_update")
            
            if st.button("🔄 Update Working Papers", key="update_wp_btn"):
                status_text = st.empty()
                status_text.text("Fetching working papers...")
                
                csv_path = os.path.join(DB_DIR, csv_file)
                cmd = [sys.executable, 'src/get_wp.py', csv_path, str(wp_year_update)]
                
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=600
                    )
                    if result.returncode == 0:
                        st.success("✅ Working papers updated!")
                    else:
                        st.error(f"Error: {result.stderr}")
                except subprocess.TimeoutExpired:
                    st.error("Timeout updating working papers")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
                
                st.cache_data.clear()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Finance Papers Analysis**
    
    This dashboard provides access to:
    - Author rankings from top finance journals
    - Working papers database
    - Database statistics
    - Data update tools
    
    Built with Streamlit
    """)

if __name__ == "__main__":
    main()
