# Static Finance Papers Viewer

This directory contains static HTML files for viewing the finance papers database without running a server.

## GitHub Pages Deployment

1. **Enable GitHub Pages**:
   - Go to your repo: https://github.com/anbrog/finance-papers
   - Settings → Pages
   - Source: Deploy from branch → `main` → `/docs`
   - Save

2. **Update Data**:
   ```bash
   # From Streamlit app, download rankings as CSV
   # Then convert to JSON:
   python3 -c "import pandas as pd; pd.read_csv('author_rankings.csv').to_json('docs/data/rankings.json', orient='records', indent=2)"
   
   # Commit and push
   git add docs/data/rankings.json
   git commit -m "Update rankings data"
   git push
   ```

3. **Access your site**:
   - URL: https://anbrog.github.io/finance-papers/rankings.html

## Files

- `rankings.html` - Interactive author rankings table (sortable, filterable)
- `index.html` - Database explorer using Datasette Lite (loads .db files in browser)
- `data/rankings.json` - Data file (export from Streamlit)

## Pros of Static Approach

✅ Free hosting on GitHub Pages  
✅ Fast loading (no server required)  
✅ Works offline  
✅ Version controlled data  

## Cons

❌ Must manually export/update data  
❌ Can't update databases directly from web  
❌ Limited to ~100MB per repo (for database files)

## Alternative: Keep Streamlit + Export Snapshots

Best of both worlds:
1. Use Streamlit Cloud for live updates: https://anbrog-finance-papers.streamlit.app
2. Export snapshots to GitHub Pages for archival/sharing
