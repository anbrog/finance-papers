# Finance Papers - Usage Comparison

## Three Ways to Use This Tool

### 1. 🖥️ Command Line (`finance-papers`)

**Best for:** Automation, scripting, power users

**Launch:**
```bash
finance-papers
```

**Pros:**
- ✅ Full control and flexibility
- ✅ Can be scripted/automated
- ✅ Fast for power users
- ✅ Works over SSH
- ✅ No additional dependencies

**Cons:**
- ❌ Text-based interface only
- ❌ No visual charts/graphs
- ❌ Must remember commands
- ❌ Less accessible for non-technical users

---

### 2. 🌐 Web Interface (`streamlit run streamlit_app.py`)

**Best for:** Interactive exploration, sharing with others, visual analysis

**Launch:**
```bash
streamlit run streamlit_app.py
# or
./start-web.sh
```

**Pros:**
- ✅ User-friendly GUI
- ✅ No command syntax to remember
- ✅ Interactive tables with sorting/filtering
- ✅ Can be accessed from any browser
- ✅ Easy to share (deploy to cloud)
- ✅ Download results as CSV
- ✅ Visual statistics dashboard
- ✅ Works on mobile devices

**Cons:**
- ❌ Requires streamlit + pandas
- ❌ Uses more memory
- ❌ Slightly slower for bulk operations

---

### 3. 🐍 Python Scripts Directly

**Best for:** Custom workflows, integration with other tools

**Launch:**
```bash
python3 src/getpapers_openalex.py jf 2024
python3 src/query_openalex_db.py rank-authors top3 --250
```

**Pros:**
- ✅ Maximum flexibility
- ✅ Can import functions in other Python code
- ✅ Fine-grained control
- ✅ Useful for debugging

**Cons:**
- ❌ Need to know individual script names
- ❌ More verbose than `finance-papers` command

---

## Recommended Workflows

### Daily Research Workflow
```bash
# Morning: Quick update
finance-papers

# Afternoon: Explore results visually
streamlit run streamlit_app.py
```

### Sharing with Colleagues
1. Deploy web interface to Streamlit Cloud (free)
2. Share URL: `https://your-app.streamlit.app`
3. Colleagues can browse without installing anything

### Automated Research Updates
```bash
# Cron job or scheduled task
0 9 * * * cd /path/to/project && finance-papers << EOF
y
2024-2025

y
EOF
```

### Custom Analysis
```python
# your_analysis.py
import sys
sys.path.append('src')

from query_openalex_db import get_author_rankings

# Use the functions directly
df = get_author_rankings(['jf', 'rfs'], year=2024, top_n=100)
# ... custom analysis ...
```

---

## Feature Comparison

| Feature | CLI | Web | Direct Scripts |
|---------|-----|-----|---------------|
| Update journal articles | ✅ | ✅ | ✅ |
| Rank authors | ✅ | ✅ | ✅ |
| Filter/search results | Limited | ✅✅✅ | Manual |
| Export to CSV | ✅ | ✅ | ✅ |
| Visual tables | ❌ | ✅ | ❌ |
| Remote access | SSH only | Browser | SSH only |
| Multi-user | ❌ | ✅ | ❌ |
| Mobile friendly | ❌ | ✅ | ❌ |
| Automation | ✅✅✅ | Limited | ✅✅✅ |
| Setup time | Instant | +2 min | Instant |

---

## When to Use Each

### Use CLI (`finance-papers`) when:
- You're comfortable with command line
- Running on a server/remote machine
- Setting up automated workflows
- You want the fastest performance
- You don't need visual exploration

### Use Web Interface when:
- Sharing with non-technical users
- You want interactive exploration
- Need to filter/search easily
- Want to see statistics visually
- Working on mobile/tablet
- Demonstrating to others

### Use Direct Scripts when:
- Building custom tools
- Integrating with other software
- Need specific low-level control
- Debugging issues
- Developing new features

---

## Getting Started

### First Time Setup
```bash
# 1. Install the package
pip install -e .

# 2. Run initial data collection
finance-papers

# 3. Launch web interface
streamlit run streamlit_app.py
```

### Quick Test
```bash
# CLI
finance-papers

# Web (in another terminal)
streamlit run streamlit_app.py
# Then open http://localhost:8501
```

---

## Tips

1. **Both worlds:** Keep the CLI for updates, use web for exploration
2. **Remote access:** Run CLI via cron, access web interface from anywhere
3. **Sharing:** Deploy web to cloud, keep CLI for personal use
4. **Development:** Use direct scripts for prototyping, CLI for production
5. **Integration:** Web interface can trigger CLI commands internally

---

## Next Steps

- See [QUICKSTART.md](QUICKSTART.md) for CLI usage
- See [STREAMLIT.md](STREAMLIT.md) for web interface deployment
- See [README.md](README.md) for complete documentation
