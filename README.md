# Finance Papers

CLI tool for analyzing academic papers from top finance and economics journals using the OpenAlex API.

## Supported Journals

**Finance (top3):** JF, RFS, JFE

**Economics (econ5):** QJE, AER, Econometrica, JPE, REStud

## Installation

```bash
pip install -e .
```

This creates the `finance-papers` command.

## Quick Start

```bash
# Interactive mode - guided workflow
finance-papers

# Update journal articles
finance-papers update articles

# Rank top 100 authors
finance-papers rank -n 100

# Search papers by author
finance-papers papers -a "Fama"

# Web dashboard
streamlit run streamlit_app.py
```

## Commands

### Update Data

```bash
finance-papers update articles                # Fetch latest articles
finance-papers update articles -y 2024        # Specific year
finance-papers update articles -y 2023-2025   # Year range
finance-papers update articles --force        # Refresh citation counts
finance-papers update working-papers          # Fetch working papers for top authors
```

### Rank Authors

```bash
finance-papers rank -n 250                    # Top 250 by publications
finance-papers rank -n 250 --citations        # Rank by citations
finance-papers rank --working-papers          # Rank by working paper count
finance-papers rank -o authors.csv            # Export to CSV
```

### Search Papers

```bash
finance-papers papers -a "Cochrane"           # By author
finance-papers papers -a "Fama" -y 2024       # By author and year
finance-papers topic "Asset Pricing"          # By topic (interactive fzf)
```

### Chat

```bash
finance-papers chat                           # Chat with Claude about papers
```

## Web Dashboard

```bash
streamlit run streamlit_app.py
```

Features:
- Interactive author rankings with filtering
- Working papers browser
- Database statistics
- Data update tools
- CSV export

## Data Storage

All databases stored in `out/data/`:

- `openalex_{journal}_{year}.db` - Journal articles
- `working_papers.db` - Working papers
- `papers.db` - Unified papers database

## Project Structure

```
finance-papers/
├── finance_papers/          # Main package
│   ├── core.py             # Core functionality
│   ├── cli.py              # CLI interface
│   └── __init__.py         # Public API
├── archive/                 # Legacy scripts (reference)
├── tests/                   # Test suite
├── streamlit_app.py         # Web dashboard
├── setup.py                 # Package config
└── requirements.txt         # Dependencies
```

## Configuration

Set `OPENALEX_MAILTO` environment variable for polite API usage (recommended).

## Dependencies

- requests
- beautifulsoup4
- openai
- anthropic
- streamlit
- pandas
