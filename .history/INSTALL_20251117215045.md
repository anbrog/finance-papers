# Installation Instructions

## Quick Install

From the project directory:

```bash
# Install in development mode (editable - changes to code reflect immediately)
pip install -e .

# Or install normally
pip install .
```

## Verify Installation

After installation, the main command should be available:

```bash
# Check if command is available
which finance-papers

# Test the command
finance-papers
```

## Uninstall

```bash
pip uninstall finance-papers
```

## Development Setup

For development with all dependencies:

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode with dependencies
pip install -e .

# Or install from requirements.txt
pip install -r requirements.txt
```

## Usage After Installation

The main script is available as a console command:

```bash
# Main workflow (recommended)
finance-papers

# Force mode - update all years
finance-papers --force
```

Individual scripts can still be run from the project directory:

```bash
cd "path/to/SS scrape-papers"
python3 src/getpapers_openalex.py jf 2024
python3 src/query_openalex_db.py rank-authors top3 --250
python3 src/get_wp.py author_list.csv 2024
python3 src/query_wp_db.py rank --250
python3 src/extract_research_agendas.py 250
```

## Troubleshooting

### Command not found

If commands aren't found after installation:

1. Make sure you're in the correct virtual environment
2. Check your PATH includes the Python scripts directory
3. Try reinstalling: `pip uninstall finance-papers && pip install -e .`

### Import errors

If you get import errors:

1. Make sure all dependencies are installed: `pip install -r requirements.txt`
2. Verify you're using Python 3.8 or higher: `python3 --version`

### Module not found

If you get "No module named 'src'":

1. Make sure you installed the package: `pip install -e .`
2. Don't run scripts directly from src/ directory when using installed commands
3. Use the console commands (e.g., `finance-papers`) instead of `python3 src/main.py`
