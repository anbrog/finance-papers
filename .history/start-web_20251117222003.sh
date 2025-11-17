#!/bin/bash
# Quick start script for Finance Papers Web Interface

echo "🚀 Starting Finance Papers Web Interface..."
echo ""

# Check if streamlit is installed
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo "📦 Installing required packages..."
    pip install streamlit pandas -q
    echo "✅ Packages installed"
    echo ""
fi

# Check if databases exist
if [ ! -d "out/data" ] || [ -z "$(ls -A out/data/*.db 2>/dev/null)" ]; then
    echo "⚠️  Warning: No databases found in out/data/"
    echo "   Run 'finance-papers' first to populate the database."
    echo ""
    read -p "Do you want to run finance-papers now? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        finance-papers
    fi
fi

echo "🌐 Launching web interface..."
echo "   Access at: http://localhost:8501"
echo ""
echo "   Press Ctrl+C to stop the server"
echo ""

streamlit run streamlit_app.py
