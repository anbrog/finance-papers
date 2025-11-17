#!/bin/bash
# Deployment helper script for Finance Papers

set -e  # Exit on error

echo "🚀 Finance Papers - Deployment Helper"
echo "======================================"
echo ""

# Check if git is initialized
if [ ! -d .git ]; then
    echo "📦 Step 1: Initialize Git Repository"
    echo "-------------------------------------"
    git init
    git add .
    git commit -m "Initial commit - Finance Papers Analysis App"
    echo "✅ Git repository initialized"
    echo ""
else
    echo "✅ Git repository already exists"
    echo ""
fi

# Check for uncommitted changes
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    echo "📝 Uncommitted changes detected"
    echo ""
    read -p "Commit changes? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git add .
        read -p "Commit message: " commit_msg
        git commit -m "$commit_msg"
        echo "✅ Changes committed"
    fi
    echo ""
fi

# Check if remote exists
if ! git remote | grep -q origin; then
    echo "🔗 Step 2: Connect to GitHub"
    echo "----------------------------"
    echo ""
    echo "First, create a new repository on GitHub:"
    echo "  1. Go to https://github.com/new"
    echo "  2. Repository name: finance-papers (or your choice)"
    echo "  3. Make it PUBLIC (required for free Streamlit Cloud)"
    echo "  4. Do NOT initialize with README"
    echo "  5. Click 'Create repository'"
    echo ""
    read -p "Enter your GitHub username: " github_user
    read -p "Enter repository name [finance-papers]: " repo_name
    repo_name=${repo_name:-finance-papers}
    
    git remote add origin "https://github.com/$github_user/$repo_name.git"
    echo "✅ Remote added: https://github.com/$github_user/$repo_name"
    echo ""
else
    echo "✅ GitHub remote already configured"
    remote_url=$(git remote get-url origin)
    echo "   $remote_url"
    echo ""
fi

# Push to GitHub
echo "📤 Step 3: Push to GitHub"
echo "-------------------------"
echo ""

if git ls-remote origin &>/dev/null; then
    echo "Repository exists on GitHub"
    read -p "Push changes? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        current_branch=$(git branch --show-current)
        git push -u origin "$current_branch"
        echo "✅ Pushed to GitHub"
    fi
else
    echo "First push to GitHub..."
    git branch -M main
    git push -u origin main
    echo "✅ Pushed to GitHub"
fi

echo ""
echo "🌐 Step 4: Deploy to Streamlit Cloud"
echo "------------------------------------"
echo ""
echo "Now deploy your app:"
echo ""
echo "  1. Go to https://share.streamlit.io/"
echo "  2. Sign in with GitHub"
echo "  3. Click 'New app'"
echo "  4. Select your repository"
echo "  5. Main file: streamlit_app.py"
echo "  6. Click 'Deploy!'"
echo ""
echo "Your app will be live at:"
remote_url=$(git remote get-url origin)
repo_path=$(echo "$remote_url" | sed 's/.*github.com[:/]\(.*\)\.git/\1/' | sed 's/.*github.com[:/]\(.*\)/\1/')
username=$(echo "$repo_path" | cut -d'/' -f1)
reponame=$(echo "$repo_path" | cut -d'/' -f2)
echo "  https://$username-$(echo $reponame | tr '[:upper:]' '[:lower:]' | tr '_' '-').streamlit.app"
echo ""
echo "⚠️  Note: The app will need databases to display data."
echo "   Use the 'Update Data' tab in the deployed app to fetch papers."
echo ""
echo "✅ Deployment preparation complete!"
echo ""
echo "📚 For more details, see DEPLOY.md"
