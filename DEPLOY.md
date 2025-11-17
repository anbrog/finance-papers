# Finance Papers - Deployment Guide

## Step-by-Step Deployment to Streamlit Cloud (Free)

### Prerequisites
- GitHub account (free)
- Your code pushed to GitHub

### Step 1: Initialize Git Repository

```bash
cd "/Users/work/Erasmus Universiteit Rotterdam Dropbox/Andreas Brogger/1 Projects/SS scrape-papers"

# Initialize git
git init

# Add all files
git add .

# Create first commit
git commit -m "Initial commit - Finance Papers Analysis App"
```

### Step 2: Create GitHub Repository

1. Go to https://github.com/new
2. Repository name: `finance-papers` (or your preferred name)
3. Description: "Academic finance papers analysis and ranking system"
4. Choose **Public** (required for free Streamlit Cloud)
5. **DO NOT** initialize with README (we already have one)
6. Click "Create repository"

### Step 3: Push to GitHub

```bash
# Add GitHub as remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/finance-papers.git

# Push to GitHub
git branch -M main
git push -u origin main
```

### Step 4: Deploy to Streamlit Cloud

1. Go to https://share.streamlit.io/
2. Click "Sign in with GitHub"
3. Authorize Streamlit to access your repositories
4. Click "New app"
5. Fill in:
   - **Repository:** YOUR_USERNAME/finance-papers
   - **Branch:** main
   - **Main file path:** streamlit_app.py
6. Click "Deploy!"

### Step 5: Initial Setup (First Time)

The app will deploy but won't have databases yet. You have two options:

#### Option A: Upload Sample Databases (Quick Demo)

1. On your local machine, run:
   ```bash
   # Generate a small sample database
   finance-papers
   # Answer: n (skip updates)
   # This creates empty/sample database structure
   ```

2. Create a GitHub repository for databases (can be private):
   ```bash
   cd out/data
   git init
   git add *.db
   git commit -m "Add sample databases"
   # Push to a separate repo or use Git LFS for large files
   ```

3. Modify `streamlit_app.py` to download databases from your repo on startup

#### Option B: Run Updates in Cloud (Slower but Complete)

The app includes an "Update Data" tab that can fetch fresh data. Users can:
1. Access your deployed app
2. Go to "Update Data" tab
3. Click "Update Journal Articles" to fetch papers
4. Click "Update Working Papers" to fetch WPs

**Note:** This can be slow on Streamlit Cloud's free tier.

#### Option C: Use Pre-populated Data (Recommended)

Create a minimal database with pre-fetched data:

```bash
# On your local machine
cd "/Users/work/Erasmus Universiteit Rotterdam Dropbox/Andreas Brogger/1 Projects/SS scrape-papers"

# Create a demo-data branch with databases
git checkout -b demo-data
git add -f out/data/*.db  # Force add despite .gitignore
git commit -m "Add demo databases"
git push origin demo-data
```

Then in Streamlit Cloud settings, point to `demo-data` branch for databases.

### Step 6: Configure Secrets (Optional)

If you want to enable OpenAI research agenda extraction:

1. In Streamlit Cloud, go to your app settings
2. Click "Secrets"
3. Add:
   ```toml
   OPENAI_API_KEY = "your-api-key-here"
   ```

### Step 7: Custom Domain (Optional)

1. In Streamlit Cloud app settings
2. Go to "General" tab
3. Click "Add custom domain"
4. Follow DNS configuration instructions

---

## Alternative: Deploy with Docker

If you prefer self-hosting:

### Create Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy application files
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -e .

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
CMD ["streamlit", "run", "streamlit_app.py", "--server.address", "0.0.0.0"]
```

### Build and Run

```bash
# Build image
docker build -t finance-papers .

# Run container
docker run -p 8501:8501 \
  -v $(pwd)/out:/app/out \
  -e OPENAI_API_KEY=your-key \
  finance-papers
```

### Deploy to Cloud

Deploy the Docker container to:
- **Heroku**: `heroku container:push web && heroku container:release web`
- **Google Cloud Run**: `gcloud run deploy --image gcr.io/PROJECT/finance-papers`
- **AWS ECS**: Use AWS Console or CLI
- **DigitalOcean App Platform**: Connect your GitHub repo

---

## Quick Commands Reference

```bash
# Initial setup
git init
git add .
git commit -m "Initial commit"

# Connect to GitHub
git remote add origin https://github.com/YOUR_USERNAME/finance-papers.git
git push -u origin main

# Update deployment (after changes)
git add .
git commit -m "Update app"
git push

# Streamlit Cloud auto-redeploys on push!
```

---

## Troubleshooting

### "No databases found"
- Use the Update Data tab in the app to fetch data
- Or push databases to a separate branch and configure app to use it

### "Module not found"
- Check `requirements.txt` includes all dependencies
- Streamlit Cloud automatically installs from requirements.txt

### App is slow
- Free tier has resource limits
- Consider using cached/pre-computed data
- Reduce the `top_n` limits in queries

### Database too large for Git
- Use Git LFS (Large File Storage)
- Or use external storage (S3, Google Cloud Storage)
- Or fetch data on-demand in the app

---

## Next Steps

1. Follow Step 1-4 above to deploy
2. Share your app URL: `https://YOUR_USERNAME-finance-papers.streamlit.app`
3. Anyone can access it without installing anything!

Need help? Check:
- [Streamlit Cloud Docs](https://docs.streamlit.io/streamlit-community-cloud)
- [GitHub Docs](https://docs.github.com)
