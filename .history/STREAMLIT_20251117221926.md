# Streamlit Web Interface

## Quick Start

1. **Install dependencies:**
```bash
pip install streamlit pandas
# Or reinstall the package
pip install -e .
```

2. **Run the app:**
```bash
streamlit run streamlit_app.py
```

The app will open in your browser at `http://localhost:8501`

## Features

### 📈 Author Rankings Tab
- View top N authors from selected journals
- Filter by year or view all years
- Sort by number of papers or total citations
- Download rankings as CSV
- Interactive table with search and sorting

### 📄 Working Papers Tab
- Browse working papers database
- Search by author name
- Filter by year
- View author affiliations and paper details
- Download results as CSV

### 💾 Database Stats Tab
- View total papers across all databases
- See breakdown by journal and year
- Monitor database growth

### 🔄 Update Data Tab
- Update journal articles from OpenAlex
- Select specific journals and years
- Force update to refresh citation counts
- Update working papers for top authors
- Progress tracking for long-running updates

## Deployment Options

### 1. Streamlit Cloud (Free)

The easiest way to deploy:

1. Push your code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Sign in with GitHub
4. Click "New app"
5. Select your repository and `streamlit_app.py`
6. Deploy!

**Note:** You'll need to upload your databases or set up automated updates.

### 2. Local Network Access

Make it accessible to others on your network:

```bash
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
```

Access from other devices: `http://YOUR_IP:8501`

### 3. Docker Container

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -e .

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.address", "0.0.0.0"]
```

Build and run:
```bash
docker build -t finance-papers-app .
docker run -p 8501:8501 -v $(pwd)/out:/app/out finance-papers-app
```

### 4. Heroku

Create `Procfile`:
```
web: streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0
```

Deploy:
```bash
heroku create your-app-name
git push heroku main
```

### 5. AWS/GCP/Azure

Use their container services (ECS, Cloud Run, Container Instances) with the Docker image.

## Configuration

### Custom Port
```bash
streamlit run streamlit_app.py --server.port 8080
```

### Custom Theme

Create `.streamlit/config.toml`:
```toml
[theme]
primaryColor = "#1f77b4"
backgroundColor = "#ffffff"
secondaryBackgroundColor = "#f0f2f6"
textColor = "#262730"
font = "sans serif"
```

### Performance

For better performance with large databases:
- Cache TTL is set to 300 seconds (5 minutes)
- Adjust in the `@st.cache_data(ttl=300)` decorators
- Increase for slower updates, decrease for more real-time data

## Tips

1. **First Run:** Make sure you have databases populated by running `finance-papers` first
2. **Updates:** Use the Update Data tab to refresh data from within the app
3. **Large Datasets:** The app uses pagination and caching for performance
4. **Mobile:** The responsive layout works on tablets and phones
5. **Security:** For public deployment, consider adding authentication

## Troubleshooting

**"No databases found"**
- Run `finance-papers` first to populate databases
- Check that `out/data/` directory exists and contains `.db` files

**"Module not found" errors**
- Install dependencies: `pip install streamlit pandas`
- Or reinstall package: `pip install -e .`

**Slow performance**
- Reduce cache TTL
- Limit the number of results (top_n parameter)
- Use year filters to reduce data scope

**Port already in use**
- Change port: `streamlit run streamlit_app.py --server.port 8502`
- Or kill existing process: `lsof -ti:8501 | xargs kill`

## Advanced Usage

### Embedding in Existing Site

Use an iframe:
```html
<iframe src="http://localhost:8501" width="100%" height="800px"></iframe>
```

### Custom Domain

With Streamlit Cloud:
1. Go to app settings
2. Add custom domain
3. Update DNS CNAME record

### API Integration

The app can be extended to provide REST API endpoints using FastAPI alongside Streamlit.

## Support

For issues or questions:
1. Check the [Streamlit documentation](https://docs.streamlit.io)
2. Review the app logs in the terminal
3. Verify database connections with `sqlite3` directly
