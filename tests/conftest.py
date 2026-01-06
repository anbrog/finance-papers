"""Pytest fixtures for finance_papers tests."""

import pytest
import sqlite3
import json
from pathlib import Path


@pytest.fixture
def sample_db(tmp_path):
    """Create a temporary test database with sample data."""
    db_path = tmp_path / "openalex_jf_2024.db"
    conn = sqlite3.connect(str(db_path))

    conn.execute('''
        CREATE TABLE openalex_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            cited_by_count INTEGER DEFAULT 0,
            abstract TEXT,
            authors_json TEXT,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Insert sample articles
    sample_articles = [
        {
            'openalex_id': 'W1234567890',
            'title': 'Asset Pricing with Machine Learning',
            'publication_date': '2024-01-15',
            'doi': '10.1111/test.12345',
            'cited_by_count': 150,
            'abstract': 'We use machine learning to study asset pricing.',
            'authors': [
                {'name': 'Eugene Fama', 'author_id': 'A1', 'institutions': ['University of Chicago']},
                {'name': 'Kenneth French', 'author_id': 'A2', 'institutions': ['Dartmouth']}
            ],
            'topics': [{'name': 'Asset Pricing', 'score': 0.95}]
        },
        {
            'openalex_id': 'W9876543210',
            'title': 'Corporate Governance and Firm Value',
            'publication_date': '2024-02-20',
            'doi': '10.1111/test.67890',
            'cited_by_count': 75,
            'abstract': 'We study corporate governance.',
            'authors': [
                {'name': 'Eugene Fama', 'author_id': 'A1', 'institutions': ['University of Chicago']},
                {'name': 'Michael Jensen', 'author_id': 'A3', 'institutions': ['Harvard']}
            ],
            'topics': [{'name': 'Corporate Finance', 'score': 0.88}]
        },
        {
            'openalex_id': 'W5555555555',
            'title': 'Market Microstructure Theory',
            'publication_date': '2024-03-10',
            'doi': '10.1111/test.11111',
            'cited_by_count': 30,
            'abstract': 'Market microstructure analysis.',
            'authors': [
                {'name': 'Andreas Brøgger', 'author_id': 'A4', 'institutions': ['EUR']}
            ],
            'topics': [{'name': 'Market Microstructure', 'score': 0.92}]
        }
    ]

    for article in sample_articles:
        conn.execute('''
            INSERT INTO openalex_articles
            (openalex_id, title, publication_date, doi, cited_by_count, abstract, authors_json, topics_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            article['openalex_id'],
            article['title'],
            article['publication_date'],
            article['doi'],
            article['cited_by_count'],
            article['abstract'],
            json.dumps(article['authors']),
            json.dumps(article['topics'])
        ))

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def sample_db_dir(tmp_path, sample_db):
    """Create a mock DB_DIR with sample database and unified papers.db."""
    # Create out/data structure
    data_dir = tmp_path / 'out' / 'data'
    data_dir.mkdir(parents=True)

    # Copy sample_db to data_dir
    import shutil
    shutil.copy(sample_db, data_dir / sample_db.name)

    # Build unified papers.db from the sample article DB
    papers_db = data_dir / 'papers.db'
    conn = sqlite3.connect(str(papers_db))

    conn.execute('''
        CREATE TABLE papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            author_name TEXT,
            author_affiliation TEXT,
            author_openalex_id TEXT,
            source TEXT,
            journal TEXT,
            cited_by_count INTEGER DEFAULT 0,
            abstract TEXT,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(openalex_id, author_name)
        )
    ''')

    # Import from article DB
    article_conn = sqlite3.connect(str(sample_db))
    article_conn.row_factory = sqlite3.Row
    cursor = article_conn.cursor()
    cursor.execute('SELECT * FROM openalex_articles')

    for row in cursor:
        authors = json.loads(row['authors_json']) if row['authors_json'] else []
        for author in authors:
            name = author.get('name')
            if name:
                institutions = author.get('institutions', [])
                affiliation = institutions[0] if institutions else ''
                conn.execute('''
                    INSERT OR IGNORE INTO papers
                    (openalex_id, title, publication_date, doi, author_name,
                     author_affiliation, source, journal, cited_by_count, topics_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['openalex_id'],
                    row['title'],
                    row['publication_date'],
                    row['doi'],
                    name,
                    affiliation,
                    'article',
                    'jf',
                    row['cited_by_count'] or 0,
                    row['topics_json'],
                ))

    conn.commit()
    conn.close()
    article_conn.close()

    return data_dir


@pytest.fixture
def mock_openalex(requests_mock):
    """Mock OpenAlex API responses."""
    import re

    # Mock works endpoint
    requests_mock.get(
        re.compile(r'api\.openalex\.org/works'),
        json={
            'results': [
                {
                    'id': 'https://openalex.org/W123',
                    'title': 'Test Paper',
                    'publication_date': '2024-01-01',
                    'doi': '10.1111/test',
                    'cited_by_count': 10,
                    'abstract_inverted_index': {'test': [0], 'abstract': [1]},
                    'authorships': [
                        {'author': {'display_name': 'Test Author', 'id': 'A1'}, 'institutions': []}
                    ],
                    'topics': [{'display_name': 'Finance', 'score': 0.9}]
                }
            ],
            'meta': {'next_cursor': None}
        }
    )

    # Mock authors endpoint
    requests_mock.get(
        re.compile(r'api\.openalex\.org/authors'),
        json={
            'results': [
                {
                    'id': 'https://openalex.org/A123',
                    'display_name': 'Test Author',
                    'works_count': 50,
                    'last_known_institutions': [{'display_name': 'Test University'}]
                }
            ]
        }
    )

    return requests_mock


@pytest.fixture
def sample_wp_db(tmp_path):
    """Create a temporary working papers database with sample data."""
    db_path = tmp_path / "working_papers.db"
    conn = sqlite3.connect(str(db_path))

    conn.execute('''
        CREATE TABLE working_papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE NOT NULL,
            title TEXT,
            publication_date TEXT,
            doi TEXT,
            author_name TEXT,
            author_affiliation TEXT,
            type TEXT,
            primary_location TEXT,
            cited_by_count INTEGER DEFAULT 0,
            topics_json TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Insert sample working papers
    sample_wps = [
        {
            'openalex_id': 'WP001',
            'title': 'Climate Risk and Asset Prices',
            'publication_date': '2024-06-01',
            'doi': 'https://doi.org/10.2139/ssrn.1234567',
            'author_name': 'Test Author',
            'primary_location': 'SSRN Electronic Journal',
            'type': 'preprint',
            'cited_by_count': 25,
            'topics': [{'name': 'Climate Finance', 'score': 0.9}]
        },
        {
            'openalex_id': 'WP002',
            'title': 'Machine Learning in Finance',
            'publication_date': '2024-05-15',
            'doi': 'https://doi.org/10.48550/arxiv.2024.12345',
            'author_name': 'Another Author',
            'primary_location': 'arXiv (Cornell University)',
            'type': 'preprint',
            'cited_by_count': 10,
            'topics': [{'name': 'Financial Machine Learning', 'score': 0.85}]
        },
        {
            'openalex_id': 'WP003',
            'title': 'Banking Regulation Effects',
            'publication_date': '2024-04-20',
            'doi': '10.3386/w12345',
            'author_name': 'Test Author',
            'primary_location': 'RePEc: Research Papers in Economics',
            'type': 'report',
            'cited_by_count': 5,
            'topics': [{'name': 'Banking', 'score': 0.88}]
        }
    ]

    for wp in sample_wps:
        conn.execute('''
            INSERT INTO working_papers
            (openalex_id, title, publication_date, doi, author_name, primary_location, type, cited_by_count, topics_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            wp['openalex_id'],
            wp['title'],
            wp['publication_date'],
            wp['doi'],
            wp['author_name'],
            wp['primary_location'],
            wp['type'],
            wp['cited_by_count'],
            json.dumps(wp['topics'])
        ))

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def sample_db_dir_with_wp(tmp_path, sample_db, sample_wp_db):
    """Create a mock DB_DIR with both articles and working papers databases."""
    data_dir = tmp_path / 'out' / 'data'
    data_dir.mkdir(parents=True)

    import shutil
    shutil.copy(sample_db, data_dir / sample_db.name)
    shutil.copy(sample_wp_db, data_dir / sample_wp_db.name)

    return data_dir
