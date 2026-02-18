"""Tests for finance_papers module."""

import pytest
import sqlite3
import json
from pathlib import Path


class TestConfiguration:
    """Test configuration constants."""

    def test_journals_defined(self):
        from finance_papers import JOURNALS
        assert 'jf' in JOURNALS
        assert 'rfs' in JOURNALS
        assert 'jfe' in JOURNALS
        assert JOURNALS['jf']['source_id'] == 'S5353659'

    def test_journal_groups_defined(self):
        from finance_papers import JOURNAL_GROUPS
        assert 'top3' in JOURNAL_GROUPS
        assert set(JOURNAL_GROUPS['top3']) == {'jf', 'rfs', 'jfe'}


class TestNormalization:
    """Test author name normalization."""

    def test_normalize_known_name(self):
        from finance_papers import normalize_name
        assert normalize_name('Jules H. van Binsbergen') == 'Jules van Binsbergen'

    def test_normalize_unknown_name(self):
        from finance_papers import normalize_name
        assert normalize_name('Unknown Author') == 'Unknown Author'

    def test_normalize_preserves_case(self):
        from finance_papers import normalize_name
        assert normalize_name('john doe') == 'john doe'


class TestDatabaseOperations:
    """Test database operations."""

    def test_get_db_files_returns_list(self, sample_db_dir, monkeypatch):
        from finance_papers import get_db_files
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        db_files = get_db_files()
        assert isinstance(db_files, list)
        assert len(db_files) >= 1

    def test_iter_articles(self, sample_db_dir, monkeypatch):
        from finance_papers import iter_articles, get_db_files
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        db_files = get_db_files()
        articles = list(iter_articles(db_files))

        assert len(articles) == 3
        assert articles[0]['title'] == 'Asset Pricing with Machine Learning'

    def test_db_connection_context_manager(self, sample_db):
        from finance_papers import db_connection

        with db_connection(sample_db) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM openalex_articles')
            count = cursor.fetchone()[0]

        assert count == 3


class TestAuthorRanking:
    """Test author ranking functionality."""

    def test_rank_authors(self, sample_db_dir, monkeypatch):
        from finance_papers import rank_authors
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        authors = rank_authors(top_n=10)

        assert len(authors) >= 1
        # Eugene Fama has 2 papers, should be first
        assert authors[0].name == 'Eugene Fama'
        assert authors[0].paper_count == 2

    def test_rank_authors_by_citations(self, sample_db_dir, monkeypatch):
        from finance_papers import rank_authors
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        authors = rank_authors(top_n=10, by_citations=True)

        assert len(authors) >= 1
        # Eugene Fama has most citations (150 + 75 = 225)
        assert authors[0].name == 'Eugene Fama'
        assert authors[0].citations == 225

    def test_highlighted_authors_initialized(self, sample_db_dir, monkeypatch):
        from finance_papers import rank_authors, HIGHLIGHTED_AUTHORS
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        authors = rank_authors(top_n=100)
        author_names = [a.name for a in authors]

        # Andreas Brøgger should appear in results (highlighted authors are initialized)
        assert 'Andreas Brøgger' in author_names


class TestPaperSearch:
    """Test paper search functionality."""

    def test_search_by_author(self, sample_db_dir, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        papers = search_papers(author='Fama')

        assert len(papers) == 2
        assert all('Fama' in str(p.authors) for p in papers)

    def test_search_by_title(self, sample_db_dir, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        papers = search_papers(title='Machine Learning')

        assert len(papers) == 1
        assert 'Machine Learning' in papers[0].title

    def test_get_author_papers(self, sample_db_dir, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        papers = search_papers(author='Eugene Fama')

        assert len(papers) == 2


class TestAbstractReconstruction:
    """Test abstract reconstruction from inverted index."""

    def test_reconstruct_abstract(self):
        from finance_papers import reconstruct_abstract

        inverted_index = {
            'This': [0],
            'is': [1],
            'a': [2],
            'test': [3],
            'abstract': [4]
        }

        result = reconstruct_abstract(inverted_index)
        assert result == 'This is a test abstract'

    def test_reconstruct_empty_abstract(self):
        from finance_papers import reconstruct_abstract

        result = reconstruct_abstract({})
        assert result == ''

    def test_reconstruct_none_abstract(self):
        from finance_papers import reconstruct_abstract

        result = reconstruct_abstract(None)
        assert result == ''


class TestDataTypes:
    """Test data type classes."""

    def test_author_dataclass(self):
        from finance_papers import Author

        author = Author(name='Test Author', paper_count=5, citations=100)
        assert author.name == 'Test Author'
        assert author.paper_count == 5
        assert author.citations == 100
        assert author.openalex_id is None

    def test_paper_dataclass(self):
        from finance_papers import Paper

        paper = Paper(title='Test Paper', authors=['Author 1'], year=2024)
        assert paper.title == 'Test Paper'
        assert paper.year == 2024
        assert paper.citations == 0  # default


class TestCLI:
    """Test CLI argument parsing."""

    def test_parse_years_single(self):
        from finance_papers.cli import parse_years

        assert parse_years('2024') == [2024]

    def test_parse_years_range(self):
        from finance_papers.cli import parse_years

        assert parse_years('2023-2025') == [2023, 2024, 2025]

    def test_parse_years_comma_separated(self):
        from finance_papers.cli import parse_years

        assert parse_years('2023,2024,2025') == [2023, 2024, 2025]

    def test_parse_years_none(self):
        from finance_papers.cli import parse_years

        assert parse_years(None) is None
        assert parse_years('') is None


class TestShortSource:
    """Test source name mapping for working papers."""

    def test_ssrn_source(self):
        from finance_papers.core import _short_source
        assert _short_source('SSRN Electronic Journal') == 'SSRN'

    def test_arxiv_source(self):
        from finance_papers.core import _short_source
        assert _short_source('arXiv (Cornell University)') == 'arXiv'

    def test_repec_source(self):
        from finance_papers.core import _short_source
        assert _short_source('RePEc: Research Papers in Economics') == 'RePEc'

    def test_nber_source(self):
        from finance_papers.core import _short_source
        assert _short_source('NBER Working Papers') == 'NBER'

    def test_unknown_source(self):
        from finance_papers.core import _short_source
        assert _short_source('Unknown Source') == 'WP'

    def test_none_source(self):
        from finance_papers.core import _short_source
        assert _short_source(None) == 'WP'

    def test_journal_in_wp(self):
        from finance_papers.core import _short_source
        assert _short_source('The Journal of Finance') == 'JF'
        assert _short_source('Review of Financial Studies') == 'RFS'


class TestWorkingPapersSearch:
    """Test working papers search functionality."""

    def test_search_working_papers_by_author(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = search_papers(author='Test Author', source='working-papers')

        assert len(papers) == 2
        assert all('Test Author' in p.authors for p in papers)

    def test_search_working_papers_by_title(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = search_papers(title='Climate', source='working-papers')

        assert len(papers) == 1
        assert 'Climate' in papers[0].title

    def test_working_papers_have_source(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = search_papers(source='working-papers')

        # Check that sources are mapped correctly
        sources = {p.journal for p in papers}
        assert 'SSRN' in sources
        assert 'arXiv' in sources
        assert 'RePEc' in sources

    def test_working_papers_have_topics(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = search_papers(source='working-papers')

        # All papers should have topics
        assert all(len(p.topics) > 0 for p in papers)


class TestTopicFiltering:
    """Test topic-based filtering."""

    def test_search_by_topic_articles(self, sample_db_dir, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        papers = search_papers(topic='Asset Pricing')

        assert len(papers) == 1
        assert 'Machine Learning' in papers[0].title

    def test_search_by_topic_working_papers(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import search_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = search_papers(topic='Climate', source='working-papers')

        assert len(papers) == 1
        assert 'Climate' in papers[0].title


class TestRecentPapers:
    """Test get_recent_papers functionality."""

    def test_recent_articles(self, sample_db_dir, monkeypatch):
        from finance_papers import get_recent_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir)

        papers = get_recent_papers(limit=2)

        assert len(papers) == 2
        # Should be sorted by date descending
        assert papers[0].pub_date >= papers[1].pub_date

    def test_recent_working_papers(self, sample_db_dir_with_wp, monkeypatch):
        from finance_papers import get_recent_papers
        import finance_papers.core as core
        monkeypatch.setattr(core, 'DB_DIR', sample_db_dir_with_wp)

        papers = get_recent_papers(source='working-papers', limit=2)

        assert len(papers) == 2
        # Should have proper source labels
        assert all(p.journal in ['SSRN', 'arXiv', 'RePEc', 'WP'] for p in papers)
