#!/usr/bin/env python3
"""Test script to verify database path configuration"""

import os
import sys
from save_db import save_articles_to_db

# Test data - note: no escaped characters
test_articles = [
    {
        "title": "Test Article for Path Verification",
        "authors": "Test Author",
        "abstract": "This is a test article to verify database path configuration.",
        "article_link": "https://example.com/test",
        "volume": "test",
        "issue": "test"
    }
]

def test_database_path():
    """Test that the database path works correctly"""
    
    # Test saving to database
    print("Testing database save operation...")
    try:
        new_articles, duplicates = save_articles_to_db(
            test_articles, 
            journal="test", 
            volume="test", 
            issue="test"
        )
        print(f"✅ Save successful: {new_articles} new articles, {duplicates} duplicates")
    except Exception as e:
        print(f"❌ Save failed: {e}")
        return False
    
    # Check if database file exists at expected location
    expected_db_path = "../out/data/articles.db"
    if os.path.exists(expected_db_path):
        print(f"✅ Database file exists at: {expected_db_path}")
    else:
        print(f"❌ Database file not found at: {expected_db_path}")
        return False
    
    print("🎉 Database path configuration test passed!")
    return True

if __name__ == "__main__":
    test_database_path()
