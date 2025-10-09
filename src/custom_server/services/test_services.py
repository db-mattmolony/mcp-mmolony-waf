"""
Test script to demonstrate the new service layer functionality.
This can be used for manual testing and verification.
"""

from query_repository import get_query_repository
from sql_service import QueryFormatter

def test_query_repository():
    """Test that the query repository returns the expected queries."""
    repo = get_query_repository()
    
    # Test that queries exist with the correct CO-XX-XX format
    test_keys = [
        "CO-01-01-table-formats",
        "CO-01-01-managed-tables", 
        "CO-01-02",
        "CO-01-03",
        "CO-01-04",
        "CO-01-06-serverless",
        "CO-01-06-sql",
        "CO-01-08",
        "CO-02-01",
        "CO-02-02",
        "CO-03-01",
        "CO-03-02-tagging",
        "CO-03-02-popular"
    ]
    
    print("Testing Query Repository:")
    print("=" * 50)
    
    for key in test_keys:
        query = repo.get_query(key)
        if query:
            print(f"✓ {key}: Found query ({len(query.strip())} chars)")
        else:
            print(f"✗ {key}: Query not found!")
    
    print(f"\nTotal queries in repository: {len(repo.list_queries())}")
    print("Query keys:", list(repo.list_queries().keys()))

def test_query_formatter():
    """Test the query formatter functions."""
    print("\nTesting Query Formatter:")
    print("=" * 50)
    
    # Test with mock data using the general formatter
    mock_table_data = [("DELTA", 150), ("PARQUET", 75), ("JSON", 25)]
    mock_column_names = ["table_format", "table_count"]
    result = QueryFormatter.format_default(mock_table_data, mock_column_names)
    print("General formatter test with column headers:")
    print(result)
    
    mock_percentage_data = [("MANAGED", 85), ("EXTERNAL", 15)]
    mock_column_names2 = ["table_type", "percentage"]
    result = QueryFormatter.format_default(mock_percentage_data, mock_column_names2)
    print("\nGeneral formatter test with different data:")
    print(result)

def demonstrate_service_usage():
    """Show how the new service layer simplifies function calls."""
    print("\nService Usage Demonstration:")
    print("=" * 50)
    
    repo = get_query_repository()
    
    print("Before (old approach):")
    print("- 30+ lines of boilerplate SQL connection code")
    print("- Inline query strings mixed with connection logic")
    print("- Repetitive error handling")
    print("- Manual result formatting")
    
    print("\nAfter (new service layer):")
    print("def COST_OPTIMISATION_C0_01_01_TABLE_TYPES():")
    print("    query = query_repo.get_query('CO-01-01-table-formats')")
    print("    return sql_service.execute_query_with_formatting(query)")
    print()
    print("Benefits:")
    print("- 2 lines instead of 30+")
    print("- Clean separation of concerns")
    print("- Centralized query management")
    print("- General formatter with column headers")
    print("- Easy to test and maintain")

if __name__ == "__main__":
    test_query_repository()
    test_query_formatter()
    demonstrate_service_usage()
