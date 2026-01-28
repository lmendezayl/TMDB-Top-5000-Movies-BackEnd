
import typesense
import os
import json

# Configuration
host = "localhost"
port = 8108
api_key = "ts-xyz123456"

try:
    client = typesense.Client({
        "nodes": [{"host": host, "port": port, "protocol": "http"}],
        "api_key": api_key,
        "connection_timeout_seconds": 2,
    })

    collection_name = "movies"
    query = "Interstellar"
    
    print(f"Testing filter syntaxes for '{query}'...")

    # Test 1: Current implementation [min, max] (Likely 'IN' operator)
    print("\n--- Test 1: 'vote_average:[0, 10]' ---")
    try:
        res = client.collections[collection_name].documents.search({
            "q": query, "query_by": "title", "filter_by": "vote_average:[0, 10]"
        })
        print(f"Hits: {res.get('found', 0)}")
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: Range syntax [min..max]
    print("\n--- Test 2: 'vote_average:[0..10]' ---")
    try:
        res = client.collections[collection_name].documents.search({
            "q": query, "query_by": "title", "filter_by": "vote_average:[0.0..10.0]"
        })
        print(f"Hits: {res.get('found', 0)}")
    except Exception as e:
        print(f"Error: {e}")

    # Test 3: Comparison syntax
    print("\n--- Test 3: 'vote_average:>=0 && vote_average:<=10' ---")
    try:
        res = client.collections[collection_name].documents.search({
            "q": query, "query_by": "title", "filter_by": "vote_average:>=0 && vote_average:<=10"
        })
        print(f"Hits: {res.get('found', 0)}")
    except Exception as e:
        print(f"Error: {e}")

except Exception as e:
    print(f"An error occurred: {e}")
