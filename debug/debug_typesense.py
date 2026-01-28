
import typesense
import os
import json

host = "localhost"
port = 8108
api_key = "ts-xyz123456"

print(f"Connecting to Typesense at {host}:{port}")

try:
    client = typesense.Client({
        "nodes": [{"host": host, "port": port, "protocol": "http"}],
        "api_key": api_key,
        "connection_timeout_seconds": 2,
    })

    healthy = client.operations.is_healthy()
    print(f"Health Check: {healthy}")
    
    collection_name = "movies"
    
    try:
        schema = client.collections[collection_name].retrieve()
        print(f"Collection '{collection_name}' FOUND.")
    except Exception as e:
        print(f"Collection '{collection_name}' NOT FOUND: {e}")
        exit()

    try:
        count_res = client.collections[collection_name].retrieve()
        print(f"Document Count: {count_res.get('num_documents')}")
    except Exception as e:
        print(f"Error counting: {e}")

    print("\n--- Searching 'Interstellar' ---")
    search_params = {
        "q": "Interstellar",
        "query_by": "title",
        "limit": 1
    }
    res = client.collections[collection_name].documents.search(search_params)
    print(f"Hits: {res.get('found', 0)}")
    for hit in res.get('hits', []):
        print(f"Match: {hit['document']['title']}")

    print("\n--- Searching 'inte' ---")
    search_params = {
        "q": "inte",
        "query_by": "title",
        "limit": 5
    }
    res = client.collections[collection_name].documents.search(search_params)
    print(f"Hits: {res.get('found', 0)}")
    for hit in res.get('hits', []):
        print(f"Match: {hit['document']['title']}")
        
    print("\n--- Searching 'inter' ---")
    search_params = {
        "q": "inter",
        "query_by": "title",
        "limit": 5
    }
    res = client.collections[collection_name].documents.search(search_params)
    print(f"Hits: {res.get('found', 0)}")
    for hit in res.get('hits', []):
        print(f"Match: {hit['document']['title']}")

except Exception as e:
    print(f"An error occurred: {e}")
