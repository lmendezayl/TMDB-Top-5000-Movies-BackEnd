
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

    collection_name = "movies"
    
    print("\n--- Searching 'Interstellar' with details ---")
    search_params = {   
        "q": "Interstellar",
        "query_by": "title",
        "limit": 1
    }
    res = client.collections[collection_name].documents.search(search_params)
    print(f"Hits: {res.get('found', 0)}")
    for hit in res.get('hits', []):
        doc = hit['document']
        print(f"Title: {doc.get('title')}")
        print(f"Popularity: {doc.get('popularity')}")
        print(f"Vote Average: {doc.get('vote_average')}")
        print(f"Full Doc: {json.dumps(doc, indent=2)}")

    print("\n--- Searching 'Interstellar' with filter 'vote_average:[0, 10]' ---")
    search_params = {
        "q": "Interstellar",
        "query_by": "title",
        "filter_by": "vote_average:[0, 10]",
        "limit": 1
    }
    res = client.collections[collection_name].documents.search(search_params)
    print(f"Hits with filter: {res.get('found', 0)}")


except Exception as e:
    print(f"An error occurred: {e}")
