# System Architecture

## Overall Data Lake Organization

The Data Lake follows a Medallion Architecture, processing data from raw files to a refined Star Schema and finally indexing it for high-performance search.

```mermaid
graph TD
    subgraph DataLake["Data Lake Storage"]
        Bronze[("Bronze Layer<br>(Raw CSV/JSON)")]
        Silver[("Silver Layer<br>(Cleaned Parquet)")]
        Gold[("Gold Layer<br>(SQLite - Star Schema)")]
    end

    subgraph SearchEngine["Search Engine"]
        Typesense[("Typesense<br>(Vector DB)")]
    end

    subgraph Application["Application Layer"]
        API["FastAPI Backend"]
        Client["Client / End User"]
    end

    Input["External Data<br>(Movies Dataset)"] --> Bronze
    Bronze -->|"ETL Phase 1<br>(Cleaning & Validation)"| Silver
    Silver -->|"ETL Phase 2<br>(Transformation & Normalization)"| Gold
    Gold -->|"Indexing Service"| Typesense
    
    API -->|"SQL Queries"| Gold
    API -->|"Vector Search"| Typesense
    Client -->|"HTTP Requests"| API
```

## Layer 1: Bronze Layer (Raw Data)

The Bronze layer ingests raw data files. These files are kept in their original format to ensure data lineage and recoverability.

```mermaid
graph LR
    Input["Source Files"]
    subgraph Bronze["Bronze Directory"]
        B1["tmdb_5000_movies.csv"]
        B2["tmdb_5000_credits.csv"]
    end
    
    Input --> Bronze
    style Bronze fill:#cd7f32,stroke:#333,stroke-width:2px
```

## Layer 2: Silver Layer (Refined Data)

In the Silver layer, data is cleaned, validated, and converted to columnar storage (Parquet) for efficient processing. Dictionaries and lists are parsed, and types are cast.

```mermaid
graph LR
    subgraph Silver["Silver Directory"]
        S1["movies_silver.parquet"]
        S2["credits_silver.parquet"]
    end
    
    Processing["Pandas Transformation"]
    
    Bronze["Bronze Data"] --> Processing
    Processing --> Silver
    
    style Silver fill:#c0c0c0,stroke:#333,stroke-width:2px
```

## Layer 3: Gold Layer (Star Schema)

The Gold layer is organized into a Star Schema optimized for analytical queries and transactional retrieval. It consists of a central Fact table surrounded by Dimension tables.

```mermaid
erDiagram
    FACT_MOVIE_RELEASE }|--|| DIM_MOVIE : "describes"
    FACT_MOVIE_RELEASE }|--|| DIM_DATE : "happened_on"
    FACT_MOVIE_RELEASE }|--|| DIM_DIRECTOR : "directed_by"
    FACT_MOVIE_RELEASE }|--|| DIM_LANGUAGE : "spoken_in"
    FACT_MOVIE_RELEASE }|--|| DIM_COUNTRY : "produced_in"
    FACT_MOVIE_RELEASE }|--|| DIM_PRODUCTION_COMPANY : "produced_by"
    
    DIM_MOVIE ||--|{ BRIDGE_MOVIE_GENRE : "has"
    BRIDGE_MOVIE_GENRE }|--|| DIM_GENRE : "links_to"
    
    DIM_MOVIE {
        int id PK
        string title
        string overview
        string original_language
    }

    DIM_DATE {
        int id PK
        date date
        int year
        int month
        int day
    }

    DIM_GENRE {
        int id PK
        string name
    }

    DIM_DIRECTOR {
        int id PK
        string name
    }

    DIM_LANGUAGE {
        int id PK
        string language_name
    }

    DIM_COUNTRY {
        int id PK
        string country_name
    }

    DIM_PRODUCTION_COMPANY {
        int id PK
        string name
    }

    FACT_MOVIE_RELEASE {
        int id PK
        int movie_info_id FK
        int release_date_id FK
        int director_id FK
        int language_id FK
        int country_id FK
        int company_id FK
        float popularity
        float vote_average
        int vote_count
        int budget
        int revenue
        int runtime
    }

    BRIDGE_MOVIE_GENRE {
        int id PK
        int movie_id FK
        int genre_id FK
    }
```
