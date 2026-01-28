#  TMDB Top 5000 Movies API - REST API Data Engineering Project

A complete data engineering solution with ETL pipeline, REST API, vector search, and comprehensive testing.

## Features

- **Medallion Architecture**
- **Star Schema**: Normalized dimensional model (7 dims + 1 fact + 1 bridge)
- **ETL Pipeline**: Automated data transformation with Pandas
- **REST API**: 6+ endpoints with Basic Auth and pagination
- **Vector Search**: Full-text search with Typesense
- **Docker Support**: Docker Compose for production deployment
- **API Documentation**: Swagger/ReDoc with interactive examples

## Quick Start (Windows)

Prerequisites:
- [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Python 3.11+](https://www.python.org/downloads/windows/)
- [Git for Windows](https://git-scm.com/download/win)

### Option 1: Docker Compose (Recommended)

Run the following commands in PowerShell or Command Prompt:

```powershell
# Start all services (API + Typesense)
docker-compose up --build

# Access:
# - API: http://localhost:8000
# - API Docs: http://localhost:8000/docs
# - Typesense: http://localhost:8108
```

### Option 2: Local Development

1. **Create and Activate Virtual Environment**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   ```

2. **Install Dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Start API (ETL runs automatically)**
   ```powershell
   python run.py
   # API available at http://localhost:8000/docs
   ```

## API Authentication

Default credentials:
- **Username**: `admin` | **Password**: `password123`
- **Username**: `user` | **Password**: `user123`

## API Endpoints

### Movies
- `GET /api/v1/movies/` - List movies (paginated)
- `GET /api/v1/movies/{id}` - Get movie details
- `GET /api/v1/movies/search/by-title?title=<query>` - Search by title
- `GET /api/v1/movies/filter/by-genre?genre_id=<id>` - Filter by genre
- `GET /api/v1/movies/filter/by-popularity?min=<n>&max=<m>` - Filter by popularity
- `GET /api/v1/movies/filter/by-vote-average?min=<n>&max=<m>` - Filter by rating

### Search (Typesense)
- `GET /api/v1/search/movies?q=<query>` - Full-text search
- `GET /api/v1/search/stats` - Search index statistics

### System
- `GET /` - API info
- `GET /health` - Health check
- `GET /docs` - Swagger documentation
- `GET /redoc` - ReDoc documentation

## Project Structure

```
.
├── src/
│   ├── main.py                          # FastAPI app entry point
│   ├── api/
│   │   └── routes/
│   │       ├── movies.py               # Movies endpoints
│   │       └── search.py               # Search endpoints
│   ├── core/
│   │   └── auth.py                     # Authentication logic
│   ├── domain/
│   │   └── DomainEntities.py           # Business entities
│   ├── infrastructure/
│   │   ├── database.py                 # DB engine and session
│   │   ├── models/                     # SQLModel ORM tables
│   │   ├── repositories/               # Data access layer
│   │   ├── mappers.py                  # Entity mappers
│   │   ├── typesense_client.py         # Typesense integration
│   │   └── typesense_indexer.py        # Search indexing
│   └── services/
│       └── etl.py                      # ETL pipeline (3 phases)
│
├── debug/
│   ├── debug_db.py
│   ├── debug_typesense.py
│   ├── debug_syntax.py
│   └── debug_typesense_details.py
│
├── data/
│   ├── bronze/                         # Raw CSV files
│   ├── silver/                         # Cleaned Parquet files
│   └── gold/                           # Star schema (SQLite)
│
├── notebooks/                          # Jupyter notebooks
│
├── docker-compose.yml                  # Multi-container setup
├── Dockerfile                          # API container
├── requirements.txt                    # Dependencies
├── run.py                              # Application entry point
├── .python-version
├── .gitignore
└── README.md                           # This file
```

## Database Schema

### Dimension Tables (Normalized)
- `dim_date` - Release dates with year/month/day
- `dim_movie` - Movie metadata
- `dim_genre` - Genre catalog
- `dim_director` - Director profiles
- `dim_language` - Language codes
- `dim_country` - Country codes
- `dim_production_company` - Company profiles

### Fact Table
- `fact_movie_release` - Movie release events

### Bridge Table
- `bridge_movie_genre` - Many-to-many movie-genre relationships

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Framework | FastAPI | 0.104.1 |
| ORM | SQLModel | 0.0.14 |
| Database | SQLite | Latest |
| ETL | Pandas | 2.1.3 |
| Search | Typesense | 1.8.2 |
| Auth | HTTPBasic | Built-in |
| Container | Docker | Latest |

## Development

### Environment Variables

Create `.env` file in the root directory:
```ini
# Database
DATABASE_URL=sqlite:///./data/gold/warehouse.db

# Typesense
TYPESENSE_HOST=localhost
TYPESENSE_PORT=8108
TYPESENSE_API_KEY=ts-xyz123456

# Auth
AUTH_USER=admin
AUTH_PASSWORD=password123
```

### Database Reset

```powershell
python reset_db.py
```

### ETL Pipeline Details

Phase 1: Bronze → Silver
- Read CSV files
- Clean and validate data
- Save as Parquet (snappy compression)

Phase 2: Silver → Gold
- Build 7 dimension tables
- Build fact table
- Build bridge table
- Handle deduplication

Phase 3: Gold → SQLite
- Load Parquet files to database
- Create indices
- Establish relationships

## Performance

- ETL Pipeline: 2 minutes for entire dataset
- API Response: 50 ms average (depends on query complexity)
- Search Index: 1 minute for 5000 movies
- Database Size: 5 Mb (SQLite)

## Troubleshooting

### ETL Fails
```powershell
python reset_db.py     # Reset database
```

### Typesense Not Available
API continues without search functionality. Start Typesense:
```powershell
docker-compose up typesense
```

### Port Already in Use
```powershell
# Change ports in docker-compose.yml or .env
docker-compose up -e PORT=8001
```

## Author

**Lautaro Evaristo Mendez** - Data Engineer

## License

MIT License - See LICENSE file for details

## Version

**1.0.0** - Complete Data Lake Implementation