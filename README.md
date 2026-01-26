
# TMDB REST API Project

A data engineering project that processes and transforms The Movie Database (TMDB) data through a Medallion data lake architecture.

## Project Structure

```
├── data/
│   ├── bronze/          # Raw data (TMDB CSV files)
│   ├── silver/          # Cleaned and validated data
│   └── gold/            # Aggregated business-ready data
├── notebooks/           # Exploratory analysis (Jupyter)
└── src/                 # Application code
    ├── api/             # API routes and endpoints
    ├── controllers/      # Request handlers
    ├── domain/          # Business logic
    ├── infrastructure/  # Database and ORM models
    │   └── models/      # SQLAlchemy table definitions
    └── services/        # Data processing services
```

## Data Sources

- `tmdb_5000_credits.csv` - Cast and crew information
- `tmdb_5000_movies.csv` - Movie metadata

## Getting Started

cambiar esto cuando terminemos

## Architecture

Star schema modeling with:
- **Dimensions**: Movie, Genre, Director, Country, Date, Production Company
- **Facts**: Movie Release events
- **Bridge Tables**: Movie-Genre relationships

## Technologies

- Python
- Pandas / SQLAlchemy
- Jupyter Notebooks
- SQLite
- FastAPI
- Docker / Docker Compose

## Development Setup

### Prerequisites
- Python 3.11+
- SQLite 
- Docker & Docker Compose

### Installation

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate it: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Configure database connection in `.env`

### Running the Pipeline

```bash
python src/main.py
```

Access the API at `http://localhost:8000/docs` for interactive documentation.

## Key Features

- **Data Validation**: Schema and quality checks at each layer
- **Incremental Processing**: Handles new and updated records efficiently
- **REST API**: Query transformed data through FastAPI endpoints
- **Scalability**: Containerized for easy deployment

## Contributing

Follow PEP 8 standards and include unit tests for new features.

## Author 
Lautaro Evaristo Mendez. Data Engineer.