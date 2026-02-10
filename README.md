# Funda Property Finder

A Dutch real estate analysis tool that scrapes property listings from [funda.nl](https://www.funda.nl), stores them in a local database, and identifies potentially undervalued properties through statistical analysis.

## Features

- **Composite scraper** with automatic fallback: mobile API (`pyfunda`) as primary, HTML scraping (`funda-scraper`) as backup
- **PostgreSQL/SQLite database** with SQLAlchemy ORM for persistent storage and price history tracking
- **Analysis engine** that scores properties against comparable listings to find undervalued ones
- **FastAPI dashboard** with property browser, price history charts, and market statistics

## Project Structure

```
funda_finder/
├── scraper/          # Composite scraper (pyfunda + funda-scraper fallback)
│   ├── base.py       # Unified scraper interface & data models
│   ├── pyfunda.py    # Primary: mobile API via pyfunda
│   ├── html.py       # Fallback: HTML scraping via funda-scraper
│   └── composite.py  # Auto-fallback orchestration
├── db/               # Database layer
│   ├── models.py     # SQLAlchemy models (Property, PriceHistory, ScrapeMeta)
│   ├── session.py    # Database connection factory
│   └── migrations/   # Alembic migrations
├── analysis/         # Property analysis
│   ├── comparables.py  # Comparable property grouping
│   ├── scoring.py      # Undervalue scoring engine
│   └── stats.py        # Market statistics
├── api/              # FastAPI web dashboard
│   ├── app.py        # FastAPI application
│   ├── routes/       # API route handlers
│   └── templates/    # Jinja2 HTML templates
├── config.py         # Settings (pydantic-settings)
└── cli.py            # CLI entrypoint
```

## Quick Start

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run a scrape
python -m funda_finder.cli scrape --city amsterdam --type buy

# Start the dashboard
python -m funda_finder.cli serve
```

## Tech Stack

| Component   | Technology                          |
|-------------|-------------------------------------|
| Scraping    | pyfunda (primary), funda-scraper (fallback) |
| Database    | PostgreSQL (default) or SQLite + SQLAlchemy + Alembic |
| Analysis    | pandas, numpy                       |
| Dashboard   | FastAPI + Jinja2 + htmx + Chart.js  |
| Config      | pydantic-settings                   |

## Configuration

Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

Key settings:
- `FUNDA_DB_URL` - Database connection URL (default: `postgresql://localhost/funda`)
- `FUNDA_DB_PATH` - SQLite database file location (fallback: `data/funda.db`)
- `FUNDA_RATE_LIMIT` - Seconds between scrape requests (default: `3`)
- `FUNDA_DEFAULT_CITIES` - Comma-separated list of cities to scrape

### Database Management

```bash
# Check which database you're using
funda-finder db info

# Clear all data from the database
funda-finder db clear

# Reset database connections
funda-finder db reset
```

See [DATABASE_MANAGEMENT.md](DATABASE_MANAGEMENT.md) for detailed information about database configuration and troubleshooting.

## License

MIT
