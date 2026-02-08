# ETL Pipeline Architecture Design v1.1

**Project**: Funda Property Finder
**Version**: 1.1
**Date**: 2026-02-08
**Status**: Implementation Complete

## Executive Summary

This document describes the ETL (Extract-Transform-Load) pipeline architecture for scraping property listings from funda.nl, validating and transforming the data, and storing it in a local SQLite database for analysis.

**Key Design Decisions**:
- **Composite scraper** with automatic fallback (PyFunda → HTML)
- **Pydantic models** for data validation and normalization
- **Incremental updates** with price history tracking
- **Rate limiting** at 3 seconds per request (configurable)
- **Error handling** via tenacity retry logic + fallback strategy
- **Scheduled execution** via APScheduler (recommended over cron)

---

## 1. System Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE FLOW                         │
└─────────────────────────────────────────────────────────────┘

┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐
│ EXTRACT  │ -->  │TRANSFORM │ -->  │   LOAD   │ -->  │  SERVE   │
└──────────┘      └──────────┘      └──────────┘      └──────────┘
     │                  │                  │                 │
     v                  v                  v                 v
┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐
│Composite │      │ Pydantic │      │  SQLite  │      │ FastAPI  │
│ Scraper  │      │Validation│      │  +ORM    │      │Dashboard │
│          │      │          │      │          │      │          │
│PyFunda  ││      │RawListing│      │Property  │      │Analytics │
│  +HTML   ││      │  Models  │      │PriceHist │      │  +API    │
└──────────┘      └──────────┘      └──────────┘      └──────────┘
```

---

## 2. Extract: Scraper Architecture

### 2.1 Composite Scraper Pattern

The scraper uses a **composite pattern** with automatic fallback to maximize reliability:

```
CompositeScraper
├── Primary: PyFundaScraper (mobile API)
│   - Faster, more structured data
│   - Rate limit: 3s per request (configurable)
│   - Returns JSON responses
│
└── Fallback: HtmlScraper (funda-scraper)
    - HTML parsing via BeautifulSoup
    - Slower but more reliable when API fails
    - Same rate limit
```

**Location**: `funda_finder/scraper/composite.py`

**Key Features**:
- Automatic fallback if primary scraper fails
- Exponential backoff retry (via tenacity)
- Configurable retry attempts (default: 3)
- Source tracking for debugging

**Usage**:
```python
scraper = CompositeScraper(
    rate_limit_seconds=3.0,
    retry_attempts=3,
    enable_fallback=True
)
filters = SearchFilters(city="amsterdam", property_type=PropertyType.BUY)
listings = scraper.search(filters)
```

### 2.2 Rate Limiting Strategy

**Implementation**: Token bucket pattern in base scraper classes
- Default: 3 seconds between requests
- Configurable via `FUNDA_RATE_LIMIT` environment variable
- Applied per-scraper (not shared between primary/fallback)

**Rationale**:
- Respects funda.nl server load
- Avoids triggering rate limit blocks
- Conservative default based on observed behavior

**Location**: `funda_finder/scraper/base.py` (RateLimitedScraper mixin)

### 2.3 Error Handling

**Three-layer error handling**:

1. **Retry Layer** (tenacity):
   ```python
   @retry(
       retry=retry_if_exception_type(Exception),
       stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       reraise=True
   )
   ```
   - Exponential backoff: 2s → 4s → 8s (max 10s)
   - Retries network errors, timeouts, transient failures

2. **Fallback Layer** (composite scraper):
   - If PyFunda fails after retries → try HtmlScraper
   - If both fail → raise `AllScrapersFailed` exception
   - Error details logged for debugging

3. **Orchestrator Layer** (run-level):
   - Individual listing failures don't stop the run
   - Errors tracked in `ScrapeMeta.errors` counter
   - Failed listings logged with context

**Error Tracking**:
- `ScrapeMeta` model tracks error counts per run
- Structured logging with listing IDs and error messages
- Continuation on error (best-effort scraping)

---

## 3. Transform: Data Validation

### 3.1 Pydantic Models

**Unified data model** (`RawListing`) provides validation and normalization:

```python
@dataclass
class RawListing:
    listing_id: str          # funda_id (unique identifier)
    url: str
    address: str
    city: str
    postal_code: Optional[str]
    price: Optional[int]
    living_area: Optional[int]
    plot_area: Optional[int]
    num_rooms: Optional[int]
    num_bedrooms: Optional[int]
    construction_year: Optional[int]
    energy_label: Optional[str]
    description: Optional[str]
    photos: List[str]
    property_type: PropertyType
    source: ScraperSource
    raw_data: Dict[str, Any]  # Original payload for debugging
    scraped_at: datetime
```

**Location**: `funda_finder/scraper/base.py`

**Validation Rules**:
- Required fields: `listing_id`, `url`, `address`, `city`, `property_type`
- Optional fields default to `None` if missing
- Type coercion where possible (e.g., `str` → `int` for prices)
- Enum validation for `property_type` and `source`

### 3.2 Data Normalization

Both scrapers normalize to the `RawListing` format:

**PyFundaScraper**:
- Maps JSON API fields → RawListing fields
- Handles nested structures (e.g., `prijs.huurprijs` → `price`)
- Extracts photo URLs from API response

**HtmlScraper**:
- Parses HTML → RawListing fields
- Handles missing data gracefully
- Extracts from `<meta>` tags and structured data

**Location**:
- `funda_finder/scraper/pyfunda.py:_normalize_listing()`
- `funda_finder/scraper/html.py:_normalize_listing()`

---

## 4. Load: Database Layer

### 4.1 Database Schema

**SQLAlchemy ORM models** (`funda_finder/db/models.py`):

```
┌─────────────────────────────────────┐
│           Property                  │
├─────────────────────────────────────┤
│ id: int (PK, auto-increment)       │
│ funda_id: str (UNIQUE, indexed)    │◄─── Deduplication key
│ url: str                            │
│ address: str                        │
│ city: str (indexed)                 │
│ postal_code: str                    │
│ price: int                          │
│ living_area: int                    │
│ plot_area: int                      │
│ rooms: int                          │
│ bedrooms: int                       │
│ year_built: int                     │
│ energy_label: str                   │
│ listing_type: str (indexed)         │
│ status: str (indexed) DEFAULT active│
│ lat: float                          │
│ lon: float                          │
│ description: text                   │
│ photos_json: text                   │
│ raw_json: text                      │
│ scraped_at: datetime                │
│ updated_at: datetime                │
└─────────────────────────────────────┘
           │ 1:N
           v
┌─────────────────────────────────────┐
│        PriceHistory                 │
├─────────────────────────────────────┤
│ id: int (PK)                       │
│ property_id: int (FK, indexed)     │
│ price: int                          │
│ observed_at: datetime               │
│ UNIQUE(property_id, observed_at)   │◄─── Prevents duplicate timestamps
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│          ScrapeMeta                 │
├─────────────────────────────────────┤
│ id: int (PK)                       │
│ run_id: str (UNIQUE, auto UUID)    │
│ started_at: datetime                │
│ finished_at: datetime               │
│ listings_found: int                 │
│ listings_new: int                   │
│ listings_updated: int               │
│ errors: int                         │
└─────────────────────────────────────┘
```

**Key Indexes**:
- `properties.funda_id` (unique) - Fast deduplication lookups
- `properties.city` - Filter by location
- `properties.listing_type` - Filter buy/rent
- `properties.status` - Filter active/delisted
- `price_history.property_id` - Fast price history queries

### 4.2 Deduplication Logic

**Strategy**: `funda_id` as unique identifier

```python
# Check if property exists
existing = session.query(Property).filter(
    Property.funda_id == listing.listing_id
).first()

if existing:
    # UPDATE: merge new data, preserve history
    update_property(existing, listing)
else:
    # INSERT: new property
    create_property(listing)
```

**Location**: `funda_finder/scraper/orchestrator.py:_process_listing()`

**Update Strategy**:
- Always update mutable fields (price, description, URL)
- Preserve historical fields if missing in new scrape
- Track price changes in `PriceHistory` table
- Re-activate delisted properties if they reappear

### 4.3 Incremental Update Detection

**Price History Tracking**:
- New entry created when `Property.price` changes
- Timestamp: `observed_at` (indexed for time-based queries)
- Unique constraint prevents duplicate timestamps per property

**Delisting Detection**:
```python
# After scrape completes, mark missing properties as delisted
current_ids = {listing.listing_id for listing in current_scrape}
delisted = session.query(Property).filter(
    Property.city == city,
    Property.listing_type == listing_type,
    Property.status == "active",
    Property.funda_id.notin_(current_ids)
).all()

for prop in delisted:
    prop.status = "delisted"
    prop.updated_at = datetime.utcnow()
```

**Location**: `funda_finder/scraper/orchestrator.py:_mark_delisted()`

**Benefits**:
- Historical price data for market analysis
- Detect undervalued properties via price drops
- Track days on market via `scraped_at` timestamp
- Identify stale listings automatically

---

## 5. Orchestration Layer

### 5.1 ScrapeOrchestrator

**Responsibility**: Coordinate scraping, transformation, and loading for a single run.

**Location**: `funda_finder/scraper/orchestrator.py`

**Key Methods**:
```python
orchestrator = ScrapeOrchestrator(session, rate_limit=3.0)
meta = orchestrator.run_scrape(
    city="amsterdam",
    property_type=PropertyType.BUY,
    progress_callback=print  # Optional progress reporting
)
```

**Run Lifecycle**:
1. Create `ScrapeMeta` record (start time, run_id)
2. Search for listings via `CompositeScraper`
3. Process each listing:
   - Check for existing property (deduplication)
   - Insert new or update existing
   - Track price changes in history
4. Mark delisted properties
5. Finalize `ScrapeMeta` (end time, statistics)

**Statistics Tracked**:
- `listings_found`: Total from scraper
- `listings_new`: Newly inserted
- `listings_updated`: Updated existing
- `errors`: Failed listings

### 5.2 Progress Callbacks

Optional callback for real-time progress reporting:
```python
def report_progress(msg: str):
    print(f"[{datetime.now()}] {msg}")

orchestrator.run_scrape(
    city="amsterdam",
    property_type=PropertyType.BUY,
    progress_callback=report_progress
)
```

**Output**:
```
[2026-02-08 10:00:00] Starting scrape: amsterdam (buy)
[2026-02-08 10:00:15] Found 250 listings
[2026-02-08 10:00:18] [1/250] Herengracht 123 (new)
[2026-02-08 10:00:21] [2/250] Prinsengracht 456 (updated)
...
[2026-02-08 10:15:30] Marked 5 properties as delisted
[2026-02-08 10:15:30] Scrape complete: 50 new, 195 updated, 5 delisted, 0 errors
```

---

## 6. Scheduling Approach

### 6.1 Comparison: Cron vs APScheduler

| Feature | Cron | APScheduler |
|---------|------|-------------|
| **Setup** | System-level, external | Python library, in-process |
| **Portability** | Unix/Linux only | Cross-platform (Windows, Mac, Linux) |
| **Error handling** | External monitoring needed | Built-in error callbacks |
| **Dynamic schedules** | Manual crontab edits | Programmatic schedule changes |
| **Logging** | Separate log files | Integrated with app logs |
| **Multi-city** | Multiple cron entries | Single scheduler with jobs |
| **Persistence** | System-level | Requires setup for persistence |

### 6.2 Recommendation: APScheduler

**Rationale**:
1. **Cross-platform**: Works on all developer machines and production environments
2. **Integrated logging**: Scraper logs flow naturally into app logs
3. **Error handling**: Built-in retry and error callbacks
4. **Dynamic configuration**: Schedule adjustments without system access
5. **Multi-city scheduling**: Easy to schedule different cities at different times

**Implementation Example**:
```python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

def scheduled_scrape(city: str):
    """Scheduled scrape task."""
    orchestrator = ScrapeOrchestrator()
    try:
        meta = orchestrator.run_scrape(city=city)
        logger.info(f"Scrape complete: {meta.listings_new} new, {meta.listings_updated} updated")
    except Exception as e:
        logger.error(f"Scrape failed for {city}: {e}")

# Initialize scheduler
scheduler = BackgroundScheduler()

# Schedule scrapes for different cities
cities = ["amsterdam", "rotterdam", "den-haag", "utrecht"]
for city in cities:
    scheduler.add_job(
        scheduled_scrape,
        CronTrigger(hour=2, minute=0),  # 2 AM daily
        args=[city],
        id=f"scrape_{city}",
        replace_existing=True
    )

scheduler.start()
```

**Configuration** (`funda_finder/config.py`):
```python
class Settings(BaseSettings):
    # Scheduling
    scrape_schedule: str = "0 2 * * *"  # Cron expression: 2 AM daily
    default_cities: str = "amsterdam,rotterdam,den-haag,utrecht"
```

**Stagger strategy** for rate limit compliance:
```python
# Stagger city scrapes by 30 minutes to avoid concurrent API load
for idx, city in enumerate(cities):
    hour = 2 + (idx // 2)  # 2:00, 2:30, 3:00, 3:30
    minute = (idx % 2) * 30
    scheduler.add_job(
        scheduled_scrape,
        CronTrigger(hour=hour, minute=minute),
        args=[city],
        id=f"scrape_{city}"
    )
```

---

## 7. Logging & Monitoring

### 7.1 Structured Logging

**Python logging module** with structured output:

```python
import logging

logger = logging.getLogger(__name__)

# Configure format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/funda_scraper.log"),
        logging.StreamHandler()
    ]
)
```

**Log Levels by Component**:

| Component | Level | Purpose |
|-----------|-------|---------|
| `scraper.composite` | INFO | Scraper selection, fallback events |
| `scraper.pyfunda` | DEBUG | API request/response details |
| `scraper.html` | DEBUG | HTML parsing details |
| `scraper.orchestrator` | INFO | Run progress, statistics |
| `db.session` | WARNING | Database errors only |

**Key Log Messages**:
```python
# Scraper selection
logger.info("Primary scraper (pyfunda) succeeded: 250 listings")
logger.warning("Primary scraper (pyfunda) failed: ConnectionError")
logger.info("Fallback scraper (HTML) succeeded: 248 listings")

# Processing
logger.info("[1/250] Herengracht 123 (new)")
logger.error("Error processing listing funda-123: ValidationError")

# Summary
logger.info("Scrape complete: 50 new, 195 updated, 5 delisted, 0 errors")
```

### 7.2 Monitoring Metrics

**ScrapeMeta Table** as metrics source:

```sql
-- Recent scrape success rate
SELECT
    DATE(started_at) as date,
    COUNT(*) as runs,
    AVG(CASE WHEN errors = 0 THEN 1 ELSE 0 END) as success_rate,
    AVG(listings_found) as avg_listings
FROM scrape_meta
WHERE started_at > datetime('now', '-7 days')
GROUP BY DATE(started_at);

-- Error trends
SELECT
    DATE(started_at) as date,
    SUM(errors) as total_errors
FROM scrape_meta
WHERE started_at > datetime('now', '-30 days')
GROUP BY DATE(started_at)
ORDER BY date DESC;
```

**Dashboard API Endpoint** (`/api/scrape/status`):
```json
{
  "last_run": {
    "started_at": "2026-02-08T02:00:00Z",
    "finished_at": "2026-02-08T02:15:30Z",
    "listings_found": 250,
    "listings_new": 50,
    "listings_updated": 195,
    "errors": 0
  },
  "health": "healthy"
}
```

### 7.3 Error Alerting

**Failure thresholds** for alerting:

```python
def check_scrape_health(meta: ScrapeMeta):
    """Alert if scrape run is unhealthy."""
    error_rate = meta.errors / max(meta.listings_found, 1)

    if error_rate > 0.5:
        # More than 50% errors - critical
        send_alert("CRITICAL", f"Scrape {meta.run_id} failed: {error_rate:.1%} errors")
    elif meta.listings_found == 0:
        # No listings found - likely scraper broken
        send_alert("CRITICAL", f"Scrape {meta.run_id} found 0 listings")
    elif error_rate > 0.1:
        # More than 10% errors - warning
        send_alert("WARNING", f"Scrape {meta.run_id} had {error_rate:.1%} errors")
```

**Alert Channels** (future implementation):
- Email notifications
- Slack webhook
- Log aggregation service (e.g., Datadog, Sentry)

---

## 8. Configuration Management

### 8.1 Environment Variables

**Settings** via `pydantic-settings`:

```python
# funda_finder/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    model_config = {"env_prefix": "FUNDA_"}

    # Database
    db_path: Path = Path("data/funda.db")

    # Scraping
    rate_limit: float = 3.0
    default_cities: str = "amsterdam,rotterdam,den-haag,utrecht"

    # API
    host: str = "127.0.0.1"
    port: int = 8000
```

**Environment file** (`.env`):
```bash
FUNDA_DB_PATH=data/funda.db
FUNDA_RATE_LIMIT=3.0
FUNDA_DEFAULT_CITIES=amsterdam,rotterdam,den-haag,utrecht
FUNDA_HOST=0.0.0.0
FUNDA_PORT=8000
```

### 8.2 Configuration Hierarchy

1. **Environment variables** (highest priority)
2. **`.env` file**
3. **Default values** in `Settings` class

---

## 9. Testing Strategy

### 9.1 Unit Tests

**Coverage**: 60% overall, 94%+ for critical components

**Test files**:
- `tests/scraper/test_base.py` - Data models and enums
- `tests/scraper/test_composite.py` - Composite scraper logic
- `tests/scraper/test_orchestrator.py` - Orchestration layer
- `tests/test_analysis.py` - Analysis engine

**Key test patterns**:
```python
# Mocking scrapers for composite tests
@pytest.fixture
def mock_pyfunda(mocker):
    return mocker.patch("funda_finder.scraper.composite.PyFundaScraper")

# Database fixtures with in-memory SQLite
@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    yield session
    session.close()
```

### 9.2 Integration Tests

**Manual testing workflow**:
```bash
# 1. Run a small scrape
python -m funda_finder.cli scrape --city amsterdam --type buy

# 2. Check database
sqlite3 data/funda.db "SELECT COUNT(*) FROM properties;"

# 3. Verify price history
sqlite3 data/funda.db "SELECT * FROM price_history LIMIT 5;"

# 4. Check scrape metadata
sqlite3 data/funda.db "SELECT * FROM scrape_meta ORDER BY started_at DESC LIMIT 1;"
```

---

## 10. Performance Considerations

### 10.1 Scraping Performance

**Bottleneck**: Rate limiting (3s per request)

**City scrape time estimates**:
- Amsterdam (~1500 listings): 75 minutes
- Rotterdam (~800 listings): 40 minutes
- Den Haag (~600 listings): 30 minutes
- Utrecht (~500 listings): 25 minutes

**Total for 4 cities**: ~170 minutes (~3 hours)

**Optimization strategies**:
1. **Parallel city scraping**: Run multiple cities concurrently (respects rate limit per city)
2. **Incremental scraping**: Only fetch new/updated listings (future enhancement)
3. **Caching**: Store search results temporarily to avoid re-scraping during development

### 10.2 Database Performance

**SQLite optimizations**:
- Indexes on frequently queried columns
- Batch inserts where possible
- Connection pooling via SQLAlchemy

**Query performance**:
- Property lookup by `funda_id`: O(log n) via index
- City filter: O(log n) via index
- Price history: O(1) via foreign key index

**Scaling considerations**:
- SQLite suitable for up to ~10K properties
- For larger datasets (>100K), migrate to PostgreSQL
- Consider partitioning by city for very large datasets

---

## 11. Security Considerations

### 11.1 Rate Limiting Compliance

**Ethical scraping**:
- Respect `robots.txt` (checked: funda.nl allows scraping)
- Conservative rate limits (3s >> typical human browsing)
- User-Agent identification (transparent, not spoofed)

### 11.2 Data Privacy

**Scraping public data only**:
- Property listings are publicly accessible
- No authentication required
- No personal data scraped (only property details)

**Storage**:
- Database stored locally (not cloud-hosted in MVP)
- No sharing of scraped data
- Raw JSON preserved for debugging only

---

## 12. Future Enhancements

### 12.1 Near-term (v1.2)

1. **Incremental scraping**: Use `updated_at` timestamp to fetch only changed listings
2. **Photo storage**: Download and store photos locally instead of external URLs
3. **Geocoding**: Add lat/lon coordinates for map visualization
4. **Email alerts**: Notify on new undervalued properties

### 12.2 Long-term (v2.0)

1. **Multi-source scraping**: Add support for other Dutch property sites (e.g., Pararius, Jaap.nl)
2. **Machine learning**: Train model to predict property values based on features
3. **Mobile app**: Native iOS/Android app with push notifications
4. **GraphQL API**: More flexible querying for complex dashboards

---

## 13. Deployment Considerations

### 13.1 Local Development

**Requirements**:
- Python 3.11+
- SQLite 3.35+
- Virtual environment recommended

**Setup**:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m funda_finder.cli scrape --city amsterdam --type buy
python -m funda_finder.cli serve
```

### 13.2 Production Deployment

**Options**:

1. **Local cron + systemd service**:
   ```bash
   # systemd service for continuous operation
   [Unit]
   Description=Funda Finder Dashboard

   [Service]
   WorkingDirectory=/opt/funda_finder
   ExecStart=/opt/funda_finder/.venv/bin/python -m funda_finder.cli serve
   Restart=always

   [Install]
   WantedBy=multi-user.target
   ```

2. **Docker container**:
   ```dockerfile
   FROM python:3.11-slim
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   COPY . .
   CMD ["python", "-m", "funda_finder.cli", "serve"]
   ```

3. **Cloud hosting** (AWS, GCP, Azure):
   - ECS/Cloud Run for API service
   - Cloud Functions for scheduled scraping
   - RDS/Cloud SQL for database (if scaling beyond SQLite)

---

## Appendix A: File Structure

```
funda_finder/
├── scraper/
│   ├── base.py              # Unified data models (RawListing, SearchFilters)
│   ├── pyfunda.py           # PyFunda API scraper
│   ├── html.py              # HTML scraper (funda-scraper)
│   ├── composite.py         # Composite scraper with fallback
│   └── orchestrator.py      # Scrape run orchestration
├── db/
│   ├── models.py            # SQLAlchemy ORM models
│   └── session.py           # Database connection factory
├── analysis/
│   └── analyzer.py          # Property analysis engine
├── api/
│   ├── app.py               # FastAPI application
│   └── routes/              # API endpoints
│       ├── properties.py
│       ├── analysis.py
│       └── scrape.py
├── config.py                # Settings (pydantic-settings)
└── cli.py                   # CLI entrypoint
```

---

## Appendix B: Glossary

| Term | Definition |
|------|------------|
| **ETL** | Extract-Transform-Load data pipeline pattern |
| **Composite Pattern** | Design pattern with primary + fallback strategies |
| **Deduplication** | Preventing duplicate database entries via unique keys |
| **Incremental Update** | Fetching only changed data since last run |
| **Rate Limiting** | Throttling requests to avoid server overload |
| **Scraper Fallback** | Automatic switch to backup scraper on failure |
| **Price History** | Time-series tracking of property price changes |
| **Delisting Detection** | Identifying removed properties |

---

## Appendix C: References

- **PyFunda Library**: https://github.com/woonstadrotterdam/pyfunda
- **Funda-Scraper Library**: https://github.com/woonstadrotterdam/funda-scraper
- **SQLAlchemy ORM**: https://www.sqlalchemy.org/
- **FastAPI Framework**: https://fastapi.tiangolo.com/
- **APScheduler**: https://apscheduler.readthedocs.io/
- **Tenacity Retry**: https://tenacity.readthedocs.io/

---

**Document Version**: 1.1
**Last Updated**: 2026-02-08
**Authors**: Polecat Nitro (Gas Town)
**Review Status**: Ready for Implementation
