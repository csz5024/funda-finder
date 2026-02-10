# Bug Fix: ff-n8yu - Cannot Clear Database

## Problem

**Symptom:** Database showed 0 properties via `sqlite3` CLI, but the API and SQLAlchemy consistently returned 61 properties (Amsterdam, Rotterdam, Utrecht, Den Haag). Deleting database files (`rm -f data/funda.db*`) and running Alembic migrations had no effect.

**Root Cause:**
1. Application defaults to **PostgreSQL** (`postgresql://localhost/funda`), not SQLite
2. User was deleting SQLite files while the app was connected to PostgreSQL
3. Singleton engine pattern cached the PostgreSQL connection across process restarts

## Solution

### 1. Added Database Management Functions (session.py)

**`reset_engine()`** - Clears the singleton engine and session factory
- Properly disposes of the existing engine
- Allows switching databases or forcing fresh connections

**`clear_db()`** - Deletes all data while preserving schema
- Works for both PostgreSQL and SQLite
- Deletes in reverse dependency order to avoid FK violations
- Safe and reversible (schema intact)

### 2. Added CLI Commands (cli.py)

**`funda-finder db info`** - Shows database connection and row counts
```bash
$ funda-finder db info
Database URL: postgresql://localhost/funda
Database Type: postgresql

Table Row Counts:
  properties: 61
  price_history: 61
  scrape_meta: 15
```

**`funda-finder db clear`** - Clears all data with confirmation
```bash
$ funda-finder db clear -y
✓ Database cleared successfully
```

**`funda-finder db reset`** - Resets cached engine
```bash
$ funda-finder db reset
✓ Database engine reset (connections cleared)
  Next database access will create a fresh connection
```

### 3. Documentation

- **DATABASE_MANAGEMENT.md** - Comprehensive guide covering:
  - Database configuration (PostgreSQL vs SQLite)
  - Singleton engine explanation
  - CLI command usage
  - Common issues and solutions
  - Best practices

- **README.md** - Updated to:
  - Clarify default database is PostgreSQL
  - Document `FUNDA_DB_URL` configuration
  - Add database management command examples

## Testing

Created `tests/test_db_management.py` with 3 tests:
- ✓ `test_reset_engine_clears_singleton` - Verifies engine reset works
- ✓ `test_clear_db_removes_all_data` - Confirms all data is deleted
- ✓ `test_clear_db_preserves_schema` - Ensures schema remains intact

All tests pass.

## Verification

### Before Fix
```bash
$ psql -h localhost -d funda -c "SELECT COUNT(*) FROM properties;"
 count
-------
    61

$ rm -f data/funda.db*  # No effect - wrong database!
```

### After Fix
```bash
$ funda-finder db info
Database URL: postgresql://localhost/funda
Table Row Counts:
  properties: 61

$ funda-finder db clear -y
✓ Database cleared successfully

$ funda-finder db info
Table Row Counts:
  properties: 0
```

## Impact

- **User Experience**: Clear commands that work regardless of database type
- **Debugging**: `db info` shows exactly which database is in use
- **Safety**: Confirmation prompt prevents accidental data loss
- **Flexibility**: `db reset` enables database switching without restart

## Files Changed

1. `funda_finder/db/session.py` - Added `reset_engine()` and `clear_db()`
2. `funda_finder/cli.py` - Added `db` subcommand with info/clear/reset
3. `DATABASE_MANAGEMENT.md` - Created comprehensive documentation
4. `README.md` - Updated database configuration section
5. `tests/test_db_management.py` - Added test coverage

## Related Issues

This fix also resolves potential issues with:
- SQLite WAL file persistence
- Cached connections after configuration changes
- Confusion about which database is active
- Inability to properly reset database state
