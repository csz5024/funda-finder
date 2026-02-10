# Database Management Guide

## Overview

Funda Finder supports both **PostgreSQL** and **SQLite** databases. By default, it uses **PostgreSQL** at `postgresql://localhost/funda`.

## Database Configuration

The database is configured via the `FUNDA_DB_URL` environment variable or the `config.yaml` file:

```bash
# Use PostgreSQL (default)
export FUNDA_DB_URL="postgresql://localhost/funda"

# Use SQLite instead
export FUNDA_DB_URL="sqlite:///data/funda.db"
```

Or in `config.yaml`:
```yaml
database:
  url: "postgresql://localhost/funda"
```

## Understanding the Singleton Engine

The application uses a **singleton pattern** for the database engine:
- The engine is created once and cached in memory
- It persists for the lifetime of the process
- Changing configuration requires restarting the process OR calling `funda-finder db reset`

This is why deleting database files doesn't always clear data - you may be connected to PostgreSQL, not SQLite!

## CLI Commands

### Show Database Information

```bash
funda-finder db info
```

Shows:
- Which database you're connected to (PostgreSQL or SQLite)
- Current row counts for all tables

### Clear All Data

```bash
funda-finder db clear
```

Deletes all data from all tables while preserving the schema. Requires confirmation unless `-y` flag is used:

```bash
funda-finder db clear -y  # Skip confirmation
```

**WARNING:** This operation cannot be undone!

### Reset Database Engine

```bash
funda-finder db reset
```

Clears the cached singleton engine and session factory. Use this when:
- Switching between databases
- After database configuration changes
- When you need to force a fresh connection

## Common Issues

### "I deleted the database file but data persists"

**Cause:** You're using PostgreSQL, not SQLite. Deleting SQLite files doesn't affect PostgreSQL.

**Solution:**
1. Check which database you're using: `funda-finder db info`
2. Clear the correct database: `funda-finder db clear -y`

### "Database shows 0 rows but API returns data"

**Cause:** The singleton engine is cached with an old connection.

**Solution:**
1. Reset the engine: `funda-finder db reset`
2. Restart the API server
3. Or check if you're querying a different database than the API uses

### "Can't switch from PostgreSQL to SQLite"

**Cause:** The singleton engine is cached with the PostgreSQL connection.

**Solution:**
1. Set the environment variable: `export FUNDA_DB_URL="sqlite:///data/funda.db"`
2. Reset the engine: `funda-finder db reset`
3. Restart your application

## SQLite WAL Mode

If using SQLite, the database may use Write-Ahead Logging (WAL) mode, which creates auxiliary files:
- `funda.db` - Main database file
- `funda.db-wal` - Write-ahead log
- `funda.db-shm` - Shared memory file

To fully delete a SQLite database, remove all three files:
```bash
rm -f data/funda.db*
```

## Database Schema Migration

Use Alembic for schema migrations:

```bash
# Create new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Revert migration
alembic downgrade -1
```

## Best Practices

1. **Always use `db info` first** - Know which database you're connected to
2. **Use `db clear` instead of deleting files** - It works for both PostgreSQL and SQLite
3. **Use `db reset` after configuration changes** - Ensures fresh connections
4. **Backup before clearing** - The operation is irreversible
5. **Use PostgreSQL for production** - Better for concurrent access and larger datasets
6. **Use SQLite for development** - Simpler setup, file-based storage
