"""CLI entrypoint for Funda Property Finder."""

import argparse
import sys


def cmd_scrape(args):
    """Run a scrape for the given city."""
    from funda_finder.scraper import PropertyType
    from funda_finder.scraper.orchestrator import ScrapeOrchestrator
    from funda_finder.config import settings

    property_type = PropertyType.BUY if args.type == "buy" else PropertyType.RENT

    def progress(msg: str):
        print(f"  {msg}")

    try:
        orchestrator = ScrapeOrchestrator(rate_limit=settings.rate_limit)
        meta = orchestrator.run_scrape(
            city=args.city,
            property_type=property_type,
            progress_callback=progress,
        )
        print(f"\n✓ Scrape complete (run_id: {meta.run_id})")
        print(f"  Found: {meta.listings_found}")
        print(f"  New: {meta.listings_new}")
        print(f"  Updated: {meta.listings_updated}")
        if meta.errors:
            print(f"  Errors: {meta.errors}")
    except Exception as e:
        print(f"\n✗ Scrape failed: {e}")
        sys.exit(1)


def cmd_serve(args):
    """Start the FastAPI dashboard."""
    import uvicorn

    from funda_finder.config import settings

    uvicorn.run(
        "funda_finder.api.app:app",
        host=args.host or settings.host,
        port=args.port or settings.port,
        reload=True,
    )


def cmd_analyze(args):
    """Run analysis on stored data."""
    print(f"Analyzing properties... [not yet implemented]")


def cmd_db(args):
    """Database management commands."""
    from funda_finder.db.session import (
        _get_default_engine,
        clear_db,
        reset_engine,
    )
    from funda_finder.db.models import Property, PriceHistory, ScrapeMeta
    from sqlalchemy.orm import Session

    if args.db_command == "info":
        # Show database connection and table row counts
        engine = _get_default_engine()
        print(f"Database URL: {engine.url}")
        print(f"Database Type: {engine.dialect.name}")
        print()

        with Session(engine) as session:
            properties = session.query(Property).count()
            price_history = session.query(PriceHistory).count()
            scrape_meta = session.query(ScrapeMeta).count()

            print("Table Row Counts:")
            print(f"  properties: {properties}")
            print(f"  price_history: {price_history}")
            print(f"  scrape_meta: {scrape_meta}")

    elif args.db_command == "clear":
        # Clear all data from the database
        if not args.yes:
            engine = _get_default_engine()
            print(f"⚠️  WARNING: This will delete ALL data from {engine.url}")
            print("   This operation cannot be undone!")
            response = input("\nAre you sure? Type 'yes' to confirm: ")
            if response.lower() != "yes":
                print("Aborted.")
                sys.exit(0)

        try:
            clear_db()
            print("✓ Database cleared successfully")
        except Exception as e:
            print(f"✗ Failed to clear database: {e}")
            sys.exit(1)

    elif args.db_command == "reset":
        # Reset the singleton engine
        reset_engine()
        print("✓ Database engine reset (connections cleared)")
        print("  Next database access will create a fresh connection")


def cmd_run(args):
    """Run the scheduler (foreground or one-time scrape)."""
    from funda_finder.scheduler import Scheduler, setup_logging

    setup_logging()

    scheduler = Scheduler()

    if args.once:
        # Run scrape job once and exit
        print("Running scrape job once...")
        results = scheduler.run_now()

        successful = sum(1 for r in results if r.success)
        total_new = sum(r.new_count for r in results if r.success)
        total_updated = sum(r.updated_count for r in results if r.success)

        print(f"\n✓ Scrape complete:")
        print(f"  Successful: {successful}/{len(results)}")
        print(f"  New listings: {total_new}")
        print(f"  Updated listings: {total_updated}")

        if successful < len(results):
            print(f"  Failed: {len(results) - successful}")
            sys.exit(1)
    else:
        # Run scheduler in foreground
        if not scheduler.config.schedule_enabled:
            print("✗ Scheduling is disabled in configuration.")
            print("  Enable it in config.yaml (scheduling.enabled: true)")
            print("  or set FUNDA_SCHEDULE_ENABLED=true")
            sys.exit(1)

        scheduler.start()
        next_run = scheduler.get_next_run_time()
        if next_run:
            print(f"✓ Scheduler started")
            print(f"  Next run: {next_run.isoformat()}")
            print(f"  Cron: {scheduler.config.cron_expression}")
            print(f"  Cities: {', '.join(scheduler.config.cities)}")
            print("\nPress Ctrl+C to stop...")

        try:
            import time
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("\n\nShutting down...")
            scheduler.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Funda Property Finder")
    sub = parser.add_subparsers(dest="command")

    # scrape
    p_scrape = sub.add_parser("scrape", help="Scrape listings from funda.nl")
    p_scrape.add_argument("--city", required=True, help="City to scrape")
    p_scrape.add_argument("--type", choices=["buy", "rent"], default="buy")
    p_scrape.set_defaults(func=cmd_scrape)

    # serve
    p_serve = sub.add_parser("serve", help="Start the web dashboard")
    p_serve.add_argument("--host", default=None)
    p_serve.add_argument("--port", type=int, default=None)
    p_serve.set_defaults(func=cmd_serve)

    # analyze
    p_analyze = sub.add_parser("analyze", help="Run property analysis")
    p_analyze.set_defaults(func=cmd_analyze)

    # run
    p_run = sub.add_parser("run", help="Run scheduled scraping")
    p_run.add_argument(
        "--once",
        action="store_true",
        help="Run scrape job once and exit (don't start scheduler)",
    )
    p_run.set_defaults(func=cmd_run)

    # db
    p_db = sub.add_parser("db", help="Database management")
    db_sub = p_db.add_subparsers(dest="db_command")

    db_info = db_sub.add_parser("info", help="Show database connection and row counts")

    db_clear = db_sub.add_parser("clear", help="Delete all data from the database")
    db_clear.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )

    db_reset = db_sub.add_parser("reset", help="Reset database engine (clear cached connections)")

    p_db.set_defaults(func=cmd_db)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
