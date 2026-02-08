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

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
