"""CLI entrypoint for Funda Property Finder."""

import argparse
import sys


def cmd_scrape(args):
    """Run a scrape for the given city."""
    print(f"Scraping {args.city} ({args.type})... [not yet implemented]")


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
