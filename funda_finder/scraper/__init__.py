"""Composite scraper module for Funda property listings.

This module provides a unified interface for scraping Funda.nl with automatic
fallback between different data sources (mobile API and HTML scraping).

Main exports:
- CompositeScraper: Main scraper interface with automatic fallback
- RawListing: Normalized listing data model
- SearchFilters: Search parameters dataclass
- PropertyType, ScraperSource: Enums for property types and scraper sources

Example usage:
    from funda_finder.scraper import CompositeScraper, SearchFilters, PropertyType

    scraper = CompositeScraper()
    filters = SearchFilters(
        city="amsterdam",
        property_type=PropertyType.BUY,
        min_price=400000,
        max_price=600000,
    )
    listings = scraper.search(filters)
"""

from .base import (
    PropertyType,
    RawListing,
    ScraperInterface,
    ScraperSource,
    SearchFilters,
)
from .composite import AllScrapersFailed, CompositeScraper, ScraperException

__all__ = [
    # Main interface
    "CompositeScraper",
    # Data models
    "RawListing",
    "SearchFilters",
    # Enums
    "PropertyType",
    "ScraperSource",
    # Exceptions
    "ScraperException",
    "AllScrapersFailed",
    # Interface (for custom implementations)
    "ScraperInterface",
]
