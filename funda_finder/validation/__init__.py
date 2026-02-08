"""Data validation module using Pydantic models.

This module provides validated data models for property listings,
ensuring data quality before database insertion.

Main exports:
- PropertyListing: Validated property listing model
- raw_to_validated: Convert RawListing to PropertyListing
- raw_to_validated_batch: Convert batch of RawListings

Example usage:
    from funda_finder.validation import PropertyListing

    # Create directly
    listing = PropertyListing(
        funda_id="abc123",
        url="https://www.funda.nl/koop/amsterdam/huis-abc123/",
        city="Amsterdam",
        price=450000,
        listing_type="buy",
    )

    # Or convert from scraper output
    from funda_finder.scraper import CompositeScraper, SearchFilters, PropertyType
    from funda_finder.validation import raw_to_validated_batch

    scraper = CompositeScraper()
    filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
    raw_listings = scraper.search(filters)
    validated, failed = raw_to_validated_batch(raw_listings)
"""

from .models import PropertyListing
from .converters import raw_to_validated, raw_to_validated_batch

__all__ = [
    "PropertyListing",
    "raw_to_validated",
    "raw_to_validated_batch",
]
