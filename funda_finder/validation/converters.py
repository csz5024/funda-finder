"""Converters for transforming scraper data to validated models.

This module provides functions to convert raw scraper data (RawListing)
into validated Pydantic models (PropertyListing).
"""

import logging
from typing import List, Optional

from pydantic import ValidationError

from funda_finder.scraper import RawListing
from .models import PropertyListing

logger = logging.getLogger(__name__)


def raw_to_validated(raw: RawListing) -> Optional[PropertyListing]:
    """Convert RawListing to validated PropertyListing.

    This function maps fields from the scraper's RawListing format
    to the validated PropertyListing model. If validation fails,
    it logs the error and returns None.

    Args:
        raw: Raw listing from scraper

    Returns:
        Validated PropertyListing, or None if validation fails

    Example:
        >>> from funda_finder.scraper import CompositeScraper, SearchFilters, PropertyType
        >>> scraper = CompositeScraper()
        >>> filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        >>> raw_listings = scraper.search(filters)
        >>> validated = [raw_to_validated(raw) for raw in raw_listings]
        >>> validated = [v for v in validated if v is not None]
    """
    try:
        # Map RawListing fields to PropertyListing fields
        # Use getattr with default None for fields that might not exist
        return PropertyListing(
            funda_id=raw.listing_id,
            url=raw.url,
            city=raw.city,
            price=raw.price,
            listing_type=raw.property_type.value,
            address=raw.address,
            postal_code=raw.postal_code,
            neighborhood=raw.neighborhood,
            lat=getattr(raw, 'lat', None),
            lon=getattr(raw, 'lon', None),
            living_area=raw.living_area,
            plot_area=raw.plot_area,
            rooms=raw.num_rooms,
            bedrooms=raw.num_bedrooms,
            bathrooms=raw.num_bathrooms,
            year_built=raw.construction_year,
            energy_label=raw.energy_label,
            description=raw.description,
            source=raw.source.value,
            scraped_at=raw.scraped_at,
        )

    except ValidationError as e:
        logger.warning(
            f"Validation failed for listing {raw.listing_id}: {e.error_count()} errors"
        )
        logger.debug(f"Validation errors: {e.errors()}")
        return None

    except Exception as e:
        logger.error(f"Unexpected error converting listing {raw.listing_id}: {e}")
        return None


def raw_to_validated_batch(
    raw_listings: List[RawListing],
) -> tuple[List[PropertyListing], List[RawListing]]:
    """Convert a batch of RawListings to validated PropertyListings.

    This function processes a list of raw listings and returns both
    the successfully validated listings and the failed ones for further
    inspection.

    Args:
        raw_listings: List of raw listings from scraper

    Returns:
        Tuple of (validated_listings, failed_raw_listings)

    Example:
        >>> validated, failed = raw_to_validated_batch(raw_listings)
        >>> print(f"Success: {len(validated)}, Failed: {len(failed)}")
    """
    validated = []
    failed = []

    for raw in raw_listings:
        result = raw_to_validated(raw)
        if result is not None:
            validated.append(result)
        else:
            failed.append(raw)

    logger.info(
        f"Batch validation complete: {len(validated)} succeeded, "
        f"{len(failed)} failed ({len(failed)/len(raw_listings)*100:.1f}% failure rate)"
    )

    return validated, failed
