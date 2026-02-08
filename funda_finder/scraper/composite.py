"""Composite scraper with automatic fallback between pyfunda and HTML."""

import logging
from typing import List, Optional

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .base import RawListing, ScraperSource, SearchFilters
from .html import HtmlScraper
from .pyfunda import PyFundaScraper

logger = logging.getLogger(__name__)


class ScraperException(Exception):
    """Base exception for scraper errors."""
    pass


class AllScrapersFailed(ScraperException):
    """Raised when both primary and fallback scrapers fail."""
    pass


class CompositeScraper:
    """Composite scraper with automatic fallback.

    This is the main interface for scraping Funda listings. It tries
    pyfunda first (faster, more reliable) and automatically falls back
    to HTML scraping if pyfunda fails.

    Usage:
        scraper = CompositeScraper()
        listings = scraper.search(city="amsterdam", property_type=PropertyType.BUY)
    """

    def __init__(
        self,
        rate_limit_seconds: float = 3.0,
        retry_attempts: int = 3,
        enable_fallback: bool = True,
    ):
        """Initialize composite scraper.

        Args:
            rate_limit_seconds: Seconds between requests for each scraper
            retry_attempts: Number of retry attempts per scraper
            enable_fallback: Whether to enable fallback to HTML scraper
        """
        self._rate_limit = rate_limit_seconds
        self._retry_attempts = retry_attempts
        self._enable_fallback = enable_fallback

        # Initialize scrapers
        self._pyfunda = PyFundaScraper(rate_limit_seconds=rate_limit_seconds)
        self._html = HtmlScraper(rate_limit_seconds=rate_limit_seconds)

        logger.info(
            f"CompositeScraper initialized (rate_limit={rate_limit_seconds}s, "
            f"retry_attempts={retry_attempts}, fallback={enable_fallback})"
        )

    def search(self, filters: SearchFilters) -> List[RawListing]:
        """Search for property listings with automatic fallback.

        Tries pyfunda first, falls back to HTML scraper if pyfunda fails.

        Args:
            filters: Search parameters

        Returns:
            List of normalized listings

        Raises:
            AllScrapersFailed: If both scrapers fail
        """
        logger.info(f"Starting composite search: {filters.city}, {filters.property_type.value}")

        # Try primary scraper (pyfunda)
        try:
            listings = self._search_with_retry(self._pyfunda, filters)
            logger.info(f"Primary scraper (pyfunda) succeeded: {len(listings)} listings")
            return listings

        except Exception as pyfunda_error:
            logger.warning(f"Primary scraper (pyfunda) failed: {pyfunda_error}")

            if not self._enable_fallback:
                logger.error("Fallback disabled, raising exception")
                raise AllScrapersFailed(
                    f"PyFunda scraper failed and fallback is disabled: {pyfunda_error}"
                ) from pyfunda_error

            # Try fallback scraper (HTML)
            logger.info("Attempting fallback to HTML scraper")
            try:
                listings = self._search_with_retry(self._html, filters)
                logger.info(f"Fallback scraper (HTML) succeeded: {len(listings)} listings")
                return listings

            except Exception as html_error:
                logger.error(f"Fallback scraper (HTML) also failed: {html_error}")
                raise AllScrapersFailed(
                    f"Both scrapers failed. PyFunda: {pyfunda_error}. HTML: {html_error}"
                ) from html_error

    def _search_with_retry(self, scraper, filters: SearchFilters) -> List[RawListing]:
        """Search with retry logic.

        Args:
            scraper: Scraper instance (PyFundaScraper or HtmlScraper)
            filters: Search parameters

        Returns:
            List of normalized listings

        Raises:
            Exception: If all retry attempts fail
        """
        @retry(
            retry=retry_if_exception_type(Exception),
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True,
        )
        def _search():
            return scraper.search(filters)

        return _search()

    def get_details(self, listing_id: str, source_hint: Optional[ScraperSource] = None) -> Optional[RawListing]:
        """Fetch full details for a specific listing.

        Args:
            listing_id: Funda listing ID
            source_hint: Preferred scraper source (if known)

        Returns:
            Normalized listing with full details, or None if not found

        Raises:
            AllScrapersFailed: If both scrapers fail
        """
        logger.info(f"Fetching details for listing {listing_id}")

        # If source hint provided, try that scraper first
        primary_scraper = self._pyfunda
        fallback_scraper = self._html

        if source_hint == ScraperSource.HTML:
            primary_scraper, fallback_scraper = fallback_scraper, primary_scraper

        # Try primary scraper
        try:
            listing = self._get_details_with_retry(primary_scraper, listing_id)
            if listing:
                logger.info(f"Primary scraper succeeded for {listing_id}")
                return listing

        except Exception as primary_error:
            logger.warning(f"Primary scraper failed for {listing_id}: {primary_error}")

        if not self._enable_fallback:
            logger.error("Fallback disabled, raising exception")
            raise AllScrapersFailed(
                f"Primary scraper failed for {listing_id} and fallback is disabled"
            )

        # Try fallback scraper
        logger.info(f"Attempting fallback scraper for {listing_id}")
        try:
            listing = self._get_details_with_retry(fallback_scraper, listing_id)
            if listing:
                logger.info(f"Fallback scraper succeeded for {listing_id}")
                return listing

        except Exception as fallback_error:
            logger.error(f"Fallback scraper also failed for {listing_id}: {fallback_error}")
            raise AllScrapersFailed(
                f"Both scrapers failed for listing {listing_id}"
            ) from fallback_error

        logger.warning(f"Listing {listing_id} not found by any scraper")
        return None

    def _get_details_with_retry(self, scraper, listing_id: str) -> Optional[RawListing]:
        """Get details with retry logic.

        Args:
            scraper: Scraper instance
            listing_id: Listing ID

        Returns:
            Normalized listing or None if not found

        Raises:
            Exception: If all retry attempts fail
        """
        @retry(
            retry=retry_if_exception_type(Exception),
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            reraise=True,
        )
        def _get_details():
            return scraper.get_details(listing_id)

        return _get_details()

    def get_scraper_status(self) -> dict:
        """Get status information about available scrapers.

        Returns:
            Dictionary with scraper status information
        """
        return {
            "primary": {
                "source": self._pyfunda.source.value,
                "rate_limit": self._rate_limit,
            },
            "fallback": {
                "source": self._html.source.value,
                "rate_limit": self._rate_limit,
                "enabled": self._enable_fallback,
            },
            "retry_attempts": self._retry_attempts,
        }
