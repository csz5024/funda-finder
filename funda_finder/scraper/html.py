"""HTML scraper implementation using funda-scraper library."""

import logging
import time
from typing import List, Optional

from bs4 import BeautifulSoup
from funda_scraper import FundaScraper

from .base import (
    PropertyType,
    RawListing,
    ScraperInterface,
    ScraperSource,
    SearchFilters,
)

logger = logging.getLogger(__name__)


class HtmlScraper(ScraperInterface):
    """Scraper implementation using funda-scraper library (HTML scraping).

    This is the fallback scraper that uses HTML parsing via BeautifulSoup.
    It's more fragile (breaks when HTML changes) but works when the mobile
    API is unavailable or rate-limited.
    """

    def __init__(self, rate_limit_seconds: float = 3.0):
        """Initialize HTML scraper.

        Args:
            rate_limit_seconds: Seconds to wait between requests (default: 3.0)
        """
        self._rate_limit = rate_limit_seconds
        self._last_request_time = 0.0

    @property
    def source(self) -> ScraperSource:
        """Return the scraper source identifier."""
        return ScraperSource.HTML

    def _rate_limit_wait(self) -> None:
        """Wait if needed to respect rate limiting."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit:
            wait_time = self._rate_limit - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _normalize_listing(self, raw_data: dict, property_type: PropertyType) -> RawListing:
        """Normalize funda-scraper data to RawListing format.

        Args:
            raw_data: Raw data from funda-scraper
            property_type: Type of property (buy/rent)

        Returns:
            Normalized listing
        """
        # Extract fields from funda-scraper response
        # Field names based on funda-scraper library structure
        listing_id = str(raw_data.get("url", "").split("/")[-2] or "")
        url = raw_data.get("url", "")

        # Address components
        address = raw_data.get("address", "")
        postal_code = raw_data.get("zip_code") or raw_data.get("postal_code")
        city = raw_data.get("city", "")
        neighborhood = raw_data.get("neighborhood")

        # Price
        price = raw_data.get("price", 0)
        if isinstance(price, str):
            # Remove currency symbols and convert
            price = int("".join(filter(str.isdigit, price)) or "0")

        # Property details
        living_area = raw_data.get("living_area") or raw_data.get("floor_area")
        plot_area = raw_data.get("plot_area") or raw_data.get("land_area")
        num_rooms = raw_data.get("num_of_rooms")
        num_bedrooms = raw_data.get("num_of_bedrooms")
        num_bathrooms = raw_data.get("num_of_bathrooms")

        # Building details
        construction_year = raw_data.get("year_built") or raw_data.get("construction_year")
        energy_label = raw_data.get("energy_label")

        # Description
        description = raw_data.get("description") or raw_data.get("desc")

        return RawListing(
            listing_id=listing_id,
            url=url,
            address=address,
            city=city,
            price=price,
            property_type=property_type,
            postal_code=postal_code,
            neighborhood=neighborhood,
            living_area=float(living_area) if living_area else None,
            plot_area=float(plot_area) if plot_area else None,
            num_rooms=int(num_rooms) if num_rooms else None,
            num_bedrooms=int(num_bedrooms) if num_bedrooms else None,
            num_bathrooms=int(num_bathrooms) if num_bathrooms else None,
            construction_year=int(construction_year) if construction_year else None,
            energy_label=energy_label,
            description=description,
            source=self.source,
            raw_data=raw_data,
        )

    def search(self, filters: SearchFilters) -> List[RawListing]:
        """Search for property listings using HTML scraping.

        Args:
            filters: Search parameters

        Returns:
            List of normalized listings

        Raises:
            Exception: If scraping fails
        """
        logger.info(
            f"Searching {filters.property_type.value} properties in {filters.city} "
            f"(price: {filters.min_price}-{filters.max_price}) via HTML"
        )

        self._rate_limit_wait()

        try:
            # Initialize funda-scraper
            # area parameter is the city name
            # want_to parameter is "buy" or "rent"
            scraper = FundaScraper(
                area=filters.city,
                want_to=filters.property_type.value,
                find_past=False,
                page_start=1,
                n_pages=1,  # Start with 1 page, can be increased
            )

            # Run scraper
            results = scraper.run(raw_data=True)

            # Filter by price if specified
            if filters.min_price or filters.max_price:
                filtered_results = []
                for result in results:
                    price = result.get("price", 0)
                    if isinstance(price, str):
                        price = int("".join(filter(str.isdigit, price)) or "0")

                    if filters.min_price and price < filters.min_price:
                        continue
                    if filters.max_price and price > filters.max_price:
                        continue

                    filtered_results.append(result)
                results = filtered_results

            # Limit results if specified
            if filters.max_results and len(results) > filters.max_results:
                results = results[: filters.max_results]

            # Normalize all listings
            listings = [
                self._normalize_listing(result, filters.property_type)
                for result in results
            ]

            logger.info(f"Found {len(listings)} listings via HTML scraper")
            return listings

        except Exception as e:
            logger.error(f"HTML scraper search failed: {e}")
            raise

    def get_details(self, listing_id: str) -> Optional[RawListing]:
        """Fetch full details for a specific listing.

        Args:
            listing_id: Funda listing ID

        Returns:
            Normalized listing with full details, or None if not found

        Raises:
            Exception: If scraping fails
        """
        logger.info(f"Fetching details for listing {listing_id} via HTML")

        self._rate_limit_wait()

        try:
            # funda-scraper doesn't have a direct get_details method
            # We need to construct the URL and scrape it directly
            # URL format: https://www.funda.nl/koop/amsterdam/huis-{listing_id}/
            # This is a limitation of the HTML scraper approach

            # For now, return None - this would require additional implementation
            # to fetch and parse individual listing pages
            logger.warning(
                f"get_details not fully implemented for HTML scraper (listing {listing_id})"
            )
            return None

        except Exception as e:
            logger.error(f"HTML scraper get_details failed for {listing_id}: {e}")
            raise
