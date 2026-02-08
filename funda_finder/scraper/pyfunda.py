"""PyFunda scraper implementation using the funda.io mobile API."""

import logging
import time
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyfunda import PyFunda as PyFundaClient

from .base import (
    PropertyType,
    RawListing,
    ScraperInterface,
    ScraperSource,
    SearchFilters,
)

logger = logging.getLogger(__name__)


class PyFundaScraper(ScraperInterface):
    """Scraper implementation using pyfunda library (mobile API).

    This is the primary scraper that uses funda.io's mobile API via
    the pyfunda library. It's faster and more reliable than HTML scraping,
    but may fail if the API changes or rate limits are hit.
    """

    def __init__(self, rate_limit_seconds: float = 3.0):
        """Initialize PyFunda scraper.

        Args:
            rate_limit_seconds: Seconds to wait between requests (default: 3.0)
        """
        # Lazy import to avoid dependency issues at module load time
        from pyfunda import PyFunda

        self._rate_limit = rate_limit_seconds
        self._last_request_time = 0.0
        self._client = PyFunda()

    @property
    def source(self) -> ScraperSource:
        """Return the scraper source identifier."""
        return ScraperSource.PYFUNDA

    def _rate_limit_wait(self) -> None:
        """Wait if needed to respect rate limiting."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit:
            wait_time = self._rate_limit - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _normalize_listing(self, raw_data: dict, property_type: PropertyType) -> RawListing:
        """Normalize pyfunda data to RawListing format.

        Args:
            raw_data: Raw data from pyfunda
            property_type: Type of property (buy/rent)

        Returns:
            Normalized listing
        """
        # Extract fields from pyfunda response
        # Note: Field names may vary depending on pyfunda version
        listing_id = str(raw_data.get("Id") or raw_data.get("id", ""))
        url = raw_data.get("Url") or raw_data.get("url", "")

        # Address components
        address = raw_data.get("Adres") or raw_data.get("address", "")
        postal_code = raw_data.get("Postcode") or raw_data.get("postal_code")
        city = raw_data.get("Woonplaats") or raw_data.get("city", "")
        neighborhood = raw_data.get("Buurt") or raw_data.get("neighborhood")

        # Price
        price = raw_data.get("Koopprijs") or raw_data.get("Huurprijs") or raw_data.get("price", 0)
        if isinstance(price, str):
            # Remove currency symbols and convert
            price = int("".join(filter(str.isdigit, price)) or "0")

        # Property details
        living_area = raw_data.get("Woonoppervlakte") or raw_data.get("living_area")
        plot_area = raw_data.get("PerceelOppervlakte") or raw_data.get("plot_area")
        num_rooms = raw_data.get("AantalKamers") or raw_data.get("num_rooms")
        num_bedrooms = raw_data.get("AantalSlaapkamers") or raw_data.get("num_bedrooms")
        num_bathrooms = raw_data.get("AantalBadkamers") or raw_data.get("num_bathrooms")

        # Building details
        construction_year = raw_data.get("Bouwjaar") or raw_data.get("construction_year")
        energy_label = raw_data.get("Energielabel") or raw_data.get("energy_label")

        # Description
        description = raw_data.get("VolledigeOmschrijving") or raw_data.get("description")

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
        """Search for property listings using pyfunda API.

        Args:
            filters: Search parameters

        Returns:
            List of normalized listings

        Raises:
            Exception: If API request fails
        """
        logger.info(
            f"Searching {filters.property_type.value} properties in {filters.city} "
            f"(price: {filters.min_price}-{filters.max_price})"
        )

        self._rate_limit_wait()

        try:
            # Build search parameters
            search_type = (
                "koop" if filters.property_type == PropertyType.BUY else "huur"
            )

            # Call pyfunda search
            # Note: API may vary by version, adjust as needed
            results = self._client.search(
                location=filters.city,
                search_type=search_type,
                min_price=filters.min_price,
                max_price=filters.max_price,
                min_rooms=filters.min_rooms,
            )

            # Limit results if specified
            if filters.max_results and len(results) > filters.max_results:
                results = results[: filters.max_results]

            # Normalize all listings
            listings = [
                self._normalize_listing(result, filters.property_type)
                for result in results
            ]

            logger.info(f"Found {len(listings)} listings via pyfunda")
            return listings

        except Exception as e:
            logger.error(f"PyFunda search failed: {e}")
            raise

    def get_details(self, listing_id: str) -> Optional[RawListing]:
        """Fetch full details for a specific listing.

        Args:
            listing_id: Funda listing ID

        Returns:
            Normalized listing with full details, or None if not found

        Raises:
            Exception: If API request fails
        """
        logger.info(f"Fetching details for listing {listing_id}")

        self._rate_limit_wait()

        try:
            # Fetch listing details
            result = self._client.get_property_details(listing_id)

            if not result:
                logger.warning(f"Listing {listing_id} not found")
                return None

            # Determine property type from result
            # This may need adjustment based on pyfunda response format
            is_buy = "koop" in str(result.get("Type", "")).lower()
            property_type = PropertyType.BUY if is_buy else PropertyType.RENT

            listing = self._normalize_listing(result, property_type)
            logger.info(f"Fetched details for {listing_id} via pyfunda")
            return listing

        except Exception as e:
            logger.error(f"PyFunda get_details failed for {listing_id}: {e}")
            raise
