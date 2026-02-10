"""PyFunda scraper implementation using the funda.io mobile API."""

import logging
import random
import time
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from funda import Funda as FundaClient

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
        from funda import Funda

        self._rate_limit = rate_limit_seconds
        self._last_request_time = 0.0
        self._client = Funda()

    @property
    def source(self) -> ScraperSource:
        """Return the scraper source identifier."""
        return ScraperSource.PYFUNDA

    def _rate_limit_wait(self) -> None:
        """Wait if needed to respect rate limiting with jitter."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit:
            wait_time = self._rate_limit - elapsed
            # Add jitter: ±20% random variation to avoid patterns
            jitter = random.uniform(-0.2, 0.2) * wait_time
            wait_time = max(0.1, wait_time + jitter)  # Ensure minimum 0.1s wait
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s (with jitter)")
            time.sleep(wait_time)
        self._last_request_time = time.time()

    def _normalize_listing(self, raw_data: dict, property_type: PropertyType) -> RawListing:
        """Normalize funda API data to RawListing format.

        Args:
            raw_data: Raw data from funda API (Listing.to_dict())
            property_type: Type of property (buy/rent)

        Returns:
            Normalized listing
        """
        # Get URL from API (should always be present)
        url = raw_data.get("url", "")

        # Extract listing_id from URL (like HTML scraper does)
        # Funda URLs have format: .../koop/city/huis-42534234/ or .../en/detail/koop/city/huis-42534234/
        # The listing_id is the last segment (before trailing slash)
        if url:
            listing_id = str(url.rstrip("/").split("/")[-1] or "")
        else:
            # Fallback to global_id if URL is not available
            listing_id = str(raw_data.get("global_id", ""))

        # Address components
        address = raw_data.get("title", "")
        postal_code = raw_data.get("postcode")
        city = raw_data.get("city", "")

        # Construct URL if not present
        if not url and listing_id and city:
            offering = "koop" if property_type == PropertyType.BUY else "huur"
            city_slug = city.lower().replace(" ", "-")
            url = f"https://www.funda.nl/en/detail/{offering}/{city_slug}/{listing_id}/"
        neighborhood = raw_data.get("neighbourhood")

        # Price (already an integer)
        price = raw_data.get("price", 0)

        # Property details
        living_area = raw_data.get("living_area")
        plot_area = raw_data.get("plot_area")
        num_rooms = raw_data.get("rooms")
        num_bedrooms = raw_data.get("bedrooms")
        num_bathrooms = None  # Not available in basic search

        # Building details
        construction_year = raw_data.get("construction_year")
        energy_label = raw_data.get("energy_label")
        construction_type = raw_data.get("construction_type")
        property_subtype = raw_data.get("property_type") or raw_data.get("object_type")

        # Description (only in detailed view)
        description = raw_data.get("description")

        # GPS coordinates
        latitude = raw_data.get("latitude") or raw_data.get("lat")
        longitude = raw_data.get("longitude") or raw_data.get("lon")

        # Property features
        has_garden = raw_data.get("has_garden") or raw_data.get("garden")
        has_parking = raw_data.get("has_parking") or raw_data.get("parking")
        has_balcony = raw_data.get("has_balcony") or raw_data.get("balcony")
        has_garage = raw_data.get("has_garage") or raw_data.get("garage")

        # Photos
        photos = raw_data.get("photos", []) or raw_data.get("images", [])
        if not isinstance(photos, list):
            photos = []

        # Agent details
        agent_info = raw_data.get("agent", {}) or {}
        agent_name = agent_info.get("name") if isinstance(agent_info, dict) else None
        agent_phone = agent_info.get("phone") if isinstance(agent_info, dict) else None
        agent_email = agent_info.get("email") if isinstance(agent_info, dict) else None
        agency_name = agent_info.get("agency") or raw_data.get("agency_name")

        # Listing metadata
        listing_date = raw_data.get("listing_date") or raw_data.get("publication_date")
        days_on_market = raw_data.get("days_on_market") or raw_data.get("days_online")

        # Parse listing_date if it's a string
        if listing_date and isinstance(listing_date, str):
            try:
                from datetime import datetime as dt
                listing_date = dt.fromisoformat(listing_date.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                listing_date = None

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
            construction_type=construction_type,
            property_subtype=property_subtype,
            description=description,
            latitude=float(latitude) if latitude else None,
            longitude=float(longitude) if longitude else None,
            has_garden=bool(has_garden) if has_garden is not None else None,
            has_parking=bool(has_parking) if has_parking is not None else None,
            has_balcony=bool(has_balcony) if has_balcony is not None else None,
            has_garage=bool(has_garage) if has_garage is not None else None,
            photos=photos,
            agent_name=agent_name,
            agent_phone=agent_phone,
            agent_email=agent_email,
            agency_name=agency_name,
            listing_date=listing_date,
            days_on_market=int(days_on_market) if days_on_market else None,
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
            # Build search parameters for funda API
            offering_type = "buy" if filters.property_type == PropertyType.BUY else "rent"

            # Call funda search_listing method
            # Returns list of Listing objects
            results = self._client.search_listing(
                location=filters.city,
                offering_type=offering_type,
                price_min=filters.min_price,
                price_max=filters.max_price,
                # Note: funda API doesn't have min_rooms in search
            )

            # Limit results if specified
            if filters.max_results and len(results) > filters.max_results:
                results = results[: filters.max_results]

            # Fetch detailed listings to get correct tiny_id and URL
            # Search results lack tiny_id and url fields, so we must fetch details
            listings = []
            for result in results:
                try:
                    global_id = result.to_dict().get("global_id")
                    if not global_id:
                        logger.warning("Search result missing global_id, skipping")
                        continue

                    # Rate limit between detail fetches
                    self._rate_limit_wait()

                    # Fetch detailed listing with correct tiny_id and url
                    detailed = self._client.get_listing(global_id)
                    if detailed:
                        listings.append(
                            self._normalize_listing(detailed.to_dict(), filters.property_type)
                        )
                    else:
                        logger.warning(f"Could not fetch details for global_id {global_id}")
                except Exception as e:
                    logger.warning(f"Failed to fetch details for result: {e}")
                    continue

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
            # Fetch listing details using get_listing
            result = self._client.get_listing(listing_id)

            if not result:
                logger.warning(f"Listing {listing_id} not found")
                return None

            # Convert to dict
            result_dict = result.to_dict()

            # Determine property type from offering_type field
            offering_type = result_dict.get("offering_type", "").lower()
            is_buy = offering_type == "buy" or "koop" in offering_type
            property_type = PropertyType.BUY if is_buy else PropertyType.RENT

            listing = self._normalize_listing(result_dict, property_type)
            logger.info(f"Fetched details for {listing_id} via pyfunda")
            return listing

        except Exception as e:
            logger.error(f"PyFunda get_details failed for {listing_id}: {e}")
            raise
