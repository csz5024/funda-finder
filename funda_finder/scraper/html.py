"""HTML scraper implementation using funda-scraper library."""

import logging
import random
import time
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
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

    # User-Agent rotation pool to avoid detection
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    def __init__(self, rate_limit_seconds: float = 3.0):
        """Initialize HTML scraper.

        Args:
            rate_limit_seconds: Seconds to wait between requests (default: 3.0)
        """
        self._rate_limit = rate_limit_seconds
        self._last_request_time = 0.0
        self._current_user_agent = random.choice(self.USER_AGENTS)

    @property
    def source(self) -> ScraperSource:
        """Return the scraper source identifier."""
        return ScraperSource.HTML

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

    def _rotate_user_agent(self) -> str:
        """Rotate to a new random User-Agent.

        Returns:
            New User-Agent string
        """
        self._current_user_agent = random.choice(self.USER_AGENTS)
        logger.debug(f"Rotated User-Agent: {self._current_user_agent[:50]}...")
        return self._current_user_agent

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
        url = raw_data.get("url", "")
        # Extract listing_id from URL path segments
        # funda-scraper URLs: .../koop/city/huis-123/detail -> listing_id is "huis-123"
        # pyfunda-style URLs: .../koop/city/huis-123/ -> listing_id is "huis-123"
        parts = url.rstrip("/").split("/")
        if parts and parts[-1] == "detail":
            listing_id = str(parts[-2] if len(parts) >= 2 else "")
        else:
            listing_id = str(parts[-1] if parts else "")

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
        construction_type = raw_data.get("construction_type")
        property_subtype = raw_data.get("property_type") or raw_data.get("object_type")

        # Description
        description = raw_data.get("description") or raw_data.get("desc")

        # GPS coordinates
        latitude = raw_data.get("latitude") or raw_data.get("lat")
        longitude = raw_data.get("longitude") or raw_data.get("lon")

        # Property features
        has_garden = raw_data.get("has_garden") or raw_data.get("garden")
        has_parking = raw_data.get("has_parking") or raw_data.get("parking")
        has_balcony = raw_data.get("has_balcony") or raw_data.get("balcony")
        has_garage = raw_data.get("has_garage") or raw_data.get("garage")

        # Photos
        photos = raw_data.get("photos", []) or raw_data.get("images", []) or raw_data.get("photo", [])
        if not isinstance(photos, list):
            photos = []

        # Agent details
        agent_info = raw_data.get("agent", {}) or {}
        agent_name = agent_info.get("name") if isinstance(agent_info, dict) else raw_data.get("agent_name")
        agent_phone = agent_info.get("phone") if isinstance(agent_info, dict) else raw_data.get("agent_phone")
        agent_email = agent_info.get("email") if isinstance(agent_info, dict) else raw_data.get("agent_email")
        agency_name = agent_info.get("agency") if isinstance(agent_info, dict) else raw_data.get("agency_name")

        # Listing metadata
        listing_date = raw_data.get("listing_date") or raw_data.get("publication_date") or raw_data.get("date_list")
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
            # Lazy import to avoid dependency issues at module load time
            from funda_scraper import FundaScraper

            # Rotate User-Agent for each request
            user_agent = self._rotate_user_agent()

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
