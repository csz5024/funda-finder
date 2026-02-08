"""Scrape orchestration: manage scrape runs, incremental updates, and progress tracking."""

import json
import logging
from datetime import datetime
from typing import Callable, List, Optional

from sqlalchemy.orm import Session

from funda_finder.db.models import PriceHistory, Property, ScrapeMeta
from funda_finder.db.session import get_db
from funda_finder.scraper import CompositeScraper, PropertyType, RawListing, SearchFilters

logger = logging.getLogger(__name__)


class ScrapeOrchestrator:
    """Manages scrape runs and incremental database updates."""

    def __init__(self, session: Optional[Session] = None, rate_limit: float = 3.0):
        """Initialize orchestrator.

        Args:
            session: Database session (creates new if None)
            rate_limit: Seconds between scrape requests
        """
        self.session = session or next(get_db())
        self.scraper = CompositeScraper(rate_limit_seconds=rate_limit)
        self.meta: Optional[ScrapeMeta] = None

    def run_scrape(
        self,
        city: str,
        property_type: PropertyType = PropertyType.BUY,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> ScrapeMeta:
        """Execute a scrape run for the given city.

        Args:
            city: City to scrape (e.g., "amsterdam")
            property_type: Type of properties to scrape
            progress_callback: Optional callback for progress updates

        Returns:
            ScrapeMeta object with run statistics
        """
        # Initialize run tracking
        self.meta = ScrapeMeta(started_at=datetime.utcnow())
        self.session.add(self.meta)
        self.session.commit()

        def report(msg: str):
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)

        try:
            report(f"Starting scrape: {city} ({property_type.value})")

            # Search for listings
            filters = SearchFilters(city=city, property_type=property_type)
            listings = self.scraper.search(filters)

            self.meta.listings_found = len(listings)
            report(f"Found {len(listings)} listings")

            # Process each listing
            for idx, listing in enumerate(listings, 1):
                try:
                    is_new = self._process_listing(listing)
                    action = "new" if is_new else "updated"
                    report(f"[{idx}/{len(listings)}] {listing.address} ({action})")
                except Exception as e:
                    logger.error(f"Error processing listing {listing.listing_id}: {e}")
                    self.meta.errors += 1

            # Mark delisted properties
            delisted_count = self._mark_delisted(city, property_type, listings)
            if delisted_count:
                report(f"Marked {delisted_count} properties as delisted")

            # Finalize run
            self.meta.finished_at = datetime.utcnow()
            self.session.commit()

            report(
                f"Scrape complete: {self.meta.listings_new} new, "
                f"{self.meta.listings_updated} updated, "
                f"{delisted_count} delisted, "
                f"{self.meta.errors} errors"
            )

            return self.meta

        except Exception as e:
            logger.exception(f"Scrape failed: {e}")
            self.meta.finished_at = datetime.utcnow()
            self.session.commit()
            raise

    def _process_listing(self, listing: RawListing) -> bool:
        """Process a single listing (insert or update).

        Returns:
            True if new listing, False if updated
        """
        # Check if property exists
        existing = (
            self.session.query(Property)
            .filter(Property.funda_id == listing.listing_id)
            .first()
        )

        is_new = existing is None

        if is_new:
            # Create new property
            prop = Property(
                funda_id=listing.listing_id,
                url=listing.url,
                address=listing.address,
                city=listing.city,
                postal_code=listing.postal_code,
                price=listing.price,
                living_area=int(listing.living_area) if listing.living_area else None,
                plot_area=int(listing.plot_area) if listing.plot_area else None,
                rooms=listing.num_rooms,
                bedrooms=listing.num_bedrooms,
                year_built=listing.construction_year,
                energy_label=listing.energy_label,
                listing_type=listing.property_type.value,
                status="active",
                description=listing.description,
                raw_json=json.dumps(listing.raw_data),
                scraped_at=listing.scraped_at,
                updated_at=datetime.utcnow(),
            )
            self.session.add(prop)
            self.session.flush()  # Get the ID

            # Add initial price history
            price_entry = PriceHistory(
                property_id=prop.id,
                price=listing.price,
                observed_at=listing.scraped_at,
            )
            self.session.add(price_entry)

            self.meta.listings_new += 1
        else:
            # Update existing property
            price_changed = existing.price != listing.price

            existing.url = listing.url
            existing.address = listing.address
            existing.postal_code = listing.postal_code
            existing.price = listing.price
            existing.living_area = (
                int(listing.living_area) if listing.living_area else existing.living_area
            )
            existing.plot_area = (
                int(listing.plot_area) if listing.plot_area else existing.plot_area
            )
            existing.rooms = listing.num_rooms or existing.rooms
            existing.bedrooms = listing.num_bedrooms or existing.bedrooms
            existing.year_built = listing.construction_year or existing.year_built
            existing.energy_label = listing.energy_label or existing.energy_label
            existing.description = listing.description or existing.description
            existing.raw_json = json.dumps(listing.raw_data)
            existing.status = "active"  # Re-activate if it was delisted
            existing.updated_at = datetime.utcnow()

            # Track price changes
            if price_changed:
                price_entry = PriceHistory(
                    property_id=existing.id,
                    price=listing.price,
                    observed_at=datetime.utcnow(),
                )
                self.session.add(price_entry)

            self.meta.listings_updated += 1

        self.session.commit()
        return is_new

    def _mark_delisted(
        self,
        city: str,
        property_type: PropertyType,
        current_listings: List[RawListing],
    ) -> int:
        """Mark properties as delisted if they're no longer in scrape results.

        Args:
            city: City that was scraped
            property_type: Property type that was scraped
            current_listings: Listings found in current scrape

        Returns:
            Number of properties marked as delisted
        """
        current_ids = {listing.listing_id for listing in current_listings}

        # Find previously active properties not in current scrape
        delisted = (
            self.session.query(Property)
            .filter(
                Property.city == city,
                Property.listing_type == property_type.value,
                Property.status == "active",
                Property.funda_id.notin_(current_ids),
            )
            .all()
        )

        for prop in delisted:
            prop.status = "delisted"
            prop.updated_at = datetime.utcnow()

        if delisted:
            self.session.commit()

        return len(delisted)
