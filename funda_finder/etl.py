"""ETL Pipeline for Funda property listings.

This module orchestrates the Extract-Transform-Load process:
1. Extract: Scrape raw listings using CompositeScraper
2. Transform: Validate and clean data using PropertyListing models
3. Load: Insert/update PostgreSQL with proper deduplication and tracking

Features:
- Deduplication by funda_id
- Detect new vs updated listings
- Handle removed listings (mark as inactive)
- Batch insertions for performance
- Transaction handling with rollback on errors
- Price history tracking
- Comprehensive logging and metrics

Example usage:
    from funda_finder.etl import ETLPipeline
    from funda_finder.scraper import SearchFilters, PropertyType

    pipeline = ETLPipeline()

    # Run for a specific city
    filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
    result = pipeline.run(filters)
    print(f"New: {result.new_count}, Updated: {result.updated_count}")

    # Run for multiple cities
    result = pipeline.run_batch(["amsterdam", "rotterdam", "utrecht"])
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Set

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from funda_finder.config import settings
from funda_finder.db.models import PriceHistory, Property, ScrapeMeta
from funda_finder.db.session import SessionLocal, init_db
from funda_finder.scraper import (
    AllScrapersFailed,
    CompositeScraper,
    PropertyType,
    RawListing,
    SearchFilters,
)
from funda_finder.validation import PropertyListing, raw_to_validated_batch

logger = logging.getLogger(__name__)


@dataclass
class ETLResult:
    """Result of an ETL pipeline run."""

    run_id: str
    started_at: datetime
    finished_at: datetime
    listings_found: int
    new_count: int
    updated_count: int
    inactive_count: int
    validation_errors: int
    db_errors: int
    success: bool
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        """Calculate run duration in seconds."""
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> dict:
        """Convert result to dictionary."""
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "listings_found": self.listings_found,
            "new_count": self.new_count,
            "updated_count": self.updated_count,
            "inactive_count": self.inactive_count,
            "validation_errors": self.validation_errors,
            "db_errors": self.db_errors,
            "success": self.success,
            "error_message": self.error_message,
        }


class ETLPipeline:
    """ETL Pipeline orchestrator for Funda property listings.

    This class manages the complete Extract-Transform-Load process with:
    - Automatic scraping with fallback
    - Data validation and cleaning
    - Deduplication and change detection
    - Transaction safety
    - Comprehensive logging and metrics
    """

    def __init__(
        self,
        scraper: Optional[CompositeScraper] = None,
        auto_init_db: bool = True,
        batch_size: int = 100,
    ):
        """Initialize ETL pipeline.

        Args:
            scraper: Optional scraper instance (creates default if None)
            auto_init_db: Whether to initialize database tables on startup
            batch_size: Number of listings to process in each database batch
        """
        self.scraper = scraper or CompositeScraper(
            rate_limit_seconds=settings.rate_limit
        )
        self.batch_size = batch_size

        if auto_init_db:
            init_db()
            logger.info("Database tables initialized")

    def run(
        self, filters: SearchFilters, session: Optional[Session] = None
    ) -> ETLResult:
        """Run the complete ETL pipeline for a single search.

        Args:
            filters: Search parameters for scraping
            session: Optional database session (creates new if None)

        Returns:
            ETLResult with metrics and status

        Example:
            >>> pipeline = ETLPipeline()
            >>> filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
            >>> result = pipeline.run(filters)
            >>> print(f"Processed {result.listings_found} listings")
        """
        started_at = datetime.utcnow()
        run_id = None
        error_message = None

        # Create session if not provided
        should_close_session = session is None
        if session is None:
            session = SessionLocal()

        try:
            # Create scrape metadata record
            meta = ScrapeMeta(started_at=started_at)
            session.add(meta)
            session.commit()
            run_id = meta.run_id

            logger.info(
                f"Starting ETL run {run_id} for {filters.city} ({filters.property_type.value})"
            )

            # EXTRACT: Scrape raw listings
            raw_listings = self._extract(filters)
            meta.listings_found = len(raw_listings)
            session.commit()

            # TRANSFORM: Validate and clean
            validated_listings, failed = self._transform(raw_listings)
            validation_errors = len(failed)

            # LOAD: Insert/update database
            new_count, updated_count, inactive_count, db_errors = self._load(
                session, validated_listings, filters
            )

            # Update metadata
            meta.listings_new = new_count
            meta.listings_updated = updated_count
            meta.errors = validation_errors + db_errors
            meta.finished_at = datetime.utcnow()
            session.commit()

            logger.info(
                f"ETL run {run_id} completed successfully: "
                f"{new_count} new, {updated_count} updated, "
                f"{inactive_count} marked inactive, "
                f"{validation_errors} validation errors, {db_errors} DB errors"
            )

            return ETLResult(
                run_id=run_id,
                started_at=started_at,
                finished_at=datetime.utcnow(),
                listings_found=len(raw_listings),
                new_count=new_count,
                updated_count=updated_count,
                inactive_count=inactive_count,
                validation_errors=validation_errors,
                db_errors=db_errors,
                success=True,
            )

        except AllScrapersFailed as e:
            error_message = f"All scrapers failed: {e}"
            logger.error(f"ETL run {run_id} failed: {error_message}")
            if session:
                session.rollback()
            return self._create_error_result(
                run_id, started_at, error_message, session
            )

        except SQLAlchemyError as e:
            error_message = f"Database error: {e}"
            logger.error(f"ETL run {run_id} failed: {error_message}")
            if session:
                session.rollback()
            return self._create_error_result(
                run_id, started_at, error_message, session
            )

        except Exception as e:
            error_message = f"Unexpected error: {e}"
            logger.exception(f"ETL run {run_id} failed with unexpected error")
            if session:
                session.rollback()
            return self._create_error_result(
                run_id, started_at, error_message, session
            )

        finally:
            if should_close_session and session:
                session.close()

    def run_batch(
        self,
        cities: Optional[List[str]] = None,
        property_type: PropertyType = PropertyType.BUY,
    ) -> List[ETLResult]:
        """Run ETL pipeline for multiple cities.

        Args:
            cities: List of city names (uses config default if None)
            property_type: Type of properties to scrape

        Returns:
            List of ETLResult for each city

        Example:
            >>> pipeline = ETLPipeline()
            >>> results = pipeline.run_batch(["amsterdam", "rotterdam", "utrecht"])
            >>> total_new = sum(r.new_count for r in results)
        """
        cities = cities or settings.city_list
        results = []

        logger.info(
            f"Starting batch ETL run for {len(cities)} cities: {', '.join(cities)}"
        )

        for city in cities:
            filters = SearchFilters(city=city, property_type=property_type)
            result = self.run(filters)
            results.append(result)

            if not result.success:
                logger.warning(f"ETL run for {city} failed: {result.error_message}")

        successful = sum(1 for r in results if r.success)
        total_new = sum(r.new_count for r in results if r.success)
        total_updated = sum(r.updated_count for r in results if r.success)

        logger.info(
            f"Batch ETL completed: {successful}/{len(cities)} successful, "
            f"{total_new} total new, {total_updated} total updated"
        )

        return results

    def _extract(self, filters: SearchFilters) -> List[RawListing]:
        """Extract raw listings from scrapers.

        Args:
            filters: Search parameters

        Returns:
            List of raw listings

        Raises:
            AllScrapersFailed: If scraping fails
        """
        logger.info(f"Extracting listings for {filters.city}")
        raw_listings = self.scraper.search(filters)
        logger.info(f"Extracted {len(raw_listings)} raw listings")
        return raw_listings

    def _transform(
        self, raw_listings: List[RawListing]
    ) -> tuple[List[PropertyListing], List[RawListing]]:
        """Transform raw listings into validated models.

        Args:
            raw_listings: Raw scraped listings

        Returns:
            Tuple of (validated_listings, failed_listings)
        """
        logger.info(f"Transforming {len(raw_listings)} listings")
        validated, failed = raw_to_validated_batch(raw_listings)
        logger.info(
            f"Transformation complete: {len(validated)} valid, {len(failed)} failed"
        )
        return validated, failed

    def _load(
        self,
        session: Session,
        validated_listings: List[PropertyListing],
        filters: SearchFilters,
    ) -> tuple[int, int, int, int]:
        """Load validated listings into database.

        This method:
        1. Deduplicates by funda_id
        2. Detects new vs existing listings
        3. Updates existing listings if changed
        4. Tracks price changes in price_history
        5. Marks listings as inactive if not in current scrape
        6. Uses batch processing for performance

        Args:
            session: Database session
            validated_listings: Validated property listings
            filters: Search filters used (for determining inactive listings)

        Returns:
            Tuple of (new_count, updated_count, inactive_count, error_count)
        """
        logger.info(f"Loading {len(validated_listings)} listings into database")

        new_count = 0
        updated_count = 0
        error_count = 0

        # Get all funda_ids from this scrape
        scraped_funda_ids: Set[str] = {
            listing.funda_id for listing in validated_listings
        }

        # Process in batches
        for i in range(0, len(validated_listings), self.batch_size):
            batch = validated_listings[i : i + self.batch_size]

            for listing in batch:
                try:
                    # Check if listing exists
                    stmt = select(Property).where(
                        Property.funda_id == listing.funda_id
                    )
                    existing = session.execute(stmt).scalar_one_or_none()

                    if existing:
                        # Update existing listing
                        updated = self._update_property(session, existing, listing)
                        if updated:
                            updated_count += 1
                    else:
                        # Insert new listing
                        self._insert_property(session, listing)
                        new_count += 1

                except IntegrityError as e:
                    logger.warning(
                        f"Integrity error for listing {listing.funda_id}: {e}"
                    )
                    session.rollback()
                    error_count += 1
                    continue

                except Exception as e:
                    logger.error(
                        f"Error loading listing {listing.funda_id}: {e}", exc_info=True
                    )
                    session.rollback()
                    error_count += 1
                    continue

            # Commit batch
            try:
                session.commit()
                logger.debug(
                    f"Committed batch {i // self.batch_size + 1} "
                    f"({len(batch)} listings)"
                )
            except SQLAlchemyError as e:
                logger.error(f"Error committing batch: {e}")
                session.rollback()
                error_count += len(batch)

        # Mark inactive listings
        inactive_count = self._mark_inactive(
            session, scraped_funda_ids, filters.city, filters.property_type
        )

        logger.info(
            f"Load complete: {new_count} new, {updated_count} updated, "
            f"{inactive_count} marked inactive, {error_count} errors"
        )

        return new_count, updated_count, inactive_count, error_count

    def _insert_property(self, session: Session, listing: PropertyListing) -> None:
        """Insert a new property into the database.

        Args:
            session: Database session
            listing: Validated property listing
        """
        prop = Property(
            funda_id=listing.funda_id,
            url=listing.url,
            address=listing.address or "",
            city=listing.city,
            postal_code=listing.postal_code,
            price=listing.price,
            living_area=int(listing.living_area) if listing.living_area else None,
            plot_area=int(listing.plot_area) if listing.plot_area else None,
            rooms=listing.rooms,
            bedrooms=listing.bedrooms,
            bathrooms=listing.bathrooms,
            year_built=listing.year_built,
            energy_label=listing.energy_label,
            listing_type=listing.listing_type,
            status="active",
            lat=listing.lat,
            lon=listing.lon,
            description=listing.description,
            raw_json=json.dumps({"source": listing.source}),
            scraped_at=listing.scraped_at,
        )

        session.add(prop)
        session.flush()  # Ensure we have the ID

        # Add initial price history entry
        price_hist = PriceHistory(
            property_id=prop.id, price=listing.price, observed_at=listing.scraped_at
        )
        session.add(price_hist)

        logger.debug(f"Inserted new property: {listing.funda_id}")

    def _update_property(
        self, session: Session, existing: Property, listing: PropertyListing
    ) -> bool:
        """Update an existing property if changes detected.

        Args:
            session: Database session
            existing: Existing property record
            listing: New validated listing data

        Returns:
            True if property was updated, False if no changes detected
        """
        updated = False

        # Update fields if changed
        if existing.price != listing.price:
            # Track price change
            price_hist = PriceHistory(
                property_id=existing.id,
                price=listing.price,
                observed_at=listing.scraped_at,
            )
            session.add(price_hist)
            existing.price = listing.price
            updated = True
            logger.debug(
                f"Price changed for {listing.funda_id}: "
                f"{existing.price} -> {listing.price}"
            )

        # Update other fields
        fields_to_update = {
            "url": listing.url,
            "address": listing.address or "",
            "postal_code": listing.postal_code,
            "living_area": int(listing.living_area) if listing.living_area else None,
            "plot_area": int(listing.plot_area) if listing.plot_area else None,
            "rooms": listing.rooms,
            "bedrooms": listing.bedrooms,
            "bathrooms": listing.bathrooms,
            "year_built": listing.year_built,
            "energy_label": listing.energy_label,
            "lat": listing.lat,
            "lon": listing.lon,
            "description": listing.description,
            "scraped_at": listing.scraped_at,
        }

        for field, value in fields_to_update.items():
            if getattr(existing, field) != value:
                setattr(existing, field, value)
                updated = True

        # Always mark as active when seen in scrape
        if existing.status != "active":
            existing.status = "active"
            updated = True
            logger.debug(f"Reactivated listing: {listing.funda_id}")

        if updated:
            existing.updated_at = datetime.utcnow()
            logger.debug(f"Updated property: {listing.funda_id}")

        return updated

    def _mark_inactive(
        self,
        session: Session,
        scraped_funda_ids: Set[str],
        city: str,
        property_type: PropertyType,
    ) -> int:
        """Mark listings as inactive if they weren't in the current scrape.

        This identifies listings that are no longer available on Funda.

        Args:
            session: Database session
            scraped_funda_ids: Set of funda_ids from current scrape
            city: City that was scraped
            property_type: Property type that was scraped

        Returns:
            Number of listings marked inactive
        """
        # Find active listings for this city/type that weren't in the scrape
        stmt = (
            select(Property)
            .where(Property.city == city)
            .where(Property.listing_type == property_type.value)
            .where(Property.status == "active")
        )

        active_listings = session.execute(stmt).scalars().all()

        inactive_count = 0
        for prop in active_listings:
            if prop.funda_id not in scraped_funda_ids:
                prop.status = "inactive"
                prop.updated_at = datetime.utcnow()
                inactive_count += 1
                logger.debug(f"Marked inactive: {prop.funda_id}")

        if inactive_count > 0:
            session.commit()
            logger.info(f"Marked {inactive_count} listings as inactive")

        return inactive_count

    def _create_error_result(
        self,
        run_id: Optional[str],
        started_at: datetime,
        error_message: str,
        session: Optional[Session],
    ) -> ETLResult:
        """Create an ETLResult for a failed run.

        Args:
            run_id: Run ID (if created)
            started_at: When the run started
            error_message: Error description
            session: Database session (to update metadata if possible)

        Returns:
            ETLResult indicating failure
        """
        finished_at = datetime.utcnow()

        # Try to update metadata if we have a run_id
        if run_id and session:
            try:
                stmt = select(ScrapeMeta).where(ScrapeMeta.run_id == run_id)
                meta = session.execute(stmt).scalar_one_or_none()
                if meta:
                    meta.finished_at = finished_at
                    meta.errors = 1
                    session.commit()
            except Exception as e:
                logger.warning(f"Could not update scrape metadata: {e}")

        return ETLResult(
            run_id=run_id or "unknown",
            started_at=started_at,
            finished_at=finished_at,
            listings_found=0,
            new_count=0,
            updated_count=0,
            inactive_count=0,
            validation_errors=0,
            db_errors=0,
            success=False,
            error_message=error_message,
        )


def run_etl_for_city(
    city: str, property_type: PropertyType = PropertyType.BUY
) -> ETLResult:
    """Convenience function to run ETL for a single city.

    Args:
        city: City name
        property_type: Property type (buy or rent)

    Returns:
        ETLResult with metrics

    Example:
        >>> from funda_finder.etl import run_etl_for_city
        >>> from funda_finder.scraper import PropertyType
        >>> result = run_etl_for_city("Amsterdam", PropertyType.BUY)
    """
    pipeline = ETLPipeline()
    filters = SearchFilters(city=city, property_type=property_type)
    return pipeline.run(filters)


def run_etl_for_all_cities(
    property_type: PropertyType = PropertyType.BUY,
) -> List[ETLResult]:
    """Convenience function to run ETL for all configured cities.

    Args:
        property_type: Property type (buy or rent)

    Returns:
        List of ETLResult for each city

    Example:
        >>> from funda_finder.etl import run_etl_for_all_cities
        >>> results = run_etl_for_all_cities()
        >>> for result in results:
        ...     print(f"{result.city}: {result.new_count} new listings")
    """
    pipeline = ETLPipeline()
    return pipeline.run_batch(property_type=property_type)
