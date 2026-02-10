"""Integration tests: end-to-end scrape → validate → DB flow."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from funda_finder.db.models import Base, PriceHistory, Property, ScrapeMeta
from funda_finder.etl import ETLPipeline
from funda_finder.scraper.base import PropertyType, RawListing, ScraperSource, SearchFilters
from funda_finder.scraper.composite import AllScrapersFailed
from tests.conftest import make_raw_listing


@pytest.fixture
def integration_db():
    """Fresh in-memory database for integration tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


class TestEndToEnd:
    """End-to-end: scrape raw listings → validate → insert into DB."""

    def test_scrape_ten_listings_to_db(self, integration_db):
        """Simulate scraping 10 real-ish listings and loading them."""
        mock_scraper = MagicMock()
        raw_listings = [
            make_raw_listing(
                listing_id=f"e2e-{i:03d}",
                city="Amsterdam",
                price=300000 + i * 25000,
                address=f"Integration Street {i}",
                living_area=60.0 + i * 5,
                num_rooms=3 + (i % 4),
                num_bedrooms=1 + (i % 3),
                postal_code=f"10{i:02d}AB",
                construction_year=1980 + i * 3,
                energy_label=["A", "B", "C", "D"][i % 4],
            )
            for i in range(10)
        ]
        mock_scraper.search.return_value = raw_listings

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=integration_db)

        # Pipeline completed successfully
        assert result.success is True
        assert result.listings_found == 10
        assert result.new_count == 10
        assert result.validation_errors == 0
        assert result.db_errors == 0

        # All properties in DB
        props = integration_db.query(Property).all()
        assert len(props) == 10

        # Properties have expected data
        first = integration_db.query(Property).filter_by(funda_id="e2e-000").first()
        assert first.city == "Amsterdam"
        assert first.price == 300000
        assert first.status == "active"
        assert first.living_area == 60

        # Price history entries created
        histories = integration_db.query(PriceHistory).all()
        assert len(histories) == 10  # one per property

        # ScrapeMeta recorded
        meta = integration_db.query(ScrapeMeta).first()
        assert meta is not None
        assert meta.listings_found == 10
        assert meta.listings_new == 10

    def test_deduplication(self, integration_db):
        """Second scrape of same listings should update, not duplicate."""
        mock_scraper = MagicMock()
        listings = [
            make_raw_listing(listing_id="dedup-1", price=400000, address="Old Address"),
            make_raw_listing(listing_id="dedup-2", price=500000),
        ]
        mock_scraper.search.return_value = listings

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # First run
        result1 = pipeline.run(filters, session=integration_db)
        assert result1.new_count == 2

        # Second run with updated price
        updated_listings = [
            make_raw_listing(listing_id="dedup-1", price=420000, address="New Address"),
            make_raw_listing(listing_id="dedup-2", price=500000),
        ]
        mock_scraper.search.return_value = updated_listings
        result2 = pipeline.run(filters, session=integration_db)

        assert result2.new_count == 0
        assert result2.updated_count >= 1  # At least dedup-1 was updated

        # Still only 2 properties in DB
        total = integration_db.query(Property).count()
        assert total == 2

        # Price was updated
        prop = integration_db.query(Property).filter_by(funda_id="dedup-1").first()
        assert prop.price == 420000
        assert prop.address == "New Address"

    def test_price_history_across_scrapes(self, integration_db):
        """Price changes tracked across multiple scrape runs."""
        mock_scraper = MagicMock()
        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # Run 1: Initial price
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="price-track", price=500000),
        ]
        pipeline.run(filters, session=integration_db)

        # Run 2: Price drop
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="price-track", price=475000),
        ]
        pipeline.run(filters, session=integration_db)

        # Run 3: Further drop
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="price-track", price=450000),
        ]
        pipeline.run(filters, session=integration_db)

        prop = integration_db.query(Property).filter_by(funda_id="price-track").first()
        assert prop.price == 450000

        # Should have 3 price history entries (initial + 2 changes)
        history = (
            integration_db.query(PriceHistory)
            .filter_by(property_id=prop.id)
            .order_by(PriceHistory.observed_at)
            .all()
        )
        assert len(history) == 3
        assert history[0].price == 500000
        assert history[1].price == 475000
        assert history[2].price == 450000

    def test_delisting_detection(self, integration_db):
        """Properties not in new scrape should be marked inactive."""
        mock_scraper = MagicMock()
        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # Run 1: Two properties
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="stays", price=400000),
            make_raw_listing(listing_id="goes", price=500000),
        ]
        pipeline.run(filters, session=integration_db)

        # Run 2: Only one property remains
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="stays", price=400000),
        ]
        result = pipeline.run(filters, session=integration_db)

        assert result.inactive_count == 1

        stays = integration_db.query(Property).filter_by(funda_id="stays").first()
        goes = integration_db.query(Property).filter_by(funda_id="goes").first()
        assert stays.status == "active"
        assert goes.status == "inactive"


class TestErrorScenarios:
    """Tests for error handling in the pipeline."""

    def test_network_failure(self, integration_db):
        """Scraper network failure should produce error result."""
        mock_scraper = MagicMock()
        mock_scraper.search.side_effect = AllScrapersFailed("Network unreachable")

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=integration_db)

        assert result.success is False
        assert "All scrapers failed" in result.error_message
        assert result.new_count == 0

    def test_partial_invalid_html(self, integration_db):
        """Mix of valid and invalid scraped data should process valid ones."""
        mock_scraper = MagicMock()
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="valid-1", price=400000),
            RawListing(
                listing_id="invalid-html",
                url="https://www.funda.nl/koop/amsterdam/invalid",
                address="Bad",
                city="Amsterdam",
                price=-1,  # Invalid
                property_type=PropertyType.BUY,
            ),
            make_raw_listing(listing_id="valid-2", price=500000),
        ]

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=integration_db)

        assert result.success is True
        assert result.new_count == 2
        assert result.validation_errors == 1

    def test_duplicate_funda_ids_in_batch(self, integration_db):
        """Handle duplicate funda_ids within a single scrape gracefully."""
        mock_scraper = MagicMock()
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="dup-1", price=400000),
            make_raw_listing(listing_id="dup-1", price=410000),  # same ID
        ]

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=integration_db)

        # Should handle gracefully (first insert, second update)
        assert result.success is True
        props = integration_db.query(Property).filter_by(funda_id="dup-1").all()
        assert len(props) == 1

    def test_relisting_after_delist(self, integration_db):
        """Property that was delisted then relisted should become active."""
        mock_scraper = MagicMock()
        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # Run 1: Property appears
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="relist-1", price=400000),
        ]
        pipeline.run(filters, session=integration_db)

        # Run 2: Property disappears (a different listing occupies its place)
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="other-listing", price=600000),
        ]
        pipeline.run(filters, session=integration_db)

        prop = integration_db.query(Property).filter_by(funda_id="relist-1").first()
        assert prop.status == "inactive"

        # Run 3: Property reappears (along with the other)
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="relist-1", price=380000),
            make_raw_listing(listing_id="other-listing", price=600000),
        ]
        pipeline.run(filters, session=integration_db)

        integration_db.refresh(prop)
        assert prop.status == "active"
        assert prop.price == 380000
