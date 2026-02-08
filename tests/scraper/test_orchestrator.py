"""Tests for scrape orchestrator."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from funda_finder.db.models import Base, Property, ScrapeMeta
from funda_finder.scraper import PropertyType, RawListing, ScraperSource
from funda_finder.scraper.orchestrator import ScrapeOrchestrator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def mock_scraper():
    """Create a mock CompositeScraper."""
    with patch("funda_finder.scraper.orchestrator.CompositeScraper") as mock:
        yield mock


def create_test_listing(
    listing_id: str = "test-123",
    city: str = "amsterdam",
    price: int = 500000,
) -> RawListing:
    """Create a test RawListing."""
    return RawListing(
        listing_id=listing_id,
        url=f"https://funda.nl/koop/amsterdam/huis-{listing_id}",
        address="Test Street 123",
        city=city,
        price=price,
        property_type=PropertyType.BUY,
        living_area=100.0,
        num_rooms=4,
        source=ScraperSource.PYFUNDA,
        scraped_at=datetime.utcnow(),
        raw_data={"test": "data"},
    )


class TestScrapeOrchestrator:
    def test_run_scrape_creates_meta(self, db_session, mock_scraper):
        """Test that run_scrape creates ScrapeMeta record."""
        mock_scraper.return_value.search.return_value = []

        orchestrator = ScrapeOrchestrator(session=db_session)
        meta = orchestrator.run_scrape("amsterdam")

        assert meta.run_id is not None
        assert meta.started_at is not None
        assert meta.finished_at is not None
        assert meta.listings_found == 0

    def test_run_scrape_new_listing(self, db_session, mock_scraper):
        """Test processing a new listing."""
        listing = create_test_listing()
        mock_scraper.return_value.search.return_value = [listing]

        orchestrator = ScrapeOrchestrator(session=db_session)
        meta = orchestrator.run_scrape("amsterdam")

        assert meta.listings_found == 1
        assert meta.listings_new == 1
        assert meta.listings_updated == 0

        # Verify property was created
        prop = db_session.query(Property).filter_by(funda_id="test-123").first()
        assert prop is not None
        assert prop.address == "Test Street 123"
        assert prop.price == 500000
        assert prop.status == "active"

    def test_run_scrape_update_listing(self, db_session, mock_scraper):
        """Test updating an existing listing."""
        # Create existing property
        prop = Property(
            funda_id="test-123",
            url="https://funda.nl/old",
            address="Old Address",
            city="amsterdam",
            price=450000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db_session.add(prop)
        db_session.commit()

        # Scrape with updated data
        listing = create_test_listing(price=475000)
        mock_scraper.return_value.search.return_value = [listing]

        orchestrator = ScrapeOrchestrator(session=db_session)
        meta = orchestrator.run_scrape("amsterdam")

        assert meta.listings_found == 1
        assert meta.listings_new == 0
        assert meta.listings_updated == 1

        # Verify property was updated
        db_session.refresh(prop)
        assert prop.price == 475000
        assert prop.address == "Test Street 123"  # Updated

    def test_price_history_tracking(self, db_session, mock_scraper):
        """Test that price changes are tracked in price_history."""
        # Create existing property
        prop = Property(
            funda_id="test-123",
            url="https://funda.nl/old",
            address="Test Street 123",
            city="amsterdam",
            price=500000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db_session.add(prop)
        db_session.commit()
        prop_id = prop.id

        # Scrape with new price
        listing = create_test_listing(price=475000)
        mock_scraper.return_value.search.return_value = [listing]

        orchestrator = ScrapeOrchestrator(session=db_session)
        orchestrator.run_scrape("amsterdam")

        # Verify price history entry was created
        from funda_finder.db.models import PriceHistory

        history = (
            db_session.query(PriceHistory)
            .filter_by(property_id=prop_id, price=475000)
            .first()
        )
        assert history is not None

    def test_mark_delisted(self, db_session, mock_scraper):
        """Test that properties not in scrape results are marked delisted."""
        # Create two properties
        prop1 = Property(
            funda_id="test-1",
            url="https://funda.nl/1",
            address="Address 1",
            city="amsterdam",
            price=500000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        prop2 = Property(
            funda_id="test-2",
            url="https://funda.nl/2",
            address="Address 2",
            city="amsterdam",
            price=600000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db_session.add_all([prop1, prop2])
        db_session.commit()

        # Scrape returns only prop1
        listing = create_test_listing(listing_id="test-1")
        mock_scraper.return_value.search.return_value = [listing]

        orchestrator = ScrapeOrchestrator(session=db_session)
        orchestrator.run_scrape("amsterdam")

        # Verify prop2 is marked delisted
        db_session.refresh(prop2)
        assert prop2.status == "delisted"

        # Verify prop1 is still active
        db_session.refresh(prop1)
        assert prop1.status == "active"

    def test_progress_callback(self, db_session, mock_scraper):
        """Test that progress callback is called."""
        listing = create_test_listing()
        mock_scraper.return_value.search.return_value = [listing]

        messages = []

        def progress(msg: str):
            messages.append(msg)

        orchestrator = ScrapeOrchestrator(session=db_session)
        orchestrator.run_scrape("amsterdam", progress_callback=progress)

        assert len(messages) > 0
        assert any("Starting scrape" in msg for msg in messages)
        assert any("Found 1 listings" in msg for msg in messages)

    def test_error_handling(self, db_session, mock_scraper):
        """Test that errors during listing processing are tracked."""
        listing1 = create_test_listing(listing_id="test-1")
        listing2 = create_test_listing(listing_id="test-2")
        mock_scraper.return_value.search.return_value = [listing1, listing2]

        orchestrator = ScrapeOrchestrator(session=db_session)

        # Mock _process_listing to raise error for second listing
        original_process = orchestrator._process_listing

        def process_with_error(listing):
            if listing.listing_id == "test-2":
                raise Exception("Test error")
            return original_process(listing)

        orchestrator._process_listing = process_with_error

        meta = orchestrator.run_scrape("amsterdam")

        assert meta.listings_found == 2
        assert meta.listings_new == 1  # Only first listing succeeded
        assert meta.errors == 1
