"""Tests for ETL Pipeline."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from funda_finder.db.models import Base, PriceHistory, Property, ScrapeMeta
from funda_finder.etl import ETLPipeline, ETLResult
from funda_finder.scraper.base import PropertyType, RawListing, ScraperSource, SearchFilters
from funda_finder.scraper.composite import AllScrapersFailed
from funda_finder.validation.models import PropertyListing
from tests.conftest import make_raw_listing


@pytest.fixture
def etl_db():
    """Create an in-memory database for ETL tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def mock_scraper():
    """Create a mock CompositeScraper."""
    return MagicMock()


@pytest.fixture
def pipeline(mock_scraper):
    """Create an ETL pipeline with mocked scraper and no auto-init."""
    return ETLPipeline(scraper=mock_scraper, auto_init_db=False)


class TestETLResult:
    """Tests for ETLResult dataclass."""

    def test_duration_seconds(self):
        start = datetime(2024, 1, 1, 12, 0, 0)
        end = datetime(2024, 1, 1, 12, 1, 30)
        result = ETLResult(
            run_id="test",
            started_at=start,
            finished_at=end,
            listings_found=10,
            new_count=5,
            updated_count=3,
            inactive_count=2,
            validation_errors=0,
            db_errors=0,
            success=True,
        )
        assert result.duration_seconds == 90.0

    def test_to_dict(self):
        result = ETLResult(
            run_id="test-run",
            started_at=datetime(2024, 1, 1),
            finished_at=datetime(2024, 1, 1, 0, 1),
            listings_found=10,
            new_count=5,
            updated_count=3,
            inactive_count=2,
            validation_errors=1,
            db_errors=0,
            success=True,
        )
        d = result.to_dict()
        assert d["run_id"] == "test-run"
        assert d["listings_found"] == 10
        assert d["new_count"] == 5
        assert d["success"] is True
        assert "duration_seconds" in d
        assert d["error_message"] is None

    def test_to_dict_with_error(self):
        result = ETLResult(
            run_id="fail-run",
            started_at=datetime(2024, 1, 1),
            finished_at=datetime(2024, 1, 1),
            listings_found=0,
            new_count=0,
            updated_count=0,
            inactive_count=0,
            validation_errors=0,
            db_errors=0,
            success=False,
            error_message="All scrapers failed",
        )
        d = result.to_dict()
        assert d["success"] is False
        assert d["error_message"] == "All scrapers failed"


class TestETLPipelineExtract:
    """Tests for the extract phase."""

    def test_extract_calls_scraper(self, pipeline, mock_scraper):
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="e1"),
            make_raw_listing(listing_id="e2"),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline._extract(filters)

        assert len(result) == 2
        mock_scraper.search.assert_called_once_with(filters)

    def test_extract_empty(self, pipeline, mock_scraper):
        mock_scraper.search.return_value = []
        filters = SearchFilters(city="EmptyCity", property_type=PropertyType.BUY)
        result = pipeline._extract(filters)
        assert len(result) == 0

    def test_extract_propagates_error(self, pipeline, mock_scraper):
        mock_scraper.search.side_effect = AllScrapersFailed("All failed")
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        with pytest.raises(AllScrapersFailed):
            pipeline._extract(filters)


class TestETLPipelineTransform:
    """Tests for the transform phase."""

    def test_transform_valid_listings(self, pipeline):
        raw_listings = [
            make_raw_listing(listing_id="t1", price=400000),
            make_raw_listing(listing_id="t2", price=500000),
        ]
        validated, failed = pipeline._transform(raw_listings)
        assert len(validated) == 2
        assert len(failed) == 0
        assert all(isinstance(v, PropertyListing) for v in validated)

    def test_transform_filters_invalid(self, pipeline):
        raw_listings = [
            make_raw_listing(listing_id="good"),
            RawListing(
                listing_id="bad",
                url="https://www.funda.nl/koop/amsterdam/bad",
                address="Test",
                city="Amsterdam",
                price=-100,
                property_type=PropertyType.BUY,
            ),
        ]
        validated, failed = pipeline._transform(raw_listings)
        assert len(validated) == 1
        assert len(failed) == 1


class TestETLPipelineLoad:
    """Tests for the load phase."""

    def test_load_new_listings(self, pipeline, etl_db):
        listings = [
            PropertyListing(
                funda_id="new-1",
                url="https://www.funda.nl/koop/amsterdam/new-1",
                city="Amsterdam",
                price=400000,
                listing_type="buy",
            ),
            PropertyListing(
                funda_id="new-2",
                url="https://www.funda.nl/koop/amsterdam/new-2",
                city="Amsterdam",
                price=500000,
                listing_type="buy",
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        new, updated, inactive, errors = pipeline._load(etl_db, listings, filters)

        assert new == 2
        assert updated == 0
        assert errors == 0

        # Verify in database
        props = etl_db.query(Property).all()
        assert len(props) == 2

    def test_load_updates_existing(self, pipeline, etl_db):
        # Insert existing property
        prop = Property(
            funda_id="upd-1",
            url="https://www.funda.nl/koop/amsterdam/upd-1",
            address="Old Address",
            city="Amsterdam",
            price=400000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        etl_db.add(prop)
        etl_db.commit()

        # Load updated data
        listings = [
            PropertyListing(
                funda_id="upd-1",
                url="https://www.funda.nl/koop/amsterdam/upd-1",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                address="New Address",
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        new, updated, inactive, errors = pipeline._load(etl_db, listings, filters)

        assert new == 0
        assert updated == 1

        etl_db.refresh(prop)
        assert prop.price == 450000
        assert prop.address == "New Address"

    def test_load_tracks_price_history(self, pipeline, etl_db):
        # Insert existing property
        prop = Property(
            funda_id="ph-1",
            url="https://www.funda.nl/koop/amsterdam/ph-1",
            address="Test",
            city="Amsterdam",
            price=400000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        etl_db.add(prop)
        etl_db.commit()
        prop_id = prop.id

        # Load with new price
        listings = [
            PropertyListing(
                funda_id="ph-1",
                url="https://www.funda.nl/koop/amsterdam/ph-1",
                city="Amsterdam",
                price=380000,
                listing_type="buy",
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        pipeline._load(etl_db, listings, filters)

        history = etl_db.query(PriceHistory).filter_by(property_id=prop_id).all()
        assert len(history) >= 1
        assert any(h.price == 380000 for h in history)

    def test_load_marks_inactive(self, pipeline, etl_db):
        # Insert two active properties
        for fid in ["active-1", "gone-1"]:
            etl_db.add(Property(
                funda_id=fid,
                url=f"https://www.funda.nl/koop/amsterdam/{fid}",
                address="Test",
                city="Amsterdam",
                price=400000,
                listing_type="buy",
                status="active",
                scraped_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            ))
        etl_db.commit()

        # Scrape only returns active-1
        listings = [
            PropertyListing(
                funda_id="active-1",
                url="https://www.funda.nl/koop/amsterdam/active-1",
                city="Amsterdam",
                price=400000,
                listing_type="buy",
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        new, updated, inactive, errors = pipeline._load(etl_db, listings, filters)

        assert inactive == 1
        gone = etl_db.query(Property).filter_by(funda_id="gone-1").first()
        assert gone.status == "inactive"

    def test_load_reactivates_listing(self, pipeline, etl_db):
        # Insert an inactive property
        prop = Property(
            funda_id="reactivate-1",
            url="https://www.funda.nl/koop/amsterdam/reactivate-1",
            address="Test",
            city="Amsterdam",
            price=400000,
            listing_type="buy",
            status="inactive",
            scraped_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        etl_db.add(prop)
        etl_db.commit()

        # Scrape finds it again
        listings = [
            PropertyListing(
                funda_id="reactivate-1",
                url="https://www.funda.nl/koop/amsterdam/reactivate-1",
                city="Amsterdam",
                price=400000,
                listing_type="buy",
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        pipeline._load(etl_db, listings, filters)

        etl_db.refresh(prop)
        assert prop.status == "active"


class TestETLPipelineRun:
    """Tests for the full pipeline run."""

    def test_run_success(self, pipeline, mock_scraper, etl_db):
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="run-1", price=400000),
            make_raw_listing(listing_id="run-2", price=500000),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=etl_db)

        assert result.success is True
        assert result.listings_found == 2
        assert result.new_count == 2
        assert result.run_id is not None

    def test_run_scraper_failure(self, pipeline, mock_scraper, etl_db):
        mock_scraper.search.side_effect = AllScrapersFailed("All failed")
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=etl_db)

        assert result.success is False
        assert "All scrapers failed" in result.error_message

    def test_run_creates_scrape_meta(self, pipeline, mock_scraper, etl_db):
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="meta-1"),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=etl_db)

        meta = etl_db.query(ScrapeMeta).first()
        assert meta is not None
        assert meta.listings_found == 1
        assert meta.listings_new == 1
        assert meta.finished_at is not None

    def test_run_with_validation_errors(self, pipeline, mock_scraper, etl_db):
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="valid-1"),
            RawListing(
                listing_id="invalid-1",
                url="https://www.funda.nl/koop/amsterdam/bad",
                address="Test",
                city="Amsterdam",
                price=-100,
                property_type=PropertyType.BUY,
            ),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        result = pipeline.run(filters, session=etl_db)

        assert result.success is True
        assert result.listings_found == 2
        assert result.validation_errors == 1
        assert result.new_count == 1


class TestETLPipelineBatch:
    """Tests for batch pipeline runs."""

    def test_run_batch_multiple_cities(self, pipeline, mock_scraper, etl_db):
        # Provide session via patching SessionLocal since run_batch creates its own
        mock_scraper.search.return_value = [
            make_raw_listing(listing_id="batch-1"),
        ]

        with patch("funda_finder.etl.SessionLocal", return_value=etl_db), \
             patch("funda_finder.etl.init_db"):
            results = pipeline.run_batch(
                cities=["Amsterdam", "Rotterdam"],
                property_type=PropertyType.BUY,
            )

        assert len(results) == 2

    def test_run_batch_partial_failure(self, pipeline, mock_scraper, etl_db):
        call_count = [0]

        def search_side_effect(filters):
            call_count[0] += 1
            if call_count[0] == 1:
                return [make_raw_listing(listing_id="ok-1")]
            raise AllScrapersFailed("Failed for city 2")

        mock_scraper.search.side_effect = search_side_effect

        with patch("funda_finder.etl.SessionLocal", return_value=etl_db), \
             patch("funda_finder.etl.init_db"):
            results = pipeline.run_batch(
                cities=["Amsterdam", "Rotterdam"],
                property_type=PropertyType.BUY,
            )

        assert len(results) == 2
        assert results[0].success is True
        assert results[1].success is False
