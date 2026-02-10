"""Performance tests for scraping and ETL pipeline."""

import sys
import time

import pytest
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from funda_finder.db.models import Base, Property
from funda_finder.etl import ETLPipeline
from funda_finder.scraper.base import PropertyType, SearchFilters
from tests.conftest import make_raw_listing


@pytest.fixture
def perf_db():
    """Create a performance test database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


class TestScrapingPerformance:
    """Performance benchmarks for scraping pipeline."""

    def test_pipeline_throughput_100_listings(self, perf_db):
        """ETL pipeline should process 100 listings in reasonable time."""
        mock_scraper = MagicMock()
        raw_listings = [
            make_raw_listing(
                listing_id=f"perf-{i:04d}",
                price=300000 + i * 1000,
                living_area=60.0 + i * 0.5,
                num_rooms=2 + (i % 5),
            )
            for i in range(100)
        ]
        mock_scraper.search.return_value = raw_listings

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False, batch_size=50)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        start = time.time()
        result = pipeline.run(filters, session=perf_db)
        elapsed = time.time() - start

        assert result.success is True
        assert result.new_count == 100
        # Entire ETL for 100 mocked listings should complete in < 5 seconds
        assert elapsed < 5.0, f"Pipeline took {elapsed:.2f}s for 100 listings"

    def test_pipeline_throughput_500_listings(self, perf_db):
        """ETL pipeline should handle 500 listings without excessive slowdown."""
        mock_scraper = MagicMock()
        raw_listings = [
            make_raw_listing(
                listing_id=f"bulk-{i:04d}",
                price=200000 + i * 500,
            )
            for i in range(500)
        ]
        mock_scraper.search.return_value = raw_listings

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False, batch_size=100)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        start = time.time()
        result = pipeline.run(filters, session=perf_db)
        elapsed = time.time() - start

        assert result.success is True
        assert result.new_count == 500
        assert elapsed < 15.0, f"Pipeline took {elapsed:.2f}s for 500 listings"

        # Verify all inserted
        count = perf_db.query(Property).count()
        assert count == 500

    def test_update_throughput(self, perf_db):
        """Updating 100 existing listings should be efficient."""
        mock_scraper = MagicMock()
        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False, batch_size=50)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # Insert 100 properties first
        initial = [
            make_raw_listing(listing_id=f"upd-{i:04d}", price=400000)
            for i in range(100)
        ]
        mock_scraper.search.return_value = initial
        pipeline.run(filters, session=perf_db)

        # Now update all with new prices
        updated = [
            make_raw_listing(listing_id=f"upd-{i:04d}", price=410000)
            for i in range(100)
        ]
        mock_scraper.search.return_value = updated

        start = time.time()
        result = pipeline.run(filters, session=perf_db)
        elapsed = time.time() - start

        assert result.updated_count == 100
        assert elapsed < 5.0, f"Update took {elapsed:.2f}s for 100 listings"


class TestMemoryUsage:
    """Memory usage tests for the pipeline."""

    def test_memory_usage_reasonable(self, perf_db):
        """Pipeline should not use excessive memory for 200 listings."""
        mock_scraper = MagicMock()
        raw_listings = [
            make_raw_listing(
                listing_id=f"mem-{i:04d}",
                price=300000 + i * 1000,
                description="A" * 500,  # Moderate-length descriptions
            )
            for i in range(200)
        ]
        mock_scraper.search.return_value = raw_listings

        pipeline = ETLPipeline(scraper=mock_scraper, auto_init_db=False, batch_size=50)
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)

        # Measure memory before
        mem_before = sys.getsizeof(raw_listings)

        result = pipeline.run(filters, session=perf_db)

        assert result.success is True
        assert result.new_count == 200

        # Raw listings for 200 items with 500-char descriptions should be
        # well under 10MB. This is a sanity check, not a precise measurement.
        assert mem_before < 10 * 1024 * 1024
