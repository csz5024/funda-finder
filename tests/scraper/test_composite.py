"""Tests for composite scraper module."""

import pytest

from funda_finder.scraper.base import (
    PropertyType,
    RawListing,
    ScraperSource,
    SearchFilters,
)
from funda_finder.scraper.composite import (
    AllScrapersFailed,
    CompositeScraper,
)


@pytest.fixture
def mock_listing():
    """Create a mock listing for testing."""
    return RawListing(
        listing_id="12345",
        url="https://funda.nl/test",
        address="Test Street 123",
        city="Amsterdam",
        price=500000,
        property_type=PropertyType.BUY,
    )


@pytest.fixture
def search_filters():
    """Create search filters for testing."""
    return SearchFilters(
        city="Amsterdam",
        property_type=PropertyType.BUY,
        min_price=400000,
        max_price=600000,
    )


class TestCompositeScraper:
    """Tests for CompositeScraper class."""

    def test_init(self):
        """Test scraper initialization."""
        scraper = CompositeScraper(
            rate_limit_seconds=2.0,
            retry_attempts=2,
            enable_fallback=False,
        )

        assert scraper._rate_limit == 2.0
        assert scraper._retry_attempts == 2
        assert scraper._enable_fallback is False

    def test_get_scraper_status(self):
        """Test getting scraper status."""
        scraper = CompositeScraper(
            rate_limit_seconds=3.0,
            retry_attempts=3,
            enable_fallback=True,
        )

        status = scraper.get_scraper_status()

        assert status["primary"]["source"] == "pyfunda"
        assert status["primary"]["rate_limit"] == 3.0
        assert status["fallback"]["source"] == "html"
        assert status["fallback"]["enabled"] is True
        assert status["retry_attempts"] == 3

    def test_search_pyfunda_success(self, mocker, search_filters, mock_listing):
        """Test search succeeds with pyfunda."""
        scraper = CompositeScraper()

        # Mock pyfunda scraper to return results
        mocker.patch.object(
            scraper._pyfunda,
            "search",
            return_value=[mock_listing],
        )

        results = scraper.search(search_filters)

        assert len(results) == 1
        assert results[0].listing_id == "12345"
        assert results[0].source == ScraperSource.PYFUNDA

    def test_search_fallback_to_html(self, mocker, search_filters, mock_listing):
        """Test search falls back to HTML when pyfunda fails."""
        scraper = CompositeScraper(enable_fallback=True)

        # Mock pyfunda to fail
        mocker.patch.object(
            scraper._pyfunda,
            "search",
            side_effect=Exception("PyFunda API error"),
        )

        # Mock HTML scraper to succeed
        html_listing = RawListing(
            listing_id="67890",
            url="https://funda.nl/test2",
            address="Test Street 456",
            city="Amsterdam",
            price=550000,
            property_type=PropertyType.BUY,
            source=ScraperSource.HTML,
        )
        mocker.patch.object(
            scraper._html,
            "search",
            return_value=[html_listing],
        )

        results = scraper.search(search_filters)

        assert len(results) == 1
        assert results[0].listing_id == "67890"
        assert results[0].source == ScraperSource.HTML

    def test_search_both_fail(self, mocker, search_filters):
        """Test search raises exception when both scrapers fail."""
        scraper = CompositeScraper(enable_fallback=True, retry_attempts=1)

        # Mock both scrapers to fail
        mocker.patch.object(
            scraper._pyfunda,
            "search",
            side_effect=Exception("PyFunda error"),
        )
        mocker.patch.object(
            scraper._html,
            "search",
            side_effect=Exception("HTML error"),
        )

        with pytest.raises(AllScrapersFailed) as exc_info:
            scraper.search(search_filters)

        assert "Both scrapers failed" in str(exc_info.value)

    def test_search_fallback_disabled(self, mocker, search_filters):
        """Test search raises exception when fallback is disabled."""
        scraper = CompositeScraper(enable_fallback=False, retry_attempts=1)

        # Mock pyfunda to fail
        mocker.patch.object(
            scraper._pyfunda,
            "search",
            side_effect=Exception("PyFunda error"),
        )

        with pytest.raises(AllScrapersFailed) as exc_info:
            scraper.search(search_filters)

        assert "fallback is disabled" in str(exc_info.value)

    def test_get_details_pyfunda_success(self, mocker, mock_listing):
        """Test get_details succeeds with pyfunda."""
        scraper = CompositeScraper()

        # Mock pyfunda scraper to return listing
        mocker.patch.object(
            scraper._pyfunda,
            "get_details",
            return_value=mock_listing,
        )

        result = scraper.get_details("12345")

        assert result is not None
        assert result.listing_id == "12345"

    def test_get_details_with_source_hint(self, mocker, mock_listing):
        """Test get_details respects source hint."""
        scraper = CompositeScraper()

        # Mock HTML scraper to return listing
        html_listing = RawListing(
            listing_id="12345",
            url="https://funda.nl/test",
            address="Test Street 123",
            city="Amsterdam",
            price=500000,
            property_type=PropertyType.BUY,
            source=ScraperSource.HTML,
        )
        mocker.patch.object(
            scraper._html,
            "get_details",
            return_value=html_listing,
        )

        result = scraper.get_details("12345", source_hint=ScraperSource.HTML)

        assert result is not None
        assert result.source == ScraperSource.HTML

    def test_get_details_not_found(self, mocker):
        """Test get_details returns None when listing not found."""
        scraper = CompositeScraper()

        # Mock both scrapers to return None
        mocker.patch.object(scraper._pyfunda, "get_details", return_value=None)
        mocker.patch.object(scraper._html, "get_details", return_value=None)

        result = scraper.get_details("nonexistent")

        assert result is None
