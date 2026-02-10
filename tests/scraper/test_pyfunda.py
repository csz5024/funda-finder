"""Tests for PyFunda scraper implementation (mocked responses)."""

import pytest
from unittest.mock import MagicMock, patch

from funda_finder.scraper.base import PropertyType, ScraperSource, SearchFilters


class TestPyFundaScraper:
    """Tests for PyFundaScraper with mocked funda library."""

    @pytest.fixture(autouse=True)
    def setup_scraper(self):
        """Create a PyFundaScraper with the funda module mocked."""
        # Mock the funda module before importing PyFundaScraper
        self.mock_funda_module = MagicMock()
        self.mock_client = MagicMock()
        self.mock_funda_module.Funda.return_value = self.mock_client

        with patch.dict("sys.modules", {"funda": self.mock_funda_module}):
            from funda_finder.scraper.pyfunda import PyFundaScraper
            self.scraper = PyFundaScraper(rate_limit_seconds=0.0)

    def _make_mock_result(self, global_id="123", city="Amsterdam", price=500000):
        """Create a mock Listing result from pyfunda."""
        result = MagicMock()
        result.to_dict.return_value = {
            "global_id": global_id,
            "title": "Test Street 1",
            "postcode": "1234AB",
            "city": city,
            "url": f"https://www.funda.nl/en/detail/koop/{city.lower()}/{global_id}/",
            "price": price,
            "living_area": 100,
            "plot_area": 200,
            "rooms": 4,
            "bedrooms": 2,
            "construction_year": 2000,
            "energy_label": "A",
            "description": "A nice house",
            "neighbourhood": "Centrum",
        }
        return result

    def test_source_property(self):
        assert self.scraper.source == ScraperSource.PYFUNDA

    def test_search_returns_listings(self):
        self.mock_client.search_listing.return_value = [
            self._make_mock_result("id1"),
            self._make_mock_result("id2"),
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        results = self.scraper.search(filters)

        assert len(results) == 2
        assert results[0].listing_id == "id1"
        assert results[1].listing_id == "id2"
        assert results[0].source == ScraperSource.PYFUNDA

    def test_search_normalizes_fields(self):
        self.mock_client.search_listing.return_value = [
            self._make_mock_result(global_id="norm-1", city="Rotterdam", price=350000)
        ]
        filters = SearchFilters(city="Rotterdam", property_type=PropertyType.BUY)
        results = self.scraper.search(filters)

        listing = results[0]
        assert listing.listing_id == "norm-1"
        assert listing.city == "Rotterdam"
        assert listing.price == 350000
        assert listing.living_area == 100.0
        assert listing.num_rooms == 4
        assert listing.num_bedrooms == 2
        assert listing.construction_year == 2000
        assert listing.energy_label == "A"
        assert listing.postal_code == "1234AB"
        assert listing.neighborhood == "Centrum"

    def test_search_max_results(self):
        self.mock_client.search_listing.return_value = [
            self._make_mock_result(f"id{i}") for i in range(10)
        ]
        filters = SearchFilters(
            city="Amsterdam", property_type=PropertyType.BUY, max_results=3
        )
        results = self.scraper.search(filters)
        assert len(results) == 3

    def test_search_empty_results(self):
        self.mock_client.search_listing.return_value = []
        filters = SearchFilters(city="SmallTown", property_type=PropertyType.BUY)
        results = self.scraper.search(filters)
        assert len(results) == 0

    def test_search_raises_on_error(self):
        self.mock_client.search_listing.side_effect = Exception("API down")
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        with pytest.raises(Exception, match="API down"):
            self.scraper.search(filters)

    def test_search_rent_type(self):
        self.mock_client.search_listing.return_value = [
            self._make_mock_result("rent-1", price=1500)
        ]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.RENT)
        results = self.scraper.search(filters)

        self.mock_client.search_listing.assert_called_once()
        call_kwargs = self.mock_client.search_listing.call_args
        assert call_kwargs.kwargs.get("offering_type") == "rent" or \
               (call_kwargs.args and "rent" in str(call_kwargs))
        assert results[0].property_type == PropertyType.RENT

    def test_get_details_found(self):
        self.mock_client.get_listing.return_value = self._make_mock_result("detail-1")
        result = self.scraper.get_details("detail-1")

        assert result is not None
        assert result.listing_id == "detail-1"
        self.mock_client.get_listing.assert_called_once_with("detail-1")

    def test_get_details_not_found(self):
        self.mock_client.get_listing.return_value = None
        result = self.scraper.get_details("missing")
        assert result is None

    def test_get_details_error(self):
        self.mock_client.get_listing.side_effect = Exception("Not found")
        with pytest.raises(Exception, match="Not found"):
            self.scraper.get_details("broken")

    def test_normalize_constructs_url_when_missing(self):
        """When URL is empty in raw data, a URL should be constructed."""
        result = MagicMock()
        result.to_dict.return_value = {
            "global_id": "no-url-123",
            "title": "Test",
            "city": "Amsterdam",
            "price": 300000,
            "url": "",  # empty URL
        }
        self.mock_client.search_listing.return_value = [result]
        filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
        results = self.scraper.search(filters)

        assert "funda.nl" in results[0].url
        assert "amsterdam" in results[0].url.lower()
        assert "koop" in results[0].url
        assert "/en/detail/" in results[0].url

    def test_normalize_handles_missing_optional_fields(self):
        """Gracefully handle missing optional fields in API response."""
        result = MagicMock()
        result.to_dict.return_value = {
            "global_id": "minimal-1",
            "title": "Bare listing",
            "city": "Utrecht",
            "price": 200000,
            "url": "https://funda.nl/test",
        }
        self.mock_client.search_listing.return_value = [result]
        filters = SearchFilters(city="Utrecht", property_type=PropertyType.BUY)
        results = self.scraper.search(filters)

        listing = results[0]
        assert listing.living_area is None
        assert listing.num_rooms is None
        assert listing.construction_year is None
        assert listing.energy_label is None
        assert listing.postal_code is None
