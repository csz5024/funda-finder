"""Tests for HTML scraper implementation (mocked responses)."""

import pytest
from unittest.mock import MagicMock, patch

from funda_finder.scraper.base import PropertyType, ScraperSource, SearchFilters
from funda_finder.scraper.html import HtmlScraper


class TestHtmlScraper:
    """Tests for HtmlScraper with mocked funda-scraper library."""

    @pytest.fixture(autouse=True)
    def setup_scraper(self):
        """Create an HtmlScraper instance (no external deps at init)."""
        self.scraper = HtmlScraper(rate_limit_seconds=0.0)

    def _make_raw_data(
        self, url="https://www.funda.nl/koop/amsterdam/huis-123/detail", **overrides
    ):
        """Create mock raw data as returned by funda-scraper."""
        data = {
            "url": url,
            "address": "Test Street 1",
            "zip_code": "1234AB",
            "city": "Amsterdam",
            "price": 500000,
            "living_area": 100,
            "plot_area": 200,
            "num_of_rooms": 4,
            "num_of_bedrooms": 2,
            "num_of_bathrooms": 1,
            "year_built": 2000,
            "energy_label": "A",
            "description": "A nice house",
            "neighborhood": "Centrum",
        }
        data.update(overrides)
        return data

    def test_source_property(self):
        assert self.scraper.source == ScraperSource.HTML

    def test_normalize_listing(self):
        raw_data = self._make_raw_data()
        listing = self.scraper._normalize_listing(raw_data, PropertyType.BUY)

        assert listing.listing_id == "huis-123"  # extracted from URL split("/")[-2]
        assert listing.city == "Amsterdam"
        assert listing.price == 500000
        assert listing.living_area == 100.0
        assert listing.num_rooms == 4
        assert listing.num_bedrooms == 2
        assert listing.num_bathrooms == 1
        assert listing.construction_year == 2000
        assert listing.energy_label == "A"
        assert listing.source == ScraperSource.HTML

    def test_normalize_string_price(self):
        raw_data = self._make_raw_data(price="€ 450.000")
        listing = self.scraper._normalize_listing(raw_data, PropertyType.BUY)
        assert listing.price == 450000

    def test_normalize_alternative_field_names(self):
        """funda-scraper can use different field names."""
        raw_data = {
            "url": "https://www.funda.nl/koop/amsterdam/huis-alt/detail",
            "address": "Alt Street 1",
            "postal_code": "5678CD",  # alternative name
            "city": "Amsterdam",
            "price": 300000,
            "floor_area": 90,  # alternative for living_area
            "land_area": 150,  # alternative for plot_area
            "construction_year": 1985,  # alternative for year_built
            "desc": "Short description",  # alternative for description
        }
        listing = self.scraper._normalize_listing(raw_data, PropertyType.BUY)

        assert listing.postal_code == "5678CD"
        assert listing.living_area == 90.0
        assert listing.plot_area == 150.0
        assert listing.construction_year == 1985
        assert listing.description == "Short description"

    def test_normalize_missing_optional_fields(self):
        raw_data = {
            "url": "https://www.funda.nl/koop/amsterdam/huis-min/detail",
            "city": "Amsterdam",
            "price": 200000,
        }
        listing = self.scraper._normalize_listing(raw_data, PropertyType.BUY)

        assert listing.living_area is None
        assert listing.num_rooms is None
        assert listing.postal_code is None
        assert listing.energy_label is None

    def test_search_with_mocked_funda_scraper(self):
        mock_scraper_cls = MagicMock()
        mock_scraper_instance = MagicMock()
        mock_scraper_instance.run.return_value = [
            self._make_raw_data(),
            self._make_raw_data(
                url="https://www.funda.nl/koop/amsterdam/huis-456/detail",
                price=600000,
            ),
        ]
        mock_scraper_cls.return_value = mock_scraper_instance

        with patch.dict("sys.modules", {"funda_scraper": MagicMock(FundaScraper=mock_scraper_cls)}):
            filters = SearchFilters(city="Amsterdam", property_type=PropertyType.BUY)
            results = self.scraper.search(filters)

        assert len(results) == 2
        assert all(r.source == ScraperSource.HTML for r in results)

    def test_search_filters_by_price(self):
        mock_scraper_cls = MagicMock()
        mock_scraper_instance = MagicMock()
        mock_scraper_instance.run.return_value = [
            self._make_raw_data(price=200000),
            self._make_raw_data(
                url="https://www.funda.nl/koop/amsterdam/huis-exp/detail",
                price=800000,
            ),
            self._make_raw_data(
                url="https://www.funda.nl/koop/amsterdam/huis-mid/detail",
                price=450000,
            ),
        ]
        mock_scraper_cls.return_value = mock_scraper_instance

        with patch.dict("sys.modules", {"funda_scraper": MagicMock(FundaScraper=mock_scraper_cls)}):
            filters = SearchFilters(
                city="Amsterdam",
                property_type=PropertyType.BUY,
                min_price=300000,
                max_price=500000,
            )
            results = self.scraper.search(filters)

        assert len(results) == 1
        assert results[0].price == 450000

    def test_search_max_results(self):
        mock_scraper_cls = MagicMock()
        mock_scraper_instance = MagicMock()
        mock_scraper_instance.run.return_value = [
            self._make_raw_data(url=f"https://www.funda.nl/koop/amsterdam/huis-{i}/detail")
            for i in range(10)
        ]
        mock_scraper_cls.return_value = mock_scraper_instance

        with patch.dict("sys.modules", {"funda_scraper": MagicMock(FundaScraper=mock_scraper_cls)}):
            filters = SearchFilters(
                city="Amsterdam",
                property_type=PropertyType.BUY,
                max_results=5,
            )
            results = self.scraper.search(filters)

        assert len(results) == 5

    def test_get_details_returns_none(self):
        """HTML scraper's get_details is not fully implemented."""
        result = self.scraper.get_details("any-id")
        assert result is None

    def test_user_agent_rotation(self):
        """User agent should be one of the known pool."""
        assert self.scraper._current_user_agent in HtmlScraper.USER_AGENTS

        new_ua = self.scraper._rotate_user_agent()
        assert new_ua in HtmlScraper.USER_AGENTS

    def test_user_agents_pool_not_empty(self):
        assert len(HtmlScraper.USER_AGENTS) >= 3
