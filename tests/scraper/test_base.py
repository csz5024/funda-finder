"""Tests for base scraper module."""

from datetime import datetime

import pytest

from funda_finder.scraper.base import (
    PropertyType,
    RawListing,
    ScraperSource,
    SearchFilters,
)


class TestPropertyType:
    """Tests for PropertyType enum."""

    def test_buy_value(self):
        assert PropertyType.BUY.value == "buy"

    def test_rent_value(self):
        assert PropertyType.RENT.value == "rent"


class TestScraperSource:
    """Tests for ScraperSource enum."""

    def test_pyfunda_value(self):
        assert ScraperSource.PYFUNDA.value == "pyfunda"

    def test_html_value(self):
        assert ScraperSource.HTML.value == "html"


class TestRawListing:
    """Tests for RawListing dataclass."""

    def test_minimal_listing(self):
        """Test creating a listing with only required fields."""
        listing = RawListing(
            listing_id="12345",
            url="https://funda.nl/test",
            address="Test Street 123",
            city="Amsterdam",
            price=500000,
            property_type=PropertyType.BUY,
        )

        assert listing.listing_id == "12345"
        assert listing.url == "https://funda.nl/test"
        assert listing.address == "Test Street 123"
        assert listing.city == "Amsterdam"
        assert listing.price == 500000
        assert listing.property_type == PropertyType.BUY
        assert listing.source == ScraperSource.PYFUNDA  # default

    def test_full_listing(self):
        """Test creating a listing with all fields."""
        listing = RawListing(
            listing_id="12345",
            url="https://funda.nl/test",
            address="Test Street 123",
            city="Amsterdam",
            price=500000,
            property_type=PropertyType.BUY,
            postal_code="1234AB",
            neighborhood="Centrum",
            living_area=120.5,
            plot_area=250.0,
            num_rooms=5,
            num_bedrooms=3,
            num_bathrooms=2,
            construction_year=1920,
            energy_label="A",
            description="Beautiful house",
            source=ScraperSource.HTML,
        )

        assert listing.postal_code == "1234AB"
        assert listing.living_area == 120.5
        assert listing.construction_year == 1920
        assert listing.source == ScraperSource.HTML

    def test_to_dict(self):
        """Test converting listing to dictionary."""
        listing = RawListing(
            listing_id="12345",
            url="https://funda.nl/test",
            address="Test Street 123",
            city="Amsterdam",
            price=500000,
            property_type=PropertyType.BUY,
            living_area=120.5,
        )

        data = listing.to_dict()

        assert data["listing_id"] == "12345"
        assert data["price"] == 500000
        assert data["property_type"] == "buy"
        assert data["living_area"] == 120.5
        assert data["source"] == "pyfunda"
        assert "scraped_at" in data
        assert isinstance(data["raw_data"], dict)


class TestSearchFilters:
    """Tests for SearchFilters dataclass."""

    def test_minimal_filters(self):
        """Test creating filters with only required fields."""
        filters = SearchFilters(city="Amsterdam")

        assert filters.city == "Amsterdam"
        assert filters.property_type == PropertyType.BUY  # default
        assert filters.min_price is None
        assert filters.max_price is None

    def test_full_filters(self):
        """Test creating filters with all fields."""
        filters = SearchFilters(
            city="Amsterdam",
            property_type=PropertyType.RENT,
            min_price=1000,
            max_price=2000,
            min_rooms=3,
            max_results=50,
        )

        assert filters.city == "Amsterdam"
        assert filters.property_type == PropertyType.RENT
        assert filters.min_price == 1000
        assert filters.max_price == 2000
        assert filters.min_rooms == 3
        assert filters.max_results == 50
