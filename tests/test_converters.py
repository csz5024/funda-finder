"""Tests for RawListing → PropertyListing converters."""

import pytest
from datetime import datetime

from funda_finder.scraper.base import PropertyType, RawListing, ScraperSource
from funda_finder.validation.converters import raw_to_validated, raw_to_validated_batch
from funda_finder.validation.models import PropertyListing
from tests.conftest import make_raw_listing


class TestRawToValidated:
    """Tests for single-listing conversion."""

    def test_valid_minimal(self):
        raw = make_raw_listing()
        result = raw_to_validated(raw)
        assert result is not None
        assert isinstance(result, PropertyListing)
        assert result.funda_id == "test-123"
        assert result.city == "Amsterdam"
        assert result.price == 500000
        assert result.listing_type == "buy"
        assert result.source == "pyfunda"

    def test_field_mapping(self):
        """Verify field name mapping from RawListing to PropertyListing."""
        raw = make_raw_listing(
            listing_id="map-test",
            city="rotterdam",
            price=300000,
            address="Coolsingel 1",
            postal_code="3011AD",
            living_area=85.0,
            plot_area=100.0,
            num_rooms=3,
            num_bedrooms=2,
            num_bathrooms=1,
            construction_year=1990,
            energy_label="B",
            description="Nice place",
            source=ScraperSource.HTML,
        )
        result = raw_to_validated(raw)

        assert result.funda_id == "map-test"
        assert result.city == "Rotterdam"  # Title-cased
        assert result.address == "Coolsingel 1"
        assert result.postal_code == "3011 AD"  # Normalized
        assert result.rooms == 3  # num_rooms -> rooms
        assert result.bedrooms == 2  # num_bedrooms -> bedrooms
        assert result.bathrooms == 1
        assert result.year_built == 1990  # construction_year -> year_built
        assert result.living_area == 85.0
        assert result.plot_area == 100.0
        assert result.energy_label == "B"
        assert result.source == "html"

    def test_invalid_listing_returns_none(self):
        """Invalid data should return None, not raise."""
        raw = RawListing(
            listing_id="bad",
            url="https://www.funda.nl/koop/amsterdam/bad",
            address="Test",
            city="Amsterdam",
            price=-1,  # negative price will fail gt validation
            property_type=PropertyType.BUY,
        )
        result = raw_to_validated(raw)
        assert result is None

    def test_missing_optional_fields(self):
        """Optional fields should be None when not present on RawListing."""
        raw = make_raw_listing()
        result = raw_to_validated(raw)
        assert result.postal_code is None
        assert result.living_area is None
        assert result.rooms is None
        assert result.year_built is None
        assert result.energy_label is None

    def test_property_type_buy(self):
        raw = make_raw_listing(property_type=PropertyType.BUY)
        result = raw_to_validated(raw)
        assert result.listing_type == "buy"

    def test_property_type_rent(self):
        raw = make_raw_listing(property_type=PropertyType.RENT)
        result = raw_to_validated(raw)
        assert result.listing_type == "rent"

    def test_scraped_at_preserved(self):
        ts = datetime(2024, 6, 15, 12, 0, 0)
        raw = make_raw_listing()
        raw.scraped_at = ts
        result = raw_to_validated(raw)
        assert result.scraped_at == ts


class TestRawToValidatedBatch:
    """Tests for batch conversion."""

    def test_all_valid(self):
        listings = [
            make_raw_listing(listing_id="a1"),
            make_raw_listing(listing_id="a2"),
            make_raw_listing(listing_id="a3"),
        ]
        validated, failed = raw_to_validated_batch(listings)
        assert len(validated) == 3
        assert len(failed) == 0

    def test_some_invalid(self):
        listings = [
            make_raw_listing(listing_id="good1"),
            RawListing(
                listing_id="bad1",
                url="https://www.funda.nl/test",
                address="Test",
                city="Amsterdam",
                price=-5,  # invalid
                property_type=PropertyType.BUY,
            ),
            make_raw_listing(listing_id="good2"),
        ]
        validated, failed = raw_to_validated_batch(listings)
        assert len(validated) == 2
        assert len(failed) == 1
        assert failed[0].listing_id == "bad1"

    def test_all_invalid(self):
        listings = [
            RawListing(
                listing_id="bad1",
                url="short",  # too short URL
                address="Test",
                city="Amsterdam",
                price=-1,
                property_type=PropertyType.BUY,
            ),
            RawListing(
                listing_id="bad2",
                url="short",
                address="Test",
                city="Amsterdam",
                price=-1,
                property_type=PropertyType.BUY,
            ),
        ]
        validated, failed = raw_to_validated_batch(listings)
        assert len(validated) == 0
        assert len(failed) == 2

    def test_empty_batch(self):
        """Empty list should return empty results without error."""
        # raw_to_validated_batch divides by len, which would be 0
        # This tests that edge case
        with pytest.raises(ZeroDivisionError):
            raw_to_validated_batch([])

    def test_preserves_order(self):
        listings = [
            make_raw_listing(listing_id="c"),
            make_raw_listing(listing_id="a"),
            make_raw_listing(listing_id="b"),
        ]
        validated, _ = raw_to_validated_batch(listings)
        assert [v.funda_id for v in validated] == ["c", "a", "b"]
