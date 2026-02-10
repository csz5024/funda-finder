"""Tests for data validation models (Pydantic)."""

import pytest
from datetime import datetime

from pydantic import ValidationError

from funda_finder.validation.models import PropertyListing


class TestPropertyListingRequired:
    """Tests for required field validation."""

    def test_minimal_valid(self):
        """Minimal valid listing with only required fields."""
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/huis-abc123/",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
        )
        assert listing.funda_id == "abc123"
        assert listing.price == 450000

    def test_missing_funda_id(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                url="https://www.funda.nl/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
            )

    def test_empty_funda_id(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="",
                url="https://www.funda.nl/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
            )

    def test_missing_url(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
            )

    def test_missing_city(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/test",
                price=450000,
                listing_type="buy",
            )

    def test_missing_price(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/test",
                city="Amsterdam",
                listing_type="buy",
            )

    def test_zero_price_invalid(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/test",
                city="Amsterdam",
                price=0,
                listing_type="buy",
            )

    def test_negative_price_invalid(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/test",
                city="Amsterdam",
                price=-100,
                listing_type="buy",
            )

    def test_invalid_listing_type(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/test",
                city="Amsterdam",
                price=450000,
                listing_type="sell",  # invalid
            )


class TestStripWhitespace:
    """Tests for whitespace stripping validator."""

    def test_strip_funda_id(self):
        listing = PropertyListing(
            funda_id="  abc123  ",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
        )
        assert listing.funda_id == "abc123"

    def test_strip_city(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="  amsterdam  ",
            price=450000,
            listing_type="buy",
        )
        # Also gets title-cased by normalize_city
        assert listing.city == "Amsterdam"

    def test_strip_address(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            address="  Keizersgracht 123  ",
        )
        assert listing.address == "Keizersgracht 123"

    def test_none_passes_through(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            address=None,
        )
        assert listing.address is None


class TestNormalizeCity:
    """Tests for city name normalization."""

    def test_lowercase_to_title(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="amsterdam",
            price=450000,
            listing_type="buy",
        )
        assert listing.city == "Amsterdam"

    def test_uppercase_to_title(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="AMSTERDAM",
            price=450000,
            listing_type="buy",
        )
        assert listing.city == "Amsterdam"

    def test_multi_word_city(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/den-haag/test",
            city="den haag",
            price=450000,
            listing_type="buy",
        )
        assert listing.city == "Den Haag"


class TestPostalCode:
    """Tests for Dutch postal code validation."""

    def test_valid_with_space(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            postal_code="1234 AB",
        )
        assert listing.postal_code == "1234 AB"

    def test_valid_without_space(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            postal_code="1234AB",
        )
        assert listing.postal_code == "1234 AB"

    def test_lowercase_letters(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            postal_code="1234ab",
        )
        assert listing.postal_code == "1234 AB"

    def test_invalid_format(self):
        with pytest.raises(ValidationError) as exc_info:
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                postal_code="ABCD12",
            )
        assert "postal code" in str(exc_info.value).lower()

    def test_too_short(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                postal_code="123",
            )

    def test_none_allowed(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            postal_code=None,
        )
        assert listing.postal_code is None


class TestEnergyLabel:
    """Tests for energy label validation."""

    @pytest.mark.parametrize(
        "label",
        ["A++++", "A+++", "A++", "A+", "A", "B", "C", "D", "E", "F", "G"],
    )
    def test_valid_labels(self, label):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            energy_label=label,
        )
        assert listing.energy_label == label

    def test_lowercase_converted(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            energy_label="b",
        )
        assert listing.energy_label == "B"

    def test_invalid_label(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                energy_label="X",
            )

    def test_none_allowed(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            energy_label=None,
        )
        assert listing.energy_label is None


class TestPriceParser:
    """Tests for price parsing from various formats."""

    def test_integer(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
        )
        assert listing.price == 450000

    def test_string_with_euro_symbol(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price="€ 450.000",
            listing_type="buy",
        )
        assert listing.price == 450000

    def test_string_with_spaces(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price="450 000",
            listing_type="buy",
        )
        assert listing.price == 450000

    def test_string_plain_number(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price="450000",
            listing_type="buy",
        )
        assert listing.price == 450000

    def test_string_with_dots(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price="€450.000",
            listing_type="buy",
        )
        assert listing.price == 450000

    def test_unparseable_price_raises(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price="free",
                listing_type="buy",
            )


class TestAreaParser:
    """Tests for area parsing from various formats."""

    def test_float_passthrough(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            living_area=120.5,
        )
        assert listing.living_area == 120.5

    def test_integer_to_float(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            living_area=120,
        )
        assert listing.living_area == 120.0

    def test_string_with_m2(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            living_area="120 m²",
        )
        assert listing.living_area == 120.0

    def test_string_plain_number(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            living_area="120",
        )
        assert listing.living_area == 120.0

    def test_none_allowed(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            living_area=None,
        )
        assert listing.living_area is None

    def test_plot_area_parsed(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            plot_area="250 m²",
        )
        assert listing.plot_area == 250.0


class TestBedroomRoomRelationship:
    """Tests for bedroom/room relationship validator."""

    def test_bedrooms_less_than_rooms(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            rooms=5,
            bedrooms=3,
        )
        assert listing.rooms == 5
        assert listing.bedrooms == 3

    def test_bedrooms_equal_rooms(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            rooms=3,
            bedrooms=3,
        )
        assert listing.bedrooms == listing.rooms

    def test_bedrooms_exceed_rooms_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                rooms=3,
                bedrooms=5,
            )
        assert "bedrooms" in str(exc_info.value).lower()

    def test_none_rooms_bypasses_check(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            rooms=None,
            bedrooms=3,
        )
        assert listing.bedrooms == 3

    def test_none_bedrooms_bypasses_check(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            rooms=5,
            bedrooms=None,
        )
        assert listing.rooms == 5


class TestOptionalFieldBounds:
    """Tests for optional field boundary validation."""

    def test_lat_bounds(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/test",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            lat=52.3676,
            lon=4.9041,
        )
        assert listing.lat == 52.3676

    def test_lat_out_of_range(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                lat=91.0,
            )

    def test_year_built_min(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                year_built=1500,
            )

    def test_year_built_max(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                year_built=2050,
            )

    def test_rooms_bounds(self):
        with pytest.raises(ValidationError):
            PropertyListing(
                funda_id="abc123",
                url="https://www.funda.nl/koop/amsterdam/test",
                city="Amsterdam",
                price=450000,
                listing_type="buy",
                rooms=51,
            )


class TestFullListing:
    """Tests for a fully populated listing."""

    def test_all_fields(self):
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/huis-abc123/",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
            address="Keizersgracht 123",
            postal_code="1015 CJ",
            neighborhood="Grachtengordel",
            lat=52.3676,
            lon=4.9041,
            living_area=120.0,
            plot_area=150.0,
            rooms=4,
            bedrooms=2,
            bathrooms=1,
            year_built=1900,
            energy_label="C",
            description="Beautiful canal house",
            source="pyfunda",
        )
        assert listing.funda_id == "abc123"
        assert listing.postal_code == "1015 CJ"
        assert listing.living_area == 120.0
        assert listing.year_built == 1900
        assert isinstance(listing.scraped_at, datetime)
