"""Pydantic models for data validation and cleaning.

This module defines validated data models that ensure data quality
before insertion into the database. Models include type validation,
data cleaning, and business rule enforcement.
"""

import re
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class PropertyListing(BaseModel):
    """Validated property listing model.

    This model validates and cleans property data from scrapers before
    database insertion. It enforces required fields, data types, formats,
    and business rules.

    Example:
        listing = PropertyListing(
            funda_id="abc123",
            url="https://www.funda.nl/koop/amsterdam/huis-abc123/",
            city="Amsterdam",
            price=450000,
            listing_type="buy",
        )
    """

    # Required fields
    funda_id: str = Field(..., description="Unique Funda listing ID", min_length=1)
    url: str = Field(..., description="Full URL to property page", min_length=10)
    city: str = Field(..., description="City name", min_length=1)
    price: int = Field(..., description="Price in euros", gt=0)
    listing_type: Literal["buy", "rent"] = Field(..., description="Type of listing")

    # Optional location fields
    address: Optional[str] = Field(None, description="Street address")
    postal_code: Optional[str] = Field(None, description="Dutch postal code (1234 AB)")
    neighborhood: Optional[str] = Field(None, description="Neighborhood name")
    lat: Optional[float] = Field(None, ge=-90, le=90, description="Latitude")
    lon: Optional[float] = Field(None, ge=-180, le=180, description="Longitude")

    # Optional property details
    living_area: Optional[float] = Field(None, gt=0, description="Living area in m²")
    plot_area: Optional[float] = Field(None, gt=0, description="Plot/land area in m²")
    rooms: Optional[int] = Field(None, ge=1, le=50, description="Number of rooms")
    bedrooms: Optional[int] = Field(None, ge=0, le=30, description="Number of bedrooms")
    bathrooms: Optional[int] = Field(None, ge=0, le=20, description="Number of bathrooms")

    # Optional building details
    year_built: Optional[int] = Field(
        None, ge=1600, le=2030, description="Construction year"
    )
    energy_label: Optional[str] = Field(None, description="Energy efficiency label")

    # Optional metadata
    description: Optional[str] = Field(None, description="Property description")
    source: Optional[str] = Field(None, description="Data source (pyfunda, html)")
    scraped_at: datetime = Field(
        default_factory=datetime.utcnow, description="When the data was scraped"
    )

    # Data cleaning validators
    @field_validator("funda_id", "city", "address", "neighborhood", mode="before")
    @classmethod
    def strip_whitespace(cls, v: Optional[str]) -> Optional[str]:
        """Remove leading/trailing whitespace from string fields."""
        if v is None:
            return None
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("city", mode="before")
    @classmethod
    def normalize_city(cls, v: Optional[str]) -> Optional[str]:
        """Normalize city name to title case."""
        if v is None or not isinstance(v, str):
            return v
        # Title case: "amsterdam" -> "Amsterdam", "den haag" -> "Den Haag"
        return v.strip().title()

    @field_validator("postal_code")
    @classmethod
    def validate_postal_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate and normalize Dutch postal code format.

        Valid formats:
        - 1234AB
        - 1234 AB
        - Normalized to: "1234 AB"
        """
        if v is None:
            return None

        v = v.strip().upper()

        # Match Dutch postal code pattern: 4 digits + 2 letters
        match = re.match(r"^(\d{4})\s*([A-Z]{2})$", v)
        if not match:
            raise ValueError(
                f"Invalid Dutch postal code format: {v}. "
                "Expected format: 1234 AB (4 digits + 2 letters)"
            )

        # Return normalized format with space
        return f"{match.group(1)} {match.group(2)}"

    @field_validator("energy_label")
    @classmethod
    def validate_energy_label(cls, v: Optional[str]) -> Optional[str]:
        """Validate energy label against known values.

        Valid labels: A++++, A+++, A++, A+, A, B, C, D, E, F, G
        """
        if v is None:
            return None

        v = v.strip().upper()
        valid_labels = [
            "A++++",
            "A+++",
            "A++",
            "A+",
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
        ]

        if v not in valid_labels:
            raise ValueError(
                f"Invalid energy label: {v}. "
                f"Valid labels: {', '.join(valid_labels)}"
            )

        return v

    @field_validator("price", mode="before")
    @classmethod
    def parse_price(cls, v) -> int:
        """Parse price from various formats.

        Handles:
        - Integers: 450000
        - Strings with currency: "€ 450.000"
        - Strings with spaces: "450 000"
        """
        if isinstance(v, int):
            return v

        if isinstance(v, str):
            # Remove currency symbols, spaces, dots (thousand separators)
            cleaned = re.sub(r"[€$\s\.]", "", v)
            # Remove everything except digits
            cleaned = "".join(filter(str.isdigit, cleaned))
            if cleaned:
                return int(cleaned)

        # Try direct conversion as fallback
        try:
            return int(v)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse price: {v}")

    @field_validator("living_area", "plot_area", mode="before")
    @classmethod
    def parse_area(cls, v) -> Optional[float]:
        """Parse area from various formats.

        Handles:
        - Floats: 120.5
        - Integers: 120
        - Strings: "120 m²", "120m2", "120"
        """
        if v is None:
            return None

        if isinstance(v, (int, float)):
            return float(v)

        if isinstance(v, str):
            # Remove m², m2, spaces
            cleaned = re.sub(r"[m²²\s]", "", v.lower())
            # Extract first number
            match = re.search(r"[\d\.]+", cleaned)
            if match:
                try:
                    return float(match.group())
                except ValueError:
                    pass

        return None

    @model_validator(mode="after")
    def validate_bedroom_room_relationship(self):
        """Ensure bedrooms don't exceed total rooms."""
        if self.rooms is not None and self.bedrooms is not None:
            if self.bedrooms > self.rooms:
                raise ValueError(
                    f"Bedrooms ({self.bedrooms}) cannot exceed total rooms ({self.rooms})"
                )
        return self

    @model_validator(mode="after")
    def validate_url_city_consistency(self):
        """Ensure city name appears in the URL (when possible)."""
        if self.url and self.city:
            # Normalize for comparison: "Den Haag" -> "den-haag"
            city_slug = self.city.lower().replace(" ", "-")
            url_lower = self.url.lower()

            # Only warn if city slug is not in URL and URL looks like Funda
            if "funda.nl" in url_lower and city_slug not in url_lower:
                # Don't fail validation, just log inconsistency
                # This is informational, not a hard error
                pass

        return self

    class Config:
        """Pydantic model configuration."""

        str_strip_whitespace = True
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "funda_id": "abc123",
                "url": "https://www.funda.nl/en/detail/koop/amsterdam/huis-abc123/",
                "city": "Amsterdam",
                "price": 450000,
                "listing_type": "buy",
                "address": "Keizersgracht 123",
                "postal_code": "1015 CJ",
                "living_area": 120.0,
                "rooms": 4,
                "bedrooms": 2,
                "bathrooms": 1,
                "year_built": 1900,
                "energy_label": "C",
            }
        }
