"""Base classes and data models for the composite scraper."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class PropertyType(str, Enum):
    """Property type enumeration."""
    BUY = "buy"
    RENT = "rent"


class ScraperSource(str, Enum):
    """Source of scraped data."""
    PYFUNDA = "pyfunda"
    HTML = "html"


@dataclass
class RawListing:
    """Normalized listing data from any scraper source.

    This represents the common schema that both pyfunda and funda-scraper
    data gets normalized into. Raw JSON from the source is preserved in
    the raw_data field.
    """
    # Required fields
    listing_id: str
    url: str
    address: str
    city: str
    price: int
    property_type: PropertyType

    # Optional fields
    postal_code: Optional[str] = None
    neighborhood: Optional[str] = None

    # Property details
    living_area: Optional[float] = None  # square meters
    plot_area: Optional[float] = None  # square meters
    num_rooms: Optional[int] = None
    num_bedrooms: Optional[int] = None
    num_bathrooms: Optional[int] = None

    # Building details
    construction_year: Optional[int] = None
    energy_label: Optional[str] = None
    construction_type: Optional[str] = None  # e.g., "brick", "wood", etc.
    property_subtype: Optional[str] = None  # e.g., "apartment", "villa", etc.

    # Description
    description: Optional[str] = None

    # GPS coordinates
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Property features
    has_garden: Optional[bool] = None
    has_parking: Optional[bool] = None
    has_balcony: Optional[bool] = None
    has_garage: Optional[bool] = None

    # Photos
    photos: List[str] = field(default_factory=list)  # List of photo URLs

    # Agent details
    agent_name: Optional[str] = None
    agent_phone: Optional[str] = None
    agent_email: Optional[str] = None
    agency_name: Optional[str] = None

    # Listing metadata
    listing_date: Optional[datetime] = None
    days_on_market: Optional[int] = None

    # Metadata
    source: ScraperSource = ScraperSource.PYFUNDA
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "listing_id": self.listing_id,
            "url": self.url,
            "address": self.address,
            "city": self.city,
            "price": self.price,
            "property_type": self.property_type.value,
            "postal_code": self.postal_code,
            "neighborhood": self.neighborhood,
            "living_area": self.living_area,
            "plot_area": self.plot_area,
            "num_rooms": self.num_rooms,
            "num_bedrooms": self.num_bedrooms,
            "num_bathrooms": self.num_bathrooms,
            "construction_year": self.construction_year,
            "energy_label": self.energy_label,
            "construction_type": self.construction_type,
            "property_subtype": self.property_subtype,
            "description": self.description,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "has_garden": self.has_garden,
            "has_parking": self.has_parking,
            "has_balcony": self.has_balcony,
            "has_garage": self.has_garage,
            "photos": self.photos,
            "agent_name": self.agent_name,
            "agent_phone": self.agent_phone,
            "agent_email": self.agent_email,
            "agency_name": self.agency_name,
            "listing_date": self.listing_date.isoformat() if self.listing_date else None,
            "days_on_market": self.days_on_market,
            "source": self.source.value,
            "scraped_at": self.scraped_at.isoformat(),
            "raw_data": self.raw_data,
        }


@dataclass
class SearchFilters:
    """Search parameters for property listings."""
    city: str
    property_type: PropertyType = PropertyType.BUY
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    min_rooms: Optional[int] = None
    max_results: Optional[int] = None
    max_pages: Optional[int] = None  # Number of pages to fetch (15 results per page)


class ScraperInterface(ABC):
    """Abstract base class for property scrapers.

    Both pyfunda and html scrapers implement this interface to provide
    a unified API for the composite scraper.
    """

    @abstractmethod
    def search(self, filters: SearchFilters) -> List[RawListing]:
        """Search for property listings.

        Args:
            filters: Search parameters

        Returns:
            List of normalized listings

        Raises:
            Exception: If scraping fails
        """
        pass

    @abstractmethod
    def get_details(self, listing_id: str) -> Optional[RawListing]:
        """Fetch full details for a specific listing.

        Args:
            listing_id: Unique identifier for the listing

        Returns:
            Normalized listing with full details, or None if not found

        Raises:
            Exception: If scraping fails
        """
        pass

    @property
    @abstractmethod
    def source(self) -> ScraperSource:
        """Return the scraper source identifier."""
        pass
