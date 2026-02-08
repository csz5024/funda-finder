"""SQLAlchemy models for Funda property data."""
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class Property(Base):
    """Property listing from Funda."""
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    funda_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)

    # Location
    address: Mapped[Optional[str]] = mapped_column(String(500))
    city: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20))
    lat: Mapped[Optional[float]] = mapped_column(Float)
    lon: Mapped[Optional[float]] = mapped_column(Float)

    # Property details
    price: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    living_area: Mapped[Optional[int]] = mapped_column(Integer)  # m²
    plot_area: Mapped[Optional[int]] = mapped_column(Integer)  # m²
    rooms: Mapped[Optional[int]] = mapped_column(Integer)
    bedrooms: Mapped[Optional[int]] = mapped_column(Integer)
    year_built: Mapped[Optional[int]] = mapped_column(Integer)
    energy_label: Mapped[Optional[str]] = mapped_column(String(10))

    # Listing metadata
    listing_type: Mapped[str] = mapped_column(String(10), nullable=False, index=True)  # 'buy' or 'rent'
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="active")  # 'active', 'sold', 'unavailable'

    # Rich content
    description: Mapped[Optional[str]] = mapped_column(Text)
    photos_json: Mapped[Optional[dict]] = mapped_column(JSON)  # List of photo URLs
    raw_json: Mapped[Optional[dict]] = mapped_column(JSON)  # Full raw scraper response

    # Timestamps
    scraped_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    # Relationships
    price_history: Mapped[list["PriceHistory"]] = relationship("PriceHistory", back_populates="property", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Property(funda_id='{self.funda_id}', address='{self.address}', price={self.price})>"


class PriceHistory(Base):
    """Historical price observations for a property."""
    __tablename__ = "price_history"
    __table_args__ = (
        UniqueConstraint("property_id", "observed_at", name="uq_property_price_observation"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id"), nullable=False, index=True)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(UTC), index=True)

    # Relationships
    property: Mapped["Property"] = relationship("Property", back_populates="price_history")

    def __repr__(self) -> str:
        return f"<PriceHistory(property_id={self.property_id}, price={self.price}, observed_at={self.observed_at})>"


class ScrapeMeta(Base):
    """Metadata about scrape runs."""
    __tablename__ = "scrape_meta"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Statistics
    listings_found: Mapped[int] = mapped_column(Integer, default=0)
    listings_new: Mapped[int] = mapped_column(Integer, default=0)
    listings_updated: Mapped[int] = mapped_column(Integer, default=0)
    errors: Mapped[Optional[dict]] = mapped_column(JSON)  # List of error details

    def __repr__(self) -> str:
        return f"<ScrapeMeta(run_id='{self.run_id}', started_at={self.started_at}, found={self.listings_found})>"
