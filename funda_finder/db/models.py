"""SQLAlchemy ORM models for Funda property data."""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True)
    funda_id = Column(String, unique=True, nullable=False, index=True)
    url = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False, index=True)
    postal_code = Column(String)
    price = Column(Integer)
    living_area = Column(Integer)  # m²
    plot_area = Column(Integer)  # m²
    rooms = Column(Integer)
    bedrooms = Column(Integer)
    year_built = Column(Integer)
    energy_label = Column(String)
    listing_type = Column(String, nullable=False, index=True)  # "buy" or "rent"
    status = Column(String, default="active", index=True)
    lat = Column(Float)
    lon = Column(Float)
    description = Column(Text)
    photos_json = Column(Text)  # JSON array of photo URLs
    raw_json = Column(Text)  # full scraped payload
    scraped_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    price_history = relationship(
        "PriceHistory", back_populates="property", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Property funda_id={self.funda_id!r} city={self.city!r} price={self.price}>"


class PriceHistory(Base):
    __tablename__ = "price_history"
    __table_args__ = (
        UniqueConstraint("property_id", "observed_at", name="uq_property_observation"),
    )

    id = Column(Integer, primary_key=True)
    property_id = Column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )
    price = Column(Integer, nullable=False)
    observed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    property = relationship("Property", back_populates="price_history")

    def __repr__(self) -> str:
        return f"<PriceHistory property_id={self.property_id} price={self.price}>"


class ScrapeMeta(Base):
    __tablename__ = "scrape_meta"

    id = Column(Integer, primary_key=True)
    run_id = Column(String, unique=True, nullable=False, default=lambda: uuid.uuid4().hex)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime)
    listings_found = Column(Integer, default=0)
    listings_new = Column(Integer, default=0)
    listings_updated = Column(Integer, default=0)
    errors = Column(Integer, default=0)

    def __repr__(self) -> str:
        return f"<ScrapeMeta run_id={self.run_id!r} found={self.listings_found}>"
