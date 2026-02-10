"""Shared fixtures for the test suite."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from funda_finder.db.models import Base, PriceHistory, Property
from funda_finder.scraper.base import PropertyType, RawListing, ScraperSource, SearchFilters


@pytest.fixture
def db_engine():
    """Create an in-memory SQLite engine."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine):
    """Create a database session for testing."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def search_filters():
    """Default search filters for testing."""
    return SearchFilters(
        city="Amsterdam",
        property_type=PropertyType.BUY,
        min_price=200000,
        max_price=800000,
    )


def make_raw_listing(
    listing_id="test-123",
    city="Amsterdam",
    price=500000,
    address="Test Street 123",
    property_type=PropertyType.BUY,
    source=ScraperSource.PYFUNDA,
    **kwargs,
) -> RawListing:
    """Factory for creating test RawListing instances."""
    offering = "koop" if property_type == PropertyType.BUY else "huur"
    # Use realistic Funda URL format with compound IDs like "huis-{listing_id}"
    # listing_id should already be in the right format (e.g., "huis-123" or "test-123")
    return RawListing(
        listing_id=listing_id,
        url=f"https://www.funda.nl/en/detail/{offering}/{city.lower()}/{listing_id}/",
        address=address,
        city=city,
        price=price,
        property_type=property_type,
        source=source,
        scraped_at=datetime.utcnow(),
        raw_data={"test": True},
        **kwargs,
    )


def make_property(
    funda_id="test-123",
    city="Amsterdam",
    price=500000,
    address="Test Street 123",
    living_area=100,
    rooms=4,
    **kwargs,
) -> Property:
    """Factory for creating test Property instances."""
    # Determine offering type from listing_type if provided, default to "buy"
    listing_type = kwargs.get("listing_type", "buy")
    offering = "koop" if listing_type == "buy" else "huur"
    # Use realistic Funda URL format - funda_id should already be in compound format (e.g., "huis-123")
    defaults = dict(
        funda_id=funda_id,
        url=f"https://www.funda.nl/en/detail/{offering}/{city.lower()}/{funda_id}/",
        address=address,
        city=city,
        price=price,
        living_area=living_area,
        rooms=rooms,
        listing_type=listing_type,
        status="active",
        scraped_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    defaults.update(kwargs)
    return Property(**defaults)


@pytest.fixture
def sample_property(db_session):
    """A single property in the DB."""
    prop = make_property()
    db_session.add(prop)
    db_session.commit()
    return prop


@pytest.fixture
def sample_properties(db_session):
    """Multiple properties in the DB for comparison tests."""
    props = [
        make_property(
            funda_id="ams-001",
            city="Amsterdam",
            price=350000,
            living_area=75,
            rooms=3,
            bedrooms=2,
            year_built=2000,
        ),
        make_property(
            funda_id="ams-002",
            city="Amsterdam",
            price=400000,
            living_area=80,
            rooms=3,
            bedrooms=2,
            year_built=2005,
        ),
        make_property(
            funda_id="ams-003",
            city="Amsterdam",
            price=320000,
            living_area=80,
            rooms=3,
            bedrooms=2,
            year_built=2002,
            scraped_at=datetime.utcnow() - timedelta(days=60),
        ),
        make_property(
            funda_id="rot-001",
            city="Rotterdam",
            price=250000,
            living_area=70,
            rooms=2,
            bedrooms=1,
            year_built=1995,
        ),
    ]
    for p in props:
        db_session.add(p)
    db_session.commit()

    # Add price history for ams-003
    prop_003 = db_session.query(Property).filter_by(funda_id="ams-003").first()
    for price, days_ago in [(360000, 60), (340000, 30), (320000, 1)]:
        db_session.add(
            PriceHistory(
                property_id=prop_003.id,
                price=price,
                observed_at=datetime.utcnow() - timedelta(days=days_ago),
            )
        )
    db_session.commit()
    return props
