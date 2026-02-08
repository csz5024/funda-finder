"""Tests for property analysis engine."""

import pytest
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from funda_finder.db.models import Base, Property, PriceHistory
from funda_finder.analysis import PropertyAnalyzer, ComparableGroup, PropertyScore


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_properties(test_db):
    """Create sample properties for testing."""
    properties = [
        Property(
            funda_id="test-001",
            url="https://funda.nl/test-001",
            address="Test Street 1",
            city="Amsterdam",
            price=350000,
            living_area=75,
            rooms=3,
            bedrooms=2,
            year_built=2000,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow() - timedelta(days=45)
        ),
        Property(
            funda_id="test-002",
            url="https://funda.nl/test-002",
            address="Test Street 2",
            city="Amsterdam",
            price=400000,
            living_area=80,
            rooms=3,
            bedrooms=2,
            year_built=2005,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow() - timedelta(days=20)
        ),
        Property(
            funda_id="test-003",
            url="https://funda.nl/test-003",
            address="Test Street 3",
            city="Amsterdam",
            price=320000,
            living_area=80,
            rooms=3,
            bedrooms=2,
            year_built=2002,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow() - timedelta(days=60)
        ),
        Property(
            funda_id="test-004",
            url="https://funda.nl/test-004",
            address="Test Street 4",
            city="Rotterdam",
            price=250000,
            living_area=70,
            rooms=2,
            bedrooms=1,
            year_built=1995,
            listing_type="buy",
            status="active",
            scraped_at=datetime.utcnow() - timedelta(days=30)
        ),
    ]

    for prop in properties:
        test_db.add(prop)

    test_db.commit()

    # Add price history for test-003
    prop_003 = test_db.query(Property).filter_by(funda_id="test-003").first()
    if prop_003:
        price_history = [
            PriceHistory(
                property_id=prop_003.id,
                price=360000,
                observed_at=datetime.utcnow() - timedelta(days=60)
            ),
            PriceHistory(
                property_id=prop_003.id,
                price=340000,
                observed_at=datetime.utcnow() - timedelta(days=30)
            ),
            PriceHistory(
                property_id=prop_003.id,
                price=320000,
                observed_at=datetime.utcnow() - timedelta(days=1)
            ),
        ]
        for ph in price_history:
            test_db.add(ph)

    test_db.commit()
    return properties


def test_property_analyzer_initialization(test_db):
    """Test PropertyAnalyzer initialization."""
    analyzer = PropertyAnalyzer(test_db)
    assert analyzer.db == test_db


def test_get_comparable_properties(test_db, sample_properties):
    """Test finding comparable properties."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-001").first()

    comparables = analyzer.get_comparable_properties(prop)

    # Should find other Amsterdam properties with 3 rooms
    assert len(comparables) >= 1
    for comp in comparables:
        assert comp.id != prop.id
        assert comp.city == prop.city
        assert comp.rooms == prop.rooms


def test_calculate_group_statistics(test_db, sample_properties):
    """Test statistical calculations for property groups."""
    analyzer = PropertyAnalyzer(test_db)
    amsterdam_props = [p for p in sample_properties if p.city == "Amsterdam"]

    stats = analyzer.calculate_group_statistics(amsterdam_props)

    assert isinstance(stats, ComparableGroup)
    assert stats.city == "Amsterdam"
    assert stats.count == len(amsterdam_props)
    assert stats.mean_price > 0
    assert stats.mean_price_per_sqm > 0
    assert stats.std_price >= 0


def test_calculate_z_score(test_db):
    """Test z-score calculation."""
    analyzer = PropertyAnalyzer(test_db)

    # Test normal case
    z = analyzer.calculate_z_score(value=150, mean=100, std=25)
    assert z == 2.0

    # Test value below mean
    z = analyzer.calculate_z_score(value=75, mean=100, std=25)
    assert z == -1.0

    # Test zero std dev
    z = analyzer.calculate_z_score(value=100, mean=100, std=0)
    assert z == 0.0


def test_get_price_drop_info(test_db, sample_properties):
    """Test price drop detection."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-003").first()

    drop_pct, num_obs = analyzer.get_price_drop_info(prop.id)

    assert drop_pct is not None
    assert drop_pct > 0  # Price dropped from 360k to 320k
    assert num_obs == 3


def test_get_days_on_market(test_db, sample_properties):
    """Test days on market calculation."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-001").first()

    days = analyzer.get_days_on_market(prop)

    assert days >= 45  # Property was scraped 45 days ago


def test_calculate_undervalue_score(test_db, sample_properties):
    """Test undervalue score calculation."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-003").first()

    # Get comparable stats
    comparables = analyzer.get_comparable_properties(prop)
    stats = analyzer.calculate_group_statistics(comparables)

    score, components, explanation = analyzer.calculate_undervalue_score(prop, stats)

    # Score should be between 0 and 100
    assert 0 <= score <= 100

    # Should have all expected components
    assert "price_per_sqm_score" in components
    assert "days_on_market_score" in components
    assert "price_drop_score" in components
    assert "composite" in components

    # Explanation should be non-empty
    assert len(explanation) > 0


def test_analyze_property(test_db, sample_properties):
    """Test complete property analysis."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-001").first()

    result = analyzer.analyze_property(prop)

    assert isinstance(result, PropertyScore)
    assert result.property_id == prop.id
    assert result.funda_id == prop.funda_id
    assert result.price == prop.price
    assert result.living_area == prop.living_area
    assert result.price_per_sqm > 0
    assert 0 <= result.composite_score <= 100
    assert len(result.explanation) > 0


def test_find_undervalued_properties(test_db, sample_properties):
    """Test finding undervalued properties."""
    analyzer = PropertyAnalyzer(test_db)

    results = analyzer.find_undervalued_properties(
        city="Amsterdam",
        listing_type="buy",
        limit=10
    )

    assert len(results) > 0
    assert all(isinstance(r, PropertyScore) for r in results)

    # Results should be sorted by score (descending)
    for i in range(len(results) - 1):
        assert results[i].composite_score >= results[i + 1].composite_score

    # All should be from Amsterdam
    assert all(r.city == "Amsterdam" for r in results)


def test_find_undervalued_with_min_score(test_db, sample_properties):
    """Test filtering by minimum score."""
    analyzer = PropertyAnalyzer(test_db)

    min_score = 70.0
    results = analyzer.find_undervalued_properties(
        city="Amsterdam",
        listing_type="buy",
        min_score=min_score,
        limit=10
    )

    # All results should meet minimum score
    for r in results:
        assert r.composite_score >= min_score


def test_get_market_statistics(test_db, sample_properties):
    """Test market statistics calculation."""
    analyzer = PropertyAnalyzer(test_db)

    stats = analyzer.get_market_statistics(
        city="Amsterdam",
        listing_type="buy"
    )

    assert stats["total_properties"] > 0
    assert "price" in stats
    assert "living_area" in stats
    assert "price_per_sqm" in stats

    # Check price stats
    assert stats["price"]["mean"] > 0
    assert stats["price"]["median"] > 0
    assert stats["price"]["std"] >= 0

    # Check price per sqm stats
    assert stats["price_per_sqm"]["mean"] > 0
    assert stats["price_per_sqm"]["median"] > 0


def test_get_market_statistics_grouped(test_db, sample_properties):
    """Test grouped market statistics."""
    analyzer = PropertyAnalyzer(test_db)

    stats = analyzer.get_market_statistics(
        listing_type="buy",
        group_by_city=True
    )

    assert "grouped_by" in stats
    assert stats["grouped_by"] == "city"
    assert "groups" in stats
    assert "Amsterdam" in stats["groups"]
    assert "Rotterdam" in stats["groups"]

    # Check each group has proper stats
    for city, city_stats in stats["groups"].items():
        assert city_stats["total_properties"] > 0
        assert "price" in city_stats


def test_analyze_property_without_comparables(test_db, sample_properties):
    """Test analyzing property without comparable group."""
    analyzer = PropertyAnalyzer(test_db)
    prop = test_db.query(Property).filter_by(funda_id="test-001").first()

    result = analyzer.analyze_property(prop, include_comparables=False)

    assert isinstance(result, PropertyScore)
    assert result.comparable_group is None
    assert result.composite_score >= 0  # Should still calculate a score


def test_analyze_property_with_missing_data(test_db):
    """Test analyzing property with missing price or area."""
    analyzer = PropertyAnalyzer(test_db)

    # Property with no price
    prop = Property(
        funda_id="test-no-price",
        url="https://funda.nl/test",
        address="Test",
        city="Amsterdam",
        price=None,
        living_area=80,
        listing_type="buy",
        status="active",
        scraped_at=datetime.utcnow()
    )
    test_db.add(prop)
    test_db.commit()

    result = analyzer.analyze_property(prop)

    # Should handle gracefully
    assert result.composite_score == 0.0
    assert "Insufficient" in result.explanation
