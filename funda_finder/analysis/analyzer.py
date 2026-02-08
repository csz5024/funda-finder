"""Property analysis engine for identifying undervalued properties."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, and_, func
from sqlalchemy.orm import Session

from funda_finder.db.models import Property, PriceHistory


@dataclass
class ComparableGroup:
    """Statistical summary for a group of comparable properties."""
    city: str
    property_type: str
    rooms: Optional[int]
    year_range: Optional[str]
    count: int
    median_price: float
    mean_price: float
    std_price: float
    median_price_per_sqm: float
    mean_price_per_sqm: float
    std_price_per_sqm: float


@dataclass
class PropertyScore:
    """Analysis score and explanation for a property."""
    property_id: int
    funda_id: str
    address: str
    city: str
    price: int
    living_area: int
    price_per_sqm: float
    composite_score: float
    percentile_rank: float
    score_components: Dict[str, float]
    explanation: str
    comparable_group: Optional[ComparableGroup]


class PropertyAnalyzer:
    """Analyzes properties to identify potentially undervalued listings."""

    def __init__(self, db: Session):
        self.db = db

    def get_comparable_properties(
        self,
        property: Property,
        max_year_diff: int = 20,
    ) -> List[Property]:
        """Find comparable properties based on key characteristics."""
        query = select(Property).where(
            and_(
                Property.id != property.id,
                Property.city == property.city,
                Property.listing_type == property.listing_type,
                Property.price.isnot(None),
                Property.living_area.isnot(None),
                Property.living_area > 0,
                Property.status == "active"
            )
        )

        # Match room count if available
        if property.rooms:
            query = query.where(Property.rooms == property.rooms)

        # Match year built within range if available
        if property.year_built:
            year_min = property.year_built - max_year_diff
            year_max = property.year_built + max_year_diff
            query = query.where(
                and_(
                    Property.year_built.isnot(None),
                    Property.year_built >= year_min,
                    Property.year_built <= year_max
                )
            )

        result = self.db.execute(query)
        return list(result.scalars().all())

    def calculate_group_statistics(
        self,
        properties: List[Property]
    ) -> Optional[ComparableGroup]:
        """Calculate statistical summary for a group of properties."""
        if not properties:
            return None

        # Calculate price per sqm for each property
        price_per_sqm_list = []
        prices = []

        for p in properties:
            if p.price and p.living_area and p.living_area > 0:
                prices.append(p.price)
                price_per_sqm_list.append(p.price / p.living_area)

        if not prices or not price_per_sqm_list:
            return None

        # Calculate statistics
        prices_sorted = sorted(prices)
        pps_sorted = sorted(price_per_sqm_list)

        median_price = pps_sorted[len(prices_sorted) // 2] if prices_sorted else 0
        mean_price = sum(prices) / len(prices)

        # Standard deviation
        variance_price = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = variance_price ** 0.5

        median_pps = pps_sorted[len(pps_sorted) // 2]
        mean_pps = sum(price_per_sqm_list) / len(price_per_sqm_list)

        variance_pps = sum((p - mean_pps) ** 2 for p in price_per_sqm_list) / len(price_per_sqm_list)
        std_pps = variance_pps ** 0.5

        # Get representative attributes
        first_prop = properties[0]
        year_range = None
        if first_prop.year_built:
            years = [p.year_built for p in properties if p.year_built]
            if years:
                year_range = f"{min(years)}-{max(years)}"

        return ComparableGroup(
            city=first_prop.city,
            property_type=first_prop.listing_type,
            rooms=first_prop.rooms,
            year_range=year_range,
            count=len(properties),
            median_price=median_price,
            mean_price=mean_price,
            std_price=std_price,
            median_price_per_sqm=median_pps,
            mean_price_per_sqm=mean_pps,
            std_price_per_sqm=std_pps
        )

    def calculate_z_score(
        self,
        value: float,
        mean: float,
        std: float
    ) -> float:
        """Calculate z-score (standard deviations from mean)."""
        if std == 0:
            return 0.0
        return (value - mean) / std

    def get_price_drop_info(
        self,
        property_id: int,
        days_lookback: int = 90
    ) -> Tuple[Optional[float], int]:
        """Get price drop information from history."""
        cutoff_date = datetime.utcnow() - timedelta(days=days_lookback)

        query = select(PriceHistory).where(
            and_(
                PriceHistory.property_id == property_id,
                PriceHistory.observed_at >= cutoff_date
            )
        ).order_by(PriceHistory.observed_at)

        result = self.db.execute(query)
        history = list(result.scalars().all())

        if len(history) < 2:
            return None, 0

        first_price = history[0].price
        last_price = history[-1].price

        if first_price > last_price:
            drop_pct = ((first_price - last_price) / first_price) * 100
            return drop_pct, len(history)

        return None, len(history)

    def get_days_on_market(self, property: Property) -> int:
        """Calculate days property has been on market."""
        return (datetime.utcnow() - property.scraped_at).days

    def calculate_undervalue_score(
        self,
        property: Property,
        comparable_stats: Optional[ComparableGroup]
    ) -> Tuple[float, Dict[str, float], str]:
        """
        Calculate composite undervalue score.

        Returns:
            - composite_score: 0-100 (higher = more undervalued)
            - score_components: breakdown of score factors
            - explanation: human-readable explanation
        """
        if not property.price or not property.living_area or property.living_area <= 0:
            return 0.0, {}, "Insufficient price or area data"

        price_per_sqm = property.price / property.living_area
        components = {}
        explanation_parts = []

        # Component 1: Price/m² vs comparables (40% weight)
        pps_score = 50.0  # neutral default
        if comparable_stats and comparable_stats.std_price_per_sqm > 0:
            z_score = self.calculate_z_score(
                price_per_sqm,
                comparable_stats.mean_price_per_sqm,
                comparable_stats.std_price_per_sqm
            )
            # Convert z-score to 0-100 scale (negative z-score = undervalued)
            # z = -2 means 2 std devs below average (very undervalued) -> score ~100
            # z = 0 means average -> score 50
            # z = +2 means 2 std devs above average -> score ~0
            pps_score = max(0, min(100, 50 - (z_score * 25)))
            components["price_per_sqm_score"] = round(pps_score, 2)

            pct_diff = ((price_per_sqm - comparable_stats.mean_price_per_sqm) /
                       comparable_stats.mean_price_per_sqm) * 100

            if pct_diff < -10:
                explanation_parts.append(
                    f"€{price_per_sqm:.0f}/m² is {abs(pct_diff):.1f}% below "
                    f"comparable properties (€{comparable_stats.mean_price_per_sqm:.0f}/m²)"
                )
            elif pct_diff > 10:
                explanation_parts.append(
                    f"€{price_per_sqm:.0f}/m² is {pct_diff:.1f}% above "
                    f"comparable properties (€{comparable_stats.mean_price_per_sqm:.0f}/m²)"
                )
            else:
                explanation_parts.append(
                    f"€{price_per_sqm:.0f}/m² is near average for comparable properties"
                )
        else:
            components["price_per_sqm_score"] = pps_score
            explanation_parts.append(f"€{price_per_sqm:.0f}/m² (no comparables for scoring)")

        # Component 2: Days on market (20% weight)
        days = self.get_days_on_market(property)
        # More days on market = potentially undervalued
        # 0-30 days: score 0-40
        # 30-60 days: score 40-70
        # 60+ days: score 70-100
        if days <= 30:
            dom_score = min(40, (days / 30) * 40)
        elif days <= 60:
            dom_score = 40 + ((days - 30) / 30) * 30
        else:
            dom_score = 70 + min(30, ((days - 60) / 60) * 30)

        components["days_on_market_score"] = round(dom_score, 2)
        explanation_parts.append(f"{days} days on market")

        # Component 3: Price drops (40% weight)
        price_drop, num_observations = self.get_price_drop_info(property.id)
        if price_drop and price_drop > 0:
            # Significant price drop = more undervalued
            # 0-5% drop: score 60-75
            # 5-10% drop: score 75-90
            # 10%+ drop: score 90-100
            if price_drop <= 5:
                drop_score = 60 + (price_drop / 5) * 15
            elif price_drop <= 10:
                drop_score = 75 + ((price_drop - 5) / 5) * 15
            else:
                drop_score = 90 + min(10, ((price_drop - 10) / 10) * 10)

            components["price_drop_score"] = round(drop_score, 2)
            explanation_parts.append(f"Price dropped {price_drop:.1f}% in last 90 days")
        else:
            drop_score = 30.0  # No price drop = less likely undervalued
            components["price_drop_score"] = drop_score
            if num_observations >= 2:
                explanation_parts.append("No price drops in last 90 days")

        # Calculate weighted composite score
        composite = (
            pps_score * 0.40 +
            dom_score * 0.20 +
            drop_score * 0.40
        )

        components["composite"] = round(composite, 2)
        explanation = ". ".join(explanation_parts) + "."

        return composite, components, explanation

    def analyze_property(
        self,
        property: Property,
        include_comparables: bool = True
    ) -> PropertyScore:
        """Perform complete analysis on a single property."""
        comparable_stats = None

        if include_comparables:
            comparables = self.get_comparable_properties(property)
            comparable_stats = self.calculate_group_statistics(comparables)

        score, components, explanation = self.calculate_undervalue_score(
            property,
            comparable_stats
        )

        # Calculate percentile rank (0-100, higher = more undervalued)
        percentile = score  # For now, use score as percentile

        price_per_sqm = 0.0
        if property.price and property.living_area and property.living_area > 0:
            price_per_sqm = property.price / property.living_area

        return PropertyScore(
            property_id=property.id,
            funda_id=property.funda_id,
            address=property.address,
            city=property.city,
            price=property.price or 0,
            living_area=property.living_area or 0,
            price_per_sqm=round(price_per_sqm, 2),
            composite_score=round(score, 2),
            percentile_rank=round(percentile, 2),
            score_components=components,
            explanation=explanation,
            comparable_group=comparable_stats
        )

    def find_undervalued_properties(
        self,
        city: Optional[str] = None,
        listing_type: str = "buy",
        min_score: Optional[float] = None,
        limit: int = 50
    ) -> List[PropertyScore]:
        """
        Find and rank potentially undervalued properties.

        Args:
            city: Filter by city name
            listing_type: "buy" or "rent"
            min_score: Minimum undervalue score (0-100)
            limit: Maximum number of results

        Returns:
            List of PropertyScore objects sorted by score (descending)
        """
        query = select(Property).where(
            and_(
                Property.listing_type == listing_type,
                Property.price.isnot(None),
                Property.living_area.isnot(None),
                Property.living_area > 0,
                Property.status == "active"
            )
        )

        if city:
            query = query.where(Property.city.ilike(f"%{city}%"))

        # Get more properties than needed for filtering
        query = query.limit(limit * 2)
        result = self.db.execute(query)
        properties = list(result.scalars().all())

        # Analyze each property
        scored_properties = []
        for prop in properties:
            try:
                score_obj = self.analyze_property(prop)

                # Filter by min_score if specified
                if min_score is None or score_obj.composite_score >= min_score:
                    scored_properties.append(score_obj)
            except Exception as e:
                # Skip properties that fail analysis
                continue

        # Sort by composite score (descending - highest score = most undervalued)
        scored_properties.sort(key=lambda x: x.composite_score, reverse=True)

        return scored_properties[:limit]

    def get_market_statistics(
        self,
        city: Optional[str] = None,
        listing_type: Optional[str] = None,
        group_by_city: bool = False
    ) -> Dict:
        """
        Get comprehensive market statistics.

        Args:
            city: Filter by city name
            listing_type: "buy" or "rent"
            group_by_city: Return statistics grouped by city

        Returns:
            Dictionary with market statistics
        """
        query = select(Property).where(
            and_(
                Property.price.isnot(None),
                Property.living_area.isnot(None),
                Property.living_area > 0,
                Property.status == "active"
            )
        )

        if city:
            query = query.where(Property.city.ilike(f"%{city}%"))
        if listing_type:
            query = query.where(Property.listing_type == listing_type)

        result = self.db.execute(query)
        properties = list(result.scalars().all())

        if not properties:
            return {"total_properties": 0, "note": "No properties found"}

        if group_by_city:
            return self._calculate_grouped_stats(properties)
        else:
            return self._calculate_aggregate_stats(properties)

    def _calculate_aggregate_stats(self, properties: List[Property]) -> Dict:
        """Calculate aggregate statistics for all properties."""
        prices = [p.price for p in properties if p.price]
        areas = [p.living_area for p in properties if p.living_area]
        price_per_sqm_list = [
            p.price / p.living_area
            for p in properties
            if p.price and p.living_area > 0
        ]

        if not prices or not price_per_sqm_list:
            return {"total_properties": len(properties), "note": "Insufficient data"}

        # Calculate statistics
        prices_sorted = sorted(prices)
        pps_sorted = sorted(price_per_sqm_list)

        mean_price = sum(prices) / len(prices)
        variance_price = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = variance_price ** 0.5

        mean_pps = sum(price_per_sqm_list) / len(price_per_sqm_list)
        variance_pps = sum((p - mean_pps) ** 2 for p in price_per_sqm_list) / len(price_per_sqm_list)
        std_pps = variance_pps ** 0.5

        return {
            "total_properties": len(properties),
            "price": {
                "mean": round(mean_price, 2),
                "median": round(prices_sorted[len(prices_sorted) // 2], 2),
                "std": round(std_price, 2),
                "min": min(prices),
                "max": max(prices),
            },
            "living_area": {
                "mean": round(sum(areas) / len(areas), 2) if areas else None,
                "median": round(sorted(areas)[len(areas) // 2], 2) if areas else None,
                "min": min(areas) if areas else None,
                "max": max(areas) if areas else None,
            },
            "price_per_sqm": {
                "mean": round(mean_pps, 2),
                "median": round(pps_sorted[len(pps_sorted) // 2], 2),
                "std": round(std_pps, 2),
                "min": round(min(price_per_sqm_list), 2),
                "max": round(max(price_per_sqm_list), 2),
            }
        }

    def _calculate_grouped_stats(self, properties: List[Property]) -> Dict:
        """Calculate statistics grouped by city."""
        groups = defaultdict(list)
        for p in properties:
            groups[p.city].append(p)

        results = {}
        for city, city_props in groups.items():
            results[city] = self._calculate_aggregate_stats(city_props)

        return {
            "grouped_by": "city",
            "groups": results,
            "total_properties": len(properties)
        }
