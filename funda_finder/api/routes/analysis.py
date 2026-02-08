"""Property analysis endpoints."""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from funda_finder.analysis import PropertyAnalyzer
from funda_finder.db.models import Property
from funda_finder.db.session import get_db

router = APIRouter()


@router.get("/undervalued")
async def get_undervalued_properties(
    city: Optional[str] = None,
    listing_type: str = Query("buy", description="Property listing type (buy or rent)"),
    min_score: Optional[float] = Query(None, description="Minimum undervalue score (0-100)"),
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db)
):
    """Get ranked list of potentially undervalued properties.

    Uses composite scoring based on:
    - Price/m² z-score vs comparable properties
    - Days on market
    - Price drop history

    Score range: 0-100 (higher score = more undervalued)
    """
    # Validate parameters
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")

    if listing_type not in ["buy", "rent"]:
        raise HTTPException(status_code=400, detail="listing_type must be 'buy' or 'rent'")

    if min_score is not None and (min_score < 0 or min_score > 100):
        raise HTTPException(status_code=400, detail="min_score must be between 0 and 100")

    # Use analyzer to find undervalued properties
    analyzer = PropertyAnalyzer(db)
    results = analyzer.find_undervalued_properties(
        city=city,
        listing_type=listing_type,
        min_score=min_score,
        limit=limit
    )

    # Format response
    properties = []
    for score_obj in results:
        prop_data = {
            "id": score_obj.property_id,
            "funda_id": score_obj.funda_id,
            "address": score_obj.address,
            "city": score_obj.city,
            "price": score_obj.price,
            "living_area": score_obj.living_area,
            "price_per_sqm": score_obj.price_per_sqm,
            "undervalue_score": score_obj.composite_score,
            "percentile_rank": score_obj.percentile_rank,
            "explanation": score_obj.explanation,
            "score_components": score_obj.score_components,
        }

        # Include comparable group stats if available
        if score_obj.comparable_group:
            prop_data["comparable_group"] = {
                "count": score_obj.comparable_group.count,
                "avg_price_per_sqm": round(score_obj.comparable_group.mean_price_per_sqm, 2),
                "median_price_per_sqm": round(score_obj.comparable_group.median_price_per_sqm, 2),
            }

        properties.append(prop_data)

    return {
        "properties": properties,
        "count": len(properties),
        "filters": {
            "city": city,
            "listing_type": listing_type,
            "min_score": min_score,
        }
    }


@router.get("/stats")
async def get_market_statistics(
    city: Optional[str] = None,
    listing_type: Optional[str] = Query(None, description="buy or rent"),
    group_by_city: bool = Query(False, description="Group statistics by city"),
    db: Session = Depends(get_db)
):
    """Get comprehensive market statistics and trends.

    Returns:
    - Price distributions (mean, median, std, min, max)
    - Living area statistics
    - Price per m² analysis
    - Optional grouping by city
    """
    # Validate listing_type
    if listing_type and listing_type not in ["buy", "rent"]:
        raise HTTPException(status_code=400, detail="listing_type must be 'buy' or 'rent'")

    # Use analyzer for comprehensive statistics
    analyzer = PropertyAnalyzer(db)
    stats = analyzer.get_market_statistics(
        city=city,
        listing_type=listing_type,
        group_by_city=group_by_city
    )

    return stats
