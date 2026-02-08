"""Property analysis endpoints."""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from funda_finder.db.models import Property
from funda_finder.db.session import get_db

router = APIRouter()


@router.get("/undervalued")
async def get_undervalued_properties(
    city: Optional[str] = None,
    min_score: Optional[float] = Query(None, description="Minimum undervalue score"),
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db)
):
    """Get ranked list of potentially undervalued properties.

    NOTE: This endpoint returns basic price/area ratio for now.
    Full undervalue scoring will be implemented when ff-3od (Property analysis engine) is complete.

    TODO (ff-3od): Integrate proper undervalue scoring based on:
    - Price/m² z-score vs comparable properties
    - Days on market
    - Price drop history
    - Composite scoring algorithm
    """
    # Validate limit
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")

    query = select(Property).where(
        Property.price.isnot(None),
        Property.living_area.isnot(None),
        Property.living_area > 0
    )

    if city:
        query = query.where(Property.city.ilike(f"%{city}%"))

    query = query.limit(limit)
    result = db.execute(query)
    properties = result.scalars().all()

    # Basic price per sqm ranking (placeholder until ff-3od is complete)
    analyzed = []
    for p in properties:
        if p.price and p.living_area and p.living_area > 0:
            price_per_sqm = p.price / p.living_area
            analyzed.append({
                "id": p.id,
                "funda_id": p.funda_id,
                "address": p.address,
                "city": p.city,
                "price": p.price,
                "living_area": p.living_area,
                "price_per_sqm": round(price_per_sqm, 2),
                "rooms": p.rooms,
                "year_built": p.year_built,
                "score": None,  # TODO: Add composite undervalue score from ff-3od
                "score_explanation": "Basic price/m² calculation - full scoring pending",
            })

    # Sort by price per sqm (ascending = cheaper per sqm)
    analyzed.sort(key=lambda x: x["price_per_sqm"])

    return {
        "properties": analyzed,
        "note": "Basic ranking by price/m². Full undervalue analysis pending (ff-3od)."
    }


@router.get("/stats")
async def get_market_statistics(
    city: Optional[str] = None,
    listing_type: Optional[str] = Query(None, description="buy or rent"),
    db: Session = Depends(get_db)
):
    """Get market statistics and trends.

    NOTE: Returns basic aggregates for now.
    Advanced statistical analysis will be implemented when ff-3od is complete.

    TODO (ff-3od): Add:
    - Price/m² distribution by neighborhood
    - Price trends over time
    - Z-scores and percentile rankings
    - Comparable property grouping stats
    """
    # Validate listing_type
    if listing_type and listing_type not in ["buy", "rent"]:
        raise HTTPException(status_code=400, detail="listing_type must be 'buy' or 'rent'")

    query = select(Property).where(Property.price.isnot(None))

    if city:
        query = query.where(Property.city.ilike(f"%{city}%"))
    if listing_type:
        query = query.where(Property.listing_type == listing_type)

    result = db.execute(query)
    properties = list(result.scalars().all())

    if not properties:
        return {
            "total_properties": 0,
            "note": "No properties found matching criteria"
        }

    # Basic statistics
    prices = [p.price for p in properties if p.price]
    areas = [p.living_area for p in properties if p.living_area and p.living_area > 0]
    price_per_sqm_list = [
        p.price / p.living_area
        for p in properties
        if p.price and p.living_area and p.living_area > 0
    ]

    stats = {
        "total_properties": len(properties),
        "price": {
            "avg": round(sum(prices) / len(prices), 2) if prices else None,
            "min": min(prices) if prices else None,
            "max": max(prices) if prices else None,
            "median": round(sorted(prices)[len(prices) // 2], 2) if prices else None,
        },
        "living_area": {
            "avg": round(sum(areas) / len(areas), 2) if areas else None,
            "min": min(areas) if areas else None,
            "max": max(areas) if areas else None,
        },
        "price_per_sqm": {
            "avg": round(sum(price_per_sqm_list) / len(price_per_sqm_list), 2) if price_per_sqm_list else None,
            "min": round(min(price_per_sqm_list), 2) if price_per_sqm_list else None,
            "max": round(max(price_per_sqm_list), 2) if price_per_sqm_list else None,
            "median": round(sorted(price_per_sqm_list)[len(price_per_sqm_list) // 2], 2) if price_per_sqm_list else None,
        },
        "note": "Basic aggregates - advanced analysis pending (ff-3od)"
    }

    return stats
