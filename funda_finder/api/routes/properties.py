"""Property listing and detail endpoints."""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from funda_finder.db.models import Property, PriceHistory
from funda_finder.db.session import get_db

router = APIRouter()


@router.get("")
async def list_properties(
    city: Optional[str] = None,
    min_price: Optional[int] = Query(None, description="Minimum price"),
    max_price: Optional[int] = Query(None, description="Maximum price"),
    min_rooms: Optional[int] = Query(None, description="Minimum number of rooms"),
    max_rooms: Optional[int] = Query(None, description="Maximum number of rooms"),
    listing_type: Optional[str] = Query(None, description="buy or rent"),
    sort_by: str = Query("updated_at", description="Field to sort by"),
    sort_order: str = Query("desc", description="asc or desc"),
    limit: int = Query(100, le=500, description="Max results"),
    offset: int = Query(0, description="Pagination offset"),
    db: Session = Depends(get_db)
):
    """List properties with optional filters and sorting.

    Returns paginated list of properties matching the given criteria.
    """
    # Validate listing_type
    if listing_type and listing_type not in ["buy", "rent"]:
        raise HTTPException(status_code=400, detail="listing_type must be 'buy' or 'rent'")

    # Validate sort fields
    allowed_sort_fields = ["updated_at", "price", "living_area", "city", "rooms", "year_built"]
    if sort_by not in allowed_sort_fields:
        raise HTTPException(status_code=400, detail=f"sort_by must be one of: {', '.join(allowed_sort_fields)}")

    # Validate sort order
    if sort_order not in ["asc", "desc"]:
        raise HTTPException(status_code=400, detail="sort_order must be 'asc' or 'desc'")

    query = select(Property)

    # Apply filters
    if city:
        query = query.where(Property.city.ilike(f"%{city}%"))
    if min_price is not None:
        query = query.where(Property.price >= min_price)
    if max_price is not None:
        query = query.where(Property.price <= max_price)
    if min_rooms is not None:
        query = query.where(Property.rooms >= min_rooms)
    if max_rooms is not None:
        query = query.where(Property.rooms <= max_rooms)
    if listing_type:
        query = query.where(Property.listing_type == listing_type)

    # Sorting
    sort_field = getattr(Property, sort_by)
    if sort_order == "asc":
        query = query.order_by(sort_field.asc())
    else:
        query = query.order_by(sort_field.desc())

    # Pagination
    query = query.limit(limit).offset(offset)

    # Execute
    result = db.execute(query)
    properties = result.scalars().all()

    # Get total count for pagination
    count_query = select(func.count(Property.id))
    if city:
        count_query = count_query.where(Property.city.ilike(f"%{city}%"))
    if min_price is not None:
        count_query = count_query.where(Property.price >= min_price)
    if max_price is not None:
        count_query = count_query.where(Property.price <= max_price)
    if min_rooms is not None:
        count_query = count_query.where(Property.rooms >= min_rooms)
    if max_rooms is not None:
        count_query = count_query.where(Property.rooms <= max_rooms)
    if listing_type:
        count_query = count_query.where(Property.listing_type == listing_type)

    total = db.execute(count_query).scalar()

    return {
        "properties": [
            {
                "id": p.id,
                "funda_id": p.funda_id,
                "url": p.url,
                "address": p.address,
                "city": p.city,
                "postal_code": p.postal_code,
                "price": p.price,
                "living_area": p.living_area,
                "rooms": p.rooms,
                "bedrooms": p.bedrooms,
                "year_built": p.year_built,
                "energy_label": p.energy_label,
                "listing_type": p.listing_type,
                "status": p.status,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            }
            for p in properties
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/{property_id}")
async def get_property_detail(
    property_id: int,
    db: Session = Depends(get_db)
):
    """Get detailed property information including price history.

    Returns full property details with historical price data.
    """
    # Get property
    result = db.execute(select(Property).where(Property.id == property_id))
    property = result.scalar_one_or_none()

    if not property:
        raise HTTPException(status_code=404, detail="Property not found")

    # Get price history
    history_result = db.execute(
        select(PriceHistory)
        .where(PriceHistory.property_id == property_id)
        .order_by(PriceHistory.observed_at.asc())
    )
    price_history = history_result.scalars().all()

    return {
        "id": property.id,
        "funda_id": property.funda_id,
        "url": property.url,
        "address": property.address,
        "city": property.city,
        "postal_code": property.postal_code,
        "lat": property.lat,
        "lon": property.lon,
        "price": property.price,
        "living_area": property.living_area,
        "plot_area": property.plot_area,
        "rooms": property.rooms,
        "bedrooms": property.bedrooms,
        "year_built": property.year_built,
        "energy_label": property.energy_label,
        "listing_type": property.listing_type,
        "status": property.status,
        "description": property.description,
        "photos": property.photos_json,
        "scraped_at": property.scraped_at.isoformat() if property.scraped_at else None,
        "updated_at": property.updated_at.isoformat() if property.updated_at else None,
        "price_history": [
            {
                "price": h.price,
                "observed_at": h.observed_at.isoformat()
            }
            for h in price_history
        ]
    }
