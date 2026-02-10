"""Scrape status and metadata endpoints."""
import asyncio
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from funda_finder.db.models import ScrapeMeta
from funda_finder.db.session import get_db, SessionLocal
from funda_finder.scraper.base import PropertyType
from funda_finder.scraper.orchestrator import ScrapeOrchestrator
from funda_finder.config import settings

router = APIRouter()


class ScrapeRequest(BaseModel):
    """Request model for triggering a scrape."""
    city: str = "amsterdam"
    property_type: str = "buy"
    max_pages: int = 10  # 10 pages = ~150 properties


def run_scrape_background(city: str, property_type: str, max_pages: int):
    """Run scrape in background thread."""
    # Create new session for background task
    session = SessionLocal()
    try:
        prop_type = PropertyType.BUY if property_type == "buy" else PropertyType.RENT
        orchestrator = ScrapeOrchestrator(rate_limit=settings.rate_limit, session=session)

        # Run the scrape with max_pages
        meta = orchestrator.run_scrape(
            city=city,
            property_type=prop_type,
            max_pages=max_pages
        )
        print(f"Scrape completed: {meta.listings_found} found, {meta.listings_new} new, {meta.listings_updated} updated")

    except Exception as e:
        print(f"Scrape error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


@router.get("/status")
async def get_scrape_status(db: Session = Depends(get_db)):
    """Get information about the last scrape run.

    Returns metadata about the most recent scrape including:
    - When it ran
    - How many listings were found/new/updated
    - Any errors encountered
    """
    # Get the most recent scrape
    result = db.execute(
        select(ScrapeMeta)
        .order_by(ScrapeMeta.started_at.desc())
        .limit(1)
    )
    last_scrape = result.scalar_one_or_none()

    if not last_scrape:
        return {
            "status": "no_scrapes",
            "message": "No scrape runs recorded yet"
        }

    return {
        "status": "completed" if last_scrape.finished_at else "in_progress",
        "run_id": last_scrape.run_id,
        "started_at": last_scrape.started_at.isoformat(),
        "finished_at": last_scrape.finished_at.isoformat() if last_scrape.finished_at else None,
        "listings_found": last_scrape.listings_found,
        "listings_new": last_scrape.listings_new,
        "listings_updated": last_scrape.listings_updated,
        "errors": last_scrape.errors,
    }


@router.get("/history")
async def get_scrape_history(
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """Get history of recent scrape runs."""
    result = db.execute(
        select(ScrapeMeta)
        .order_by(ScrapeMeta.started_at.desc())
        .limit(limit)
    )
    scrapes = result.scalars().all()

    return {
        "scrapes": [
            {
                "run_id": s.run_id,
                "started_at": s.started_at.isoformat(),
                "finished_at": s.finished_at.isoformat() if s.finished_at else None,
                "listings_found": s.listings_found,
                "listings_new": s.listings_new,
                "listings_updated": s.listings_updated,
                "has_errors": bool(s.errors),
            }
            for s in scrapes
        ]
    }


@router.post("/trigger")
async def trigger_scrape(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Trigger a scrape run with configurable parameters.

    Args:
        request: Scrape configuration (city, property_type, max_pages)
        background_tasks: FastAPI background tasks
        db: Database session

    Returns:
        Status message and estimated time
    """
    # Validate inputs
    if request.max_pages < 1 or request.max_pages > 100:
        raise HTTPException(status_code=400, detail="max_pages must be between 1 and 100")

    if request.city not in ["amsterdam", "rotterdam", "den-haag", "utrecht"]:
        raise HTTPException(status_code=400, detail="Invalid city")

    if request.property_type not in ["buy", "rent"]:
        raise HTTPException(status_code=400, detail="property_type must be 'buy' or 'rent'")

    # Calculate estimated time
    # Each page: ~3s rate limit, plus ~3s per property for detailed fetch
    # With 15 properties per page: ~3s + (15 * 3s) = ~48s per page
    estimated_minutes = (request.max_pages * 48) / 60

    # Trigger background scrape
    background_tasks.add_task(
        run_scrape_background,
        request.city,
        request.property_type,
        request.max_pages
    )

    return {
        "status": "started",
        "message": f"Scraping {request.max_pages} pages from {request.city}",
        "estimated_properties": request.max_pages * 15,
        "estimated_minutes": round(estimated_minutes, 1),
        "note": "Check /api/scrape/status for progress"
    }
