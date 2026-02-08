"""Scrape status and metadata endpoints."""
from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from funda_finder.db.models import ScrapeMeta
from funda_finder.db.session import get_db

router = APIRouter()


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
