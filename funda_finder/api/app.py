"""FastAPI application for Funda Finder dashboard."""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from funda_finder.api.routes import analysis, properties, scrape

app = FastAPI(
    title="Funda Property Finder",
    description="Property browser and analysis dashboard",
    version="0.1.0"
)

# Templates
templates = Jinja2Templates(directory="funda_finder/api/templates")

# API Routes
app.include_router(properties.router, prefix="/api/properties", tags=["properties"])
app.include_router(analysis.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(scrape.router, prefix="/api/scrape", tags=["scrape"])

# Static files (for CSS, JS, images)
try:
    app.mount("/static", StaticFiles(directory="funda_finder/api/static"), name="static")
except RuntimeError:
    pass  # Directory doesn't exist yet, will be created later


# HTML Page Routes
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Home page with overview."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/properties", response_class=HTMLResponse)
async def properties_page(request: Request):
    """Property listing page."""
    return templates.TemplateResponse("properties.html", {"request": request})


@app.get("/undervalued", response_class=HTMLResponse)
async def undervalued_page(request: Request):
    """Undervalued properties dashboard."""
    return templates.TemplateResponse("undervalued.html", {"request": request})


@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request):
    """Market statistics page."""
    return templates.TemplateResponse("stats.html", {"request": request})


@app.get("/scrapes", response_class=HTMLResponse)
async def scrapes_page(request: Request):
    """Scrape status page."""
    return templates.TemplateResponse("scrapes.html", {"request": request})


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}
