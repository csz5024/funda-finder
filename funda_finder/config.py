from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    All settings can be overridden via environment variables with the FUNDA_ prefix.
    Example: FUNDA_DB_URL=postgresql://user:pass@localhost/funda
    """
    model_config = {"env_prefix": "FUNDA_"}

    # Database configuration
    db_url: str = "postgresql://localhost/funda"
    db_path: Optional[Path] = Path("data/funda.db")  # Fallback SQLite path

    # Scraping configuration
    rate_limit: float = 3.0
    default_cities: str = "amsterdam,rotterdam,den-haag,utrecht"
    property_types: str = "buy"  # Comma-separated: "buy,rent"

    # Scheduling configuration
    schedule_enabled: bool = False
    schedule_cron: str = "0 2 * * *"  # Daily at 2 AM by default
    schedule_timezone: str = "Europe/Amsterdam"

    # Logging configuration
    log_level: str = "INFO"
    log_file: Optional[Path] = Path("logs/funda.log")

    # Dashboard configuration
    host: str = "127.0.0.1"
    port: int = 8000

    @property
    def city_list(self) -> list[str]:
        """Parse comma-separated city list."""
        return [c.strip() for c in self.default_cities.split(",")]

    @property
    def property_type_list(self) -> list[str]:
        """Parse comma-separated property types."""
        return [t.strip() for t in self.property_types.split(",")]


settings = Settings()
