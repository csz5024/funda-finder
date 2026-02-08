"""Scheduling module for periodic scraping with APScheduler.

This module provides automatic scheduling of scraping jobs using APScheduler.
It supports cron-style scheduling and can run multiple cities in sequence or parallel.

Example usage:
    from funda_finder.scheduler import Scheduler

    scheduler = Scheduler()
    scheduler.start()  # Starts background scheduler

    # Or run a one-time scrape
    scheduler.run_scrape_job()
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from funda_finder.config import settings
from funda_finder.etl import ETLPipeline, ETLResult
from funda_finder.scraper import PropertyType, SearchFilters

logger = logging.getLogger(__name__)


class SchedulerConfig:
    """Configuration for the scheduler loaded from config.yaml."""

    def __init__(self, config_path: Path = Path("config.yaml")):
        """Load scheduler configuration from YAML file.

        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            logger.warning(f"Config file {self.config_path} not found, using defaults")
            return {}

        with open(self.config_path) as f:
            return yaml.safe_load(f) or {}

    @property
    def cities(self) -> List[str]:
        """Get list of cities to scrape."""
        return self._config.get("scraping", {}).get("cities", settings.city_list)

    @property
    def property_types(self) -> List[str]:
        """Get list of property types to scrape."""
        return self._config.get("scraping", {}).get(
            "property_types", settings.property_type_list
        )

    @property
    def rate_limit(self) -> float:
        """Get rate limit for scraping."""
        return self._config.get("scraping", {}).get("rate_limit", settings.rate_limit)

    @property
    def schedule_enabled(self) -> bool:
        """Check if scheduling is enabled."""
        return (
            self._config.get("scheduling", {}).get("enabled", False)
            or settings.schedule_enabled
        )

    @property
    def cron_expression(self) -> str:
        """Get cron expression for scheduling."""
        return self._config.get("scheduling", {}).get(
            "cron", settings.schedule_cron
        )

    @property
    def timezone(self) -> str:
        """Get timezone for scheduling."""
        return self._config.get("scheduling", {}).get(
            "timezone", settings.schedule_timezone
        )

    @property
    def max_concurrent(self) -> int:
        """Get maximum concurrent scrapes."""
        return self._config.get("scheduling", {}).get("max_concurrent", 3)


class Scheduler:
    """Scheduler for periodic property scraping.

    This class manages scheduled scraping jobs using APScheduler. It can run
    scrapes on a cron schedule and handles logging and error reporting.

    Example:
        >>> scheduler = Scheduler()
        >>> scheduler.start()  # Runs in background
        >>> # ... application continues running ...
        >>> scheduler.shutdown()  # Stop scheduler
    """

    def __init__(
        self,
        config_path: Optional[Path] = None,
        pipeline: Optional[ETLPipeline] = None,
    ):
        """Initialize scheduler.

        Args:
            config_path: Path to config.yaml file (uses default if None)
            pipeline: Optional ETL pipeline instance (creates new if None)
        """
        self.config = SchedulerConfig(config_path or Path("config.yaml"))
        self.pipeline = pipeline or ETLPipeline()
        self.scheduler = BackgroundScheduler(timezone=self.config.timezone)
        self._job_id = "funda_scrape_job"

    def run_scrape_job(self) -> List[ETLResult]:
        """Execute a scrape job for all configured cities and property types.

        This is the main job function that gets executed on schedule.

        Returns:
            List of ETLResult for each scrape
        """
        start_time = datetime.now()
        logger.info(
            f"Starting scheduled scrape job at {start_time.isoformat()} "
            f"({len(self.config.cities)} cities, "
            f"{len(self.config.property_types)} property types)"
        )

        results = []

        for city in self.config.cities:
            for prop_type_str in self.config.property_types:
                try:
                    property_type = PropertyType[prop_type_str.upper()]
                    filters = SearchFilters(city=city, property_type=property_type)

                    logger.info(f"Scraping {city} ({prop_type_str})...")
                    result = self.pipeline.run(filters)
                    results.append(result)

                    if result.success:
                        logger.info(
                            f"✓ {city} ({prop_type_str}): "
                            f"{result.new_count} new, {result.updated_count} updated"
                        )
                    else:
                        logger.error(
                            f"✗ {city} ({prop_type_str}) failed: {result.error_message}"
                        )

                except Exception as e:
                    logger.error(
                        f"Error scraping {city} ({prop_type_str}): {e}",
                        exc_info=True,
                    )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        successful = sum(1 for r in results if r.success)
        total_new = sum(r.new_count for r in results if r.success)
        total_updated = sum(r.updated_count for r in results if r.success)

        logger.info(
            f"Scheduled scrape job completed in {duration:.1f}s: "
            f"{successful}/{len(results)} successful, "
            f"{total_new} new, {total_updated} updated"
        )

        return results

    def add_job(self) -> None:
        """Add the scrape job to the scheduler with cron trigger."""
        # Parse cron expression
        cron_parts = self.config.cron_expression.split()
        if len(cron_parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.config.cron_expression}. "
                "Expected format: 'minute hour day month day_of_week'"
            )

        minute, hour, day, month, day_of_week = cron_parts

        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=self.config.timezone,
        )

        self.scheduler.add_job(
            func=self.run_scrape_job,
            trigger=trigger,
            id=self._job_id,
            name="Funda Property Scraper",
            replace_existing=True,
            max_instances=1,  # Don't run overlapping jobs
        )

        logger.info(
            f"Scheduled scrape job with cron: {self.config.cron_expression} "
            f"(timezone: {self.config.timezone})"
        )

    def start(self) -> None:
        """Start the scheduler.

        This starts the background scheduler and adds the scrape job if
        scheduling is enabled in configuration.
        """
        if not self.config.schedule_enabled:
            logger.warning("Scheduling is disabled in configuration")
            return

        self.add_job()
        self.scheduler.start()

        logger.info("Scheduler started successfully")

        # Log next scheduled run
        job = self.scheduler.get_job(self._job_id)
        if job and job.next_run_time:
            logger.info(f"Next scheduled scrape: {job.next_run_time.isoformat()}")

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the scheduler.

        Args:
            wait: If True, wait for running jobs to complete
        """
        logger.info("Shutting down scheduler...")
        self.scheduler.shutdown(wait=wait)
        logger.info("Scheduler shutdown complete")

    def pause(self) -> None:
        """Pause the scheduler (jobs won't run but scheduler stays alive)."""
        self.scheduler.pause()
        logger.info("Scheduler paused")

    def resume(self) -> None:
        """Resume a paused scheduler."""
        self.scheduler.resume()
        logger.info("Scheduler resumed")

    def get_next_run_time(self) -> Optional[datetime]:
        """Get the next scheduled run time.

        Returns:
            Next run time as datetime, or None if no job scheduled
        """
        job = self.scheduler.get_job(self._job_id)
        return job.next_run_time if job else None

    def run_now(self) -> List[ETLResult]:
        """Run the scrape job immediately (outside of schedule).

        Returns:
            List of ETLResult for each scrape
        """
        logger.info("Running scrape job immediately (manual trigger)")
        return self.run_scrape_job()


def setup_logging():
    """Configure logging for the scheduler."""
    log_level = getattr(logging, settings.log_level.upper())
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))

    # File handler (if log file is configured)
    handlers = [console_handler]
    if settings.log_file:
        settings.log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(settings.log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(level=log_level, handlers=handlers, force=True)


def run_scheduler():
    """Run the scheduler in foreground (blocking).

    This is the main entry point for running the scheduler as a standalone service.
    It will keep running until interrupted (Ctrl+C).
    """
    setup_logging()

    logger.info("Starting Funda Property Finder Scheduler")
    logger.info(f"Configuration: {Path('config.yaml').absolute()}")

    scheduler = Scheduler()

    if not scheduler.config.schedule_enabled:
        logger.error("Scheduling is disabled in configuration. Enable it to run scheduler.")
        logger.error("Set 'scheduling.enabled: true' in config.yaml or FUNDA_SCHEDULE_ENABLED=true")
        return

    scheduler.start()

    try:
        # Keep the main thread alive
        logger.info("Scheduler is running. Press Ctrl+C to stop.")
        import time
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received shutdown signal")
        scheduler.shutdown()


if __name__ == "__main__":
    run_scheduler()
