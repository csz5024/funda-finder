from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_prefix": "FUNDA_"}

    db_path: Path = Path("data/funda.db")
    rate_limit: float = 3.0
    default_cities: str = "amsterdam,rotterdam,den-haag,utrecht"
    host: str = "127.0.0.1"
    port: int = 8000

    @property
    def city_list(self) -> list[str]:
        return [c.strip() for c in self.default_cities.split(",")]


settings = Settings()
