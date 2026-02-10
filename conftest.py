"""Root conftest: override DB to SQLite for tests."""

import os

os.environ.setdefault("FUNDA_DB_URL", "sqlite:///:memory:")
