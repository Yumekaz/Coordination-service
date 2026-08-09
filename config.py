"""
Configuration constants for the Coordination Service.
All configurable parameters are centralized here.
"""

import os

# Database configuration
DATABASE_PATH = os.environ.get("COORD_DB_PATH", "database.db")
WAL_MODE = True  # Enable WAL for better concurrency

# Session configuration
DEFAULT_SESSION_TIMEOUT = 30  # seconds
SESSION_CHECK_INTERVAL = 1.0  # seconds between timeout checks
MIN_SESSION_TIMEOUT = 5  # minimum allowed timeout
MAX_SESSION_TIMEOUT = 300  # maximum allowed timeout

# Watch configuration
WATCH_WAIT_TIMEOUT = 30  # default timeout for watch wait
MAX_WATCHES_PER_SESSION = 1000  # prevent resource exhaustion
WATCH_EVENT_HISTORY_LIMIT = 1000  # bounded in-memory fired-watch history for postmortems

# Server configuration
# Bind locally by default. Deployments that intentionally expose the API must
# opt in with COORD_HOST and should put authentication/TLS in front of it.
HOST = os.environ.get("COORD_HOST", "127.0.0.1")
PORT = int(os.environ.get("COORD_PORT", os.environ.get("PORT", "8000")))

# The visualizer is served by this application, so same-origin requests do not
# need CORS. These two origins keep the documented localhost workflow useful
# when the visualizer is opened through either common loopback name.
_default_cors_origins = (
    "http://127.0.0.1:8000",
    "http://localhost:8000",
)
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "COORD_CORS_ORIGINS", ",".join(_default_cors_origins)
    ).split(",")
    if origin.strip()
]

# Replication traffic is authenticated by default whenever clustering is
# configured. The opt-in is intentionally explicit for isolated local tests.
ALLOW_INSECURE_REPLICATION = os.environ.get(
    "COORD_ALLOW_INSECURE_REPLICATION", ""
).lower() in {"1", "true", "yes", "on"}

# Logging configuration
LOG_LEVEL = os.environ.get("COORD_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Performance tuning
FSYNC_ON_COMMIT = True  # Ensure durability on every commit
BATCH_SIZE = 100  # For bulk operations during recovery
