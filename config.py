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
HOST = os.environ.get("COORD_HOST", "0.0.0.0")
PORT = int(os.environ.get("COORD_PORT", "8000"))

# Logging configuration
LOG_LEVEL = os.environ.get("COORD_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Performance tuning
FSYNC_ON_COMMIT = True  # Ensure durability on every commit
BATCH_SIZE = 100  # For bulk operations during recovery
