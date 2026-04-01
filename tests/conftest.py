"""
Pytest fixtures and configuration for the Coordination Service tests.

Provides reusable fixtures for:
- Isolated database instances
- Coordinator instances
- Test clients
- Common test data
"""

import pytest
import tempfile
import os
import time
import threading
from typing import Generator

from coordinator import Coordinator
from metadata_tree import MetadataTree
from session_manager import SessionManager
from watch_manager import WatchManager
from operation_log import OperationLog
from persistence import Persistence
from models import Node, Session, NodeType


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    """Create a temporary database file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)
    # Also clean up WAL and SHM files
    for suffix in ["-wal", "-shm"]:
        wal_path = db_path + suffix
        if os.path.exists(wal_path):
            os.unlink(wal_path)


@pytest.fixture
def persistence(temp_db_path: str) -> Generator[Persistence, None, None]:
    """Create a Persistence instance with a temporary database."""
    p = Persistence(temp_db_path)
    yield p
    p.close()


@pytest.fixture
def metadata_tree() -> MetadataTree:
    """Create a fresh MetadataTree instance."""
    return MetadataTree()


@pytest.fixture
def session_manager() -> Generator[SessionManager, None, None]:
    """Create a SessionManager instance."""
    sm = SessionManager()
    yield sm
    sm.stop()


@pytest.fixture
def watch_manager() -> WatchManager:
    """Create a WatchManager instance."""
    return WatchManager()


@pytest.fixture
def operation_log() -> OperationLog:
    """Create an OperationLog instance."""
    return OperationLog()


@pytest.fixture
def coordinator(temp_db_path: str) -> Generator[Coordinator, None, None]:
    """Create a Coordinator instance with a temporary database."""
    coord = Coordinator(temp_db_path)
    coord.start()
    yield coord
    coord.stop()


@pytest.fixture
def started_coordinator(coordinator: Coordinator) -> Coordinator:
    """Alias for coordinator (already started by fixture)."""
    return coordinator


@pytest.fixture
def sample_nodes() -> list:
    """Create sample nodes for testing."""
    return [
        {"path": "/config", "data": b"config_root", "persistent": True},
        {"path": "/config/db", "data": b"db_config", "persistent": True},
        {"path": "/config/db/host", "data": b"localhost", "persistent": True},
        {"path": "/config/db/port", "data": b"5432", "persistent": True},
        {"path": "/services", "data": b"services_root", "persistent": True},
        {"path": "/locks", "data": b"locks_root", "persistent": True},
    ]


@pytest.fixture
def populated_tree(metadata_tree: MetadataTree, sample_nodes: list) -> MetadataTree:
    """Create a metadata tree with sample nodes."""
    for node_data in sample_nodes:
        metadata_tree.create(
            path=node_data["path"],
            data=node_data["data"],
            node_type=NodeType.PERSISTENT,
        )
    return metadata_tree


def wait_for_condition(condition_fn, timeout=5.0, interval=0.1):
    """Wait for a condition to become true."""
    start = time.time()
    while time.time() - start < timeout:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def run_concurrent(fn, count=10, timeout=30.0):
    """Run a function concurrently and collect results."""
    results = []
    errors = []
    threads = []
    lock = threading.Lock()
    
    def wrapper():
        try:
            result = fn()
            with lock:
                results.append(result)
        except Exception as e:
            with lock:
                errors.append(e)
    
    for _ in range(count):
        t = threading.Thread(target=wrapper)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join(timeout=timeout)
    
    return results, errors
