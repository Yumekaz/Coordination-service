"""Regression tests for safe local and clustered defaults."""

import importlib
import os
import tempfile

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import config
from cluster import ClusterManager
from coordinator import Coordinator
import main


def _temp_db_path() -> str:
    handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    handle.close()
    return handle.name


def test_server_defaults_to_loopback(monkeypatch):
    monkeypatch.delenv("COORD_HOST", raising=False)
    reloaded = importlib.reload(config)
    assert reloaded.HOST == "127.0.0.1"


def test_cors_defaults_are_explicit_loopback_origins(monkeypatch):
    monkeypatch.delenv("COORD_CORS_ORIGINS", raising=False)
    reloaded = importlib.reload(config)
    assert reloaded.CORS_ALLOW_ORIGINS == [
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ]


def test_clustered_from_env_requires_replication_token(monkeypatch):
    monkeypatch.setenv("COORD_CLUSTER_ROLE", "leader")
    monkeypatch.delenv("COORD_REPLICATION_TOKEN", raising=False)
    monkeypatch.delenv("COORD_ALLOW_INSECURE_REPLICATION", raising=False)
    db_path = _temp_db_path()
    coordinator = Coordinator(db_path)
    try:
        with pytest.raises(RuntimeError, match="COORD_REPLICATION_TOKEN"):
            ClusterManager.from_env(coordinator)
    finally:
        coordinator.stop()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(db_path + suffix)
            except FileNotFoundError:
                pass


def test_insecure_replication_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("COORD_CLUSTER_ROLE", "leader")
    monkeypatch.delenv("COORD_REPLICATION_TOKEN", raising=False)
    monkeypatch.setenv("COORD_ALLOW_INSECURE_REPLICATION", "1")
    db_path = _temp_db_path()
    coordinator = Coordinator(db_path)
    try:
        manager = ClusterManager.from_env(coordinator)
        assert manager._allow_insecure_replication is True
    finally:
        coordinator.stop()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(db_path + suffix)
            except FileNotFoundError:
                pass


def test_replication_endpoint_auth_fails_closed_without_token():
    db_path = _temp_db_path()
    coordinator = Coordinator(db_path)
    manager = ClusterManager(coordinator, role="leader")
    old_manager = main.cluster_manager
    main.cluster_manager = manager
    request = Request({
        "type": "http",
        "method": "GET",
        "path": "/internal/replication/state",
    })
    try:
        with pytest.raises(HTTPException) as exc_info:
            main._require_replication_auth(request)
        assert exc_info.value.status_code == 503
    finally:
        main.cluster_manager = old_manager
        coordinator.stop()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(db_path + suffix)
            except FileNotFoundError:
                pass
