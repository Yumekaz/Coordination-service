#!/usr/bin/env python3
"""
Demo 4: Configuration Management

This demo shows how to use the coordination service for
centralized configuration management with live updates.

Pattern:
1. Store configuration as persistent nodes
2. Applications watch config nodes for changes
3. Updates are propagated to all watchers in real-time
4. Config survives restarts (persistent nodes)

Run:
    python demos/demo_config_management.py
"""

import sys
import time
import threading
import json
sys.path.insert(0, '.')

from coordinator import Coordinator
from models import EventType


class ConfigClient:
    """A configuration client that watches for config changes."""
    
    def __init__(self, coord: Coordinator, client_name: str):
        self.coord = coord
        self.client_name = client_name
        self.session = coord.open_session(timeout_seconds=30)
        self.session_id = self.session.session_id
        self.config = {}
        self._stop = False
    
    def load_config(self, config_path: str):
        """Load configuration from a path."""
        node = self.coord.get(config_path)
        if node:
            self.config = json.loads(node.data.decode())
            print(f"  [{self.client_name}] Loaded config: {self.config}")
        return self.config
    
    def watch_config(self, config_path: str, callback):
        """Watch a config path for changes."""
        
        def watcher():
            while not self._stop:
                # Register watch for updates
                watch = self.coord.register_watch(
                    config_path,
                    self.session_id,
                    event_types=[EventType.UPDATE]
                )
                
                # Wait for change
                event = self.coord.wait_watch(watch.watch_id, timeout_seconds=1.0)
                
                if event:
                    # Config changed - reload
                    new_config = self.load_config(config_path)
                    callback(self.client_name, new_config)
                
                # Heartbeat
                try:
                    self.coord.heartbeat(self.session_id)
                except:
                    break
        
        thread = threading.Thread(target=watcher, daemon=True)
        thread.start()
        return thread
    
    def stop(self):
        """Stop watching."""
        self._stop = True
        try:
            self.coord.close_session(self.session_id)
        except:
            pass


def on_config_change(client_name: str, new_config: dict):
    """Callback when configuration changes."""
    print(f"\n  [{client_name}] Config updated: {new_config}")


def main():
    print("=" * 60)
    print("DEMO 4: Configuration Management")
    print("=" * 60)
    print()
    
    # Use temp file instead of memory for thread safety
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Setup config namespace
        admin_session = coord.open_session()
        coord.create("/config", b"", persistent=True, session_id=admin_session.session_id)
        
        # Create initial configuration
        initial_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "pool_size": 10
            },
            "cache": {
                "enabled": True,
                "ttl_seconds": 300
            },
            "feature_flags": {
                "new_ui": False,
                "beta_features": False
            }
        }
        
        coord.create(
            "/config/app-settings",
            json.dumps(initial_config).encode(),
            persistent=True,
            session_id=admin_session.session_id
        )
        
        print("Created initial configuration:")
        print(json.dumps(initial_config, indent=2))
        print()
        
        # Start config clients
        print("Starting 3 application instances watching config...")
        clients = []
        
        for i in range(3):
            client = ConfigClient(coord, f"app-{i + 1}")
            client.load_config("/config/app-settings")
            client.watch_config("/config/app-settings", on_config_change)
            clients.append(client)
            time.sleep(0.2)
        
        print()
        time.sleep(1)
        
        # Update configuration
        print("\n" + "=" * 40)
        print("Admin: Updating configuration...")
        print("=" * 40)
        
        # Update 1: Enable new UI
        updated_config = initial_config.copy()
        updated_config["feature_flags"]["new_ui"] = True
        
        print(f"\nAdmin: Enabling new_ui feature flag...")
        coord.set("/config/app-settings", json.dumps(updated_config).encode())
        
        time.sleep(1.5)  # Allow watchers to react
        
        # Update 2: Change database pool size
        updated_config["database"]["pool_size"] = 20
        
        print(f"\nAdmin: Increasing database pool size to 20...")
        coord.set("/config/app-settings", json.dumps(updated_config).encode())
        
        time.sleep(1.5)  # Allow watchers to react
        
        # Update 3: Enable beta features
        updated_config["feature_flags"]["beta_features"] = True
        updated_config["cache"]["ttl_seconds"] = 600
        
        print(f"\nAdmin: Enabling beta features and increasing cache TTL...")
        coord.set("/config/app-settings", json.dumps(updated_config).encode())
        
        time.sleep(1.5)  # Allow watchers to react
        
        # Stop clients
        print()
        print("Stopping clients...")
        for client in clients:
            client.stop()
        
        time.sleep(0.5)
        
        # Show final config
        print()
        print("=" * 40)
        print("Final configuration state:")
        print("=" * 40)
        node = coord.get("/config/app-settings")
        final_config = json.loads(node.data.decode())
        print(json.dumps(final_config, indent=2))
        
    finally:
        coord.stop()
        import os
        os.unlink(db_path)
    
    print()
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
