#!/usr/bin/env python3
"""
Demo 2: Service Discovery

This demo shows how to use the coordination service for
dynamic service registration and discovery.

Pattern:
1. Services register as ephemeral nodes under /services/{service_name}
2. Each instance creates /services/{name}/{instance_id}
3. Clients watch the service path to get notified of changes
4. When a service dies, its ephemeral node disappears automatically

Run:
    python demos/demo_service_discovery.py
"""

import sys
import time
import threading
import json
import random
sys.path.insert(0, '.')

from coordinator import Coordinator


def register_service(coord: Coordinator, service_name: str, instance_id: str, 
                     host: str, port: int, duration: float):
    """Register a service instance."""
    
    session = coord.open_session(timeout_seconds=5)
    session_id = session.session_id
    
    service_info = json.dumps({
        "host": host,
        "port": port,
        "instance_id": instance_id,
        "status": "healthy"
    })
    
    path = f"/services/{service_name}/{instance_id}"
    
    try:
        coord.create(
            path,
            service_info.encode(),
            persistent=False,  # Ephemeral
            session_id=session_id
        )
        print(f"  [{instance_id}] Registered at {host}:{port}")
    except Exception as e:
        print(f"  [{instance_id}] Registration failed: {e}")
        return
    
    # Simulate service running
    start = time.time()
    while time.time() - start < duration:
        time.sleep(0.2)
        try:
            coord.heartbeat(session_id)
        except:
            break
    
    # Service dies (session closes)
    print(f"  [{instance_id}] Service stopping...")
    coord.close_session(session_id)


def discover_services(coord: Coordinator, service_name: str) -> list:
    """Discover all instances of a service."""
    
    path = f"/services/{service_name}"
    
    try:
        instances = coord.list_children(path)
        services = []
        
        for instance_id in instances:
            node = coord.get(f"{path}/{instance_id}")
            if node:
                info = json.loads(node.data.decode())
                info["instance_id"] = instance_id
                services.append(info)
        
        return services
    except:
        return []


def monitor_service(coord: Coordinator, service_name: str, duration: float):
    """Monitor a service for changes."""
    
    session = coord.open_session()
    session_id = session.session_id
    path = f"/services/{service_name}"
    
    print(f"\nMonitor: Watching {path}...")
    
    start = time.time()
    last_count = 0
    
    while time.time() - start < duration:
        services = discover_services(coord, service_name)
        
        if len(services) != last_count:
            print(f"\nMonitor: Service count changed: {last_count} -> {len(services)}")
            for svc in services:
                print(f"  - {svc['instance_id']}: {svc['host']}:{svc['port']}")
            last_count = len(services)
        
        time.sleep(0.3)
        try:
            coord.heartbeat(session_id)
        except:
            break
    
    coord.close_session(session_id)
    print("\nMonitor: Stopping")


def main():
    print("=" * 60)
    print("DEMO 2: Service Discovery")
    print("=" * 60)
    print()
    
    # Use temp file instead of memory for thread safety
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    coord = Coordinator(db_path=db_path)
    coord.start()
    
    try:
        # Setup service namespace
        session = coord.open_session()
        coord.create("/services", b"", persistent=True, session_id=session.session_id)
        coord.create("/services/api-server", b"", persistent=True, session_id=session.session_id)
        print("Created service namespace: /services/api-server")
        print()
        
        # Start monitor in background
        monitor = threading.Thread(
            target=monitor_service,
            args=(coord, "api-server", 8.0)
        )
        monitor.start()
        
        time.sleep(0.5)
        
        # Register service instances with varying lifetimes
        services = [
            ("instance-1", "192.168.1.10", 8080, 6.0),
            ("instance-2", "192.168.1.11", 8080, 4.0),
            ("instance-3", "192.168.1.12", 8080, 2.0),
        ]
        
        print("Starting service instances...")
        threads = []
        for instance_id, host, port, duration in services:
            t = threading.Thread(
                target=register_service,
                args=(coord, "api-server", instance_id, host, port, duration)
            )
            threads.append(t)
            t.start()
            time.sleep(0.3)
        
        # Wait for all
        for t in threads:
            t.join()
        
        monitor.join()
        
        # Final state
        print()
        print("Final service state:")
        services = discover_services(coord, "api-server")
        print(f"  Active instances: {len(services)}")
        
    finally:
        coord.stop()
        import os
        os.unlink(db_path)
    
    print()
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
