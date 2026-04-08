"""
Persistence Layer for the Coordination Service.

Implements crash-safe persistence using SQLite with:
- Atomic transactions (BEGIN/COMMIT)
- Write-ahead logging (WAL mode)
- Custom WAL file format (Section 10 spec compliance)
- fsync for durability
- Operation log for recovery

WAL File Format (Section 10):
[timestamp][operation_type][path][data][session_id][sequence_number]

Invariants:
- All writes are atomic (all-or-nothing)
- Acknowledged operations survive crashes
- No partial state visible
"""

import sqlite3
import threading
import json
import os
import struct
from typing import Dict, Generator, List, Optional, Tuple
from datetime import datetime
from contextlib import contextmanager

from models import Node, Session, Operation, NodeType, OperationType, WatchFireRecord, EventType, encode_delete_operation_payload
from logger import get_logger
from config import DATABASE_PATH, WAL_MODE, FSYNC_ON_COMMIT

logger = get_logger("persistence")


class WALWriter:
    """
    Custom Write-Ahead Log file writer.
    
    Implements the WAL format used for crash recovery.
    The on-disk payload preserves the original operation semantics,
    including node type for CREATE operations.
    
    Each entry is written as:
    - 8 bytes: timestamp (double)
    - 4 bytes: operation_type length
    - N bytes: operation_type (UTF-8)
    - 4 bytes: path length
    - N bytes: path (UTF-8)
    - 4 bytes: data length
    - N bytes: data (bytes)
    - 4 bytes: session_id length (0 if None)
    - N bytes: session_id (UTF-8, if present)
    - 8 bytes: sequence_number (long)
    - 4 bytes: checksum (CRC32)
    """
    
    def __init__(self, wal_path: str):
        """Initialize WAL writer."""
        self._wal_path = wal_path
        self._lock = threading.Lock()
        self._file = None
        self._open()
    
    def _open(self) -> None:
        """Open or create the WAL file."""
        self._file = open(self._wal_path, 'ab')
        logger.info(f"WAL file opened: {self._wal_path}")
    
    def append(self, operation: Operation) -> None:
        """
        Append an operation to the WAL file.
        
        Format:
        [timestamp][operation_type][path][data][session_id][sequence_number][node_type?]
        """
        import zlib
        
        with self._lock:
            # Build the entry
            entry_parts = []
            
            # timestamp (8 bytes double)
            entry_parts.append(struct.pack('<d', operation.timestamp))
            
            # operation_type (length-prefixed string)
            op_type_bytes = operation.operation_type.value.encode('utf-8')
            entry_parts.append(struct.pack('<I', len(op_type_bytes)))
            entry_parts.append(op_type_bytes)
            
            # path (length-prefixed string)
            path_bytes = operation.path.encode('utf-8')
            entry_parts.append(struct.pack('<I', len(path_bytes)))
            entry_parts.append(path_bytes)
            
            # data (length-prefixed bytes)
            data_bytes = operation.data if operation.data else b''
            entry_parts.append(struct.pack('<I', len(data_bytes)))
            entry_parts.append(data_bytes)
            
            # session_id (length-prefixed string, 0 length if None)
            if operation.session_id:
                session_bytes = operation.session_id.encode('utf-8')
                entry_parts.append(struct.pack('<I', len(session_bytes)))
                entry_parts.append(session_bytes)
            else:
                entry_parts.append(struct.pack('<I', 0))

            # sequence_number (8 bytes long)
            entry_parts.append(struct.pack('<q', operation.sequence_number))

            # node_type (length-prefixed string, 0 length if None)
            if operation.node_type:
                node_type_bytes = operation.node_type.value.encode('utf-8')
                entry_parts.append(struct.pack('<I', len(node_type_bytes)))
                entry_parts.append(node_type_bytes)
            else:
                entry_parts.append(struct.pack('<I', 0))
            
            # Combine all parts
            entry_data = b''.join(entry_parts)
            
            # Add checksum (CRC32)
            checksum = zlib.crc32(entry_data) & 0xffffffff
            entry_parts.append(struct.pack('<I', checksum))
            
            # Write complete entry with length prefix
            full_entry = b''.join(entry_parts)
            entry_with_length = struct.pack('<I', len(full_entry)) + full_entry
            
            # Write and fsync
            self._file.write(entry_with_length)
            self._file.flush()
            os.fsync(self._file.fileno())
            
            logger.debug(f"WAL entry appended: seq={operation.sequence_number}")
    
    def read_all(self) -> List[Operation]:
        """
        Read all operations from the WAL file.
        
        Used during recovery to replay operations.
        """
        import zlib
        
        operations = []
        
        try:
            with open(self._wal_path, 'rb') as f:
                while True:
                    # Read entry length
                    length_bytes = f.read(4)
                    if not length_bytes or len(length_bytes) < 4:
                        break
                    
                    entry_length = struct.unpack('<I', length_bytes)[0]
                    entry_data = f.read(entry_length)
                    
                    if len(entry_data) < entry_length:
                        logger.warning("Incomplete WAL entry, stopping")
                        break
                    
                    # Parse entry
                    offset = 0
                    
                    # timestamp
                    timestamp = struct.unpack('<d', entry_data[offset:offset+8])[0]
                    offset += 8
                    
                    # operation_type
                    op_type_len = struct.unpack('<I', entry_data[offset:offset+4])[0]
                    offset += 4
                    op_type_str = entry_data[offset:offset+op_type_len].decode('utf-8')
                    offset += op_type_len
                    
                    # path
                    path_len = struct.unpack('<I', entry_data[offset:offset+4])[0]
                    offset += 4
                    path = entry_data[offset:offset+path_len].decode('utf-8')
                    offset += path_len
                    
                    # data
                    data_len = struct.unpack('<I', entry_data[offset:offset+4])[0]
                    offset += 4
                    data = entry_data[offset:offset+data_len]
                    offset += data_len
                    
                    # session_id
                    session_len = struct.unpack('<I', entry_data[offset:offset+4])[0]
                    offset += 4
                    session_id = None
                    if session_len > 0:
                        session_id = entry_data[offset:offset+session_len].decode('utf-8')
                        offset += session_len
                    
                    # sequence_number
                    sequence_number = struct.unpack('<q', entry_data[offset:offset+8])[0]
                    offset += 8

                    node_type = None
                    remaining_bytes = len(entry_data) - offset
                    if remaining_bytes > 4:
                        node_type_len = struct.unpack('<I', entry_data[offset:offset+4])[0]
                        offset += 4
                        if node_type_len > 0:
                            node_type_str = entry_data[offset:offset+node_type_len].decode('utf-8')
                            offset += node_type_len
                            node_type = NodeType(node_type_str)
                    
                    # Verify checksum
                    stored_checksum = struct.unpack('<I', entry_data[offset:offset+4])[0]
                    computed_checksum = zlib.crc32(entry_data[:offset]) & 0xffffffff
                    
                    if stored_checksum != computed_checksum:
                        logger.warning(f"WAL checksum mismatch at seq={sequence_number}, stopping")
                        break
                    
                    # Create operation
                    operation = Operation(
                        sequence_number=sequence_number,
                        operation_type=OperationType(op_type_str),
                        path=path,
                        data=data,
                        session_id=session_id,
                        timestamp=timestamp,
                        node_type=node_type,
                    )
                    operations.append(operation)
        
        except FileNotFoundError:
            logger.info("No WAL file found")
        except Exception as e:
            logger.error(f"Error reading WAL: {e}")
        
        logger.info(f"Read {len(operations)} operations from WAL")
        return operations
    
    def truncate(self) -> None:
        """Truncate the WAL file after successful checkpoint."""
        with self._lock:
            if self._file:
                self._file.close()
            self._file = open(self._wal_path, 'wb')
            self._file.close()
            self._file = open(self._wal_path, 'ab')
            logger.info("WAL file truncated")
    
    def close(self) -> None:
        """Close the WAL file."""
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None
        logger.info("WAL file closed")
    
    def get_path(self) -> str:
        """Get the WAL file path."""
        return self._wal_path


class Persistence:
    """
    Thread-safe SQLite persistence layer.
    
    Guarantees:
    - ACID transactions
    - Crash-safe writes (WAL + fsync)
    - Atomic operations
    - Thread-safe access
    
    Uses both SQLite WAL mode AND a custom WAL file format
    as specified in Section 10 of the spec.
    """
    
    def __init__(self, db_path: str = DATABASE_PATH):
        """Initialize the persistence layer."""
        self._db_path = db_path
        self._lock = threading.RLock()
        self._local = threading.local()
        self._connections: Dict[int, sqlite3.Connection] = {}
        self._closed = False
        
        # Custom WAL file (Section 10 format)
        wal_path = db_path.replace('.db', '.wal')
        if not wal_path.endswith('.wal'):
            wal_path = db_path + '.wal'
        self._wal_writer = WALWriter(wal_path)
        
        # Initialize database
        self._init_database()
        logger.info(f"Persistence initialized: {db_path}")

    def _create_connection(self) -> sqlite3.Connection:
        """Create and configure a new SQLite connection."""
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,  # Auto-commit mode
        )
        conn.row_factory = sqlite3.Row

        # Enable WAL mode for better concurrency
        if WAL_MODE:
            conn.execute("PRAGMA journal_mode=WAL")

        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys=ON")

        # Synchronous mode for durability
        if FSYNC_ON_COMMIT:
            conn.execute("PRAGMA synchronous=FULL")

        return conn

    def _register_connection(self, conn: sqlite3.Connection) -> sqlite3.Connection:
        """Track a connection so it can be closed deterministically."""
        thread_id = threading.get_ident()
        self._connections[thread_id] = conn
        self._local.connection = conn
        return conn

    def _unregister_connection(self, conn: sqlite3.Connection) -> None:
        """Remove a connection from the tracked connection table."""
        thread_id = threading.get_ident()
        if self._connections.get(thread_id) is conn:
            del self._connections[thread_id]
        if getattr(self._local, "connection", None) is conn:
            self._local.connection = None

    def _close_connection(self, conn: sqlite3.Connection) -> None:
        """Close a connection and ignore errors during shutdown."""
        try:
            conn.close()
        except Exception as e:
            logger.debug(f"Ignoring SQLite close error during shutdown: {e}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local database connection."""
        if self._closed:
            raise RuntimeError("Persistence layer is closed")

        connection = getattr(self._local, 'connection', None)
        if connection is None:
            with self._lock:
                if self._closed:
                    raise RuntimeError("Persistence layer is closed")
                connection = self._create_connection()
                self._register_connection(connection)

        return connection
    
    @contextmanager
    def _transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for atomic transactions."""
        conn = self._get_connection()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
            logger.debug("Transaction committed")
        except Exception as e:
            conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back: {e}")
            raise
    
    def _init_database(self) -> None:
        """Initialize the database schema."""
        with self._lock:
            conn = self._get_connection()
            
            # Nodes table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    path TEXT PRIMARY KEY,
                    data BLOB NOT NULL,
                    version INTEGER NOT NULL,
                    node_type TEXT NOT NULL,
                    session_id TEXT,
                    created_at REAL NOT NULL,
                    modified_at REAL NOT NULL
                )
            """)
            
            # Sessions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    timeout_seconds INTEGER NOT NULL,
                    ephemeral_nodes TEXT NOT NULL,
                    is_alive INTEGER NOT NULL
                )
            """)
            
            # Operations table (WAL)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS operations (
                    sequence_number INTEGER PRIMARY KEY,
                    operation_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    data BLOB,
                    session_id TEXT,
                    timestamp REAL NOT NULL,
                    node_type TEXT
                )
            """)
            
            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_nodes_session 
                ON nodes(session_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_operations_timestamp 
                ON operations(timestamp)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watch_fires (
                    cause_sequence_number INTEGER NOT NULL,
                    ordinal INTEGER NOT NULL,
                    watch_id TEXT NOT NULL,
                    watch_session_id TEXT NOT NULL,
                    watch_path TEXT NOT NULL,
                    observed_path TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    PRIMARY KEY (cause_sequence_number, ordinal)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_watch_fires_observed_path
                ON watch_fires(observed_path, cause_sequence_number DESC, ordinal ASC)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cluster_state (
                    node_id TEXT PRIMARY KEY,
                    current_term INTEGER NOT NULL,
                    voted_for TEXT,
                    leader_id TEXT,
                    leader_url TEXT,
                    updated_at REAL NOT NULL
                )
            """)
            
            # Note: DDL statements are auto-committed in isolation_level=None mode
            logger.info("Database schema initialized")
    
    # ========== Node Operations ==========
    
    def save_node(self, node: Node) -> None:
        """
        Save or update a node atomically.
        
        Uses INSERT OR REPLACE for upsert semantics.
        """
        with self._lock:
            with self._transaction() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO nodes 
                    (path, data, version, node_type, session_id, created_at, modified_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    node.path,
                    node.data,
                    node.version,
                    node.node_type.value,
                    node.session_id,
                    node.created_at,
                    node.modified_at,
                ))
                logger.debug(f"Saved node: {node.path}")
    
    def delete_node(self, path: str) -> bool:
        """
        Delete a node atomically.
        
        Returns True if node was deleted.
        """
        with self._lock:
            with self._transaction() as conn:
                cursor = conn.execute(
                    "DELETE FROM nodes WHERE path = ?",
                    (path,)
                )
                deleted = cursor.rowcount > 0
                if deleted:
                    logger.debug(f"Deleted node: {path}")
                return deleted
    
    def delete_nodes(self, paths: List[str]) -> int:
        """
        Delete multiple nodes atomically.
        
        Returns count of deleted nodes.
        """
        if not paths:
            return 0
        
        with self._lock:
            with self._transaction() as conn:
                placeholders = ",".join("?" * len(paths))
                cursor = conn.execute(
                    f"DELETE FROM nodes WHERE path IN ({placeholders})",
                    paths
                )
                count = cursor.rowcount
                logger.debug(f"Deleted {count} nodes")
                return count
    
    def load_node(self, path: str) -> Optional[Node]:
        """Load a node from the database."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                "SELECT * FROM nodes WHERE path = ?",
                (path,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            return Node(
                path=row["path"],
                data=row["data"],
                version=row["version"],
                node_type=NodeType(row["node_type"]),
                session_id=row["session_id"],
                created_at=row["created_at"],
                modified_at=row["modified_at"],
            )
    
    def load_all_nodes(self) -> List[Node]:
        """Load all nodes from the database."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute("SELECT * FROM nodes ORDER BY path")
            
            nodes = []
            for row in cursor:
                nodes.append(Node(
                    path=row["path"],
                    data=row["data"],
                    version=row["version"],
                    node_type=NodeType(row["node_type"]),
                    session_id=row["session_id"],
                    created_at=row["created_at"],
                    modified_at=row["modified_at"],
                ))
            
            return nodes
    
    def delete_nodes_by_session(self, session_id: str) -> List[str]:
        """
        Delete all nodes owned by a session atomically.
        
        Returns list of deleted paths.
        """
        with self._lock:
            conn = self._get_connection()
            
            # First get the paths
            cursor = conn.execute(
                "SELECT path FROM nodes WHERE session_id = ?",
                (session_id,)
            )
            paths = [row["path"] for row in cursor]
            
            if paths:
                with self._transaction() as conn:
                    conn.execute(
                        "DELETE FROM nodes WHERE session_id = ?",
                        (session_id,)
                    )
                    logger.debug(f"Deleted {len(paths)} ephemeral nodes for session {session_id}")
            
            return paths
    
    # ========== Session Operations ==========
    
    def save_session(self, session: Session) -> None:
        """Save or update a session atomically."""
        with self._lock:
            with self._transaction() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO sessions
                    (session_id, created_at, last_heartbeat, timeout_seconds, ephemeral_nodes, is_alive)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    session.session_id,
                    session.created_at,
                    session.last_heartbeat,
                    session.timeout_seconds,
                    json.dumps(list(session.ephemeral_nodes)),
                    1 if session.is_alive else 0,
                ))
                logger.debug(f"Saved session: {session.session_id}")

    def atomic_save_session(
        self,
        session: Session,
        operation: Optional[Operation] = None,
        extra_operations: Optional[List[Operation]] = None,
    ) -> None:
        """Atomically persist a session update and an optional operation log entry."""
        with self._lock:
            with self._transaction() as conn:
                if operation is not None:
                    self._insert_operation(conn, operation)
                for extra_operation in extra_operations or []:
                    self._insert_operation(conn, extra_operation)

                conn.execute("""
                    INSERT OR REPLACE INTO sessions
                    (session_id, created_at, last_heartbeat, timeout_seconds, ephemeral_nodes, is_alive)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    session.session_id,
                    session.created_at,
                    session.last_heartbeat,
                    session.timeout_seconds,
                    json.dumps(list(session.ephemeral_nodes)),
                    1 if session.is_alive else 0,
                ))
                logger.debug(f"Atomically saved session: {session.session_id}")
    
    def delete_session(self, session_id: str) -> bool:
        """Delete a session atomically."""
        with self._lock:
            with self._transaction() as conn:
                cursor = conn.execute(
                    "DELETE FROM sessions WHERE session_id = ?",
                    (session_id,)
                )
                return cursor.rowcount > 0
    
    def load_session(self, session_id: str) -> Optional[Session]:
        """Load a session from the database."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?",
                (session_id,)
            )
            row = cursor.fetchone()
            
            if row is None:
                return None
            
            session = Session(
                session_id=row["session_id"],
                created_at=row["created_at"],
                last_heartbeat=row["last_heartbeat"],
                timeout_seconds=row["timeout_seconds"],
                is_alive=bool(row["is_alive"]),
            )
            session.ephemeral_nodes = set(json.loads(row["ephemeral_nodes"]))
            
            return session
    
    def load_all_sessions(self) -> List[Session]:
        """Load all sessions from the database."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute("SELECT * FROM sessions")
            
            sessions = []
            for row in cursor:
                session = Session(
                    session_id=row["session_id"],
                    created_at=row["created_at"],
                    last_heartbeat=row["last_heartbeat"],
                    timeout_seconds=row["timeout_seconds"],
                    is_alive=bool(row["is_alive"]),
                )
                session.ephemeral_nodes = set(json.loads(row["ephemeral_nodes"]))
                sessions.append(session)
            
            return sessions
    
    def mark_all_sessions_dead(self) -> int:
        """
        Mark all sessions as dead.
        
        Called during crash recovery.
        Returns count of affected sessions.
        """
        with self._lock:
            with self._transaction() as conn:
                cursor = conn.execute(
                    "UPDATE sessions SET is_alive = 0"
                )
                count = cursor.rowcount
                logger.info(f"Marked {count} sessions as dead")
                return count
    
    # ========== Operation Log ==========
    
    def append_operation(self, operation: Operation) -> None:
        """Append an operation to the log atomically."""
        with self._lock:
            with self._transaction() as conn:
                conn.execute("""
                    INSERT INTO operations
                    (sequence_number, operation_type, path, data, session_id, timestamp, node_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation.sequence_number,
                    operation.operation_type.value,
                    operation.path,
                    operation.data,
                    operation.session_id,
                    operation.timestamp,
                    operation.node_type.value if operation.node_type else None,
                ))
                logger.debug(f"Appended operation: seq={operation.sequence_number}")

    def save_cluster_state(
        self,
        node_id: str,
        current_term: int,
        voted_for: Optional[str] = None,
        leader_id: Optional[str] = None,
        leader_url: Optional[str] = None,
    ) -> None:
        """Persist cluster election metadata for one local node."""
        with self._lock:
            with self._transaction() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO cluster_state
                    (node_id, current_term, voted_for, leader_id, leader_url, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    node_id,
                    int(current_term),
                    voted_for,
                    leader_id,
                    leader_url,
                    datetime.now().timestamp(),
                ))
                logger.debug(
                    "Saved cluster state: node=%s term=%s voted_for=%s leader=%s",
                    node_id,
                    current_term,
                    voted_for,
                    leader_id,
                )

    def load_cluster_state(self, node_id: str) -> Optional[Dict[str, object]]:
        """Load persisted cluster election metadata for one local node."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                """
                SELECT node_id, current_term, voted_for, leader_id, leader_url, updated_at
                FROM cluster_state
                WHERE node_id = ?
                """,
                (node_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None

            return {
                "node_id": row["node_id"],
                "current_term": int(row["current_term"]),
                "voted_for": row["voted_for"],
                "leader_id": row["leader_id"],
                "leader_url": row["leader_url"],
                "updated_at": row["updated_at"],
            }
    
    def load_operations_since(self, sequence_number: int) -> List[Operation]:
        """Load operations after a given sequence number."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                "SELECT * FROM operations WHERE sequence_number > ? ORDER BY sequence_number",
                (sequence_number,)
            )
            
            operations = []
            for row in cursor:
                operations.append(Operation(
                    sequence_number=row["sequence_number"],
                    operation_type=OperationType(row["operation_type"]),
                    path=row["path"],
                    data=row["data"] or b"",
                    session_id=row["session_id"],
                    timestamp=row["timestamp"],
                    node_type=NodeType(row["node_type"]) if row["node_type"] else None,
                ))
            
            return operations
    
    def load_all_operations(self) -> List[Operation]:
        """Load all operations from the log."""
        return self.load_operations_since(0)
    
    def get_last_sequence_number(self) -> int:
        """Get the last sequence number in the operation log."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(
                "SELECT MAX(sequence_number) as max_seq FROM operations"
            )
            row = cursor.fetchone()
            return row["max_seq"] if row["max_seq"] is not None else 0
    
    def truncate_operations_before(self, sequence_number: int) -> int:
        """
        Remove operations before a sequence number (log compaction).
        
        Returns count of removed operations.
        """
        with self._lock:
            with self._transaction() as conn:
                cursor = conn.execute(
                    "DELETE FROM operations WHERE sequence_number < ?",
                    (sequence_number,)
                )
                count = cursor.rowcount
                logger.info(f"Truncated {count} operations before seq={sequence_number}")
                return count

    def _append_wal_after_commit(self, operation: Operation) -> None:
        """
        Best-effort custom WAL append after the SQLite commit succeeds.

        SQLite plus the operations table are the canonical committed state.
        The custom WAL is supplemental recovery evidence and must never cause
        an uncommitted operation to reappear after restart.
        """
        try:
            self._wal_writer.append(operation)
        except Exception as e:
            logger.error(f"Custom WAL append failed after commit for seq={operation.sequence_number}: {e}")

    def _insert_operation(self, conn: sqlite3.Connection, operation: Operation) -> None:
        """Insert one committed operation row."""
        conn.execute("""
            INSERT INTO operations
            (sequence_number, operation_type, path, data, session_id, timestamp, node_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            operation.sequence_number,
            operation.operation_type.value,
            operation.path,
            operation.data,
            operation.session_id,
            operation.timestamp,
            operation.node_type.value if operation.node_type else None,
        ))

    def _insert_watch_fires(
        self,
        conn: sqlite3.Connection,
        watch_fires: Optional[List[WatchFireRecord]],
    ) -> None:
        """Insert persisted watch-fire records for a committed operation."""
        if not watch_fires:
            return

        conn.executemany("""
            INSERT OR REPLACE INTO watch_fires
            (cause_sequence_number, ordinal, watch_id, watch_session_id, watch_path, observed_path, event_type, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                record.cause_sequence_number,
                record.ordinal,
                record.watch_id,
                record.watch_session_id,
                record.watch_path,
                record.observed_path,
                record.event_type.value,
                record.timestamp,
            )
            for record in watch_fires
        ])
    
    # ========== Atomic Multi-Operation ==========
    
    def atomic_create_node(
        self,
        node: Node,
        operation: Operation,
        session: Optional[Session] = None,
        watch_fires: Optional[List[WatchFireRecord]] = None,
    ) -> None:
        """
        Atomically create a node and log the operation.
        
        This is the primary write path ensuring durability.
        Writes to both SQLite and custom WAL file (Section 10).
        """
        with self._lock:
            with self._transaction() as conn:
                self._insert_operation(conn, operation)
                
                # Create node
                conn.execute("""
                    INSERT INTO nodes 
                    (path, data, version, node_type, session_id, created_at, modified_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    node.path,
                    node.data,
                    node.version,
                    node.node_type.value,
                    node.session_id,
                    node.created_at,
                    node.modified_at,
                ))
                
                # Update session if ephemeral
                if session:
                    conn.execute("""
                        UPDATE sessions
                        SET created_at = ?, last_heartbeat = ?, timeout_seconds = ?, ephemeral_nodes = ?, is_alive = ?
                        WHERE session_id = ?
                    """, (
                        session.created_at,
                        session.last_heartbeat,
                        session.timeout_seconds,
                        json.dumps(list(session.ephemeral_nodes)),
                        1 if session.is_alive else 0,
                        session.session_id,
                    ))

                self._insert_watch_fires(conn, watch_fires)
                
                logger.debug(f"Atomic create: {node.path}")

            self._append_wal_after_commit(operation)
    
    def atomic_update_node(
        self,
        node: Node,
        operation: Operation,
        watch_fires: Optional[List[WatchFireRecord]] = None,
    ) -> None:
        """Atomically update a node and log the operation."""
        with self._lock:
            with self._transaction() as conn:
                self._insert_operation(conn, operation)
                
                # Update node
                conn.execute("""
                    UPDATE nodes SET data = ?, version = ?, modified_at = ?
                    WHERE path = ?
                """, (
                    node.data,
                    node.version,
                    node.modified_at,
                    node.path,
                ))

                self._insert_watch_fires(conn, watch_fires)
                
                logger.debug(f"Atomic update: {node.path}")

            self._append_wal_after_commit(operation)
    
    def atomic_delete_node(
        self,
        paths: List[str],
        operation: Operation,
        sessions: Optional[List[Session]] = None,
        watch_fires: Optional[List[WatchFireRecord]] = None,
        extra_operations: Optional[List[Operation]] = None,
    ) -> None:
        """Atomically delete nodes and log the operation."""
        with self._lock:
            if not operation.data:
                operation.data = encode_delete_operation_payload(paths)
            
            with self._transaction() as conn:
                for extra_operation in extra_operations or []:
                    self._insert_operation(conn, extra_operation)
                self._insert_operation(conn, operation)
                
                # Delete nodes
                if paths:
                    placeholders = ",".join("?" * len(paths))
                    conn.execute(
                        f"DELETE FROM nodes WHERE path IN ({placeholders})",
                        paths
                    )
                
                # Update any affected session records.
                for session in sessions or []:
                    conn.execute("""
                        UPDATE sessions
                        SET created_at = ?, last_heartbeat = ?, timeout_seconds = ?, ephemeral_nodes = ?, is_alive = ?
                        WHERE session_id = ?
                    """, (
                        session.created_at,
                        session.last_heartbeat,
                        session.timeout_seconds,
                        json.dumps(list(session.ephemeral_nodes)),
                        1 if session.is_alive else 0,
                        session.session_id,
                    ))

                self._insert_watch_fires(conn, watch_fires)
                
                logger.debug(f"Atomic delete: {paths}")

            self._append_wal_after_commit(operation)
    
    # ========== Utility ==========
    
    def clear(self) -> None:
        """Clear all data (used for testing)."""
        with self._lock:
            with self._transaction() as conn:
                conn.execute("DELETE FROM nodes")
                conn.execute("DELETE FROM sessions")
                conn.execute("DELETE FROM operations")
                conn.execute("DELETE FROM watch_fires")
                conn.execute("DELETE FROM cluster_state")
            # Also truncate WAL file
            self._wal_writer.truncate()
            logger.info("Database cleared")
    
    def close(self) -> None:
        """Close the database connection and WAL file."""
        # Close WAL writer
        self._wal_writer.close()

        with self._lock:
            if self._closed:
                return

            connections = list(self._connections.values())
            self._connections.clear()
            self._closed = True

        for connection in connections:
            self._close_connection(connection)

        if hasattr(self._local, 'connection'):
            self._local.connection = None

        logger.info("Database connection closed")
    
    def read_wal(self) -> List[Operation]:
        """
        Read all operations from the custom WAL file.
        
        Per Section 10 & 12: On recovery, WAL entries are read
        and replayed in order to rebuild state.
        
        Returns:
            List of operations from WAL file
        """
        return self._wal_writer.read_all()
    
    def get_wal_operations(self) -> List[Operation]:
        """
        Read all operations from the custom WAL file.
        
        Used during recovery to verify/replay operations.
        """
        return self._wal_writer.read_all()
    
    def get_wal_path(self) -> str:
        """Get the path to the custom WAL file."""
        return self._wal_writer.get_path()
    
    def truncate_wal(self) -> None:
        """
        Truncate the custom WAL file after checkpoint.
        
        Per Section 12: After successful recovery or snapshot,
        the WAL can be truncated as all operations are now
        persisted in the database.
        """
        self._wal_writer.truncate()
    
    def get_operation(self, sequence_number: int) -> Optional[Operation]:
        """
        Get an operation by sequence number.
        
        Used during recovery to check if an operation has
        already been applied (idempotent replay).
        
        Args:
            sequence_number: The sequence number to look up
            
        Returns:
            Operation if found, None otherwise
        """
        with self._lock:
            conn = self._get_connection()
            row = conn.execute(
                "SELECT * FROM operations WHERE sequence_number = ?",
                (sequence_number,)
            ).fetchone()
            
            if row:
                return Operation(
                    sequence_number=row[0],
                    operation_type=OperationType(row[1]),
                    path=row[2],
                    data=row[3] if row[3] else b'',
                    session_id=row[4],
                    timestamp=row[5],
                    node_type=NodeType(row[6]) if row[6] else None,
                )
            return None

    def load_watch_fires_for_path(
        self,
        path: str,
        limit: int = 20,
        cause_sequence_number: Optional[int] = None,
    ) -> List[WatchFireRecord]:
        """Load persisted watch-fire records for an observed path."""
        with self._lock:
            conn = self._get_connection()
            query = """
                SELECT cause_sequence_number, ordinal, watch_id, watch_session_id, watch_path, observed_path, event_type, timestamp
                FROM watch_fires
                WHERE observed_path = ?
            """
            params: List[object] = [path]
            if cause_sequence_number is not None:
                query += " AND cause_sequence_number = ?"
                params.append(cause_sequence_number)
            query += " ORDER BY cause_sequence_number DESC, ordinal ASC"
            if limit > 0:
                query += " LIMIT ?"
                params.append(limit)

            cursor = conn.execute(query, params)
            return [
                WatchFireRecord(
                    cause_sequence_number=row["cause_sequence_number"],
                    ordinal=row["ordinal"],
                    watch_id=row["watch_id"],
                    watch_session_id=row["watch_session_id"],
                    watch_path=row["watch_path"],
                    observed_path=row["observed_path"],
                    event_type=EventType(row["event_type"]),
                    timestamp=row["timestamp"],
                )
                for row in cursor
            ]

    def load_watch_fires_for_session(
        self,
        session_id: str,
        limit: int = 20,
    ) -> List[WatchFireRecord]:
        """Load persisted watch-fire records for a watcher session."""
        with self._lock:
            conn = self._get_connection()
            query = """
                SELECT cause_sequence_number, ordinal, watch_id, watch_session_id, watch_path, observed_path, event_type, timestamp
                FROM watch_fires
                WHERE watch_session_id = ?
                ORDER BY cause_sequence_number DESC, ordinal ASC
            """
            params: List[object] = [session_id]
            if limit > 0:
                query += " LIMIT ?"
                params.append(limit)

            cursor = conn.execute(query, params)
            return [
                WatchFireRecord(
                    cause_sequence_number=row["cause_sequence_number"],
                    ordinal=row["ordinal"],
                    watch_id=row["watch_id"],
                    watch_session_id=row["watch_session_id"],
                    watch_path=row["watch_path"],
                    observed_path=row["observed_path"],
                    event_type=EventType(row["event_type"]),
                    timestamp=row["timestamp"],
                )
                for row in cursor
            ]

    def load_watch_fires_for_operation(
        self,
        cause_sequence_number: int,
        limit: int = 50,
    ) -> List[WatchFireRecord]:
        """Load persisted watch-fire records caused by one committed operation."""
        with self._lock:
            conn = self._get_connection()
            query = """
                SELECT cause_sequence_number, ordinal, watch_id, watch_session_id, watch_path, observed_path, event_type, timestamp
                FROM watch_fires
                WHERE cause_sequence_number = ?
                ORDER BY ordinal ASC
            """
            params: List[object] = [cause_sequence_number]
            if limit > 0:
                query += " LIMIT ?"
                params.append(limit)

            cursor = conn.execute(query, params)
            return [
                WatchFireRecord(
                    cause_sequence_number=row["cause_sequence_number"],
                    ordinal=row["ordinal"],
                    watch_id=row["watch_id"],
                    watch_session_id=row["watch_session_id"],
                    watch_path=row["watch_path"],
                    observed_path=row["observed_path"],
                    event_type=EventType(row["event_type"]),
                    timestamp=row["timestamp"],
                )
                for row in cursor
            ]
    
    def save_operation(self, operation: Operation) -> None:
        """
        Save an operation to the database.
        
        Used during WAL replay to persist operations.
        
        Args:
            operation: The operation to save
        """
        with self._lock:
            with self._transaction() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO operations
                    (sequence_number, operation_type, path, data, session_id, timestamp, node_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation.sequence_number,
                    operation.operation_type.value,
                    operation.path,
                    operation.data,
                    operation.session_id,
                    operation.timestamp,
                    operation.node_type.value if operation.node_type else None,
                ))
    
    def checkpoint(self) -> None:
        """
        Force a checkpoint - ensure all data is written to database.
        
        Per Section 12: Checkpoints create a consistent baseline
        state. After checkpoint, WAL can be truncated.
        """
        with self._lock:
            conn = self._get_connection()
            # Force WAL checkpoint in SQLite
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            logger.info("Database checkpoint completed")
    
    def get_stats(self) -> dict:
        """Get database statistics."""
        with self._lock:
            conn = self._get_connection()
            
            node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            session_count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
            operation_count = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
            
            return {
                "node_count": node_count,
                "session_count": session_count,
                "operation_count": operation_count,
                "db_path": self._db_path,
            }
