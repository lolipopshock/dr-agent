"""Background job execution with persistence."""
import asyncio
import json
import sqlite3
import uuid
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Coroutine, Optional


class JobManager:
    def __init__(self, db_path: Optional[str] = None, cache_size: int = 100):
        self._jobs = OrderedDict()  # LRU cache
        self._tasks = {}
        self._subscribers = {}  # subscribers for each job
        self._cache_size = cache_size
        self._db_path = Path(db_path) if db_path else Path(".cache/jobs.db")
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._mark_orphaned_jobs()
        self._load_persisted_jobs()
    
    def _init_db(self):
        """Initialize SQLite database with single connection and WAL mode."""
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL") # write-ahead logging mode
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                result TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_updated_at ON jobs(updated_at DESC)")
    
    def _mark_orphaned_jobs(self):
        """Mark queued/running jobs as failed on startup."""
        self._conn.execute("""
            UPDATE jobs
            SET status = 'failed', error = 'server_restart', updated_at = ?
            WHERE status IN ('queued', 'running')
        """, (datetime.utcnow().isoformat(),))
    
    def _load_persisted_jobs(self):
        """Load persisted jobs from database into cache."""
        cursor = self._conn.execute("""
            SELECT job_id, status, result, error, created_at, updated_at
            FROM jobs
            WHERE status IN ('completed', 'failed', 'cancelled')
            ORDER BY updated_at DESC
            LIMIT ?
        """, (self._cache_size,))
        
        for row in cursor:
            job_id, status, result_json, error, created_at, updated_at = row
            self._jobs[job_id] = {
                "status": status,
                "result": json.loads(result_json) if result_json else None,
                "error": error,
                "created_at": created_at,
                "updated_at": updated_at,
            }
            self._jobs.move_to_end(job_id)
    
    def _serialize_result(self, result: Any) -> Optional[str]:
        """Serialize result to JSON for job persistence."""
        if result is None:
            return None
        
        # Convert Pydantic models to dict
        if hasattr(result, "model_dump"):
            result = result.model_dump()
        elif hasattr(result, "dict"):
            result = result.dict()
        
        try:
            return json.dumps(result, default=str)
        except (TypeError, ValueError):
            return json.dumps({"__error__": "Serialization failed", "__repr__": str(result)})
    
    def _persist_job(self, job_id: str, job: dict):
        """Persist job to SQLite using shared connection."""
        result_json = self._serialize_result(job.get("result"))
        self._conn.execute("""
            INSERT OR REPLACE INTO jobs (job_id, status, result, error, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            job["status"],
            result_json,
            job.get("error"),
            job["created_at"],
            job.get("updated_at", job["created_at"]),
        ))
    
    def _evict_lru(self):
        """Evict least recently used job from cache if over limit."""
        if len(self._jobs) >= self._cache_size:
            # Remove oldest (first) item
            self._jobs.popitem(last=False)

    async def submit(self, fn, **kwargs) -> str:
        job_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        job = {
            "status": "queued",
            "result": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
        }
        self._add_to_cache(job_id, job)
        self._persist_job(job_id, job)
        self._subscribers[job_id] = []  # initially no subscribers

        async def run():
            self._update_job(job_id, "running")
            await self._notify(job_id, "running")
            try:
                result = await fn(**kwargs)
                self._update_job(job_id, "completed", result=result)
                await self._notify(job_id, "completed", result=result)
            except asyncio.CancelledError:
                self._update_job(job_id, "cancelled")
                await self._notify(job_id, "cancelled")
                raise
            except Exception as e:
                self._update_job(job_id, "failed", error=str(e))
                await self._notify(job_id, "failed", error=str(e))
            finally:
                self._tasks.pop(job_id, None)

        self._tasks[job_id] = asyncio.create_task(run())
        return job_id
    
    def _add_to_cache(self, job_id: str, job: dict):
        """Add job to LRU cache."""
        self._evict_lru()
        self._jobs[job_id] = job
        # move to end
        self._jobs.move_to_end(job_id) # recent
    
    def _update_job(self, job_id: str, status: str, result: Any = None, error: str = None):
        """Update job status and persist on every state change."""
        if job_id not in self._jobs:
            return
        
        job = self._jobs[job_id]
        job["status"] = status
        job["updated_at"] = datetime.utcnow().isoformat()
        if result is not None:
            job["result"] = result
        if error is not None:
            job["error"] = error
        
        self._jobs.move_to_end(job_id)
        self._persist_job(job_id, job)

    async def cancel(self, job_id: str) -> bool:
        """Cancel a running job. Returns True if cancelled, else False."""
        task = self._tasks.get(job_id)
        if task and not task.done():
            task.cancel()
            return True
        return False

    async def _notify(self, job_id, status, **data):
        """Push status change to all subscribers."""
        for queue in self._subscribers.get(job_id, []):
            await queue.put({"status": status, **data})

    def subscribe(self, job_id):
        """Be added to subscribers list."""
        queue = asyncio.Queue()
        self._subscribers.setdefault(job_id, []).append(queue)
    
        job = self._jobs.get(job_id)
        if job and job["status"] != "queued": # push current status if job started
            queue.put_nowait({"status": job["status"], "result": job.get("result"), "error": job.get("error")})
        return queue

    def unsubscribe(self, job_id, queue):
        """Remove from subscribers list."""
        if job_id in self._subscribers:
            try:
                self._subscribers[job_id].remove(queue)
            except ValueError:
                pass

    def get(self, job_id: str) -> Optional[dict]:
        """Get job by ID. First check cache, then database."""
        # Check cache
        if job_id in self._jobs:
            self._jobs.move_to_end(job_id)  # update LRU
            return self._jobs[job_id]
        
        cursor = self._conn.execute("""
            SELECT status, result, error, created_at, updated_at
            FROM jobs
            WHERE job_id = ?
        """, (job_id,))
        row = cursor.fetchone()
        
        if row:
            status, result_json, error, created_at, updated_at = row
            job = {
                "status": status,
                "result": json.loads(result_json) if result_json else None,
                "error": error,
                "created_at": created_at,
                "updated_at": updated_at,
            }
            # Add to cache
            self._add_to_cache(job_id, job)
            return job
        
        return None
    
    def close(self):
        """Close database connection."""
        if hasattr(self, "_conn"):
            self._conn.close()

