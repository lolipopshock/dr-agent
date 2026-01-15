"""Background job execution."""
import asyncio
import uuid
from datetime import datetime
from typing import Any, Callable, Coroutine


class JobManager:
    def __init__(self):
        self._jobs = {}
        self._tasks = {}
        self._subscribers = {} # subscribers for each job

    async def submit(self, fn, **kwargs) -> str:
        job_id = str(uuid.uuid4())
        self._jobs[job_id] = {
            "status": "queued",
            "result": None,
            "error": None,
            "created_at": datetime.utcnow().isoformat(),
        }
        self._subscribers[job_id] = [] # initially no subscribers

        async def run():
            self._jobs[job_id]["status"] = "running"
            await self._notify(job_id, "running")
            try:
                result = await fn(**kwargs)
                self._jobs[job_id].update(status="completed", result=result)
                await self._notify(job_id, "completed", result=result)
            except Exception as e:
                self._jobs[job_id].update(status="failed", error=str(e))
                await self._notify(job_id, "failed", error=str(e))

        self._tasks[job_id] = asyncio.create_task(run())
        return job_id

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

    def get(self, job_id):
        return self._jobs.get(job_id)

