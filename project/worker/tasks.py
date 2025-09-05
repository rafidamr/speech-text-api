import asyncio
from arq.connections import RedisSettings
from datetime import datetime

# Redis connection settings used by both FastAPI and the worker.
# Edit host/port/password as needed.
REDIS_SETTINGS = RedisSettings(host="localhost", port=6379, database=0)


# A simple job function. In arq, job coroutines accept a 'ctx' dict as first arg.
# We'll take a single payload dict argument.
async def process_item(ctx, payload: dict):
    """
    Example job:
      - payload: arbitrary dict (e.g. {"id": 123, "text": "hello"})
    """
    # ctx contains useful things, e.g. ctx["enqueue_time"] and ctx["job_id"]
    job_id = ctx.get("job_id")
    enqueue_time = ctx.get("enqueue_time")  # datetime
    started = datetime.utcnow()
    print(f"[worker] starting job {job_id} enqueued_at={enqueue_time} started_at={started}")
    # simulate work
    await asyncio.sleep(1)
    # pretend we processed payload
    result = {"job_id": job_id, "processed_at": started.isoformat(), "payload": payload}
    print(f"[worker] finished job {job_id} -> result: {result}")
    return result
