from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from arq.connections import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job
from typing import Any

from project.worker.tasks import REDIS_SETTINGS

app = FastAPI(title="FastAPI → arq example")

# Pydantic model for the payload that will be enqueued
class Item(BaseModel):
    id: int
    text: str


@app.on_event("startup")
async def startup():
    # create an arq redis pool and store it on app.state
    # create_pool returns an ArqRedis object with enqueue_job etc.
    app.state.arq_redis = await create_pool(REDIS_SETTINGS)


@app.on_event("shutdown")
async def shutdown():
    # close pool
    await app.state.arq_redis.close()


@app.post("/enqueue")
async def enqueue(item: Item):
    """
    Enqueue a job named 'process_item' (name is function name in tasks.py).
    Returns the job object id.
    """
    redis = app.state.arq_redis
    # enqueue_job returns an arq.jobs.Job instance (or None if duplicate _job_id)
    job = await redis.enqueue_job("process_item", item.dict())
    if job is None:
        raise HTTPException(status_code=409, detail="Job not enqueued (maybe duplicate _job_id)")
    return {"job_id": job.job_id}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str) -> Any:
    """
    Query job metadata and result (if available).
    """
    redis = app.state.arq_redis
    job = Job(job_id=job_id, redis=redis)

    info = await job.info()        # metadata + possibly result if finished
    if info is None:
        raise HTTPException(status_code=404, detail="Job not found")

    status = await job.status()    # queued/running/finished/failed
    # result_info() returns result metadata if present (does not wait)
    result_info = await job.result_info()

    return {"job_id": job_id, "status": status.name, "info": info, "result_info": result_info}
