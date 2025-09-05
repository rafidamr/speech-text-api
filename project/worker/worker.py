import asyncio
import logging
from arq import Worker
from tasks import process_item, REDIS_SETTINGS

logger = logging.Logger("worker")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
logger.addHandler(ch)

class WorkerSettings:
    functions = [process_item]
    redis_settings = REDIS_SETTINGS
    # optional: tune worker settings:
    # allow_abort_jobs = True
    # health_check_interval = 5
    # log_results = True

if __name__ == "__main__":
    # run the worker (blocking)
    w = Worker(functions=[process_item], redis_settings=REDIS_SETTINGS)
    # synchronous run (blocks until stopped)
    logger.info("[worker] Worker is starting")
    w.run()
