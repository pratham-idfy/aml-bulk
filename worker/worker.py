
import os
import redis
from rq import Worker, Queue, Connection

# Determine the correct path for the processing script
# This assumes the worker is run from the root of the bulk_job_app directory
from worker.processing_script import run_bulk_job

listen = ['high_priority']

# In a real deployment, this URL would come from an environment variable
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        # Create a worker that processes jobs on the specified queues
        worker = Worker(map(Queue, listen))
        print(f"Worker started. Listening on queues: {', '.join(listen)}")
        worker.work()
