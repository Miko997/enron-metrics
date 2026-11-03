import asyncio
import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from service import IngestionService

DEFAULT_URL = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"
url = os.getenv("ENRON_DATASET_URL", DEFAULT_URL)
timeout = float(os.getenv("ENRON_HTTP_TIMEOUT", "10"))  # socket timeout in seconds
source_url = urlsplit(url)
if source_url.scheme not in {"http", "https"} or not source_url.netloc:
    raise ValueError("ENRON_DATASET_URL must be an HTTP(S) URL")
if not 0 < timeout <= 60:
    raise ValueError("ENRON_HTTP_TIMEOUT must be between 0 (exclusive) and 60 seconds")
service = IngestionService(url, timeout)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        yield
    finally:
        await asyncio.to_thread(service.close)


app = FastAPI(title="Enron Metrics", version="1.0.0", lifespan=lifespan)


@app.post("/start", status_code=202)
def start():
    """start ingestion, or return the current run"""
    status, status_code = service.start()
    return JSONResponse(status, status_code=status_code)


@app.get("/status")
def status():
    """current progress and any error"""
    return service.status()


@app.get("/top-senders")
def top_senders():
    """top ten senders so far"""
    return service.top_senders()
