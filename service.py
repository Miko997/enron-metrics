import logging
from collections import Counter
from threading import Event, Lock, Thread

from ingestion import IngestionCancelled, iter_senders

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self, url: str, timeout: float = 10.0):
        self.url = url
        self.timeout = timeout
        self._lock = Lock()  # protects counts and job state
        self._stop_requested = Event()  # set during shutdown
        self._worker: Thread | None = None
        self._state = "idle"
        self._processed = 0  # all email files read
        self._skipped = 0  # files without one valid sender
        self._sender_counts: Counter[str] = Counter()  # messages per sender
        self._error: str | None = None

    def _snapshot(self) -> dict:
        """caller must hold the lock"""
        return {
            "state": self._state,
            "finished": self._state == "completed",
            "total_messages_processed": self._processed,
            "skipped_messages": self._skipped,
            "error": self._error,
        }

    def status(self) -> dict:
        with self._lock:
            return self._snapshot()

    def top_senders(self) -> dict:
        with self._lock:
            result = {"state": self._state, "finished": self._state == "completed"}
            counts = list(self._sender_counts.items())
        # sort outside the lock so ingestion can continue
        counts.sort(key=lambda item: (-item[1], item[0]))
        result["senders"] = [{"sender": sender, "count": count} for sender, count in counts[:10]]
        return result

    def start(self) -> tuple[dict, int]:
        # prevent two requests from starting separate workers
        with self._lock:
            if self._stop_requested.is_set():
                return {**self._snapshot(), "error": "Service is shutting down."}, 503
            if self._state == "completed":
                return self._snapshot(), 200
            if self._state == "running":
                return self._snapshot(), 202
            self._state = "running"
            self._processed = 0
            self._skipped = 0
            self._sender_counts.clear()  # clear partial results before retrying
            self._error = None
            self._worker = Thread(target=self._ingest, name="enron-ingestion", daemon=True)
            try:
                self._worker.start()
            except RuntimeError:
                self._worker = None
                self._state = "failed"
                self._error = "Could not start ingestion. See server logs."
                logger.exception("Could not start ingestion")
                return self._snapshot(), 503
            return self._snapshot(), 202

    def _ingest(self) -> None:
        error = None
        logger.info("Ingestion started")
        try:
            for sender in iter_senders(self.url, self.timeout, self._stop_requested):
                with self._lock:
                    self._processed += 1
                    if sender is None:
                        self._skipped += 1
                    else:
                        self._sender_counts[sender] += 1
        except IngestionCancelled:
            error = "Ingestion interrupted by shutdown."
        except Exception as exc:
            error = f"Ingestion failed ({type(exc).__name__}). See server logs."
            logger.exception("Ingestion failed")
        # mark completion after the stream has closed
        state = "completed" if error is None else "failed"
        with self._lock:
            self._state = state
            self._error = error
            processed, skipped = self._processed, self._skipped
        logger.info("Ingestion %s: processed=%d skipped=%d", state, processed, skipped)

    def close(self) -> None:
        with self._lock:
            self._stop_requested.set()
            worker = self._worker
        # release the lock before waiting for the worker
        if worker is not None:
            worker.join(timeout=self.timeout + 2)
            if worker.is_alive():
                logger.warning("Worker still blocked after shutdown grace period")
