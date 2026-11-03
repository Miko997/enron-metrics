# Elisa coding assignment

Run with Docker:

```bash
docker build -t enron-metrics .
docker run --rm --stop-timeout 30 -p 127.0.0.1:8000:8000 enron-metrics
```

In another terminal:

```bash
# download and process the dataset
curl -X POST http://localhost:8000/start

# check progress; repeat until finished is true
curl http://localhost:8000/status

# show the top ten senders
curl http://localhost:8000/top-senders
```

Results update during ingestion. If `state` is `failed`, check `error`.
Counts are kept in memory. Run one instance; restarting clears them.
Each email file counts once. Invalid senders are excluded from the ranking.

API in the browser: http://localhost:8000/docs
