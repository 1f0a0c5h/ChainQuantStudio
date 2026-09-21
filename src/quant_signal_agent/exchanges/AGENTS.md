# Exchange adapter rules

- Use documented unauthenticated public market-data endpoints only.
- Keep venue-specific symbols, timestamps, units, limits, sequence, checksum,
  restart, and reconnect behavior inside the adapter.
- Emit only normalized models and set `TimestampQuality` honestly.
- Never mark a book synchronized until the venue-specific snapshot/delta bridge passes.
- Invalidate on every detected gap; never reuse history across book generations.
- Pass every retry through the venue limiter and retry only transient failures.
- Add deterministic parser, ordering, gap, reconnect, and rate-limit tests.
- Do not add create/cancel order, withdrawal, transfer, auth signing, or private endpoints.
