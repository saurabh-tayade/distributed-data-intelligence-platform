# 0001: RSS/Atom raw ingestion boundary

- Date: 2026-10-11
- Status: Accepted
- Implementation: Java 21 RSS/Atom retrieval contract and bounded owned buffering implemented and reviewed for M1 Day 2.

## Context and requirements

The repository has no existing Java framework, exception convention, or build tool. The issue requires Java 21 source definitions, raw HTTP retrieval, provenance, explicit failures, and deterministic offline tests. Article parsing and Bronze storage are out of scope.

## Decision and alternatives

Use a validated source record, one `FeedFetcher` interface, a Java standard HTTP client implementation, a raw byte response record, and one checked exception with explicit failure kinds. Keep HTTP error bodies available. Caller-supplied client, timeout, and clock avoid hidden lifecycle and environment settings.

Preserving bytes avoids the loss introduced by decoding to strings or parsing XML before Bronze. Returning all statuses as successful results was considered; checked HTTP failures make unsuccessful ingestion explicit while retaining evidence. Transport failures use the same exception contract and retain causes.

Use a JDK-only compilation/test script and loopback HTTP servers for this dependency-free slice. Maven/Gradle plus JUnit offer richer tooling and reporting, but introduce dependency bootstrapping before there are application dependencies. Reconsider when the Java surface grows. No storage abstraction is introduced ahead of a real storage contract.

## Resource safety refinement

Default to a 5 MiB payload limit, configurable as a positive integer through a constructor overload. A per-response bounded BodySubscriber counts received bytes before copying them into owned bounded storage, cancelling on overflow. A BodyHandler alone could inspect Content-Length but cannot enforce actual size for missing or misleading headers; it therefore selects the bounded subscriber instead. Checking after full accumulation would not bound memory. InputStream-based reading would require a separate read/close loop and different timeout lifecycle; retaining body completion within the existing synchronous send is simpler here.

The accumulator uses one lazy growable byte array, with an initial growth target of 1 KiB and geometric expansion capped at the configured limit. A single array avoids segment management and allocations per incoming fragment. Accepted readable bytes are copied immediately, including read-only and sliced buffers; incoming buffers and lists are never retained. The earlier standard byte-array subscriber delegate was removed because it can retain small slices backed by much larger upstream allocations. This ownership correction preserves the ingestion boundary and failure contract.

The subscriber requests one batch at a time after copying prior data, guards terminal completion, clears accumulator/subscription references on termination, and cancels on overflow. Success transfers an exact-sized array or trims it to the actual byte count. Regression tests inspect ownership and capacity deterministically, mutate upstream backing arrays, and cover fragmented input and lifecycle behavior without GC-dependent assertions.

The bound applies to delivered payload bytes after transfer framing, without content decompression. Overflow raises `PAYLOAD_TOO_LARGE` through a dedicated IOException cause, ahead of HTTP status classification, with no partial result. Non-2xx responses within the bound retain existing HTTP failure semantics. Conflicting Transfer-Encoding and Content-Length are rejected as malformed `TRANSPORT` responses, because the JDK can otherwise expose only the misleading declared length. Bytes outside a fixed-length HTTP message cannot be observed by the subscriber.

## Consequences and tradeoffs

The retrieval library remains independent of parsing and storage. Payloads are bounded per response and copied for ownership safety; each accumulator capacity is capped, but old/new arrays coexist during growth or trimming, and transport buffers, pending garbage collection, and defensive copies add overhead beyond the payload limit. Large-feed streaming, decompression limits, and aggregate concurrency budgets are deferred. A fresh ingestion UUID identifies each completed retrieval, not a deduplication key. No exactly-once or persistence guarantee is implied. Java serialization of exceptions is outside the contract.

## Validation and follow-up

The [contract documentation](../architecture/rss-ingestion.md) defines behavior, tests, and current limits. Run `bash scripts/test-ingestion.sh` and `npm run check`. Architecture accepted after owner review. Offline tests include boundary sizes, fixed/chunked/EOF-delimited delivery, misleading headers, cancellation, and failure precedence. Bronze persistence, replay identity, aggregate memory limits, and downstream article validation remain future work.
