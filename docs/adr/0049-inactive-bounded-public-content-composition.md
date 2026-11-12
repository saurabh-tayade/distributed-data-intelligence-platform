# ADR 0049: Inactive bounded public FETCH and immutable Content composition v2

Date: November 12, 2026

Status: Implementation scope authorized by the owner in
[#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6831203628).
Proposed acceptance pending exact-head review. No merge, deployment or activation authority.

## Context

Strict FETCH launch v1 means literal loopback with testEndpoint=true. Public source
configuration cannot safely reuse that identity. Closed/unmerged PR #137 supplies candidate
implementation evidence, including broader worker activation that is excluded here.
ADR 0046 on current main concerns Reliability cancellation and is unchanged.

## Decision for this checkpoint

Prepare a separate public child and resolver-provider JAR using the JDK 21 resolver SPI.
Resolve the original DNS hostname through the native resolver once, requesting IPv4.
Classify every answer (at most 64, before deduplication), reject the whole set on any
forbidden result, sort valid unique bytes and seal exactly one address. Resolver callbacks
then return only that address for the original hostname; all other/reverse callbacks reject.
No alternate address, proxy, redirect, authentication fallback or application refetch exists.

The exact conservative IANA deny snapshot is
[0049-public-fetch-ipv4-deny-snapshot.json](../architecture/0049-public-fetch-ipv4-deny-snapshot.json),
SHA-256 `85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd`.
Its 27 CIDRs retain special-purpose exceptions and multicast as denied. No live registry
lookup changes policy. The public URI is unchanged for Host/SNI and default TLS validation;
body is at most 5 MiB, connect timeout 10 seconds, request timeout at most 30 seconds,
and the existing child supervision deadline is 45 seconds. JVM internal reconnects may
occur within these bounds and only to the pin; this is not exactly one wire request.

Launch, reservation and bootstrap v2 bind the exact network policy and frozen endpoint.
Bronze launch/verification v2 distinguish the public source from literal-loopback storage.
Composition v2 binds all three launch/verification IDs and hashes. Additive V23 extends
registration/correspondence guards without rewriting V1–V22, old rows or receipts.
Historical reconstruction is read-only and creates no live owner/session or release authority.
New legacy FETCH-only recovery handoffs remain v1-only in Java and SQL.

Ordinary worker capability, lifecycle and production dispatch remain unchanged. The existing
internal 17-operation original pipeline may validate these identities, without any new
ordinary caller, retry, continuation, recovery reauthorization or publication promotion.

## Alternatives and limits

Preflight DNS followed by an unpinned HTTP client permits rebinding. Rewriting an HTTPS URI
to an IP loses the normal TLS identity path. A custom HTTP/TLS parser duplicates mature
framing and validation; a new transport dependency requires separate review. The JDK SPI
keeps existing HTTP handling but requires exact provider packaging and fresh JVM tests.

This is a cooperative application boundary on a trusted installed runtime, not an OS egress
sandbox. Native resolution may use host naming configuration; one delegate call can involve
multiple DNS packets. Numeric literals and localhost have JDK shortcuts, so public URI
validation is essential. IPv6-only sources fail closed. An uncertain process exit retains
occupancy. Installed runtime/host routing integrity and real public DNS/TLS egress remain
separate acceptance gates; offline loopback transport tests cannot establish them.

## Validation and follow-up

See the [implementation/evidence record](../operations/e5-2a-inactive-public-content.md).
E5.2-B should propose separately authorized packaged public-path qualification and its exact
immutable synthetic source, runtime and evidence gates. Dispatch/activation requires a further
owner decision; no passing offline or hosted test implicitly makes that decision.
