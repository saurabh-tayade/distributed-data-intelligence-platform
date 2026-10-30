# 0017: Managed Content verification and normal successful finalization

Date: October 30, 2026. Status: **Implementation record; owner acceptance pending**.

D5a extends the internal D1–D4 Content checkpoints. Accepted ADRs 0006, 0007, 0010
and 0011 remain unchanged. ADRs 0015 and 0016 retain their actual approval status.
Production Content dispatch remains disabled.

Use the existing native `content_publication.publish` and independent Spark-free
`content_analytics.open_publication`. Do not implement enrichment in Java or create a
second Spark pipeline. The native Content fingerprint binds exact Gold input/completion
bytes, the captured reference snapshot fingerprint, dataset versions and existing semantic
configuration. Management run IDs, mutable source configuration and publication roots do
not replace those native identities.

The D5a seam starts from the current attempt's one committed D4 Gold result. It reopens the
original D4 private proof, D3 artifacts and D2 receipts, and the original submitted qualified
publisher-reference snapshot. A durable `CHECK_INPUTS` now qualifies these Content
dependencies after the D4 checkpoint. This is the existing documented stage name, used as
the Content publication preflight; this internal seam does not add a pre-fetch worker or
production dispatch. Its exact prepared manifest/fingerprint is anchored before any output
launch. Both dependencies are linked to this stage and `PUBLISH_CONTENT`.

Reuse D4's operational stage fence as `PublicationStage`, with distinct Content input and
completion checks. `CHECK_INPUTS`, `PUBLISH_CONTENT` and `VERIFY_RESULT` each commit
intent and consume a single-use launch authorization. Only the publisher launches Spark.
The independent verifier and final recheck reopen actual files and execute bounded named
Content analytics reports. Private evidence contains inventory, native completion, identities,
report hashes and scalar accounting; report rows do not become public management data.

Commit verified Content artifact registration, Gold/reference edges, `VERIFY_RESULT`
verification, result linkage, attempt/run `SUCCEEDED`, safe counts, audit evidence and
coordinator release in **one** fenced short transaction. Require the exact complete
`3 * fresh_source_count + 5` stage set (per-source fetch/store/Silver; Gold prepare/publish;
Content dependency check/publish/verify), all prior stages verified, consistent dependencies,
current ownership/versions/guard, confirmed child cleanup and no pending applicable control.
Spark and filesystem qualification occur outside PostgreSQL transactions.

An alternative separate artifact-registration transaction followed by terminal release creates
an unnecessary partial-completion boundary. A database artifact record or zero exit code
alone cannot qualify Content. Calling the Reliability recovery command is also inappropriate:
its supported pipeline and recovery proof differ. D5a exposes no Content recovery command.

Exact existing native output is verified read-only; incomplete output is retained without
repair. Any ambiguous launch, cleanup, stage acknowledgment or uncommitted terminal
outcome keeps the existing occupied state. A lost final commit acknowledgment can be
confirmed only by read-only committed terminal facts and full native verification. Confirmation
never adopts ownership, heartbeats, registers again, republishes or releases a subsequent run.
Duplicate finalization is fenced; it cannot mutate a terminal run.

No schema migration, native publication interface/schema, analytical selection, accepted ADR,
public API activation or production worker capability changes are required. The original Gold module bytes remain unchanged because retained D4 profiles pin their
hashes. The narrow Content adapter uses the same bounded private envelope locally, avoiding
a shared-helper edit that would invalidate those existing proofs. This small protocol duplication
is preferable to weakening runtime matching; callers cannot submit scripts or commands.

Managed Content Processing remains **Partially implemented**. D5b needs reviewed Content
operator reconciliation, retained-evidence/process review procedures, hosted acceptance,
worker orchestration/activation decisions and explicit operational readiness approval. D5a
does not implement those capabilities or automatic retries/cancellation.
