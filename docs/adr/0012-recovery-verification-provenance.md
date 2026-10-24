# ADR 0012: Recovery verification ownership and original provenance

Status: **Implementation record — scoped C3c.2 authorization; no new architecture acceptance claimed**

## Context

ADR 0010 already requires a fenced RECONCILE owner to run independent read-only verification.
The original Reliability command protocol deliberately requires its attempt/epoch to equal the
retained CHECK_INPUTS command. Substituting recovery identity would weaken that provenance check;
changing deployed native modules would invalidate the run's frozen deployment digest.

## Scoped implementation choices

- Reuse VERIFY_RESULT/item 0 under the RECONCILE attempt. Its attempt kind and distinct
  E5_RECOVERY_VERIFICATION_V1 policy distinguish inspection from an EXECUTE publication. No migration
  or public/native publication-format change is needed.
- Retain the unchanged original command/pack inside a private recovery envelope. The outer command
  binds the recovery run/control/attempt/instance/epoch/qualification and checkpoint. The inner
  VERIFY_RESULT command retains the original EXECUTE attempt/epoch/runtime and intended fingerprint.
- Package a versioned adapter in the API JAR, passed directly to the pinned interpreter with -c.
  Record its digest in the envelope. Explicitly allow only the known original native entry-point
  hashes. Invoke original runtime readiness and the full original independent inspect function;
  never invoke execute/publish or replace original deployment modules.
- Commit one stage INTENT and RECONCILE CLAIMED -> RUNNING first. A conditional INTENT -> RUNNING
  stage transition consumes launch permission once, before ProcessBuilder.start. Every mutation
  checks guard, installation, exact owner/run/control/attempt/epoch/versions and stage identity.
- Persist only verification-stage outcomes and private hashes/cleanup evidence. Keep the run in
  RECOVERY_REQUIRED, control REQUESTED and coordinator occupied in RECOVERY_ONLY. Historical stage
  inspection grants no token and never relaunches, including after uncertain acknowledgments.

Acceptance: actual JSONL/Parquet independent verification; distinct unavailable/changed/incomplete
classifications; fail-closed ownership, launch, cleanup and acknowledgment behavior; byte-identical
native snapshots; no final disposition, registry insertion, slot release or production activation.

## Alternatives and tradeoffs

Rewriting native attempt identity is incompatible with retained evidence. Extending original
modules would require separately approved replacement runtime profiles/deployments. A new stage
name/table adds migration and coordination work without improving this bounded operation. The
private adapter preserves original contracts at the cost of a deliberately narrow compatibility
allowlist. Unsupported deployments fail closed. Missing verified preflight/provenance cannot be
reconstructed by discovery. Broader operational recovery and architecture acceptance remain separate.
