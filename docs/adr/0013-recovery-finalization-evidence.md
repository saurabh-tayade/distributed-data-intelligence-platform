# ADR 0013: Bounded recovery finalization evidence

Status: **Implementation record — C3c.3 authorized by the checkpoint request; not owner-accepted ADR status**

## Accepted basis and distinctions

ADR 0010 and its reconciliation matrix already authorize independent read-only recovery followed
by atomic attempt/control/run/artifact closure and conditional release. ADR 0011's import ownership
remains separate. ADR 0012 is an implementation record, not an accepted architecture decision.
This record explains scoped implementation choices without changing those accepted contracts.

A successful reconciliation operation can conclude that its interrupted run failed. Both known
run outcomes therefore close RECONCILE RUNNING to SUCCEEDED and apply its control; the attempt's
RECONCILED_SUCCEEDED or RECONCILED_FAILED code and completion audit explicitly record the run outcome.
A failed verifier is not a known failed run. Unavailable, contradictory or incomplete qualification
leaves the run, control, recovery attempt and occupied coordinator unresolved. CANCELLED is deferred.

## Implementation choices

- Keep VERIFY_RESULT/item 0 as the historical C3c.2 observation. Add exactly one fixed item 1 for
  fresh finalization qualification, with its own persisted intent, one-time launch gate, child,
  heartbeats and immutable result. It reuses the original private adapter unchanged. The native
  command's itemOrdinal remains the original execution item 0; the recovery SQL ordinal identifies
  the qualification step, not a new native input or publication. Audits distinguish recoveryItem=1.
- Seal readable selected dependency/output bytes and directory/file identities before and after
  this full independent verifier. Reread the exact seal before disposition. This avoids treating
  historical success as current readability and permits a still-fresh in-memory qualification to
  be reconsidered after a proven SQL rollback without launching again. Qualification expires after
  60 seconds and cannot be reconstructed as live authority after process replacement.
- Resolve exact committed facts under the coordinator lock before any disposition write. Register
  or reuse only byte-identical result evidence and dependency edges. Commit attempt, control, run,
  result link, bounded audit, STOPPED worker and conditional coordinator release together. Keep the
  epoch unchanged. A receipt contains facts, not a worker session or owner token.
- Releasing the coordinator sets it READY but stops the recovery worker. A fresh host-guard and
  worker registration lifecycle is required before dispatch; no running READY worker is fabricated.
- Use existing tables, indexes and constraints. No migration, publisher, native module, public
  response shape or data format changes. Existing read-only history allowlists add only the fixed
  recovery classifications/actions needed to inspect these facts. Production recovery invocation
  remains absent.

## Contract ambiguity handled conservatively

The original independent reader sometimes maps OSError to PUBLICATION_INVALID. Therefore that code
alone cannot establish corruption. The finalizer additionally requires readable, stable captured
bytes demonstrating malformed completion metadata, a manifest fingerprint mismatch or a declared
file inventory/hash mismatch. Other invalid/query/path/resource diagnoses remain blocked unless
this bounded policy establishes a supported known outcome. Unavailable I/O is never absence or
proof of mutation. Native modules are preserved rather than changing a frozen deployment contract.

## Alternatives and remaining limits

Reusing a historical VERIFIED receipt alone lacks fresh native readability. Relaunching item 0
would violate consumed intent and ambiguous-child guarantees. Adding a new table/stage name or
changing frozen native modules would expand schema/protocol scope without improving this operation.
The extra fixed item and bounded byte reads cost another independent read, but retain explicit
process persistence and accepted native analytical authority.

Filesystem observation and SQL commit remain separate. Cooperative guard/quiescence review and
identity/hash rechecks narrow but do not eliminate the final filesystem/database time-of-check/
time-of-use gap. A hostile/unmanaged writer, universal orphan containment, filesystem power-loss
atomicity and hard filesystem I/O deadlines are not solved. Production activation, cancellation,
guarded retry, broader product recovery and CI readiness remain separate work. Public Execution
Recovery stays partially implemented.
