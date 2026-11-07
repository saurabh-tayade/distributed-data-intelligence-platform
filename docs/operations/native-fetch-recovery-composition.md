# Test-only native FETCH recovery composition

This verification slice composes the existing inactive contracts from
[ADR 0034](../adr/0034-inactive-native-fetch-recovery-assessment.md),
[ADR 0035](../adr/0035-inactive-fetch-checkpoint-recovery-handoff.md) and
[ADR 0036](../adr/0036-inactive-terminal-fetch-recovery.md).
It introduces no production code, migration, operational command or activation decision.
Issue #49, ordinary Content dispatch and Gate B0 remain open.

## Test contract

`NarrowContentFetchRecoveryComposition` lives exclusively in test sources. It assesses the
original native FETCH checkpoint, requests an explicit exact-target handoff review, and retains
only the opaque owner returned by the first committed transfer in that live object. A second
call requests a separate terminal-target review. Assessment observations are never converted
into approval. The test supplies each attestation explicitly; synthetic process observations
are fixture inputs, not evidence that real handlers or unmanaged writers have stopped.

A confirmed terminal rollback retains the same owner but requires a newly inspected target and
fresh review. An uncertain terminal result is inspected before any further action. Confirmed
terminal replay returns historical facts without calling the reviewer or mutating state.
Neither a handoff receipt nor a restarted harness can construct an owner. Lost handoff return
and process death before terminal commit leave ownership occupied.

The outcome remains `FAILED / NATIVE_FETCH_INTERRUPTED` with retry disabled. The original
attempt remains `INTERRUPTED`, evidence is retained, and successful terminal reconciliation
stops the recovery worker and releases only the global slot. It never launches another native
process, continues a stage, publishes success, infers cancellation or reauthorizes restart.

## Verification scope

The new PostgreSQL integration class exercises the complete native-child fixture lifecycle,
independent review bindings, every handoff and terminal mutation boundary, incomplete prefixes,
changed files, process uncertainty, lost method returns and actual JDBC commit acknowledgment
loss. Disposable recovery JVMs halt after handoff commit, after retaining the owner, before
terminal mutation, inside the terminal transaction and after terminal commit. Fresh inspection
must distinguish occupied ownership from committed terminal history without recreating authority.
Later unrelated queue work must leave both historical receipts inspectable.

The class is explicitly included in the Content recovery CI partition and inventory tests.
Hosted exact-head CI is required for PostgreSQL and native-child integration evidence; Java
compilation and policy/hygiene checks alone are not integration execution evidence.
