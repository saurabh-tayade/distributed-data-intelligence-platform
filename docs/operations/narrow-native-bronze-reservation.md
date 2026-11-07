# Inactive native Bronze reservation

This is a package-private reservation seam for an original, uninterrupted narrow Content run.
It is not an operator command or an available processing route. See
[ADR 0038](../adr/0038-inactive-native-bronze-reservation.md) for the approved boundary.

## Contract

- A native FETCH checkpoint is independently reopened and linked to one `STORE_BRONZE/0` slot.
- The original live owner, READY ledger and frozen runtime/storage identities are required.
- The canonical command and reservation facts are bounded, immutable and single-use.
- Historical inspection returns facts, never a live owner or launch authorization.
- The attempt remains CLAIMED, the run RUNNING and the global slot occupied.
- No Bronze child, storage request, legacy stage transition or ordinary Content dispatch occurs.
- FETCH-only terminal recovery rejects Bronze reservation history. There is no new recovery path.

An acknowledgment lost after commit can leave a consumed reservation. Do not retry with another
operation ID, reconstruct an owner from SQL, remove rows, clear the slot or repair the filesystem.
The existing FETCH terminal operator does not dispose of this downstream state.

## Required verification

The PostgreSQL integration suite must exercise successful exact linkage without effects;
duplicate reservation; wrong FETCH, run, attempt, source, checkpoint or runtime; changed journal,
marker, credentials or directory identity; stale owner versions; controls/deadlines; and uncertain
commits. Direct SQL tests must cover invalid scalar/shape/digest inputs, immutable rows, recovery
exclusion and concurrent/stale-snapshot writers. Migration tests preserve populated historical
evidence without backfill. Existing native FETCH recovery must continue to work without Bronze.

Local verification passed production Java compilation, the new test bridge/integration class
and changed recovery test compilation, 50 CI-policy tests, 28 local-demo safety tests, Markdown,
repository hygiene and whitespace checks. The broader local demo discovery could not run its
two native fixture classes because DuckDB was unavailable. PostgreSQL integration execution
requires hosted CI; compilation is not a claim that those cases passed.

The new class covers 55 cases, including lost FETCH-checkpoint and Bronze-reservation commit
acknowledgments, a heartbeat-updated same-live owner, invalid successor arguments before HTTP,
changed evidence/credentials, direct SQL rejection, concurrency and snapshot isolation. Six
additional recovery tests cover successor exclusion and transactional rollback. The V15 upgrade
fixture explicitly emulates only the new successor-absence queries while the table is proven
absent on V15; production readers retain fail-closed schema behavior.

The mandatory PostgreSQL inventory now has 42 classes, including 12 in the native partition.
All existing partitions, report verification and aggregate gates remain required. Exact-head CI
results belong in the PR verification record; no operational readiness is claimed here.
