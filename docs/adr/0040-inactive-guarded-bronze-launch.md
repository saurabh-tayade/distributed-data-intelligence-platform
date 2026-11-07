# ADR 0040: Inactive guarded native Bronze launch

Date: November 7, 2026

Status: **Bounded implementation within the approved original-run journal direction; dispatch remains inactive.**

## Decision and scope

Connect the original-live-owner reservation in [ADR 0038](0038-inactive-native-bronze-reservation.md)
to the strict child in [ADR 0039](0039-inactive-strict-bronze-child.md), ending at candidate inventory.
This follows the separate installed-runtime and ordered-phase pattern of
[ADR 0031](0031-inactive-guarded-fetch-launch.md). It does not independently verify Bronze storage,
commit a receipt, grant downstream execution, release the attempt or activate ordinary dispatch.

A separate immutable launch profile binds registered Bronze and storage correspondence to installed
Java/JAR observations and the strict Bronze entry point. Historical Bronze runtime-v2 registrations
retain their ledger-v1 meaning. Their linked FETCH manifest supplies the installed classpath and
Java information; the separate strict Bronze profile qualifies those installed bytes again rather
than treating either the historical registration or child protocol digest as executable attestation.
Only the existing bounded local-loopback storage semantics are supported. No registration CLI,
operator launch command, remote endpoint expansion or AWS infrastructure is introduced.

## Ordered original-run effects

V18 adds separate immutable Bronze launch profiles and phases referencing the V17 reservation.
The original uninterrupted owner chain reserves and launches once. Historical inspection returns
facts, never a resume token or reconstructed authority. Lost acknowledgment invalidates the live
channel; an interrupted reservation remains occupied and cannot be adopted or automatically retried.

The six ordered phases bind the launch profile, record fresh files, authorize process start, record
child PID/start identity, authorize the PID-bound bootstrap and finally record candidate inventory.
Each phase uses coordinator-first READ COMMITTED owner fencing, versions, controls, deadline and
recovery exclusions. Filesystem, process and network work remains outside SQL transactions.

The parent independently reconstructs the bootstrap from original READY enrollment, current owner,
frozen runtime/storage selection and completed FETCH dependency. It reopens exact dependency and
credential identities at effect boundaries and qualifies installed runtime bytes again. The packet
is released only through the bounded stdin/EOF protocol after the final delivery checks.

Only fresh STORE_BRONZE/0 directories and the existing command slot are created. A dedicated reader
admits precisely completed FETCH plus the reserved Bronze subtree, including bounded active prefixes;
existing FETCH-only readers are not broadened. The original 67,108,864-byte / 221-entry budget remains
unchanged. The child retains its forced one-shot result prefix and native writer race semantics.

## Candidate boundary and alternatives

A completed observation requires bounded, stable command/evidence/result inventory and consistent
child identity, storage-state claim and exit status. EXACT pairs with exit zero and the other five
storage states with exit one are structurally distinguished. Receipt-shaped child evidence, clean
exit and an EXACT claim are not independently verified remote facts or a durable Bronze receipt.
Independent GET-only verification remains a later boundary and must not rerun the publishing child.

Standard output must be empty. Standard error must be empty or match the existing 210-byte
SLF4J no-provider warning by exact length and SHA-256. Raw diagnostics are discarded rather than
persisted; other messages, extensions and repetitions are rejected. Other launchers retain their
existing output policy. This narrow compatibility allowance avoids changing installed dependencies.

Reusing historical launch identity would reinterpret old runs. Broadening FETCH-only readers would
weaken existing boundaries. Adopting an interrupted child or consumed slot could repeat uncertain
storage effects. Treating candidate output as success would skip independent verification. These
alternatives are not selected.

There is no semantic verification, Bronze checkpoint, downstream authority, ordinary Content dispatch,
release, restart or recovery extension. Issue #49 and Gate B0 remain open. See the
[verification record](../operations/narrow-guarded-bronze-launch.md).
