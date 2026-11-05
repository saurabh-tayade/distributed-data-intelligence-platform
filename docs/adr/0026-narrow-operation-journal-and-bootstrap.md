# ADR 0026: Narrow operation journal and bounded startup pipe

Date: November 5, 2026

Status: **Owner-approved separate-journal and pipe direction; inactive read-only probe only.**

## Context and decision

The owner approved a separate durable operation journal, reserving an operation before effects,
and a bounded startup pipe that releases a waiting child only after its process identity is
recorded. Interrupted or uncertain operations stay consumed and require explicit recovery.
Changing the existing stage-history model or adding private files beyond the fully allocated
64 MiB original plan would be broader alternatives. Neither is needed for this slice.

Migration V11 adds append-only PostgreSQL operation and phase tables beside the V10 admission
records. The first integration is deliberately a fixed ORIGINAL CHECK_INPUTS/0 **read-only
transport probe**, not the native CHECK_INPUTS adapter. It reads a parent-prepared command through
the strict Python sink. No publisher, result sidecar, stage-history transition, recovery
continuation, operator command or dispatcher calls the probe.

One operation ID and one fixed slot are consumed by a reservation. The journal binds its exact
canonical bootstrap facts to enrollment and READY: policy, run/attempt, owner instance/epoch,
CONTROL/root/lock identities, ledger, selection/profile/runtime digests, checkpoint and command.
The ordered phases are FILES_READY, START_AUTHORIZED, CHILD_RECORDED, RELEASE_AUTHORIZED and
PROBE_COMPLETED. Each phase is immutable; child identity includes PID, OS process-start instant
and a fresh instance UUID. The release phase records the complete bootstrap digest.

A completed probe is historical transport evidence, never successful native execution. A fresh
operation ID cannot reuse the fixed slot. A lost acknowledgment, failed process start or uncertain
cleanup does not return the reservation. Facts lookup grants no Owner/Session and cannot resume.
Even this probe's claimed attempt remains occupied; the legacy interruption path can mark it
RECOVERY_REQUIRED but no narrow recovery disposition or safe slot-release path is added.

## Side-effect and child boundaries

The parent acknowledges reservation before creating operation directories or the command. It
checks the exact live owner, controls, deadline and strict READY context immediately before
namespace creation, command creation, process start and actual pipe delivery. The final delivery
check runs on the writer thread, after scheduling delay. Persistent stop/interruption and the
startup/global deadline are checked again immediately before bytes are written.

No SQL transaction spans filesystem I/O or process execution. As accepted in ADR 0010, the final
SQL observation and external effect have a cooperative local time-of-check/time-of-use gap.
Cancellation cannot provide a database-atomic veto once delivery begins. The installation guard,
no automatic takeover, consumed journal and explicit quiescence/recovery requirements remain
necessary. This is not distributed filesystem fencing or exactly-once effects.

The child accepts fewer than 64 KiB of canonical bootstrap bytes through a pipe, with a fixed
five-second receive deadline. It requires EOF before interpreting the packet or touching any
supplied evidence path. Missing, extra, duplicate, malformed, noncanonical, oversized or incomplete
input is refused. A parent that dies before supplying a complete valid packet cannot start the
probe. Death after delivering valid bytes may release it via EOF; the journal already contains
CHILD_RECORDED and RELEASE_AUTHORIZED, and the outcome remains consumed and possibly uncertain.

The child verifies its actual PID against the packet. The parent records the OS start identity;
the child does not independently authenticate SQL authority or its process-start timestamp.
Corresponding Expected/Current values are parent-supplied facts, not authority learned from disk.
Local probe modules are checked for changed source bytes before launch/release, and Python bytecode
cache reads are disabled by the fixed launch settings. This is not a qualified native runtime
chain or a defense against a privileged local administrator replacing executables or files.

## Bounds, compatibility and remaining work

The original filesystem plan remains exactly 67,108,864 reserved bytes and 221 entries. Only its
existing CHECK_INPUTS/0 command slot is used. The SQL addition is at most one reservation of up to
64 KiB plus five fixed-size phase rows per admitted attempt; it does not consume the private-file
budget or insert arbitrary audit events. Existing unmarked attempts and native formats are unchanged.
Startup verifies both new tables as well as the complete migration history.

The [implementation and validation record](../operations/narrow-content-operation-journal.md)
distinguishes executed local checks from database/hosted checks. Native stage allocation, qualified
runtime-chain routing, publishers, original/inherited recovery, effectful continuation and full
fault/restart acceptance remain open. Ordinary Content dispatch stays disabled and Gate B0 stays
incomplete. Historical ADR acceptance statements are unchanged.
