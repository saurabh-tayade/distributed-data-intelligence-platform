# ADR 0022: Narrow managed Content profile

Date: November 4, 2026

Status: **Owner-approved design direction; foundation implemented, operational qualification incomplete.**

## Context

The owner selected the narrower, versioned managed profile for issue #49 instead of
widening native readers. [ADR 0021](0021-content-evidence-producer-reservations.md) records
the earlier bounded-writer candidate and its unresolved native-output envelope. A file
count or aggregate reservation alone cannot prove that every complete proof fits every
downstream reader. In particular, the Java Silver qualification and native selection
paths both reparse complete documents with the existing 64 KiB reader.

## Decision

Introduce the explicit policy identity `ddip-content-narrow-v1`, without changing standalone
native contracts, historical runtime hashes, existing RunProfile submission contracts or
reader maxima. The approved direction is:

* At most four Silver analytical files per dataset, 24 regular namespace files and four
  directories, including the namespace root. Qualify the complete Silver envelope within
  60 KiB, rather than measuring only a component.
* Reserve the complete core operation plan before effects. Reservations include final and
  interrupted preparation files, directories and conservative canonical inventory bytes.
  Keep one recovery attempt available before admitting another effectful continuation.
* Reject oversized complete projected report rows before publication completion. The
  candidate limit is 60 KiB for each public canonical row, including keys, escaping,
  arrays, reference attributes and reference-only coverage rows. Never truncate values.
* Keep ordinary Content dispatch disabled until native precompletion enforcement, runtime
  adoption, complete nested evidence bounds and restart/fault qualification are proven.

The first implementation is deliberately a foundation, not an available execution profile.
It adds pure envelope/row validators and an opt-in fixed reservation grammar. No existing
runtime calls these helpers. The new version-2 reservation ledger is distinct from the
existing version-2 runtime candidate, whose Python sink still requires a version-1 ledger.
It must not be used with that legacy sink. A later versioned integration must bind policy,
producer hashes, operation allocation, durable admission and recovery together.

## Alternatives and tradeoffs

Keeping wider Silver layouts would require a separately reviewed versioned reader change.
The narrower profile avoids that protocol expansion but rejects otherwise valid standalone
publications. Intermediate component budgets provide early conservative rejection; they
do not replace the complete-envelope bound or native semantic/provenance validation.

A fixed operation grammar prevents low-byte inspections from consuming all future recovery
space. The original candidate reserves core work plus one inspection pair, exactly 64 MiB;
the recovery candidate reserves nine verification directories. These are enumerated plans,
not proof that every reachable execution uses those counts. Further inspection cannot grow
the sealed plan. Reservations are cooperative accounting, not disk allocation or OS quotas.

## Implementation and remaining evidence

The [foundation report](../operations/narrow-content-profile.md) records exact limits,
production-serializer tests, actual-reader roundtrips and ledger interruption tests.
The ordinary dispatcher, SQL attempt admission, native completion points and runtime
selection remain unchanged. No new operational capability, automatic retry, repair,
publication activation or legacy migration is introduced.

Before operational acceptance, implement and independently review precompletion guards,
exact stage-to-slot allocation, versioned child consumption, durable attempt admission,
full Gold/Content/recovery/resume envelope closure, inherited evidence handling and real
PostgreSQL restart tests. The provisional budget model is not evidence for these gates.
Historical ADRs retain their statuses; this decision does not declare Gate B0 complete.
