# Inactive versioned projection transport codec

November 5, 2026. Additive experiment for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49),
building on the [temporary-marker projection](narrow-publication-projection.md) and
[whole-envelope qualification](narrow-publication-envelopes.md).
**This codec has no production callers and grants no execution or recovery authority.**
Ordinary Content dispatch remains disabled and Gate B0 remains incomplete.

## Exact experimental wire shape

The new `narrow_projection_transport` module defines `protocolVersion: 2`. All original
Gold or Content fields are retained, with exactly one additional top-level field:

```json
{
  "projectionContext": {
    "projectionContextVersion": 1,
    "policy": "ddip-content-narrow-v1",
    "kind": "GOLD",
    "outputBindingId": "the-independently-supplied-Gold-binding-ID"
  }
}
```

The example shows only the additional field, not a complete command. `kind` is exactly
`GOLD` or `CONTENT`. The policy and context version are fixed. Unknown, missing and extra
command/context fields are rejected. Booleans are not integer versions. Content requires
a v2 Gold command inside its complete committed proof. Nested v1/v2 mixing is rejected;
there is no automatic upgrade, downgrade or extension-stripping dispatch path.

`create_command_v2` requires all original fields except `protocolVersion`, plus independent
expectations; it supplies the version/context explicitly. Passing an existing command to
that constructor is rejected. `read_command_v2` reads exact canonical bytes and returns an
immutable `CommandV2` snapshot. Its value/receipt accessors return fresh copies. The snapshot
is deliberately not JSON-serializable as native evidence. Prepared fingerprints must match
the complete manifest. Original run, attempt, checkpoint and epoch remain in the command.

This top-level extension adds no container around the command. There is no descriptor file,
new reservation, allocation or protocol-specific file-count allowance. A separately sealed
descriptor would require a reviewed allocation change because ORIGINAL already reserves
exactly 64 MiB / 221 entries. That design is not implemented here.

## Independently supplied correspondence inputs

The pure factory takes a separate in-memory expectations object, not a wire sidecar. Its
exact keys are `policy`, `readyContentRuntimeSha256`, `contentRuntime`, `goldRuntime`,
`silverRuntime`, `bindings`, `original`, `reference` and `goldProof`.

- Each runtime record has `id`, its complete `configuration`, and `sha256` of that canonical
  configuration. These are supplied registered values, not inferred from the command.
- The supplied READY digest must match the **Content** runtime. The Content configuration's
  Gold runtime ID/digest must match the supplied Gold record. The Gold configuration's Silver
  runtime ID/digest must match the supplied Silver record. A Gold command uses the Gold digest;
  a Content command uses the Content digest. Comparing Gold directly to READY is rejected.
- `bindings` has exactly GOLD, CONTENT, REFERENCE, SILVER and TEMP records. Each record has
  `bindingId`, `role`, and the complete command-facing `{root, rootDevice, rootInode}`
  `configuration`. IDs are checked against the corresponding runtime fields, roles against
  their map key, and configurations against command bindings. Native Java/scratch settings
  must match the supplied Silver runtime and TEMP binding.
- `original` has `runId`, `attemptId`, `sourceId` and the complete `silverInput`. The command
  must contain exactly that one fresh selected Silver input. Retained-source or multi-source
  commands are outside this profile. `reference` is the complete independently supplied
  reference record and must match the Content command byte-for-byte. `goldProof` may be null
  for Gold; Content requires the independently supplied complete committed Gold proof, with
  its real artifact identity, to match exactly. Self-consistent substituted IDs are rejected.

The codec checks these correspondences, digests, exact command/context fields, dependency
links, full Gold artifact/proof and reference identity. It cannot establish where supplied
expectations came from. It does not validate registration, current ownership, approved modes,
runtime module bytes, root identities on disk, SQL delivery state or native data semantics.
Consistently fabricated inputs can satisfy a pure correspondence check; successful decoding
must never be treated as native verification, admission or permission to publish.

Before any future integration, the caller must independently reconstruct READY from immutable
SQL, validate frozen selection/profile and binding membership, check the host guard and
root/lock/ledger identities, load the registered runtime chain, validate source delivery and
registered Silver proof, perform native Bronze/Silver verification, and seal the full command.
The child must receive independently supplied current-writer/operation/command-digest and
READY expectations. Obtaining those expectations from the inspected command would be circular.

## Complete receipts and whole-envelope accounting

Receipt derivation is from the sole `inputs[0].receipt`, or the identical receipt in Content's
`goldProof.command.inputs[0]`. Every original receipt field is required, including nested
expected metadata/payload keys, byte length and hash, configuration/journal/storage digests,
content types, original run/attempt/source and ingestion identity. Its selected input must match
the independent full input, its Silver selection and captured Bronze identity must correspond,
and the complete qualification and input digests must agree. The returned array contains that
full receipt unchanged. There is no empty, shortened or synthetic receipt fallback.

`qualify_publication_v2` constructs/checks complete PUBLISH, PREPARE, VERIFY and RECHECK
commands, preparation/verification results, artifact, recovery requests and observations.
Content inspection's VERIFY command with complete prior verification is included separately;
its preparation must match that verification. Publication/recovery VERIFY still clears it.
The context remains in every nested original, including the committed Gold command inside
Content. Gold uses the full local checkpoint observation; Content uses the full successful
observation with artifact and original receipt. Both preparation boundaries are included.
Gold remains stage-local and does not promise that later Content fits.

`gold_proof_v2` requires a caller-supplied artifact UUID and preserves the entire original
PUBLISH command. It does not invent an ID to predict Content capacity. The proof has no
independent persisted slot: its complete enclosing command is charged. `resume_v2` checks
both complete request and result at BEFORE_FETCH, GOLD and CONTENT_INPUTS boundaries, including
repeated reference, Gold verification and Content preparation where applicable.

Recovery changes only the embedded mode/preparation/verification required by its read path.
Historical run, attempt, checkpoint and epoch are preserved. A future recovery/RESUME writer
has a separate current control identity and reserved operation; this codec supplies neither.
It does not re-admit historical commands or extend recursively inherited origins.

## Bounded serialization and unchanged readers

`narrow_transport_json` emits canonical bytes incrementally and checks the budget before
each small fragment. It does not first materialize an arbitrarily large encoded envelope.
Complete stage/recovery/RESUME envelopes use the existing 1 MiB slots; result sidecars use
64 KiB. The existing 16 MiB reader ceiling bounds internal complete-value operations, not
an invented independent proof slot. Existing Java allocations/readers are unchanged.

Grammar matches the unchanged `SourceJson` reader: at most 16 containers, exact UTF-8 key
ordering and escaping, 50,000 UTF-16 units per key, 131,072 per large-reader value string,
65,536 for the small reader, and 20 integer digits **excluding the minus sign**. Empty
containers count toward depth. Duplicate keys, malformed UTF-8, BOM, unpaired surrogates,
floating-point values, noncanonical bytes and trailing content are rejected. The earlier
profile parser intentionally counts signed tokens differently and is not reused or changed.

## Verification and limits

Always-run tests cover strict versions/context, nested version mixing, independently supplied
runtime chains and binding roles, same-root/wrong-ID substitutions, source/origin/attempt
correspondence, every missing receipt field, complete receipt retention, copy isolation,
full nested proof/recovery/RESUME records, exact/plus-one destination limits and a command
that fits depth 16 while its recovery wrapper fails. Mandatory Java tests compare Python
canonical grammar and complete envelope bytes against the unchanged actual `SourceJson` reader.

The four-case #103 native consumer corpus is generated afresh. Its original measurement
commands intentionally contain abbreviated synthetic receipts and cannot pass this codec.
Tests explicitly reject those commands. Actual consumer verification/capture bodies are reused
inside **distinct complete synthetic correspondence vectors**, labeled as neither registered
nor executable. The entire actual native reference qualification/capture and reference binding
are retained, with frozen wrapper role/ordinal added explicitly. Content and all RESUME cases
assert that full reference, including repeated verification, is preserved. Pure Java parity
uses a complete static disposable reference capture shape, not an abbreviated stub or a live
filesystem claim. This checks new transport overhead without inventing native source provenance.
Small Gold/Content fit; the 902-file Gold artifact and Content whole-stage cases remain rejected.

No existing command constructor, Python publisher, legacy module, runtime manifest/pinned hash,
SQL schema, sink, registration or dispatcher is modified. `bounded_evidence.Sink` remains a
ledger-v1 sink and cannot consume the READY-bound ledger-v2 contract. This slice does not adopt
it. READY/launch/sink integration, actual publisher rejection before completion, post-rename
native verification, interruption/restart, recovery ancestry and lifetime capacity are still
required before activation. Independent review and hosted CI are required before merge.

```sh
python -m unittest discover -s processing/tests -p test_narrow_projection_transport.py -v
python -m unittest discover -s processing/tests -p test_narrow_transport_json.py -v
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowTransportJsonTest,NarrowProjectionTransportTest test
```

Local validation used Python 3.12.14 / DuckDB 1.4.5 and Corretto JDK 21.0.13:

- 44 Python regression tests passed, including all 11 transport tests, the fresh four-case
  consumer corpus, seven bounded-JSON tests, and unchanged projection/profile regressions.
- Full content-api unit suite: 537 tests, 534 passed and three existing optional corpus cases
  skipped. Both new mandatory Java parity tests passed, covering 33 complete envelopes.
- Independent review reran all 11 transport tests and both Java parity tests after fixing
  Content inspection VERIFY coverage and preserving the complete reference corpus bodies.
- Markdown/relative links, repository hygiene and whitespace checks passed.

These are local tests, not hosted CI, Spark-producer or PostgreSQL restart/recovery evidence.
