# Publisher-reference snapshots: E1a contract

Status: **Accepted by the owner on October 19, 2026, for E1a only.** Originally proposed in Stage A on October 19, 2026, this detailed contract accompanies [ADR 0006](../adr/0006-publisher-reference-snapshots.md). The merged [Content Intelligence specification](../data-products/content-intelligence.md) remains Proposed. Implementation and executed validation are recorded separately [below](#implementation-and-validation); acceptance does not imply merge, hosted CI success, or completion of E1b/M6.

## Scope and existing boundaries

E1a validates one explicitly selected publisher-reference CSV against one explicitly selected retained vocabulary and publishes a self-contained local snapshot. It does not contact publishers, classify articles, enrich Gold, select a latest publication or expose an API. `source_id` continues to identify a feed source, not a cross-publisher story or necessarily a whole publishing organization.

Existing [Gold publication](gold-publication.md) and [consumer contracts](analytics-api-contracts.md) remain unchanged. The [canonical serializer](../../processing/publication_metadata.py), [Gold writer](../../processing/gold_publication.py), [reader](../../processing/gold_reader.py) and their [publication](../../processing/tests/test_gold_publication.py) and [analytics](../../processing/tests/test_gold_analytics.py) tests provide precedents. E1a reuses only the unchanged canonical serializer; its [standalone module](../../processing/publisher_reference.py) supplies separate schema, JSON duplicate-key, integer-type and bounded-read checks.

## CSV schema and dialect

The first physical record must be this literal, unquoted ASCII header, in this order, followed by LF, CRLF or end of file:

```text
source_id,publisher_name,publisher_category,declared_language,country_code
```

| Field | Exact decoded-cell contract |
| --- | --- |
| `source_id` | Required; ASCII full match `[a-z0-9][a-z0-9_-]{0,127}`; 1–128 characters; unique throughout this reference version |
| `publisher_name` | Required nonempty plain text, at most 200 Unicode scalar values; never interpreted as HTML |
| `publisher_category` | Empty means null; otherwise an exact approved vocabulary token, at most 64 ASCII characters; source context, not article classification |
| `declared_language` | Empty means null; otherwise an exact approved vocabulary token, at most 32 ASCII characters; declared source language, not article language detection |
| `country_code` | Empty means null; otherwise an exact approved two-letter uppercase ASCII token; curated source association, not story geography |

All nonempty cells reject leading/trailing whitespace as defined below. No trimming, case folding, Unicode normalization, URL interpretation or identifier rewriting is permitted. Two canonically equivalent Unicode spellings remain different text. Lengths count decoded Unicode scalar values, not UTF-8 bytes, grapheme clusters or UTF-16 code units; combining marks count separately. Strict UTF-8 excludes surrogate code points and malformed/overlong encodings.

The complete dialect is:

- UTF-8 without BOM. Comma delimiter. Double quotes may surround an entire data field, and a quote within a quoted field must be doubled. Backslash has no escaping role. An unquoted field cannot contain a quote or comma. After a closing quote only a comma or record end is legal; whitespace there is not ignored.
- Exactly five fields per data record, including trailing empty fields. Quoted empty and unquoted empty optional cells both map to null. Required empty cells fail. Literal `NULL` is ordinary text: it is a valid publisher name, but an unapproved optional token fails. There is no automatic null sentinel.
- LF and CRLF record endings are allowed, including a mixture. A final newline is optional. A lone CR is invalid. Embedded CR/LF, even inside quotes, is invalid. No multiline records.
- Empty physical records anywhere are errors, including an extra blank line after the final record. One terminating newline does not add a blank record. A zero-byte file fails; the header alone, with or without one ending, succeeds with zero data rows.
- No duplicate `source_id`, even if every field of both rows is identical. A repeated header in data fails validation. Extra/missing/reordered/quoted header fields, malformed quoting and incorrect field counts fail the whole input.
- In every cell, disallow C0 controls U+0000–U+001F, DEL/C1 U+007F–U+009F, line/paragraph separators U+2028/U+2029, and U+FEFF anywhere. Record-ending CR/LF outside cells are the only exception. Disallow surrogate code points. Other scalar values are allowed in names subject to the rules above; format characters are not silently stripped.
- Boundary whitespace is exactly this fixed set: U+0009–U+000D, U+0020, U+0085, U+00A0, U+1680, U+2000–U+200A, U+2028, U+2029, U+202F, U+205F and U+3000. Do not delegate this definition to a changing runtime Unicode category. Internal whitespace from this set is allowed only when it is not otherwise a disallowed control/separator. Thus internal ordinary spaces are allowed; tabs are not. Whitespace-only names fail.

Reject the entire input on the first invalid record. Never publish a partial reference or a file of accepted rows with rejected rows silently removed. Preserve original CSV bytes, including permitted quoting, row order and line endings.

### Bounded intake and parser approach

| Resource | Maximum, inclusive |
| --- | --- |
| Original CSV bytes, including header, quotes and endings | 10 MiB = 10,485,760 bytes |
| Data records, excluding header | 10,000 |
| One physical record, excluding LF/CRLF ending | 4,096 bytes; exceeds the maximum possible valid row under field limits |
| Decoded cells | Field-specific limits above; no field exceeds 200 scalar values |
| Vocabulary file | 32 KiB = 32,768 bytes; at most 32 tokens per list |
| Manifest / completion file | 16 KiB / 4 KiB respectively |

Stage B should use the existing Python runtime and standard `csv`, `json` and `hashlib` facilities. Use strict CSV parsing with comma/double-quote/doubled-quote rules, no escape character and no whitespace skipping. Review parser behavior explicitly: strict mode does not establish this header, blank-record, control, vocabulary, resource or quote-placement contract. Add narrowly scoped lexical/field checks around the mature parser; do not replace it with a general-purpose custom CSV parser.

Read regular files with bounded reads and at most one excess-byte sentinel for the whole-file limit. Bound physical-line buffering before decoding/parsing, configure a bounded parser field limit, stop at the first excess row, and bound row/key collections before inserting another item. File size checks are an early rejection, not the only defense against a growing file. Retain no more than the capped original byte buffer and capped validated structures. Metadata must also be capped before JSON decoding; parser recursion/type errors become safe validation failures. Do not call unbounded `read()` and check length afterward.

Fixed limits and text/dialect semantics belong to validator revision 1. There are no per-invocation relaxations. Chunk sizes and log verbosity are operational settings. These bounds are not a measured Python heap limit; Stage B must report any memory measurements separately. A byte-limit boundary test need not be a semantically valid CSV if other limits make that combination impossible: distinguish successful bounded intake from successful validation.

## Retained vocabulary v1

The following are the complete initial allowlists. They are deliberately small, reviewed project vocabularies, not complete international language/country registries and not assignments to real publishers.

| Attribute | Literal token | Meaning |
| --- | --- | --- |
| Publisher category | `business` | Source curated as primarily covering business/economic affairs |
| Publisher category | `culture` | Source curated as primarily covering arts/cultural affairs |
| Publisher category | `general-news` | Source curated as covering general news across subjects |
| Publisher category | `science` | Source curated as primarily covering science |
| Publisher category | `technology` | Source curated as primarily covering technology |
| Declared language | `en`, `es`, `fr` | English, Spanish, French respectively, as declared for the source |
| Country code | `CA`, `GB`, `US` | Canada, United Kingdom, United States respectively, as curated source associations |

No token means that every article has that attribute. Optional nulls remain null; do not invent an `unknown` token.

The exact vocabulary schema has five required keys: `schema_version` (integer 1), `version` (integer 1), and `publisher_category`, `declared_language`, `country_code` (arrays of strings). No extra/missing/duplicate keys or null arrays. Each array has 1–32 unique tokens sorted by ascending ASCII byte order. Category tokens full-match `[a-z][a-z0-9-]{0,63}`; language tokens full-match `[a-z][a-z0-9-]{0,31}`; country tokens full-match `[A-Z]{2}`. Duplicate tokens fail rather than collapse. The schema accepts only these shapes; the supported-version check additionally requires the exact v1 lists below.

Canonical vocabulary file contents, with **no final newline**, are:

```json
{"country_code":["CA","GB","US"],"declared_language":["en","es","fr"],"publisher_category":["business","culture","general-news","science","technology"],"schema_version":1,"version":1}
```

The file must already have the canonical encoding defined below; reject pretty-printing or alternative escapes rather than silently reserialize submitted vocabulary bytes. Retain the accepted bytes unchanged as `vocabulary.json`; record their byte length, SHA-256 and version in the manifest. Verification needs no original vocabulary path or network lookup.

Changing, adding or removing tokens or changing their meanings requires a reviewed new vocabulary version and corresponding supported-version definition in a future validator release. Reusing version 1 for different lists fails even if its recorded checksum is internally consistent. Stage B supports only schema/version 1; an unsupported version fails. Review occurs in repository documentation/code review, not a registry service. No authoring tool, database or runtime registry is proposed.

## Identities and canonical metadata

`reference_id` is the logical dataset identity, full-match `[a-z0-9][a-z0-9_-]{0,63}` (1–64 ASCII characters). `revision` is its immutable human-facing version, full-match `[a-z0-9][a-z0-9_-]{0,31}` (1–32). Examples are `fictional-publishers` and `r1`. Both are case-sensitive, require no normalization and reject dots, separators, whitespace and traversal. Lowercase-only names also avoid case-only collisions on common local filesystems.

The snapshot fingerprint is lowercase hexadecimal SHA-256 of the **exact canonical `manifest.json` bytes**. The manifest has no embedded self-digest. It binds original CSV bytes through their length and SHA-256, not through normalized rows. It also binds vocabulary bytes/version, schema/validator versions, fixed semantic configuration, reference ID/revision and authoring metadata. Completion evidence stores the fingerprint outside the hashed payload.

All JSON artifacts use the unchanged `canonical_bytes` serialization in `processing/publication_metadata.py`: keys recursively sorted, compact `,` and `:` separators, `ensure_ascii=False`, `allow_nan=False`, strict UTF-8, no BOM and no trailing newline. Arrays retain order. Do not normalize strings or numbers. Require exact schema/type validation before serialization, reject duplicate keys at every depth during decoding, and reject unknown fields. All integers must be JSON integer tokens, never booleans, floats, exponent notation or negative zero; ranges are given below and never exceed signed 64-bit. No metadata field accepts a floating-point value. Require persisted bytes to equal canonical reserialization, so whitespace, alternate escapes and alternate number representations fail. Invalid Unicode scalars fail before encoding.

### Manifest schema v1

Every listed key is required; nested objects also have exactly their listed keys. There are no additional extension fields. Manifest version 1 defines the layout and completion protocol below; unsupported versions are errors.

| Key | Type and constraint |
| --- | --- |
| `manifest_version`, `csv_schema_version`, `validator_revision` | Integers, each exactly `1` |
| `reference_id`, `revision` | Strings with the identifier rules above; must match selected directory components |
| `semantic_config` | Exactly `{"csv_dialect":"publisher-csv-v1","null_policy":"empty-only-v1","text_policy":"scalar-text-v1"}`; no selectable overrides in v1 |
| `csv` | Object with `file` exactly `reference.csv`, `byte_length` integer 74–10,485,760, `row_count` integer 0–10,000, `sha256` lowercase 64-character hex |
| `vocabulary` | Object with `file` exactly `vocabulary.json`, `byte_length` integer 1–32,768, `sha256` lowercase 64-character hex, `version` integer exactly 1 |
| `authoring` | Object with `maintainer_id` and `note` as specified below |

The shortest CSV is the 74-byte header without a newline. Recorded lengths and row counts must equal read-back evidence, not merely fit ranges. Vocabulary version must equal the retained file version and the supported content definition. Filename fields are constants, not paths to resolve from untrusted metadata.

`authoring.maintainer_id` is a required non-sensitive team/curator label, full-match `[a-z0-9][a-z0-9_-]{0,63}`. `authoring.note` is required but nullable; when present it is nonempty plain text of at most 256 scalar values with the same control/boundary-whitespace policy as publisher names. No automatic host, username, source path or credential capture. Both authoring fields participate in identity: correcting them requires a new revision. This is a claimed provenance label, not authenticated authorship.

Operational settings are the approved local root and input locations, bounded I/O chunk sizes, logging and invocation clock. They do not enter the manifest. There is no automatic `latest` pointer, run-history store, or mutable revision alias. Creation time is only in completion evidence, written once; replay preserves it unchanged. All required provenance for semantic reproduction is in retained artifacts, not transient logs.

### Identity cases

| Candidate versus existing reference/revision | Required result |
| --- | --- |
| Identical identity-bearing content | Same fingerprint; verify existing completion and all artifacts, then read-only replay |
| Changed CSV bytes | Different fingerprint; revision conflict, even if decoded rows are equal |
| Changed vocabulary content/version | Unsupported/altered vocabulary fails validation now; after a reviewed supported version exists, a valid changed candidate conflicts with the old revision |
| New revision, identical CSV and vocabulary | Different fingerprint because revision is identity-bearing; new destination |
| Same rows with LF versus CRLF, different quoting, Unicode spelling or row order | Different CSV hash and fingerprint; same revision conflicts |
| Changed authoring fields or supported semantic/validator revision | Different fingerprint; same revision conflicts |
| Changed operational root, input path, log level or invocation time | Same fingerprint for the same semantic candidate; within the same root replay does not rewrite timestamps/bytes; another root has a separate binding namespace |

No revision can point to multiple snapshots within one configured root. A future layout/schema upgrade must still enforce this same root-wide binding; adding a versioned parent namespace must not bypass it.

## Local layout and completion evidence

The operator supplies an existing dedicated local directory as root, for example `outputs/publisher-reference`, separate from Gold. A missing root fails `SNAPSHOT_UNAVAILABLE`; the writer creates only reference/revision descendants, not arbitrary root ancestors. This example is not a client-supplied filesystem API or a new storage-profile service. Resolve a trusted root once and form only these fixed descendants:

```text
<root>/
  reference_id=<reference_id>/
    revision=<revision>/
      reference.csv
      vocabulary.json
      manifest.json
      complete.json
```

There is no fingerprint child directory, revision index or mutable selector. A reference/revision directory is the exclusive reservation/binding. Parent directories may contain other reference/revision snapshots; verify only the explicitly selected revision. Within that revision, exactly these four regular files and no subdirectories or other artifacts are permitted for a completed snapshot. `complete.tmp` is allowed only as the writer's pre-completion transient file; its presence makes public open/verify fail, including if `complete.json` is also present.

`complete.json` is canonical JSON with exactly these required fields:

| Field | Constraint |
| --- | --- |
| `completion_version` | Integer exactly 1 |
| `snapshot_fingerprint` | Lowercase 64-character SHA-256 of canonical manifest bytes; must equal the caller's expected fingerprint |
| `manifest_byte_length` | Integer 1–16,384, matching retained bytes |
| `created_at` | UTC string `YYYY-MM-DDTHH:mm:ss.ffffffZ`, valid Gregorian date, year 0001–9999, seconds 00–59; six fractional digits, no offset/naive time/leap second |

`created_at` records the completion attempt after successful read-back, not an article time or fingerprint input. It is immutable on replay but not authenticated by the snapshot fingerprint. Validly changing it cannot be detected from that fingerprint alone; byte comparisons detect changes during verification, not historical tampering without independent trusted evidence. Self-consistent rewrites of artifacts are not proof of authenticity.

### Paths and writer scope

Use only operator-selected local regular input files and a dedicated operator-controlled output root. Reject lexical `..` components, symlink input leaves, symlinks at each input's immediate parent, and symlinks at or below the output root. Ancestors above those parents/root are trusted environment configuration and resolved once; inspect the input parents and output root themselves before resolving them so their symlinks cannot be hidden. Reject special files (including FIFO/device/socket) before reads, and reject hard-linked input/artifact files (`st_nlink != 1`). Never follow a path supplied inside JSON; artifact basenames are constants. Resolve/inspect selected parents and containment before creating/opening children and recheck paths around reads. Input CSV/vocabulary must be outside the output root. File identity comparisons use device/inode plus size and nanosecond modification time, alongside byte/hash comparisons; none of these machine-specific observations enters content identity.

The operator ensures **one writer for the entire configured root**, including future tool versions, and no concurrent editor of inputs/artifacts during publication or verification. Exclusive directory/file creation and no-overwrite checks enforce ordinary conflicts under this assumption. They do not establish hostile-race resistance, cross-process locking or multiwriter safety. Readers can encounter a reservation and fail as incomplete. Paths observed to change type, identity, size, modification time or content during a checked operation fail explicitly.

### Completion-last protocol

1. Validate identifiers, authoring fields, paths and bounded input files. Capture exact input bytes and file identity/size/mtime before and after reading; reject observed changes. Validate vocabulary, then CSV in physical record order. Compute the candidate canonical manifest and fingerprint without writing output.
2. Inspect any existing destination. Publicly verify it without changes before comparing its fingerprint to the candidate. A valid equal binding is replay; a valid unequal binding is a conflict. Incomplete/corrupt output fails as such, even for an identical candidate. If absent, create the reference parent if necessary and reserve the revision directory exclusively. Never replace a file or directory.
3. Write `reference.csv`, `vocabulary.json` and `manifest.json` using exclusive creates; close files before read-back. Copy the captured bytes, not an unchecked second read from the input path. No completion evidence exists yet.
4. Perform the same bounded structural, hash, canonical, CSV and vocabulary verification as an independent opener, except that this private writer-only check requires exactly the three pre-completion artifacts. Compare their identity to the candidate. Re-read/hash both original inputs with bounds, compare captured file identity/size/mtime and bytes, and recheck retained artifacts/inventory. Any observed input/output change fails without promotion.
5. Create canonical `complete.tmp` exclusively after successful checks. Close it, verify its bytes/schema, and check the three artifacts, directory inventory and original inputs again. Rename it to the absent `complete.json` only under the single-writer assumption and return the verified identity. Completion is recorded last; a reader never accepts the temporary marker.

For replay, fully verify the existing snapshot and recheck the candidate input captures before returning success; write nothing, including completion time, and preserve file and directory modification times. Reading may update access times; those are not promised. Changed inputs fail rather than silently adopting a second version. Stat comparisons alone are insufficient: compare content hashes/bytes as well. Mutation that is reverted between checks or occurs after the last check is outside this trusted single-writer guarantee; do not claim race-free input snapshots.

No transactional rollback or power-loss durability is promised. A failed write/read-back leaves an explicit incomplete destination; later invocations refuse to resume, repair, delete, overwrite or promote it. An interruption after the final rename can leave a valid completed snapshot even if the caller never received success; retry verifies it normally. Failure must not modify unrelated completed snapshots. Recovery is owner investigation outside these interfaces: preserve evidence, correct inputs/environment and normally select a new revision. An independently invalid completed snapshot remains unusable; the marker alone grants no validity.

## Independent open and future reader boundary

Open/verify requires explicit root, reference ID, revision and expected snapshot fingerprint; no discovery, directory glob or latest selection. IDs locate the binding and the fingerprint pins its expected meaning. Existing-destination checks during publish may obtain its candidate fingerprint from completion, but must verify the entire binding before deciding replay/conflict; this does not create a consumer discovery feature.

The independent opener must:

1. Enforce selection syntax, containment, regular-file/symlink/hard-link rules and the exact selected-directory inventory.
2. Capture bounded completion/manifest bytes and file identities. Validate strict keys/types, canonical encoding, supported versions, UTC completion time, manifest length/hash and the expected fingerprint. Match reference ID/revision to the selected directory.
3. Read bounded retained vocabulary/CSV bytes; verify their lengths and SHA-256 against the manifest. Verify vocabulary version/content, revalidate every CSV rule and duplicate key, and compare actual row count. Reject unsupported CSV schema, validator revision or semantic configuration.
4. Recheck directory inventory, metadata bytes and artifact identities/lengths/hashes before returning. Fail on any observed change, even if a replacement is otherwise valid. Do not return partial rows on failure.
5. Return detached immutable validated rows in original CSV order, plus reference ID, revision, fingerprint, row count and immutable manifest/provenance values. Do not return open file handles, lazy iterators or paths whose contents the caller must later trust.

Verification works after original CSV/vocabulary paths have been removed and without Gold, Spark, SeaweedFS, a database or network. The required files are entirely retained under the selected revision. Detached rows stay stable if disk files later change; that does not certify continued on-disk validity. A later open re-verifies everything. E1b may rely on exact source keys, uniqueness, validated nullable attributes, supported retained vocabulary, row count and pinned provenance from this result. E1b must separately define its join/publication/dependency recheck protocol; it must not bypass verification by reopening returned filenames. Missing reference matches remain an E1b concern, never an E1a reason to drop articles.

## Local interfaces and diagnostics

The reusable interfaces and CLI are in [processing/publisher_reference.py](../../processing/publisher_reference.py). They require only Python's standard library and the existing canonical serializer.

| Interface | Inputs and result |
| --- | --- |
| `validate` | CSV path, vocabulary path, reference ID, revision, maintainer ID and nullable note; return validated candidate identity, row count and hashes; no root writes |
| `publish` | Same inputs plus dedicated root; validate afresh, then return `published` or `replayed` with reference ID/revision/fingerprint/row count |
| Library `open_snapshot` / CLI `verify` | Root, reference ID, revision, required expected fingerprint; library returns detached rows/provenance, CLI returns verification summary only |

CLI subcommands are `validate`, `publish`, `verify`. Flags are `--csv`, `--vocabulary`, `--reference-id`, `--revision`, `--maintainer-id`, optional `--note` (omitted means null), `--root`, and `--expected-fingerprint`, according to the input table. No command accepts arbitrary SQL, remote locations, force/repair/latest options or unconstrained metadata keys. Do not echo CSV contents in CLI output. Success uses one bounded JSON summary on stdout and exit 0; argument errors use exit 2, all validation/integrity/conflict/I/O failures use exit 1, with empty stdout and one safe JSON diagnostic on stderr. No traceback or raw OS exception text in normal CLI output.

Diagnostics have fixed `code` and fixed `message`, with optional validated field-name identifier and 1-based physical `record` (header is 1). Duplicate IDs may include the first record number. Vocabulary diagnostics may identify a fixed list name and bounded token index. Never include raw cell/token values, arbitrary input identifiers, credentials, caller-provided paths or source exception strings. A valid fingerprint and validated identifiers may appear in successful summaries; invalid values never appear in errors. Reject unsupported/duplicate CLI options without echoing their supplied values. Usage text contains fixed option names only. Cap each CLI summary/diagnostic at 4 KiB; no input-derived text is interpolated into a message. For example, the fixed conflict message is `Reference revision already binds different content.` and the fixed incomplete message is `Snapshot is incomplete; owner investigation is required.` Other classes use fixed messages describing their class, with wording covered by Stage B tests.

### Behavior and failure matrix

Codes below define stable classes; messages must be fixed per class. Where several conditions apply, validate arguments/paths and intake bounds first, vocabulary next, CSV sequentially next, then destination state. Within a destination, missing completion precedes artifact validation; otherwise check inventory, metadata, then retained vocabulary/CSV. Report one failure, not raw diagnostic dumps.

| Case | Result / safe diagnostic class | Output and replay behavior |
| --- | --- | --- |
| Valid first publication | `published` | New reserved revision, verified artifacts, completion last |
| Valid header-only CSV | `published`, row count 0 | Complete empty reference, not a missing input |
| Identical completed replay | `replayed` | Verify all evidence; preserve bytes and modification times |
| Valid different candidate for completed same ID/revision | `REVISION_CONFLICT` | No writes; select a new revision |
| Missing selected input / selected revision | `INPUT_UNAVAILABLE` / `SNAPSHOT_UNAVAILABLE` | No substitute or discovery |
| Invalid CSV structure, controls, fields, duplicates or tokens | `CSV_INVALID` | Safe record/field identifier; no reservation or partial publication |
| Invalid/altered/unsupported vocabulary | `VOCABULARY_INVALID` | No token values or automatic version upgrade |
| Missing/corrupt CSV or vocabulary in completed destination | `SNAPSHOT_INVALID` | No reuse, repair or overwrite |
| Missing/corrupt/noncanonical manifest or completion fields | `SNAPSHOT_INVALID` | No trust based on marker presence |
| Destination without completion, including stale temporary marker | `SNAPSHOT_INCOMPLETE` | Do not resume or promote, even when files look valid |
| Extra file/subdirectory in completed destination | `SNAPSHOT_INVALID` | Reject unexpected artifacts, including `complete.tmp` |
| Unexpected fingerprint or directory/manifest identity mismatch | `IDENTITY_MISMATCH` | No rows returned or automatic adoption |
| Failed write/read-back | `PUBLICATION_FAILED` | May leave reserved incomplete output; unrelated snapshots unchanged |
| Observed source mutation / output mutation | `INPUT_CHANGED` / `SNAPSHOT_CHANGED` | Abort before completion or returning rows; no automatic retry |
| Invalid ID/revision/authoring/CLI syntax | `ARGUMENT_INVALID` | No path creation; no raw invalid value in diagnostic |
| Traversal, prohibited symlink/hard link, special file | `PATH_UNSAFE` | Reject before following/reading/creating affected child |
| Byte/row/record/field/metadata cap exceeded | `RESOURCE_LIMIT` | Stop bounded intake/parsing; no partial success |

For independent verification, malformed retained CSV/vocabulary maps to `SNAPSHOT_INVALID`; the original safe validation reason/field/record may be a fixed nested code, not a raw value. Resource/path/change violations retain their specific classes. An observed disappearance or replacement during reads is a change failure; a file already missing at initial inspection is unavailable/invalid as above. After completion rename, an interrupted caller must verify status; failure reporting does not authorize deleting possibly completed evidence.

## Fictional examples and canonical vectors

All examples are synthetic Markdown only. This valid CSV demonstrates a comma, doubled quotes, Unicode and optional nulls:

```csv
source_id,publisher_name,publisher_category,declared_language,country_code
example-science,"Example Science, North",science,en,CA
example-culture,"Example ""Arts""",culture,fr,GB
example-plain,Café Example,,,
example-null,NULL,,,
```

For each optional column, `""` is equivalent to an empty cell in validated rows, but changing its byte representation changes snapshot identity. `<b>Example</b>` is also a valid name rendered as plain text; it is never trusted markup. No example assigns attributes to a real publisher.

This header-only CSV is valid; the canonical vector below uses exactly one LF after it:

```csv
source_id,publisher_name,publisher_category,declared_language,country_code
```

This invalid CSV fails at record 3 for a duplicate source key (including an identical duplicate); it must not publish record 2:

```csv
source_id,publisher_name,publisher_category,declared_language,country_code
example-a,Example A,science,en,CA
example-a,Example A,science,en,CA
```

Each following data record, when placed individually after the required header, is invalid: boundary space; unknown token; malformed quote placement; missing field; unknown literal `NULL` token, respectively.

```csv
example-a, Example A,science,en,CA
example-a,Example A,unreviewed,en,CA
example-a,Exam"ple A,science,en,CA
example-a,Example A,science,en
example-a,Example A,NULL,en,CA
```

Canonical vector A is inherited from the [existing serializer test](../../processing/tests/test_silver_inputs.py): the logical value with `z = [1, "é"]` and `a = {"b": false}` serializes to the following exact UTF-8 bytes, without a newline. This is a serializer vector, not a valid reference manifest (booleans are not reference integer fields).

```json
{"a":{"b":false},"z":[1,"é"]}
```

Its SHA-256 is `73d069d0f3964dd58881492a708b60441e9e119d805019f1f4b4906e57f938b0`.

Canonical vector B is a complete header-only reference. CSV bytes are the 74-byte header plus one LF (75 bytes total); SHA-256 is `9c742f47d8ba5e928ae2c1eb84648b87ae149a7137c7dd5d904da93d413b29e0`. Vocabulary is the exact 183-byte canonical v1 line above without newline; SHA-256 is `734af479169642f0b9fd699cb8cfec0d3edf40aec0da07f40cc235de3074b005`. Its canonical manifest is:

```json
{"authoring":{"maintainer_id":"example-team","note":null},"csv":{"byte_length":75,"file":"reference.csv","row_count":0,"sha256":"9c742f47d8ba5e928ae2c1eb84648b87ae149a7137c7dd5d904da93d413b29e0"},"csv_schema_version":1,"manifest_version":1,"reference_id":"fictional-publishers","revision":"r1","semantic_config":{"csv_dialect":"publisher-csv-v1","null_policy":"empty-only-v1","text_policy":"scalar-text-v1"},"validator_revision":1,"vocabulary":{"byte_length":183,"file":"vocabulary.json","sha256":"734af479169642f0b9fd699cb8cfec0d3edf40aec0da07f40cc235de3074b005","version":1}}
```

Without a trailing newline, the manifest is 577 bytes and its snapshot fingerprint is `c6140d147ca67df65e761873d7ba88fb293192902b723feae706def89cc462db`. A completion example using that vector is below; formatting is canonical and its timestamp is synthetic, outside identity:

```json
{"completion_version":1,"created_at":"2026-10-19T12:00:00.000000Z","manifest_byte_length":577,"snapshot_fingerprint":"c6140d147ca67df65e761873d7ba88fb293192902b723feae706def89cc462db"}
```

Stage B must freeze these vectors as independent expectations, not derive expected hashes with the same code under test. Key insertion order cannot change canonical identity. CSV LF/CRLF, final newline, row order, alternative legal quoting, revision, authoring and any future supported semantic/vocabulary change must change identity. Operational paths/logging/time must not.

## Stage B acceptance cases

These are the approved acceptance requirements. The [focused suite](../../processing/tests/test_publisher_reference.py) maps each group to test names prefixed `test_a01` through `test_a17`; actual execution evidence is recorded below, separately from the requirements.

| Case | Contract and required evidence |
| --- | --- |
| A01 — whole-file validation | Fictional valid and header-only cases yield exact rows/nulls/counts; invalid record prevents any reservation/partial publication |
| A02 — header and records | Exact unquoted header accepted with LF/CRLF/EOF; altered/order/quoted/missing/repeated header, wrong field counts, empty file, blank records and lone CR fail |
| A03 — quoting and endings | Quoted commas/doubled quotes accepted; bare quotes, text/space after closing quote, unmatched quote and embedded LF/CRLF fail; mixed endings/final newline both handled exactly without normalization |
| A04 — text and nulls | Non-ASCII/emoji/combining scalars retain exact text; invalid UTF-8/BOM/surrogates and every forbidden control range fail; boundary whitespace set is exhaustive; internal ordinary spaces accepted; empty optional cells become null and `NULL` stays text |
| A05 — field identity | Source regex first/subsequent characters and 1/128/129 boundaries; identical/conflicting duplicates fail with safe record numbers; name 1/200/201 scalars and blank failure; category/language/country syntax and 64/32/2 bounds, including lengths before membership checks |
| A06 — bounded parsing | Byte cap and cap+1 for CSV/vocabulary/manifest/completion; 10,000/10,001 data rows; 4,096/4,097 physical-record intake; field limits and one-over; growing files/oversized single lines stop without unbounded reads or collection growth |
| A07 — vocabularies | Exact v1 bytes accepted; each token/null validated; unknown tokens, duplicate/unsorted lists, duplicate/unknown/missing JSON keys, wrong types, unsupported schema/version, altered v1 lists and noncanonical bytes fail; list 32/33 capacity independently checked before supported-content equality |
| A08 — canonical metadata | Vectors A/B match literal lengths/hashes; object key insertion order stable; duplicate nested keys, unknown fields, bool-as-int, decimal/exponent/negative integers, negative zero, NaN/infinity, malformed Unicode and noncanonical encodings fail |
| A09 — semantic identity | All identity cases above verified, including authoring changes and exact-byte changes with equal logical rows; operational changes do not affect fingerprint; future vocabulary versions require new reviewed vectors, not invented supported v2 behavior |
| A10 — identities/provenance | Reference ID 1/64/65, revision 1/32/33, maintainer 1/64/65 and note null/1/256/257 boundaries; invalid characters/whitespace/traversal and arbitrary author fields fail safely |
| A11 — publication and replay | Initial/empty snapshots have exact four-file layout; completion absent during injected pre-completion faults; unchanged replay preserves all file/directory bytes and mtimes, including creation time; valid changed candidate conflicts without writes |
| A12 — independent opening | Remove original input locations, then verify and read exact detached rows offline; wrong explicit fingerprint/ID/revision and absent snapshot fail; no Gold/Spark/storage/network dependency |
| A13 — corruption and versions | Missing/tampered CSV/vocabulary/manifest/marker, false lengths/counts, unsupported versions/config, changed known vocabulary with recomputed local hash, invalid completion date and extra artifacts fail; changing marker creation time alone is not falsely claimed detectable by fingerprint |
| A14 — failures and recovery | Inject exclusive-create/write/close/read-back/temporary-marker/rename failures; no automatic repair/retry/promotion; incomplete destinations remain blocked; interruption after rename can replay only after full verification; unrelated completed snapshots unchanged |
| A15 — mutations | Change inputs during capture/before completion and during replay; change outputs/inventory between verification phases; observed changes fail; detached rows remain unchanged after later file edits |
| A16 — path safety | Traversal, symlink root/input/artifact/selected parent, escaped containment, hard links and special files rejected; ordinary siblings remain untouched; unsupported multiwriter races are not represented as proven safe |
| A17 — diagnostics/interfaces | All matrix classes, exit codes and stdout/stderr boundaries checked with malicious values/paths/credentials; no raw data, traceback or arbitrary paths leak; invalid CLI flags safe; validate performs no output writes |

For resource cases where valid v1 content cannot reach a cap (for example 32 tokens when the supported list is smaller), test the intake/schema bound separately from vocabulary membership. Record actual memory observations separately from normative limits. Use independent expected rows/bytes, fault injection and deterministic synthetic inputs; no retained publisher attributes or live runs are needed.

Stage B remains limited to the owner-approved local reference interfaces and tests. E1b enrichment, derived publication semantics, broader platform run management, storage-profile services, authentication, exports, orchestration and multiwriter durability are deferred. This contract neither selects tools for those capabilities nor changes accepted Gold/consumer behavior.

## Local use

Use the existing Python 3.12 runtime; no new packages, Java, Spark, Gold, SeaweedFS or network access are required for E1a. The repository's existing development environment is one option; verify it before use. All example content below is fictional, temporary local data. Run from the repository root:

```bash
e1a_python="$PWD/outputs/silver-python312/.venv/bin/python"
"$e1a_python" --version
e1a_demo=$(mktemp -d)
mkdir "$e1a_demo/snapshots"
printf '%s\n' 'source_id,publisher_name,publisher_category,declared_language,country_code' > "$e1a_demo/reference.csv"
"$e1a_python" processing/publisher_reference.py validate \
  --csv "$e1a_demo/reference.csv" \
  --vocabulary config/publisher-reference-vocabulary-v1.json \
  --reference-id fictional-publishers --revision r1 --maintainer-id example-team
"$e1a_python" processing/publisher_reference.py publish \
  --root "$e1a_demo/snapshots" --csv "$e1a_demo/reference.csv" \
  --vocabulary config/publisher-reference-vocabulary-v1.json \
  --reference-id fictional-publishers --revision r1 --maintainer-id example-team
"$e1a_python" processing/publisher_reference.py verify \
  --root "$e1a_demo/snapshots" --reference-id fictional-publishers --revision r1 \
  --expected-fingerprint c6140d147ca67df65e761873d7ba88fb293192902b723feae706def89cc462db
```

This intentionally empty reference is canonical vector B. Successful publication emits one JSON line:

```json
{"reference_id":"fictional-publishers","revision":"r1","row_count":0,"snapshot_fingerprint":"c6140d147ca67df65e761873d7ba88fb293192902b723feae706def89cc462db","status":"published"}
```

Repeating `publish` with unchanged inputs emits `status: "replayed"` without rewriting any artifact or modification time. A valid changed candidate for the same revision fails with exit 1 and only this stderr diagnostic:

```json
{"code":"REVISION_CONFLICT","message":"Reference revision already binds different content."}
```

Corrections require a new explicit revision. Preserve prior completed snapshots. If a failed attempt leaves an incomplete directory, investigate it as the owner; the CLI never repairs, resumes, deletes or promotes it. Correct the input/environment and normally select a new revision. A lost response after completion rename can be resolved by independent `verify` or identical replay. Neither failure nor a marker alone proves the snapshot's state.

The approved [vocabulary configuration](../../config/publisher-reference-vocabulary-v1.json) is 183 exact UTF-8 bytes with no final newline. Avoid editor reformatting; the validator deliberately rejects alternative encodings. Always pass its path explicitly. Publication retains its own copy, so verification remains independent of that configuration path and the original CSV. To verify a nonempty reference, use the fingerprint returned by its own successful validation/publication; vector B's fingerprint is specific to its documented bytes and authoring identity.

Python callers import `validate`, `publish`, `open_snapshot` with `PYTHONPATH=processing`. `validate` and `open_snapshot` return a frozen `Reference` with a tuple of frozen `ReferenceRow` values and recursively immutable manifest mappings. `publish` returns a summary. There are no lazy file reads or filesystem handles in the returned reference. CLI options use separate `--name value` arguments; unsupported/duplicate options fail without echoing supplied values. `validate` creates no root; the operator explicitly creates the dedicated root before `publish`.

## Implementation and validation

Stage B was implemented in one standalone module, one vocabulary configuration and one focused test module. No runtime dependencies, Gold/consumer changes or workflow changes were introduced. Existing processing CI discovery includes `test_publisher_reference.py`; the required `React publisher dashboard` job is unchanged. [PR #39](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/39) was merged and [issue #38](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/38) was closed. Hosted PR #39 [CI run 39316671940](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/39316671940) succeeded, including all four existing jobs. This historical closeout is separate from the issue #40 correction below.

Local commands used with the established runtime:

```bash
PYTHONPATH=processing outputs/silver-python312/.venv/bin/python \
  -m unittest discover -s processing/tests -p test_publisher_reference.py -v
JAVA_HOME=$(/usr/libexec/java_home -v 21) TZ=UTC SPARK_LOCAL_IP=127.0.0.1 \
  PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python" PYTHONPATH=processing \
  outputs/silver-python312/.venv/bin/python -m unittest discover -s processing/tests -v
npm run check
git diff --check
```

The Java selector above is for the existing macOS development environment; CI supplies its runner JDK 21. E1a's focused suite itself needs no Java. The full suite exercises existing synthetic local Spark fixtures. No live ingestion, retained-data mutation, large benchmarks or application servers are part of E1a verification.

The original October 19 focused suite passed 37 tests with parameterized subcases. Its names map the acceptance matrix to that historical execution evidence:

| Acceptance groups | Tests | Evidence |
| --- | --- | --- |
| A01–A05 | 6 | Whole-input rejection, exact header/CSV grammar, all forbidden control ranges, fixed boundary whitespace, scalar limits, duplicates and nullable values |
| A06–A10 | 7 | Intake limits and growth, exact vocabulary/configuration, literal vectors, nested metadata, semantic identity and authoring bounds |
| A11–A12 | 3 | Publication/replay/conflicts, preservation of bytes/mtimes, original-input removal, recursive immutability and isolated imports |
| A13 | 5 | Fingerprint selection, missing/corrupt/unexpected artifacts, recorded counts/lengths, recomputed-hash tampering and completion-time semantics |
| A14 | 6 | Incomplete refusal, exclusive creates, write/close/read-back/marker/rename faults, error classification and interruption after completion |
| A15 | 6 | Input/output/inventory mutation, replay rechecks, root replacement and selected-directory disappearance |
| A16–A17 | 4 | Traversal, symlinks/hard links/special files, CLI summaries, safe diagnostics and error precedence |

The CLI was also exercised as subprocesses using two fictional source rows in a temporary directory: validation, initial publication, unchanged replay, verification, changed-content revision conflict (exit 1), and verification after removing both original inputs. Successful steps retained fingerprint `e48d8dd5830073c7c67af50911f4ce91da64f18beef202599632531050260f76`; this is synthetic evidence, not retained publisher data. Temporary demonstration files were cleaned up.

Final local results on October 19, 2026: **37/37 focused tests passed; 118/118 full processing tests passed** (37 E1a plus 81 existing regressions). The full run used Python 3.12.15 and the existing Java 21 runtime and completed in 102.631 seconds. The first sandboxed attempt could not bind Spark's loopback gateway; the same suite passed with that local restriction lifted. Existing Spark socket `ResourceWarning`s were emitted without test failures. `npm run check` passed Markdown/relative-link and repository hygiene checks; `git diff --check` passed. No standalone Java ingestion/Maven, JDBC POC, Spring Boot or frontend suites were run in this local pass; hosted CI had not yet been verified at that point. The subsequent hosted result is recorded above. No CI commands or job names changed.

The focused suite additionally launches an isolated `python -S` subprocess to demonstrate that importing E1a needs no producer or installed runtime packages. Resource limits are tested independently from semantic acceptance where needed; no peak-memory benchmark is claimed. The implementation targets the existing POSIX local development/CI environments, not a validated Windows port. The one-writer/trusted-filesystem assumption, nontransactional completion protocol, lack of power-loss guarantees and unauthenticated creation time remain deliberate limitations. E1b can consume detached validated rows and pinned provenance only under its own future enrichment/publication contract.

### Issue #40: recheck I/O classification

The post-merge defect was in `_recheck()`: it supplied the mutation class as both `_capture()`'s unavailable/read-failure class and its observed-change class. The operation already failed closed, but a read or close `OSError` on unchanged evidence incorrectly claimed `INPUT_CHANGED` or `SNAPSHOT_CHANGED`.

Each recheck caller now supplies an explicit I/O class. `_capture()` retains the previous observation during rechecks, distinguishing a file already missing at initial inspection from disappearance after capture. It compares previously observed file metadata, parent identity and exact bytes as well as the existing before/after-read checks. This also preserves evidence of mutation when an I/O failure occurs in the same operation. File/directory inspection, guards and inventory rechecks distinguish `FileNotFoundError` from other filesystem errors; inability to inspect metadata does not itself prove mutation. No failed check returns rows or promotes a temporary marker.

| Operation / condition | Before correction | After correction |
| --- | --- | --- |
| Initial input read/close failure, unchanged evidence | `INPUT_UNAVAILABLE` | `INPUT_UNAVAILABLE` |
| Candidate validation or identical-replay input recheck I/O failure | `INPUT_CHANGED` | `INPUT_UNAVAILABLE` |
| Independent open or existing-binding artifact recheck I/O failure | `SNAPSHOT_CHANGED` | `SNAPSHOT_INVALID` |
| New publication artifact/input/temporary-marker recheck I/O failure | `INPUT_CHANGED` or `SNAPSHOT_CHANGED` | `PUBLICATION_FAILED` through the existing publication failure boundary |
| Initial private publication read-back I/O failure | `PUBLICATION_FAILED` | `PUBLICATION_FAILED` |
| Observed source/snapshot disappearance, replacement or metadata/byte/parent change | `INPUT_CHANGED` / `SNAPSHOT_CHANGED` | Same mutation classes |

The correction uses explicit classifications at the affected call sites rather than translating all mutation exceptions at a public boundary. Such blanket translation would hide real mutations. Argument/path/resource, vocabulary/CSV, destination-completion and artifact-validation precedence remain intact. Accepted ADR 0006, schemas, canonical vocabulary and identity vectors, exact-byte checks, exclusive creation, completion-last publication, replay/conflict behavior and detached immutable results are unchanged.

Nine additional tests extend A14, A15 and A17; all original A01–A17 cases remain. Deterministic mocks exercise actual stream reads and closes for both inputs and all retained artifacts, initial capture and rechecks, candidate rechecks during replay, every private artifact/temporary-marker read-back and recheck phase, and candidate rechecks after reservation. They also cover open/stat/fstat errors, failure to inspect metadata while diagnosing a read error, and directory/guard/inventory errors. Assertions require exact fixed diagnostic codes/messages, unchanged identity/metadata/bytes for ordinary I/O faults, failure without returned data, no completed marker after pre-completion failure, no automatic retry repair, and preservation of existing completed artifacts. CLI tests require exit 1, empty stdout and only the fixed safe JSON diagnostic.

Mutation regressions cover changed bytes with identical size and restored mtime, same-byte replacement with a different inode, mtime changes, disappearance and parent replacement for both inputs and snapshots. Additional combined mutation/I/O cases ensure an observed disappearance still reports mutation after a read error, and a byte difference remains a mutation even when close also fails. Existing mutation, inventory, path-safety, resource, corruption and precedence tests remain active. Five selected new regression tests run against an isolated copy of the merged implementation reproduced the defect with 44 failed subcase assertions; the working implementation passed them.

Issue #40 local validation on October 20, 2026 used the commands above after verifying the existing interpreter and Java selector: **46/46 focused tests passed in 0.750 seconds; 127/127 full processing tests passed in 103.520 seconds** (46 E1a plus 81 existing regressions). The runtime was Python 3.12.15 and Java 21.0.7, with the supported `SPARK_LOCAL_IP=127.0.0.1`, `TZ=UTC` and existing `PYSPARK_PYTHON`. The sandboxed full-suite attempt failed during six Spark class setups because socket binding was prohibited (`Operation not permitted`); the final full run passed in the authorized local execution environment outside that restriction, without dependency or test workarounds. Existing Spark socket `ResourceWarning`s and native-Hadoop/incubator warnings remained non-failing. `npm run check` passed Markdown, relative-link and repository hygiene checks; `git diff --check` passed. The final diff is limited to the implementation, its focused tests and this implementation/validation section. No live publishers, retained datasets, infrastructure, CI job definitions or accepted ADRs were changed. No new hosted CI run has been executed for this correction; owner PR review and subsequent hosted validation remain pending.
