# Narrow native precompletion guards

November 4, 2026. **Inactive library entry points. Gate B0 remains incomplete.**

This implements part of the owner-approved [narrow profile](narrow-content-profile.md) and
the decision in [ADR 0024](../adr/0024-narrow-native-precompletion-guards.md). Ordinary
Content dispatch remains disabled. The existing tracked admission path does not call these
publishers, and successful read-only verification does not authorize processing or recovery.

## Scope

The new code owns only the publication ordering needed for two guards:

* Silver: inspect actual unfinished output, then bound the full prospective metadata before
  making completion visible. Include all namespace bookkeeping and exact native marker bytes.
* Content: inspect every complete public report row after native readback and before temporary
  completion creation. Include unused reference rows and rows outside any default report page.

Existing native transformations, semantic checks, canonical serialization, product identities,
standalone publication contracts and pinned modules remain unchanged. The additive APIs have
explicit version suffixes and are not selected by any production runtime.

## Explicit entry points

`processing/narrow_silver_precompletion.py` provides:

* `process_one_v1(spark, client, bucket, key, output, *, artifact, binding)`: an explicit
  native-compatible publisher for the selected Silver namespace. It returns native counts
  and status, not a shutdown-confirmed verification receipt. The caller owns its Spark session.
* `precompletion_guard_v1(artifact, binding, marker, bronze)`: a read-only check of unfinished
  output. Its result contains the actual unfinished snapshot, planned marker bytes and measured
  prospective sizes. It conveys no durable policy enrollment or authority to execute.
* `reconcile_v1(request)`: the existing explicit Silver qualification/recheck request shape,
  plus actual narrow validation. Real native qualification supplies the verification evidence.

`processing/narrow_content_publication.py` provides `publish_v1(spark, **selections)`, using
the same explicit output, Gold and reference selections as the standalone publisher. It owns
the completion sequence and inserts the all-row guard after native readback verification.

`processing/narrow_content_reports.py` provides:

* `validate_precompletion_v1(dependency, evidence, counts)`: the complete read-only scan over
  exact inventoried paths, for use after native readback and before completion.
* `verify_publication_v1(content_root, product_fingerprint, *, gold_root, reference_root)`:
  Spark-free, read-only native verification and all-row validation of completed output.

These are library seams for isolated qualification. They are not supported operator commands,
new submitted execution profiles or an alternative to the durable admission protocol. No
environment variable or existing command selects them implicitly.

Content row-limit failures use the existing `RESOURCE_LIMIT` diagnostic. Silver's pure
profile checks retain the foundation's private `CONTENT_PROFILE_INVALID` exception; native
read/I/O and provenance failures keep fixed classifications. No existing public error
vocabulary or child protocol is changed. Future runtime adoption must explicitly map the
new helper's private failures rather than sending raw exception text to an operator.

## Bounds and authority

Silver retains the foundation's limits: four analytical files per dataset, 24 total files,
four directories including the root, 4 KiB completion, 16 KiB inventory identity, 32 KiB
capture, 36 KiB native selection, 52 KiB verification and 60 KiB complete qualification.
The final file count includes `complete.json`. Hidden CRC and success files and hidden
bookkeeping directories are charged as well as Parquet files.

The precompletion projection uses actual paths, hashes, lengths, native marker bytes and
Bronze evidence. It conservatively budgets future stamp strings, whose existing grammar
permits up to 21 characters including a minus sign. The projection is never a stored
qualification or a claim of Spark shutdown. Actual native qualification and actual envelope
validation remain mandatory after completion.

Each complete public Content row is limited to 61,440 canonical UTF-8 bytes. The scan uses
the unchanged catalog, reference coverage and daily-discovery projections and conversion,
not a page or smoke sample. A bounded SQL raw-string lower bound protects Python
materialization; exact canonical serialization decides acceptance. Values are never cut or
silently omitted. A valid standalone row can therefore be refused by the narrow publisher.

## Retained and uncertain output

An incomplete namespace is retained and blocked, including one containing `complete.tmp`.
The entry points do not repair, clean up, relabel or resume it. Failure after the rename may
leave a completed native publication. Reconcile it with the read-only entry point rather than
retrying publication or interpreting an exception as proof that no output exists.

Completed replay requires native semantic/provenance verification and the new profile checks.
Successful or rejected read-only checks must preserve data, metadata, namespace membership
and file/directory modification times. A missing path is distinguished from inaccessible or
unsafe output. No policy enrollment or execution authority is inferred from native markers.
These checks retain the cooperative single-writer/local-operator trust boundary. They detect
observed mutations and unsafe paths; they are not an OS sandbox, disk reservation, atomic
filesystem snapshot or defense against an arbitrary same-user process racing every check.

## Validation and reproduction

Run the complete focused set on the pinned processing runtime:

```bash
PYTHONPATH=processing python -m unittest discover \
  -s processing/tests -p 'test_narrow*.py' -v
```

The existing CI processing job discovers these tests with Python 3.12.15, Spark 4.0.2,
DuckDB 1.4.5 and JDK 21. The native test classes explicitly skip when PySpark is absent;
such a local result is not native qualification. Pinned CI execution and independent review
are required before merging this inactive slice. CI timeout and test selection are unchanged.

The test evidence has deliberately separate layers:

* Silver pure tests exercise actual filesystem inventories, exact serializers, conservative
  stamp widths, configured-cap overflow from escaped filenames, file/directory limits,
  mutation, unsafe paths and retained prefixes. Their placeholder files are not Parquet
  semantic evidence.
* Content consumer tests use real DuckDB, unchanged native metadata/schema/relational checks
  and exact public projections. The test fixture emits the native-accepted legacy
  `TIMESTAMP_MICROS` representation, verifies physical schema annotations and changes no
  production validation. These direct fixtures are not a Bronze-to-Content production proof.
* Content lifecycle tests explicitly double Spark transformations while retaining real
  native filesystem and SQL evidence. They exercise guard ordering, temporary-write faults,
  rename acknowledgement loss, read-only reconciliation and mutations during the added scan.
* Native Spark tests exercise the additive publishers against the real transformations and
  readback, including accepted and rejected boundary publications, native equivalence and
  Silver's actual shutdown-confirmed qualification. These require the supported CI runtime.
  Empty RSS feeds retain the native `NO_SUPPORTED_ENTRIES` rejection, rather than inventing
  a producer result with both datasets empty.

Exact and plus-one catalog cases use title/summary text that can occur in RSS/Atom, including
legal whitespace, quotes, backslashes and supplementary Unicode. A maximal valid unused
reference row is visited, but cannot approach 60 KiB under the existing reference contract.
Artificial oversized identifiers/arrays remain clearly labeled resource/serializer tests.

Local execution uses Python 3.12.14, DuckDB 1.4.5 and JDK 21.0.13. PySpark and Boto3 are absent.
The unchanged artifact-import regression suite was attempted and refused this unpinned Python
runtime with `RUNTIME_INCOMPATIBLE`; that attempt is not a passing regression suite. No runtime
check was weakened to make it pass, and no local PostgreSQL startup or sandbox workaround
was attempted for this slice.

The final local focused run discovers 80 tests: 67 pass and 13 native Spark tests are skipped
because the dependency is absent. The 67 passing tests comprise 23 new Silver filesystem
tests, 29 new Content scan/lifecycle tests and 15 existing foundation tests. Separate unchanged
Content dependency/reference regressions pass 59 tests, backend units pass 517 tests with no
failures/errors/skips, and CI policy tests pass 45 tests. Repository hygiene, Markdown and
legacy-file hash comparison also pass. These counts do not replace the pending native CI.

## Remaining Gate B0 work

The following remain obligations of the existing issue #49 work, not separate completed or
discarded scope:

1. Establish complete Gold and Content metadata allocations from real maximum outputs and
   all downstream readers. This slice defines no new Gold completion or proof cap.
2. Close complete command, nested proof, reference, observation, recovery/resume and inherited
   evidence envelopes. The provisional budget model does not establish any of these.
3. Bind these new entry points into an explicitly versioned child runtime and fixed ledger
   consumer, with exact stage allocation and authoritative durable admission/recovery rules.
4. Prove reachable original and inherited operation plans, one-recovery-attempt headroom,
   end-to-end interruption/restart behavior and all legacy regressions before dispatch adoption.

No SQL, Java stage adapter, runtime selection, ordinary dispatch or active publication changes
are part of this slice. See [tracked admission](narrow-content-admission.md) for that earlier
inactive boundary, rather than treating a native file inspection as a claim or READY receipt.
