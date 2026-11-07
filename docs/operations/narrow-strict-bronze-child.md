# Inactive strict Bronze child

[ADR 0039](../adr/0039-inactive-strict-bronze-child.md) describes the bounded child-only integration
for original uninterrupted narrow Content work. There is no supported operator command or
production launch route. A valid bootstrap alone is not managed write authorization.

## Retained contracts

- Exact original ledger-v2 identities and only the fixed `STORE_BRONZE/0` slot.
- Completed FETCH journal and original ingestion ID/time, never a new retrieval or legacy history.
- Separate registered Bronze/runtime correspondence and strict child identity.
- Forced one-shot result prefix before any storage request, including read-only GET.
- Only bounded evidence/result publication; command and verified anchor remain parent-owned.
- Native initial-ABSENT-only writer invocation and independent readback verification.
- No ambient credentials/proxies or remote endpoint expansion.

An interrupted child may have completed some or all storage effects. A retained prefix, missing
result or failed child does not prove absence, quiescence, cancellation or permission to repeat.
Do not remove evidence, reuse the slot or adopt the old prefix. No downstream recovery is added.

The existing low-level writer performs a second object check. A matching payload-only pair that
appears after the initial ABSENT inspection can be completed there. Tests must preserve this
specific behavior rather than imply atomic object-store publication or stronger race exclusion.

## Verification requirements

Exercise real child-pipe framing, timeout and EOF, wrong identity/command/runtime/storage facts,
ledger-v1 rejection, changed FETCH/credentials and exact filesystem scope. Confirm zero requests
for invalid startup and consumed prefixes. Cover all native object states, preexisting exact
pairs with zero PUT, absent/partial/ambiguous writes, fresh-client readback mismatch and the
second-check race. Fault injection must cover force/publication failures, concurrent child
invocations, interruption and process death with no restart adoption.

## Local verification

The complete ingestion suite passed **234 tests**, with zero failures, errors or skips.
The new child suite contains 45 cases and the sink suite 14. An independent reviewer reran those
59 cases successfully. Existing FETCH, Bronze, batch and ingestion tests remained in the full run.

The local Java 21 installation lacks `ct.sym`, so Maven used a command-line-only override of the
release setting with source/target 21. No POM or repository build settings changed. A standard
JDK with `--release 21` remains required in hosted CI; the local run is not a substitute for it.
The local invocation used the repository's pinned dependencies and Maven test goal:

```sh
mvn -o -f ingestion/pom.xml -Dmaven.compiler.release= \
  -Dmaven.compiler.source=21 -Dmaven.compiler.target=21 test
```

Also passed: 50 CI-policy tests, 28 local-demo safety tests, Markdown, repository hygiene and
whitespace checks. Exact-head hosted results belong in the PR verification record. This does
not claim managed execution activation, operational availability or full pipeline readiness.
