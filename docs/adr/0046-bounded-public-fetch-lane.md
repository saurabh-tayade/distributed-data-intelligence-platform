# ADR 0046: Separately identified bounded public FETCH lane

Date: November 8, 2026

Status: **Accepted as an implementation contract after independent design review.
Implementation, operational qualification and ordinary-worker activation remain pending.**

## Decision requested and scope

Implement a new strict public-DNS HTTP(S) FETCH launcher and child, using the JDK 21
resolver SPI to pin the actual connection address. Acceptance of this proposal is
not implementation acceptance or ordinary-worker activation. ADR 0045 remains
blocked until this lane and the packaged production API-to-worker path qualify.

Preserve `ReviewedSourceCatalog` v1, RunProfile v2, all historical registrations,
strict launch v1's literal-loopback/testEndpoint=true semantics, native publication
formats, the 64 MiB / 221-entry evidence envelope, 5 MiB payload and <64 KiB startup
packet. No dependency, service, cloud resource, parser replacement, application
retry, continuation, adoption, cleanup authority or recovery capability is added.
The existing supervisor's termination handling remains required, not expanded.

## Evidence and feasibility assessment

Repository observations:

- `ReviewedSourceCatalog` admits reviewed DNS names, not literal loopback. Submission
  freezes endpoint evidence and its digest; startup does not fetch or resolve.
- `StrictFetchLaunchRuntime` and migration V13 require launch version 1 and
  testEndpoint=true. Replacing their checks in place would change accepted meaning.
- `ManagedRssFetch` command protocol 1 already distinguishes public DNS endpoints
  with testEndpoint=false. Its journal version 1 and reopen contract can therefore
  remain unchanged. This does not make its existing transport SSRF-safe.
- `StrictManagedRssFetch` has bounded pipe input, exact command correspondence,
  strict evidence sink and bounded `HttpFeedFetcher`, but no connection pinning.
- `HttpClient` defaults can retry internally. One application `send()` call does
  not prove one request or one connection attempt.

[JDK 21 provider documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/spi/InetAddressResolverProvider.html)
confirms a JVM-wide resolver, chosen lazily by the system class loader's first
service-provider match. Initialization must avoid recursive name lookups. Early
VM lookups can use the built-in resolver. A service descriptor is not proof that
our resolver was selected.

[Configuration API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/spi/InetAddressResolverProvider.Configuration.html)
provides `builtinResolver()`. The provider can capture this delegate without doing
DNS in its constructor or `get(Configuration)`. No reflective JDK-internal access
or custom DNS protocol implementation is needed.

[Resolver API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/spi/InetAddressResolver.html)
provides `lookupByName(String, LookupPolicy)` returning a stream and
`lookupByAddress(byte[])`. The proposed resolution policy explicitly requests IPv4 only.
[InetAddress documentation](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/net/InetAddress.html)
confirms positive, negative and stale caching, and that built-in resolution may
use host configuration/naming services, not exclusively DNS packets.

[HTTP module properties](https://docs.oracle.com/en/java/javase/21/docs/api/java.net.http/module-summary.html)
document retry controls and hostname verification behavior. The proposed safety
contract is one consumed application invocation, not exactly one wire request.
Internal JDK transport retries, if any, must remain on the same sealed address and
within existing request/process deadlines. Optional retry-suppression properties
are implementation-specific defense in depth, not a correctness dependency or a
portable exactly-once guarantee. Do not add them as unqualified launch requirements.
[OpenJDK MultiExchange implementation](https://raw.githubusercontent.com/openjdk/jdk21u/master/src/java.net.http/share/classes/jdk/internal/net/http/MultiExchange.java)
is supporting design evidence only; its moving branch is not the deployment's
pinned source or proof that every reconnect path is covered.

The bounded local feasibility proof used the supplied OpenJDK 21.0.13 compiler,
packaging tools and actual resolver SPI. Its final rerun passed provider packaging,
selected-callback attestation, immutable test-address pinning and isolated transport
checks. It observed original Host/SNI at the pinned
numeric destination, rejection of a trusted certificate for the wrong hostname using
stock hostname verification, no redirect following, and an explicit NO_PROXY selector
defeating a hostile default proxy. The retained local report is
`outputs/public-resolver-proof/README.md`, accompanied by `run.sh`, `run.log`,
`SHA256SUMS`, source/JARs and installed-build disassembly. These are ignored local
design artifacts, not production or distributed test code. No public request was made.
The localhost/literal shortcut findings below limit the guarantee. These component findings do
not qualify production address classification, full-chain composition, a real public
catalog path, or activation. This design acceptance does not qualify implementation or activate ordinary dispatch.

## Identity and wire contract

Add `StrictPublicFetchLaunchRuntime` and main
`platform.ingestion.managed.StrictPublicManagedRssFetch`. Do not repurpose the old
main. Add a dedicated provider JAR containing only the provider/sealing support
and `META-INF/services/java.net.spi.InetAddressResolverProvider`, naming exactly
`platform.ingestion.managed.PublicFetchResolverProvider`. Do not place that service
resource in the shared ingestion JAR. Old launch classpaths exclude the provider
JAR; no fallback/pass-through provider mode exists.

The proposed canonical launch configuration has exactly:

- strictFetchLaunchVersion=2, fetchRuntimeId, fetchRuntimeSha256, runProfileId,
  runProfileSha256, controlBindingId, javaHome, manifest, deploymentSha256,
  testEndpoint=false, networkPolicy, networkPolicySha256.
- manifest has adapter (the new main), javaRuntime and classpath (ordered absolute
  path/sha256 entries, existing count/byte bounds). deploymentSha256 hashes it.
- networkPolicy is a fixed versioned object: policyVersion=1,
  policyId="ddip-public-feed-pinned-v1", resolverProvider (the exact class above),
  maxResolvedAddresses=64, addressPolicyVersion=1, addressPolicySha256 (the snapshot
  digest below), addressFamily="IPV4_ONLY",
  httpVersion="HTTP_1_1", maxApplicationInvocations=1,
  transportRetryPolicy="JDK_BOUNDED_PINNED_V1", redirects=false, proxies=false.
  Fixed JVM switches, cache initialization and TLS rules below are requirements
  of policy version 1, not caller options. networkPolicySha256 hashes canonical
  policy bytes. Runtime/JAR bytes bind its implementation.
- No second endpoint allowlist is copied into the launch profile. Existing reviewed
  catalog/admission correspondence and the frozen selected endpoint tuple/digest
  remain the source authority. Entire launch configuration must fit 64 KiB.

A new immutable registration may reference an existing FETCH runtime-v2 descriptor
with testEndpoint=false, preserving its old ledger-v1 meaning as a descriptor.
Only the new strict launch grants the new network capability. Its copied manifest
substitutes the new main and adds the separately hashed provider JAR; all other
entries must correspond exactly. Old registration rows are never rewritten.
Run/selection/runtime digests remain distinct from launch/policy digests.

Add an additive migration (next available version, never edit V12/V13) with exact
version dispatch for registration, reservation and launch-phase correspondence.
Version 1 branches remain byte-for-byte equivalent in accepted inputs. Ordinary
public work must bind launchVersion=2 before release, and cannot invoke a historical
adapter directly. Registration/storage/run-profile SQL links remain mandatory.

Preserve FETCH command protocolVersion=1, journalVersion=1, candidate/result fields,
Bronze command/readers, strict downstream Content protocol 2 and native formats.
The existing `feedUri`, endpointSha256, runtimeSha256 and testEndpoint=false are
already expressible. The full selected endpoint tuple must hash to endpointSha256
and equal the endpoint evidence frozen through the existing reviewed-catalog
admission path. Recheck endpointId, sourceId and exact URI against that immutable
selection, source revision/configuration and admission digests; matching only a
hostname or trusting a caller-supplied tuple is insufficient. New policy binding is in the run-bound reservation/launch envelope,
not added to native metadata.

Introduce reservationVersion=2, mode="PUBLIC_FETCH_SOURCE_RESERVATION", retaining
operationId/expected/current/fetch and adding `publicFetch` with exactly launchProfileId,
launchProfileSha256, networkPolicySha256, selectedEndpoint and networkPolicy. Child bootstrapVersion=2, mode="PUBLIC_FETCH_SOURCE_CANDIDATE"
contains those same sections plus childPid, with exact unknown/duplicate-key
rejection. No mutable file path supplies policy to the child. Parent re-derives
selected-endpoint/admission correspondence, digests and packet from frozen SQL bytes
before release. Child verifies
policy digest, selectedEndpoint digest/URI/source correspondence and exact command
bytes before DNS. A copied digest without launch-row correspondence is not authority.

Version-aware consumers requiring explicit changes/tests are the reservation SQL
guard and producer, `NarrowContentFetchLaunch.packet` and read-only phase reopening,
launch registration/phase SQL guards, startup selection, the new launch loader and
new child parser. Existing `StrictManagedRssFetch.parse` rejects version 2.
Shared strict evidence validation remains unchanged unless exact schema inspection
finds a necessary version dispatch, in which case enumerate and review it before
implementation. Candidate inventories gain no new files or ledger entries. A
postmortem resolved-IP record is deliberately not promised under this envelope.

## Full-chain composition changes required, not only FETCH

The current strict Bronze profiles cannot compose with the proposed public FETCH
runtime descriptor. Both `StrictBronzeLaunchRuntime.validateRegistration` and
`StrictBronzeVerificationRuntime.validateRegistration` require the FETCH descriptor's
`testEndpoint=true`, and construct version-1 profiles with that same value. These
are source-composition constraints in addition to the separate local storage checks.
Removing those conditions in the v1 branch would silently widen old profiles.

Concrete SQL correspondence blockers are:

- V18 `guard_content_bronze_launch_profile`: fetched.testEndpoint=true at line 65;
  exact strictBronzeLaunchVersion=1/testEndpoint=true object at lines 98–102.
- V19 `guard_content_bronze_verification_profile`: equivalent fetched constraint
  at line 86 and version-1 expected object at lines 119–123.
- V19 `guard_content_bronze_verification_phase`, VERIFY_BOUND: compares launch and
  verifier testEndpoint along with runtime/storage/host bindings at line 185.
- V21 `guard_content_pipeline_profile`: strictContentPipelineVersion=1 at line 113
  and referenced FETCH launch testEndpoint=true at line 150. Its current Bronze
  profile link checks alone do not establish the new three-profile composition.
- `StrictContentPipelineRuntime.validateRegistration` accepts only version 1;
  `load` invokes the old FETCH loader. Startup checks and run-bound pipeline dispatch
  must select the same complete versioned composition, not just a new Fetch main.

The implementation contract includes the following additional immutable composition versions:

1. `strictBronzeLaunchVersion=2` and `strictBronzeVerificationVersion=2` retain
   their respective existing field sets **except** replacing ambiguous testEndpoint
   with sourceTestEndpoint=false and storageEndpointPolicy="LITERAL_LOOPBACK_HTTP_V1",
   and adding fetchLaunchProfileId and fetchLaunchProfileSha256. All these fields
   are exact and mandatory. Both reference the same public FETCH launch-v2 row,
   whose FETCH runtime ID/digest must equal the Bronze runtime's existing FETCH
   descriptor references. Preserve all existing run-profile/control/storage/host
   correspondences. The fixed storage policy preserves literal 127.0.0.1 or [::1],
   HTTP, explicit valid port, no credentials/query/fragment and root-only path.
   This grants no public Bronze endpoint capability.
2. Their native mains remain StrictManagedBronze and StrictBronzeVerifier; their
   strictRuntimeSha256 definitions, native bootstrapVersion=1 and command/result/
   receipt protocols remain unchanged. Their classpaths derive from the historical
   FETCH descriptor manifest, **not** the public launch manifest, and exclude the
   resolver service JAR and public-fetch launch property. Thus local S3 traffic
   cannot inherit the public-only resolver. Storage SDK/configuration is unchanged.
3. `strictContentPipelineVersion=2` retains the existing v1 fields and adds
   fetchLaunchProfileSha256, bronzeLaunchProfileSha256 and
   bronzeVerificationProfileSha256. It requires exactly FETCH launch v2 plus the
   two Bronze composition-v2 profiles above, all cross-links and hashes matching.
   Reject mixed v1/v2 compositions. Keep RunProfile v2, registered runtime-family
   versions and strict native Content command/bootstrap protocol 2 unchanged.
   The pipeline composition version is not a new native execution protocol.
4. Use additive migration replacements with exact v1/v2 dispatch for V18/V19
   registration guards, V18 LAUNCH_BOUND and V19 VERIFY_BOUND phase checks, and V21
   pipeline-profile guard. For v2, Bronze LAUNCH_BOUND must also match the actual
   predecessor FETCH operation's LAUNCH_BOUND ID/digest to the registered public
   FETCH link; VERIFY_BOUND must compare the new source/storage policy and FETCH
   link fields across launch/verifier profiles. V21 pipeline-operation checks must
   retain actual predecessor/profile correspondence, including the new hashes.
   Neither registration nor common runProfileId alone proves that relationship.
   V1 branches retain their existing inputs and exact semantics.
5. Version-dispatch the two Bronze Java registration loaders/startup checks,
   StrictContentPipelineRuntime registration/load and NarrowContentWorkerStartup,
   plus NarrowContentPipeline's fetch-launch selection. Existing Bronze packet
   construction/reopening in NarrowContentBronzeLaunch and
   NarrowContentBronzeCompletion must be tested with composition v2 even though
   their bytes are intended to remain unchanged. V17 Bronze reservations, V20
   Silver reservations and their exact descriptor inventories must be regression
   tested with testEndpoint=false FETCH descriptors and unchanged runtime-family
   schemas. If those checks expose additional fixed values, stop and enumerate
   the required versioned change rather than weakening an existing branch.

Add SQL rejection tests for mixed versions, wrong predecessor, public storage,
missing/mismatched FETCH launch digest, copied policy fields, and a verifier linked
to a different public launch. Assert all old profile tests unchanged. Until these
composition changes qualify, successful public FETCH alone cannot imply an executable
Bronze-to-Content chain. The new profiles reuse existing evidence slots; configuration
and startup-packet byte ceilings still apply. This proposal is broader than a Fetch-only
launcher patch, but does not grant downstream network or recovery authority.

## Historical/read-only closure and interrupted public attempts

Success-path dispatch is insufficient. The following existing readers must dispatch
on the stored, independently validated versions without accepting arbitrary fields:

- `NarrowContentFetchReservation.inspect` and `reconstruct` currently use `build`
  to reconstruct exact reservation-v1 bytes. Introduce an exact v2 historical builder
  deriving publicFetch from independently registered immutable launch/policy evidence
  and the selected endpoint frozen through reviewed-catalog admission. Preserve canonical-byte equality,
  command/bootstrap hashes, operation/checkpoint identity, expected/current writer
  checks and bounds. Never accept stored bootstrap claims as their own validation.
  A consumed public reservation can exist before LAUNCH_BOUND: its referenced launch
  row must be reconstructible even at that prefix, with subsequent phases required
  to match it. Do not require a later phase to inspect an earlier valid prefix.
- `NarrowContentFetchLaunch.inspect` reconstructs RELEASE_AUTHORIZED's packet with
  `packet(reservation,pid)`. Make that conversion exact-version-aware and compare the
  recomputed bootstrap hash, child identity and existing phase prefix as before.
  Preserve v1 byte output and reject version/policy/launch substitutions. Historical
  reconstruction is digest material only, never permission to write another pipe.
- `NarrowContentFetchCandidateVerification.facts` currently calls old
  `StrictFetchLaunchRuntime.validate` (line 83). Select pure registered-correspondence
  validation for launch v1 or v2. For v2, recheck exact frozen endpoint/admission correspondence and
  policy/digest links without resolving DNS or requiring the old installed runtime
  still exists. Installed-byte readiness is a live-launch requirement, not a reason
  to reject otherwise valid historical evidence. Preserve candidate inventory,
  payload/metadata/journal validation and exact command comparisons.
- `NarrowContentPipelineRecoveryAssessment.validateProfile` (lines 184–190) currently
  accepts only the pipeline-v1 field set/version. Add the exact composition-v2 set
  and cross-profile digest checks described above, while preserving runtime/module,
  owner/attempt, operation ordering, predecessor, result and receipt comparisons.
  Thread version dispatch through historicalSqlDigest and retained snapshot readers.
  The historical digest must cover every new composition/policy link via validated
  canonical bytes/digests; never omit new fields to make the old digest fit.
- `NarrowContentTerminalEvidence` derives its history digest through that assessment
  (line 204). Requalify existing safe-end snapshot/receipt/recheck consumers with
  public histories, including applicable SQL receipt correspondence. No new terminal
  schema version is assumed necessary solely because the opaque history hash changes;
  if an exact schema or algorithm change is necessary, enumerate/version it before
  implementation. Do not weaken snapshot equality or transfer live owner/session
  authority from reconstructed history.

Add interrupted public-attempt coverage at consumed reservation, LAUNCH_BOUND,
RELEASE_AUTHORIZED, ambiguous child exit, complete FETCH candidate, Bronze launch/
verification and downstream partial pipeline prefixes. Assert deterministic historical
reopening, exact digest stability, existing eligibility/failure outcomes and retained
occupancy where evidence is insufficient. Exercise existing safe-end assessment and,
only where already authorized/eligible, its guarded finalization; a changed policy,
wrong profile version/digest, changed payload, missing phase or live/uncertain child
must retain the existing rejection semantics. Run v1 safe-end regressions unchanged.
These tests grant no retry, recovery adoption, continuation, cleanup, publication or
new safe-end capability. Read-only reinspection performs zero DNS/HTTP requests and
never restarts a child. Ordinary activation remains blocked until this closure is
qualified as well as the successful chain.

### Authority isolation for existing FETCH-only recovery

Shared readers accepting public v2 history must **not** make that history eligible
for old FETCH-only mutation paths. Deferred issue #124 is not advanced by this lane.
Explicit v1-only eligibility is required in preparation of a **new**
`NarrowContentFetchRecoveryHandoff` before mutation or executable authority is produced. Require the historical reservation/launch/composition versions and
original test-endpoint constraints, not only a successful shared inspection.

Apply the same new-handoff restriction through an additive migration to V15
`guard_content_fetch_recovery_handoff`, including direct SQL insertion. Its current
phase/checkpoint checks at lines 70–112 do not establish launch version. Java
Handoff.read (lines 240–251) currently rejects Bronze/legacy history before calling
generalized FetchCompletion.inspect; preparation must additionally prove reservation-v1
and launch-v1/testEndpoint=true. Keep historical V15/V16 receipt reads, binding
checks and already-created-handoff finalization semantics unchanged: V16 trusts the
qualified V15 handoff, so this proposal does not redefine old terminal receipts.
Review the new-entry gate independently; never rely on Java-only exclusion.
Public originals use only the existing ADR 0044 full-Content safe-end capability,
with its original eligibility restrictions, not a new FETCH-only recovery route.

Tests must prove public v2 histories are readable but rejected by FETCH-only handoff
entry, both through Java and direct SQL, before Bronze exists
as well as after an interrupted FETCH. Assert no new attempt/control/finalization,
no ownership transfer or coordinator release, and unchanged evidence. Also retain
v1 FETCH-only recovery regressions and eligible full-Content safe-end tests. Do not
remove these exclusions merely because historical reader tests pass.

### Metadata discovery

`DiscoveryRepository.CONTENT_PROFILES` currently filters strict pipeline-v1 profiles.
Add exact composition-v2 eligibility for metadata discovery, with registered digest/
cross-link validation matching the new profiles. This remains metadata-only: no DNS,
process launch, storage probe or claim that registration proves live readiness.
Startup/run-bound checks remain independent and mandatory; mixed or incomplete
compositions are not advertised as eligible.

## Resolution, sealing and connection contract

1. A fresh child accepts only the bounded EOF-terminated startup pipe and fixed
   launch property `-Dddip.publicFetch.policy=ddip-public-feed-pinned-v1`. It refuses
   missing/other values. Property presence alone is not authorization: command,
   lease/ownership, ledger, frozen input and launch correspondence remain required.
2. Before any InetAddress/HttpClient use, establish security cache TTLs of zero
   for positive, negative and stale results. These are Java security properties,
   not ordinary `-Dnetworkaddress.cache.ttl` switches. Do not replace the system
   security configuration or trust store. Exact-build tests must prove initialization
   occurs before cache policy initialization. No javaagent, JAVA_TOOL_OPTIONS,
   JDK_JAVA_OPTIONS, CLASSPATH, inherited proxy/TLS overrides or hosts-file override.
3. Only after existing forced result prefix/one-shot operation consumption,
   validate packaged service resources and enumerate provider types without
   constructing extra providers: exactly our provider is allowed. Force lazy
   initialization with the selected hostname while SEALED=false. The installed
   resolver must reject that lookup and publish an in-process attestation from its
   actual `lookupByName` callback. Class presence or constructing a provider manually
   is not attestation. If callback/identity/state mismatch occurs, abort before any
   HTTP. Initialization must neither recurse into InetAddress nor call the built-in
   delegate. A missing provider must fail resource validation before this operation.
4. Call a package-private `resolveAndSeal(host)` exactly once. State is monotonic
   UNSEALED -> RESOLVING -> SEALED, or permanently FAILED. General resolver callbacks
   reject during UNSEALED/RESOLVING. Only this method may call the captured built-in
   delegate once, explicitly requesting IPv4 A-address results only. Collect at most 65 results: empty,
   over-64, malformed, scoped, or any forbidden address fails the entire set.
   Close the stream. No separate five-second DNS cutoff is introduced. Resolution
   occurs only inside the fresh supervised child, never parent startup or a parent
   resolver executor. The existing 45-second FETCH child supervision deadline is
   the outer process bound, followed by existing bounded termination handling.
   A hung OS resolver may ignore thread interruption; if process termination is
   uncertain, retain occupancy/recovery-required status. A late result never grants
   another invocation or bypasses stopping/deadline fences. No guaranteed kernel-call
   cancellation or precise instantaneous process kill is claimed.
5. Address policy is the exact candidate source snapshot
   [0046-public-fetch-ipv4-deny-snapshot.json](../architecture/0046-public-fetch-ipv4-deny-snapshot.json),
   retrieved November 8, 2026 from the official IANA special-purpose and multicast
   registries linked inside it. SHA-256 of its exact UTF-8/LF file bytes is
   `85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd`.
   The companion .sha256 file provides the same checksum. All 25 special-purpose
   rows are retained (26 CIDRs because one row lists two addresses), plus 224.0.0.0/4
   for all multicast: 27 CIDRs total. No overlaps are merged, no terminated entry
   removed, and no globally reachable exception allowed. The special-purpose
   240.0.0.0/4 and 255.255.255.255/32 rows cover reserved/broadcast space explicitly.
   This is a checked-in design snapshot, not a live registry download or an activated
   production resource. Independent design review is complete; implementation and
   boundary qualification remain required. networkPolicy.addressPolicySha256 binds these exact snapshot
   bytes; no new ledger file or startup-packet copy of the table is introduced.
   All IPv6 results are rejected; there is no AAAA or IPv6 fallback. Classify actual
   bytes and reject unexpected address types before pinning. Native normalization
   can lose mapped-answer representation: an actual Inet4Address gets full IPv4
   checks, not a DNS wire-form provenance guarantee. isSiteLocalAddress alone is
   insufficient.
6. Sort the validated unique IPv4 addresses by unsigned bytes;
   choose exactly one. Seal an InetAddress made from numeric bytes with the original
   hostname (`getByAddress(host, bytes)`, no resolution). No alternate address or
   family fallback. Subsequent lookupByName accepts only the canonical selected
   host and compatible lookup-family policy and returns only that address. It
   never calls the built-in resolver again. All other/reverse **provider callbacks**
   throw. This is not a blanket restriction on every InetAddress API call: the
   supplied JDK 21.0.13 invokes the SPI for localhost, but after that callback
   throws, InetAddress can apply its special fallback to 127.0.0.1. Numeric-literal
   parsing can bypass provider callbacks through a different path. Therefore the frozen, reviewed public
   URI check and absence of any other target operation in the qualified child are
   essential parts of the safety boundary. Never admit localhost/literal URIs on
   the assumption that the provider will block them.
7. Construct a fresh HttpClient only after sealing. Use original DNS URI unchanged,
   HTTP_1_1, NO_PROXY selector, Redirect.NEVER, no authenticator/cookies, no custom
   Host header, no permissive SSLContext, normal certificate chain/hostname checks
   and original-host SNI. Reject disableHostnameVerification and custom trust-store
   overrides. Use the qualified JDK's default JSSE behavior under the existing
   trusted-host/runtime assumptions, cleared child environment and fixed launch
   arguments; no caller-supplied JVM, proxy or TLS options. This proposal adds no
   whole-JDK integrity subsystem or production hashing of lib/modules, security
   configuration or trust stores. Extra hashes in the isolated feasibility proof
   are diagnostic evidence only. No new OCSP/AIA configuration or network retrieval
   feature is introduced; qualify the selected runtime's default behavior.
8. One application invocation of GET through the existing bounded HttpFeedFetcher;
   no application HEAD/probe, redirects, linked content or error-triggered refetch.
   JDK internal reconnect/retry behavior is observed and documented on the qualified
   build, not described as exactly-once delivery. Every attempted connection must
   use the same sealed public address: no re-resolution, alternate address, authority
   change or proxy, and no extension beyond existing deadlines. Tests must observe
   destinations and request counts under faults, but multiple pinned wire attempts
   alone do not violate the no-application-retry contract. A failed or interrupted
   invocation may already have reached the server one or more times; retain that
   ambiguity and never start another invocation to disambiguate.
   Connection timeout remains <=10 seconds, request <=30 seconds, existing child
   supervision remains the outer bound. Body <=5 MiB including error bodies, with
   unchanged journal semantics. Existing HTTP framing tests must remain green.

This is a cooperative, pinned application boundary, not an OS egress sandbox or a
promise against malicious installed bytecode/native code or hostile host routing.
The OS resolver may use locally configured services and multiple DNS packets/CNAME
resolution internally; “one resolution” means one delegate call, not one DNS packet.
A public numeric destination does not prove physical routing avoids private networks.
Such a stronger network guarantee needs separately approved containment.

## Deterministic qualification and production-path acceptance

No public requests are necessary for the first gates:

- Pure vectors cover every denied CIDR boundary, accepted neighbors, empty/mixed
  sets, >64 entries, duplicates, ordering, unexpected IPv6/mapped/translation addresses and
  failure-terminal sealing. Freeze the address table and its digest in test evidence.
- Fork fresh child JVMs for service packaging: missing/duplicate/foreign providers,
  incorrect property/main, classpath mutation, premature lookup, other host, reverse
  lookup, seal twice, concurrent lookup/timeout and stale/negative cache poisoning.
  Use test-only resolver delegates, absent from production JARs; count delegate calls.
- Isolated test transport fixtures prove original Host/SNI, certificate mismatch
  rejection, redirects never followed, explicit proxy refusal despite hostile
  defaults, rebinding attempt never reaches the replacement address, and observed
  wire-request counts/destinations on preconnect failure, accepted-then-closed socket, dropped response,
  malformed response, timeout and interruption. Assert zero application refetches,
  no other destination and bounded termination, not an unsupported single-wire-attempt
  guarantee. Those synthetic loopback fixtures
  are component evidence only and cannot authorize a production catalog bypass.
- Re-run byte-bound/EOF/bootstrap, command correspondence, SQL race/fence, source
  mutation, installed-byte replacement, result/journal, payload overflow and all
  existing v1 loopback tests unchanged. Prove old main rejects new packet and old
  launch never gains DNS authority; wrong policy/digest/selected endpoint cannot release.
- Separately invoked, opt-in positive packaged acceptance uses real PostgreSQL,
  the production source catalog loader,
  API submission, ordinary worker and all native children with a reviewed public
  DNS endpoint. Preferred future fixture: tiny synthetic Atom bytes at a fixed
  commit URL on raw.githubusercontent.com in the already-public project repository,
  exact expected payload hash and known downstream shape. No publication is authorized
  by this ADR. Pin actual URL/commit/hash only after coordinated publication.
  The endpoint requires no credentials/query, returns directly without redirect,
  and stays within existing payload and native content bounds. ADR 0002's accepted
  default-offline and opt-in-Failsafe-loopback-only contract, plus ADR 0001 and the
  RSS ingestion contract, remain unchanged. A public positive smoke requires a
  separately opt-in qualification entrypoint: do not add it to normal CI or repurpose
  the existing loopback-only Failsafe profile without an explicit contract amendment,
  even though its payload is synthetic and immutable.
- That fixture makes content deterministic, not network availability. External DNS,
  TLS trust, GitHub availability/rate limits remain dependencies. A failed attempt
  is retained and reported; no automatic application retry/refetch/skip promoted to success.
  Distinguish availability failures from implementation failures. Do not claim NASA,
  Guardian or xkcd feed-shape support from a synthetic Atom success. Mutable live
  feeds are a worse deterministic gate; a new hosted fixture service adds unnecessary
  infrastructure authority. Loopback acceptance alone leaves the production gate open.

Independent review must examine exact registered/installed runtime and wire captures,
not only mocks. Final exact-head deterministic CI, old-profile regressions and the opt-in production
fixture result must all be observed before ordinary public activation is described
as qualified. An unreachable fixture does not relax acceptance.

## Alternatives and forecast

Plain preflight DNS followed by ordinary HttpClient is rejected: it can resolve again.
Rewriting HTTPS URI to an IP plus a Host header is rejected: it breaks the default
TLS identity path and invites unsupported header/TLS workarounds. A custom SSLSocket
plus hand-written HTTP parser would require correct framing, interim responses,
chunking, trailers, truncation, limits and timeout handling; that exceeds this small
lane and duplicates the existing client. A mature transport with supported custom
DNS could simplify pinning but adds a dependency and needs separate approval. OS
network containment is a different authority/deployment decision.

The selected proposal is IPv4-only, conditional on reviewed source hosts having
usable public A records and the deployment having IPv4 egress. The currently named
hosts `www.nasa.gov`, `www.theguardian.com` and `xkcd.com` need that prerequisite verified.
The coordinator's AF_INET getaddrinfo diagnostics returned gaierror for all three
in the managed workspace. That leaves native DNS/IPv4 egress unverified; it is not
evidence that those hosts have no A records. Separate proxy-backed public DNS
metadata observations support current A-record availability: NASA resolves through
nasa-gov.go-vip.net to 192.0.66.108; Guardian to 151.101.{1,65,129,193}.111;
xkcd to 151.101.{0,64,128,192}.67. These are diagnostic observations, never runtime
IP allowlists or hardcoded connection addresses. Sources are the Google DNS JSON
A-query responses for [NASA](https://dns.google/resolve?name=www.nasa.gov&type=A),
[Guardian](https://dns.google/resolve?name=www.theguardian.com&type=A) and
[xkcd](https://dns.google/resolve?name=xkcd.com&type=A). Proxy-backed metadata reads
do not qualify direct child egress. Before activation, the actual public child must prove
DNS/TLS/IPv4 egress over a permitted route. No production read was performed here.
The immutable fixture host needs the same prerequisite check. IPv6-only hosts are unsupported and fail closed. A future dual-family lane
would need a separately reviewed address-policy/version change and qualification;
this design does not authorize silently enabling AAAA fallback.

Planning estimate, not measured throughput or a commitment: public lane implementation,
SQL/wire closure and independent network qualification 45–80 engineering hours;
ordinary worker wiring and packaged API/queue/failure acceptance 25–45 hours after
that. This lane plus narrow ordinary activation is roughly 70–125 hours. These
are provisional remaining-work estimates, with no reliable consumed-hours baseline.
Broader controls/recovery/UI work is not assessed by this design and is not authorized
by it. Hidden transport behavior, address-policy qualification, provider/cache
initialization or production-fixture failure could invalidate this approach and
requires re-estimation, not stretching the budget or quietly weakening policy.
