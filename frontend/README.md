# Content Intelligence dashboard — M6.3 and M6.5

The React/TypeScript application provides Publisher Overview, Daily Activity, Data Quality and Article Explorer over the existing [M6.2/M6.4 Content API](../services/content-api/README.md). M6.5 adds the three analytical views without backend capabilities, direct Gold access or changes to the Python verification boundary. [Accepted ADR 0005](../docs/adr/0005-gold-consumer-integration.md) and the [analytical contracts](../docs/architecture/analytics-api-contracts.md) remain unchanged. The implementation is ready for the remaining browser acceptance and final M6 end-to-end closeout; M6 is not marked complete.

## E5-F2a.1 — Operations foundation and Sources list

The same application now has Analytics and Operations workspace links. Open
`http://127.0.0.1:5173/#/operations/sources` for the read-only Sources list, or
`#/analytics` for the existing dashboard. Empty hashes and existing analytical table anchors
remain supported. Browser Back/Forward switches workspaces; unknown routes show a navigation
fallback. Analytics filters and its publication pin survive workspace switches, including a
latched publication-change error. Only the visible workspace fetches; a blocked Analytics
session does not block Operations.

The list calls only `GET /api/management/v1/sources`. It shows source/product identity,
current revision, admission gate, exact decimal row version, configuration digest and UTC
timestamps. Product/enabled filters and 20/50/100-row pages use opaque signed server cursors.
Previous/Next fetch one page at a time; filter/page-size changes and Refresh sources start
a fresh traversal. There are no invented totals or background polling. Rows are current
projections, not a snapshot of mutable fields across all pages. Invalid/repeated cursors,
out-of-scope or malformed rows fail closed; a rejected cursor offers an explicit restart.
Obsolete responses cannot replace the current query, even if the transport ignores abort.

Operations needs the existing local **`api` role**, compatible management PostgreSQL schema
and configured installation; the default `analytics` role does not expose management routes.
Use the existing [management setup](../services/content-api/README.md) and
[F1 contract audit](../docs/operations/e5-f1-operations-ui-audit.md), adding the actual UI/API
origins to the approved startup configuration. For the default development proxy:

```text
--ddip.role=api
--ddip.management.origins=http://127.0.0.1:8080,http://127.0.0.1:5173
```

These are flags for an already configured API invocation, not a database/bootstrap command.
Vite retains the browser Host header; an unapproved UI authority is rejected even on GET.
Preview uses port 4173 and needs its own approved origin. No CORS/security bypass, backend
change or new dependency is introduced. Operations can work without a configured Analytics
publication. Mutations, source details/revisions, execution pages, imports and recovery actions
are outside this slice; enabled admission never implies ordinary Content dispatch is active.

The management client uses its own typed envelope, bounded parsers and safe errors, separately
from Analytics publication validation. Loading, an empty catalog, filtered-empty results, local
boundary rejection, management unavailability, network failure and invalid responses have distinct
presentations. Only recognized public error codes and validated correlation IDs are displayed;
server/proxy messages and private diagnostics are never rendered. Semantic tables, labeled filters,
keyboard paging, area-specific skip links, route focus, horizontal overflow and responsive layout
reuse the existing visual conventions. F1 remains an unchanged historical audit/planning artifact.

F2a.1 local validation used Node.js **24.22.1** within the existing supported Node 24 range;
the runtime/version files and dependency pins were not changed. Type checking, ESLint,
**250 frontend tests** (184 existing plus 66 new), and production build passed. Tests use
mocked HTTP responses and cover protocol rejection, exact values, composite source identity,
paging/filter resets, cursor failures/cycles, late responses, StrictMode, workspace history,
keyboard controls, Analytics pin isolation and existing analytical table anchors. The build
retains the existing 500 kB chunk warning: the main chunk is now **621.49 kB minified /
185.19 kB gzip**; no threshold was raised. Backend/Spark/Docker suites were not run.

The browser inventory reported that its access host was unavailable. Live management API,
desktop/narrow browser rendering and full screen-reader acceptance remain pending; mocked DOM
tests are not those runtime checks. Repository Markdown/link/hygiene and whitespace checks
are recorded with the implementation handoff. The owner handles commits, pushes and PR updates.

## Acceptance and design

M6.5 acceptance, recorded before implementation: preserve all publisher tests and functionality; load only the active analytical view; pin one publication across views and permanently block results after a current-response identity mismatch until explicit reload. Daily/quality must use exact source and inclusive UTC ingestion-date filters. Article pages must come from the server, preserve order, reset on source changes and obey `hasMore` and offset bounds. Counters remain exact, distinct daily counts are never aggregated as global identities, and quality rates are never presented as percentages. Untrusted article metadata is rendered as text with explicit HTTP(S)-only navigation. Synthetic automated coverage and retained-data/browser evidence are recorded separately; M6 closeout remains a separate review.

Acceptance requires exact publisher counts and scoped totals, stable publication identity across filter changes, strict API validation, clear loading/error/empty states, accessible controls and an exact text alternative to the chart. Tests must run without Python, Spark, SeaweedFS, AWS, a backend or retained Gold data. Live browser/API checks are recorded separately from isolated tests.

The dashboard uses a navy, teal and warm-neutral palette with system fonts, a full publication fingerprint, exact publisher selector, four summary cards, grouped comparison bars, metric definitions and a detail table. It adapts to smaller screens with two-column cards, a stacked chart/guide and a keyboard-focusable horizontally scrollable table. Focus styles, a skip link, semantic table headers, labeled controls, live status messages, non-color chart labels and reduced-motion styling are included. A table remains the authoritative textual alternative to the visualization.

Observations mean **accepted content observations**, not views, clicks or engagement. Articles without a publication timestamp are not warning counts. No trends or global totals are invented. The quality view displays only the supplied warning-codes-per-observation rate.

## Runtime and dependencies

Use **Node.js 24.15+ within major 24**; local checks used **24.19.0**, pinned in `.nvmrc`. `package.json` and `package-lock.json` are independent of the root documentation tooling. Direct versions are exact; install the lockfile with `npm ci`.

| Dependency | Version | Purpose |
| --- | --- | --- |
| React / React DOM | 19.3.1 | Rendering and local component state |
| Recharts | 3.10.1 | Publisher, daily and quality visualizations |
| react-is | 19.3.1 | Required Recharts peer, aligned with React |
| TypeScript | 6.0.3 | Strict static checking |
| Vite / React plugin | 8.3.4 / 6.1.2 | Local proxy, development server and static production build |
| Vitest | 5.0.5 | Isolated tests |
| React Testing Library / DOM / user-event / jest-dom | 16.3.3 / 10.4.2 / 14.6.8 / 7.0.1 | User-oriented component assertions and interactions |
| jsdom | 30.1.3 | Test DOM; requires Node 24.15+ on this major |
| ESLint / @eslint/js | 9.39.5 | Compatible lint baseline for the accessibility plugin |
| typescript-eslint | 8.71.2 | TypeScript lint rules; supports TypeScript below 6.1 |
| React Hooks / JSX accessibility lint plugins | 7.1.1 / 6.10.2 | Hook and accessibility checks |
| React / React DOM / Node types | 19.3.0 / 19.3.0 / 24.19.2 | Development-only type definitions |

Versions, engines and peers were checked against npm registry metadata before installation on October 18, 2026, alongside the [Vite guide](https://vite.dev/guide/), [Recharts guide](https://recharts.github.io/en-US/guide/) and [Vitest guide](https://vitest.dev/guide/). TypeScript 7 and ESLint 10 were not selected because the current TypeScript/accessibility lint peers do not support them together. npm labels ESLint 9 deprecated; this is a deliberate development-tool compatibility limitation to revisit when the accessibility plugin supports ESLint 10. No forced peer overrides are used.

There is no application Redux store, global state framework, UI framework, router dependency, SSR, authentication infrastructure, external font or image service. Recharts may bring internal state dependencies transitively; application state remains React hooks. No additional runtime validation library is needed for this small fixed contract.

## Local startup

First start the existing API from the **repository root**, following the [M6.2 procedure](../services/content-api/README.md#local-startup):

```sh
python3.12 -m venv outputs/analytics-python312/.venv
outputs/analytics-python312/.venv/bin/python -m pip install -r processing/requirements-analytics.txt
mvn -B -ntp -f services/content-api/pom.xml verify
export ANALYTICS_PYTHON_EXECUTABLE="$PWD/outputs/analytics-python312/.venv/bin/python"
export ANALYTICS_PUBLICATION='<exact-completed-publication-fingerprint>'
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

Replace the fingerprint placeholder; no latest-publication selection exists. The API defaults to `127.0.0.1:8080`, `processing/gold_analytics.py` and `outputs/gold`, with paths resolved from its startup directory. An unconfigured API can be live while analytical queries correctly return HTTP 503.

In another terminal, from the repository root:

```sh
npm ci --prefix frontend
npm run dev --prefix frontend
```

Open `http://127.0.0.1:5173`. The browser requests relative `/api/v1/publishers`, `/daily`, `/quality` and `/articles` URLs under the same `/api/v1/` prefix. Vite forwards `/api/` to `http://127.0.0.1:8080`, retaining paths and query parameters. No backend CORS change is required. Development and preview bind to loopback and use strict ports.

To use another local API port, set a server-only proxy variable:

```sh
CONTENT_API_PROXY_TARGET=http://127.0.0.1:18083 npm run dev --prefix frontend
```

Alternatively copy `frontend/.env.example` to ignored `frontend/.env.local` and edit the target. Restart Vite after changing it. This variable is consumed by Vite, not bundled into browser code; no credentials or Gold paths belong in frontend variables. Stop the API and Vite with Ctrl-C.

For a local production-build preview:

```sh
npm run build --prefix frontend
npm run preview --prefix frontend
```

Open `http://127.0.0.1:4173`. Preview uses the same configurable proxy for local verification. `dist/` is a static bundle; an actual deployment must serve `/api/` on the same origin through deployment routing. Vite's development/preview proxy is not a production hosting service. No hosting or cloud deployment is introduced here.

## Structure and component responsibilities

| File | Responsibility |
| --- | --- |
| `src/api.ts` | Four REST contracts, shared primitive/envelope validators, report-specific validation, supported query construction and safe errors |
| `src/useAnalytics.ts` | Replaces the publisher-only hook with one active request, source/date/page state, stale protection and one latched publication identity |
| `src/metrics.ts` | Exact BigInt aggregation/formatting, additive daily grouping and chart conversion guards |
| `src/App.tsx` | Compose shared shell/navigation, source filter and on-demand views |
| `src/views/PublisherOverview.tsx` | Preserved publisher content, chart, metrics and table |
| `src/views/DailyActivity.tsx` | Date visualization, additive scoped totals and complete source/day table |
| `src/views/DataQuality.tsx` | Exact counts, supplied per-source rates, comparison bars and table |
| `src/views/ArticleExplorer.tsx` | Server page navigation, safe article list and native expandable details |
| `src/articleLinks.ts` | Strict explicit HTTP(S) navigation check; no fetching or URL rewriting |
| `components/DateFilters.tsx` | Draft date form, validation, explicit apply/clear and applied-range display |
| `components/ReportParts.tsx` | Shared bounded notices, scoped summaries, overflow tables and chart fallback |
| `components/Timestamp.tsx` | Shared UTC microsecond and null display |
| `components/AppShell.tsx` | Header, main landmark, skip link and footer |
| `components/PublicationIndicator.tsx` | Full pinned fingerprint and initial waiting state |
| `components/PublisherFilter.tsx` | Exact source selector and reset control |
| `components/SummaryCards.tsx` | Publishers, articles, observations and missing-publication-date counts for returned rows |
| `components/PublisherComparisonChart.tsx` | Grouped bars, legend, unsafe-number fallback and table link |
| `components/PublisherTable.tsx` | Exact counts, full publisher IDs, microsecond UTC timestamps and null labels |
| `components/States.tsx` | Loading, sanitized error/retry and empty states |
| `src/styles.css` | Responsive layout, typography, focus and reduced-motion rules |
| `src/test/` and `src/*.test.*` | Deterministic synthetic fixtures and isolated tests |

Plain `fetch` and one app-owned hook cover these four fixed reports without a global store or query framework. New views are lazy-loaded with React `lazy`/`Suspense`; only the active view issues an analytical request. Navigation uses ordinary keyboard-operable buttons with `aria-current`, without a routing dependency. Source selection is shared across views. Dates are shared only between Daily Activity and Data Quality; article offset is reset whenever source selection changes. Navigating away aborts the active request and unmounts its view. Returning requests fresh data; there is no result cache, hidden prefetch, polling, automatic retry or persistence across reloads.

## Views and API integration

| View | Request | Behavior |
| --- | --- | --- |
| Publisher Overview | `GET /api/v1/publishers?limit=100[&sourceId=...]` | Existing four KPIs, comparison chart and exact publisher table |
| Daily Activity | `GET /api/v1/daily?limit=100[&sourceId=...][&from=...][&to=...]` | Returned-source observations by UTC ingestion date, additive totals and all source/day metrics |
| Data Quality | `GET /api/v1/quality?limit=100[&sourceId=...][&from=...][&to=...]` | Exact observation/code totals, per-source codes-per-observation chart and table |
| Article Explorer | `GET /api/v1/articles?limit=10&offset=0[&sourceId=...]` | Server-side pages, selected article metadata, exact counts and expandable details |

The bracket notation describes optional parameters, not literal request syntax. Requests use `URLSearchParams`, `AbortSignal`, relative same-origin paths, `cache: no-store` and no credentials. API errors retain the established safe classifications. No publication, SQL or filesystem request parameters are introduced.

The selector prefers the first successful unfiltered publisher inventory and retains it across views/filters. If that initial request was cancelled before completion, returned source IDs from another unfiltered report provide a labeled temporary inventory. Visiting Publisher Overview supplies its normal bounded inventory. API source syntax is `[a-z0-9][a-z0-9_-]{0,127}`; IDs outside that syntax are displayed as text but not selectable. No wildcard search or extra catalog endpoint is invented. The selector cannot discover sources outside returned rows.

### Date filtering and daily meaning

From/To are optional strict `YYYY-MM-DD` values, inclusive UTC **ingestion** dates. Either bound can stand alone. The form submits only on Apply dates; invalid calendar dates, year zero and reversed ranges are rejected before fetching. The displayed applied range stays explicit while the draft is edited. Clear dates removes both bounds. Neither publisher nor article requests receive dates.

Daily rows retain API order (`sourceId`, then `ingestionDate`). Observations are additive across returned source/day groups and grouped exactly with BigInt for the chart. No absent date is filled with a zero, and a one-date response is shown as one date. Daily distinct article/ingestion counts remain in their original groups; there is no global distinct sum. Chart text alternatives include exact per-date totals and the full source/day table.

### Quality meaning

`qualityWarningCount` counts codes on all accepted observations in the selected date scope. `warningCodesPerObservation` is the API's finite nonnegative numeric rate, may exceed 1.0, and is never formatted as a percentage. The UI sums only exact observation and code counters; it does not sum, average or invent a combined rate. Chart/table rates remain per source. A missing article publication time is not automatically a warning.

### Article pagination and untrusted content

The modest page size is 10. Next requests `meta.offset + meta.returned` only when `hasMore` is true and the next offset is at most 1,000,000. Previous requests the preceding page and is disabled at offset zero. Both controls are disabled while loading; Previous remains available after an empty/error page above zero. Source changes reset offset to zero atomically. The client validates matching limit/offset, returned length and boolean `hasMore`, including rejecting a short page that claims more rows. There is no local slicing, invented page count or global article total. `hasMore` is authoritative even for a full final page. Order remains `lastObservedAt DESC, articleId ASC`.

Cards show title/source, article observation and distinct-ingestion counts, last-observed and publication times. Native `details`/`summary` reveals the unchanged identity, raw summary/URL text, update/first-observed times and selected-observation warning codes. These codes are not historical article warning totals. Nulls display **Not available** and empty values are not rewritten.

Title and summary render only as normal React text. No HTML interpretation, embedded images, remote previews or automatic article fetches occur. Explicit new-tab navigation is allowed only for an absolute, parseable `http://` or `https://` URL with a hostname. Other schemes, relative/prefix-guessed URLs, malformed URLs, whitespace/control characters, backslashes and credentials in authority are rejected. Allowed href text is preserved; the browser applies its ordinary navigation parsing. Links use `target="_blank" rel="noopener noreferrer"`. Unsafe/null URLs leave a plain title. Long text wraps and full summaries remain available inside expandable details.

## Runtime validation, precision and publication identity

The client validates exact envelope/row fields, fingerprint format, requested limits/source, returned length, pagination metadata, required/nullable primitives, canonical counters, warning lists and timestamp/calendar validity. Publisher/quality rows must be in binary UTF-8 source order; daily rows must be in source/date order and within the requested date range; article rows must be unique and correctly ordered. Quality responses contain no date column, so their date filtering remains authoritative in Python; the client validates and transmits the requested bounds. Invalid or partial responses never render as successful data.

Counters are decimal strings in the API's signed 64-bit range. BigInt sums remain exact beyond that range, and `Intl.NumberFormat` formats BigInt directly. Publisher/daily chart conversion happens only within `Number.MAX_SAFE_INTEGER`, including checking daily totals **after** grouping. Unsafe charts show a clear fallback while exact summaries/tables remain. Quality charts use the API's existing floating-point rates, never converted integer counters; extreme finite rates that would overflow chart calculations fall back to the table.

UTC timestamps keep all six fractional digits and `Z`; temporary millisecond date parsing validates the calendar without replacing stored/displayed strings. Null timestamps are not warnings.

The first successful **current** response from any report pins the in-memory browser application session. All later responses must match. Request keys hide old rows immediately on source/date/page/view changes; cleanup aborts requests and an active-request guard also ignores late success/failure when abort is disregarded. A current-response identity mismatch permanently hides analytical content and source options across every view, retaining the original fingerprint and requiring explicit Reload dashboard. Navigation or retry cannot adopt the new publication. No client publication selection, discovery, session persistence or server verifier replacement is added.

Publisher/daily/quality responses have no global totals; cards explicitly describe returned scope and a limit-reaching response warns that more records may exist. Publisher/quality charts show at most 12 sources, daily charts at most 30 dates, with explicit notices; all returned rows remain in tables. Article pagination uses its supplied `hasMore` instead of guessing from page length.

## Loading, errors and accessibility

No placeholder metrics are presented as actual data while loading or after failure. A valid empty result displays exact zero totals for that returned scope and an empty-state message. HTTP 400, 500, 503 and 504, network failures, invalid JSON/schema and publication changes have fixed user-facing explanations. Recognized capacity failures receive a busy message. Server error paths, raw bodies and exception text are never shown. Retry is manual; publication changes require explicit reload.

Charts include Recharts' accessibility layer plus semantic tables; daily charts additionally expose exact grouped date totals as accessible text. Select/reset/retry controls are labeled and keyboard-accessible. The table overflow region intentionally has `tabIndex=0` so keyboard users can scroll it; the intentional accessibility lint exceptions are documented inline. Long fingerprints wrap, long source IDs remain complete in the table, chart labels may be abbreviated, and large totals wrap without dropping digits. Full screen-reader and cross-browser certification are not claimed.

## Tests, build and CI

From the repository root:

```sh
npm ci --prefix frontend
npm run typecheck --prefix frontend
npm run lint --prefix frontend
npm test --prefix frontend
npm run build --prefix frontend
npm run check
```

For watch mode, run `npm run test:watch --prefix frontend`. Vitest/React Testing Library tests mock `fetch`; they need no backend, retained files or external services. A deterministic `ResizeObserver` supplies jsdom layout dimensions so the real Recharts components can render. This does not prove visual responsiveness; native-browser layout evidence is separate.

The independent [frontend CI job](../.github/workflows/ci.yml) uses Node.js 24 and the frontend lockfile cache, then runs clean installation, type checking, linting, tests and production build. Existing documentation/hygiene, ingestion, Python/Spark, JDBC POC and Spring Boot checks are intact. Dependency installation needs registry access; tests and production build run locally after installation. M6.5 keeps the existing dependency versions and lockfile unchanged. CI uses the existing frontend job with an updated descriptive label; no pipeline is duplicated.

## Historical M6.3 validation

Local validation on October 18, 2026 used macOS ARM64, Node.js 24.19.0 and npm 11.17.0:

- A clean lockfile installation, type checking, linting, all 72 unit/component tests, production build, root documentation/hygiene checks and `git diff --check` passed. npm audit reported zero vulnerabilities at the time of the check.
- Native Chrome inspection covered the publication identity, scoped KPIs, grouped comparison bars and complete publisher table. Selecting NASA returned 10 articles and 20 observations; reset restored all three publishers. A 400 CSS-pixel emulated viewport was visually inspected for the wrapped fingerprint and two-column cards. This was not a physical-device, cross-browser or screen-reader certification. Error states were checked in component tests. Native UI automation was intermittently unavailable; an observed favicon 404 was fixed and its asset response verified over HTTP.
- The existing local API and Vite proxy returned identical retained-publication responses: three publishers, 59 articles, 118 observations and four articles without publication timestamps. Guardian returned 45/90 articles/observations, NASA 10/20 and xkcd 4/8. These dataset-specific values are validation evidence only, never application logic or frontend fixtures. All 10 files in that Gold publication retained their original SHA-256 hashes, sizes and modification times. No publisher data was fetched or Gold files modified. Both local servers were stopped after verification.
- The production JavaScript bundle is approximately 595 kB minified (177 kB gzip). Vite reports its default chunk-size warning. The chart library is loaded with this single-screen dashboard; splitting it can be reconsidered with measured deployment performance or additional routes.

Hosted CI execution remains pending owner publication. Existing ingestion, processing and backend regression suites were not rerun for this frontend-only implementation; their workflow checks remain intact. Local validation artifacts are ignored under `outputs/m6-3/` and are not repository inputs. Production hosting and same-origin API routing are not implemented.

## M6.5 validation and remaining closeout

Local checks on October 18, 2026 used the existing macOS ARM64/Node.js 24.19.0/npm 11.17.0 environment. Git safety checks confirmed `feature/m6-5-dashboard-analytics` with a clean starting tree. No commits, pushes, branch changes or GitHub operations were performed.

- Clean `npm ci --prefix frontend`, type checking, linting, all **184 frontend tests** (including all original 72 unchanged), and production build passed. Tests use deterministic mocked reports, including multiple dates, rates above 1.0, unsafe counts, malicious/long metadata, pagination and cross-view publication changes. They require no backend. The existing ESLint deprecation warning remains; npm reported zero vulnerabilities and an unapproved optional fsevents install-script notice without failing installation.
- The unchanged Content API passed **120 unit/component tests and 20 real HTTP/Python synthetic integration tests**. The existing fixture directory was reused. No backend source, verifier or accepted ADR was changed. Ingestion/full processing/JDBC suites were not rerun for this frontend-only change; their CI checks remain intact.
- Actual TypeScript client code, transpiled with the existing compiler, executed through the Vite proxy against retained Gold. It confirmed **3 publishers, 59 articles, 118 observations, 3 daily rows, one actual ingestion date (`2026-10-13`), and 0 warning codes**. Six article pages had sizes **10, 10, 10, 10, 10, 9**; all 59 identities were distinct and matched Python reference order. Source/inclusive date filters and publication identity checks passed. Proxy responses matched direct API responses. All 59 supplied article URLs passed navigation validation; no external article was fetched. These are API/client checks, not browser traversal claims.
- Native Chrome returned one accessibility snapshot showing the new navigation, pinned retained fingerprint and publisher totals. Further Chrome and Brave capture attempts repeatedly failed with macOS ScreenCaptureKit error `-3811`; no successful M6.5 screenshot or interaction traversal was obtained. Desktop/narrow visual acceptance of the new views, browser traversal of all pages, external-link attribute inspection in the browser and full screen-reader checks therefore remain pending. Prior M6.3 visual evidence above does not certify the new views.

All **10 retained Gold files** have identical SHA-256 hashes, sizes and modification times after verification. Both temporary local servers were stopped. The main production JavaScript chunk is **607.02 kB minified / 181.19 kB gzip**; the three new view chunks are approximately 4.9–5.1 kB each. Vite retains its default 500 kB chunk warning. Documentation/link checks, repository hygiene and `git diff --check` passed. Local evidence stays ignored under `outputs/m6-5/`. Hosted CI has not run for these changes.

Each analytical request starts and verifies a publication in Python. Only the active view fetches; browser abort does not guarantee immediate cancellation of already-running server work, so rapid navigation can still receive the existing capacity response. Retry is manual. Default process/output bounds may reject unusually large article metadata even at ten rows. There is no result cache, URL-based view/history state, engagement report, authentication, deployment routing or publication switching. The bounded source selector and chart limits are intentional, not global completeness guarantees.

Before M6 closeout: restore browser automation or perform owner-led desktop/narrow checks for all views, traverse all article pages and inspect safe links without fetching publisher data, run hosted CI after owner publication, and review accessibility plus deployment/runtime operating limits. M6 remains in progress until that end-to-end acceptance and closeout review are recorded.
