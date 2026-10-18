# M6.3 — React dashboard foundation and publisher analytics

The first browser consumer of the platform displays publisher analytics from the existing [M6.2 content API](../services/content-api/README.md). It does not add backend reports, access Gold files directly, or weaken the Python verification boundary. [Accepted ADR 0005](../docs/adr/0005-gold-consumer-integration.md) remains unchanged. M6.4 and M6.5 remain planned; the full M6 milestone is not complete.

## Acceptance and design

Acceptance requires exact publisher counts and scoped totals, stable publication identity across filter changes, strict API validation, clear loading/error/empty states, accessible controls and an exact text alternative to the chart. Tests must run without Python, Spark, SeaweedFS, AWS, a backend or retained Gold data. Live browser/API checks are recorded separately from isolated tests.

The dashboard uses a navy, teal and warm-neutral palette with system fonts, a full publication fingerprint, exact publisher selector, four summary cards, grouped comparison bars, metric definitions and a detail table. It adapts to smaller screens with two-column cards, a stacked chart/guide and a keyboard-focusable horizontally scrollable table. Focus styles, a skip link, semantic table headers, labeled controls, live status messages, non-color chart labels and reduced-motion styling are included. A table remains the authoritative textual alternative to the visualization.

Observations mean **accepted content observations**, not views, clicks or engagement. Articles without a publication timestamp are not warning counts. No trends, rates or global totals are invented.

## Runtime and dependencies

Use **Node.js 24.15+ within major 24**; local checks used **24.19.0**, pinned in `.nvmrc`. `package.json` and `package-lock.json` are independent of the root documentation tooling. Direct versions are exact; install the lockfile with `npm ci`.

| Dependency | Version | Purpose |
| --- | --- | --- |
| React / React DOM | 19.3.1 | Rendering and local component state |
| Recharts | 3.10.1 | Requested publisher comparison visualization |
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

There is no application Redux store, global state framework, UI framework, router, SSR, authentication infrastructure, external font or image service. Recharts may bring internal state dependencies transitively; application state remains React hooks. No additional runtime validation library is needed for this small fixed contract.

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

Replace the fingerprint placeholder; no latest-publication selection exists. The API defaults to `127.0.0.1:8080`, `processing/gold_analytics.py` and `outputs/gold`, with paths resolved from its startup directory. An unconfigured API can be live while publisher queries correctly return HTTP 503.

In another terminal, from the repository root:

```sh
npm ci --prefix frontend --ignore-scripts --no-audit --no-fund
npm run dev --prefix frontend
```

Open `http://127.0.0.1:5173`. The browser requests only relative `/api/v1/publishers` URLs. Vite forwards `/api/` to `http://127.0.0.1:8080`, retaining paths and query parameters. No backend CORS change is required. Development and preview bind to loopback and use strict ports.

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
| `src/api.ts` | REST types, strict response validation, source/limit query construction and safe error classification |
| `src/usePublishers.ts` | Selection/request state, abort and stale-response protection, session publication pinning |
| `src/metrics.ts` | Exact BigInt aggregation, decimal formatting and chart safe-integer guard |
| `src/App.tsx` | Compose the dashboard states and returned-scope sections |
| `components/AppShell.tsx` | Header, main landmark, skip link and footer |
| `components/PublicationIndicator.tsx` | Full pinned fingerprint and initial waiting state |
| `components/PublisherFilter.tsx` | Exact source selector and reset control |
| `components/SummaryCards.tsx` | Publishers, articles, observations and missing-publication-date counts for returned rows |
| `components/PublisherComparisonChart.tsx` | Grouped bars, legend, unsafe-number fallback and table link |
| `components/PublisherTable.tsx` | Exact counts, full publisher IDs, microsecond UTC timestamps and null labels |
| `components/States.tsx` | Loading, sanitized error/retry and empty states |
| `src/styles.css` | Responsive layout, typography, focus and reduced-motion rules |
| `src/test/` and `src/*.test.*` | Deterministic synthetic fixtures and isolated tests |

Plain `fetch` and a custom hook fit one endpoint without introducing cache invalidation or a global store. The chart is limited to the first 12 returned publishers for readable comparison, with an explicit notice when shortened. The table includes every returned row. There is no client-side wildcard search, polling, automatic retry, snapshot switch or persistence across page reloads.

## REST contract and data integrity

Only `GET /api/v1/publishers` is used, with `limit=100` and optional exact `sourceId`. The source selector is seeded by the first successful unfiltered response and retained across filter requests. Selecting a publisher issues a new backend request; resetting issues another unfiltered request. API source syntax is `[a-z0-9][a-z0-9_-]{0,127}`. Response identifiers outside that syntax are displayed safely as text but cannot be selected as filters.

The successful envelope is `{ publication, data, meta: { returned, limit } }`. Every row contains `sourceId`, decimal-string `articleCount`, `observationCount`, `articlesWithoutPublishedAt`, and nullable UTC `firstObservedAt`/`lastObservedAt`. The client checks exact fields, fingerprint format, requested limit/source, returned length, unique nonblank identifiers, integer bounds and timestamp syntax/calendar validity. Wrong structures, non-JSON success, rounded numeric counters or mismatched filters fail closed. Rendering escapes strings; no raw HTML, arbitrary links or backend diagnostics are injected.

Each analytical counter is a canonical nonnegative decimal string in the API's signed 64-bit range. Totals use `BigInt`, including sums beyond a single 64-bit value. `Intl.NumberFormat` formats BigInt directly. Chart numbers are created only after checking every plotted metric against `Number.MAX_SAFE_INTEGER`; otherwise an explanatory fallback replaces the chart while exact KPIs/table remain available. There is no floating-point conversion of unsafe counters.

UTC strings retain their six fractional digits. Calendar validation uses a temporary millisecond representation only to reject impossible dates; stored and displayed strings retain all six digits and UTC. Null timestamps display **Not available** and never become warnings.

The first successful response pins the publication identity in memory. All later responses must match it. When a filter changes, old results disappear immediately, previous requests are aborted, and a stale-result guard ignores transports that resolve despite cancellation. A changed fingerprint clears displayed metrics and asks for an explicit reload; rows from two publications are never combined. Pinning detects API identity changes; it does not send a nonexistent publication parameter or replace server-side Gold verification.

The API supplies no global publisher total. KPIs are explicitly labeled as **returned-scope totals**. When `returned === limit`, a notice says additional publishers may exist. The selector also identifies its bounded inventory. A short response is reported as returned data without inventing a separate global total; pagination is not available.

## Loading, errors and accessibility

No placeholder metrics are presented as actual data while loading or after failure. A valid empty result displays exact zero totals for that returned scope and an empty-state message. HTTP 400, 500, 503 and 504, network failures, invalid JSON/schema and publication changes have fixed user-facing explanations. Recognized capacity failures receive a busy message. Server error paths, raw bodies and exception text are never shown. Retry is manual; publication changes require explicit reload.

The chart includes Recharts' accessibility layer, but the semantic table is the complete textual representation. Select/reset/retry controls are labeled and keyboard-accessible. The table overflow region intentionally has `tabIndex=0` so keyboard users can scroll it; the single accessibility lint exception is documented inline. Long fingerprints wrap, long source IDs remain complete in the table, chart labels may be abbreviated, and large totals wrap without dropping digits. Full screen-reader and cross-browser certification are not claimed.

## Tests, build and CI

From the repository root:

```sh
npm ci --prefix frontend --ignore-scripts --no-audit --no-fund
npm run typecheck --prefix frontend
npm run lint --prefix frontend
npm test --prefix frontend
npm run build --prefix frontend
npm run check
```

For watch mode, run `npm run test:watch --prefix frontend`. Vitest/React Testing Library tests mock `fetch`; they need no backend, retained files or external services. A deterministic `ResizeObserver` supplies jsdom layout dimensions so the real Recharts components can render. This does not prove visual responsiveness; native-browser layout evidence is separate.

The independent [frontend CI job](../.github/workflows/ci.yml) uses Node.js 24 and the frontend lockfile cache, then runs clean installation, type checking, linting, tests and production build. Existing documentation/hygiene, ingestion, Python/Spark, JDBC POC and Spring Boot checks are intact. Dependency installation needs registry access; tests and production build run locally after installation. The lockfile is included in the working-tree changes for the owner to commit; this task performs no commit or push.

## Validation evidence and limitations

Local validation on October 18, 2026 used macOS ARM64, Node.js 24.19.0 and npm 11.17.0:

- A clean lockfile installation, type checking, linting, all 72 unit/component tests, production build, root documentation/hygiene checks and `git diff --check` passed. npm audit reported zero vulnerabilities at the time of the check.
- Native Chrome inspection covered the publication identity, scoped KPIs, grouped comparison bars and complete publisher table. Selecting NASA returned 10 articles and 20 observations; reset restored all three publishers. A 400 CSS-pixel emulated viewport was visually inspected for the wrapped fingerprint and two-column cards. This was not a physical-device, cross-browser or screen-reader certification. Error states were checked in component tests. Native UI automation was intermittently unavailable; an observed favicon 404 was fixed and its asset response verified over HTTP.
- The existing local API and Vite proxy returned identical retained-publication responses: three publishers, 59 articles, 118 observations and four articles without publication timestamps. Guardian returned 45/90 articles/observations, NASA 10/20 and xkcd 4/8. These dataset-specific values are validation evidence only, never application logic or frontend fixtures. All 10 files in that Gold publication retained their original SHA-256 hashes, sizes and modification times. No publisher data was fetched or Gold files modified. Both local servers were stopped after verification.
- The production JavaScript bundle is approximately 595 kB minified (177 kB gzip). Vite reports its default chunk-size warning. The chart library is loaded with this single-screen dashboard; splitting it can be reconsidered with measured deployment performance or additional routes.

Hosted CI execution remains pending owner publication. Existing ingestion, processing and backend regression suites were not rerun for this frontend-only implementation; their workflow checks remain intact. Local validation artifacts are ignored under `outputs/m6-3/` and are not repository inputs. Production hosting and same-origin API routing are not implemented.

M6.4 should follow its assigned scope and build on the existing API/client boundary. Define any additional report/filter contracts before implementation and extend exact-integer, publication-consistency and mocked response tests. M6.5 remains planned. Do not infer unimplemented endpoints, engagement metrics, pagination, authentication or publication switching from this dashboard.
