# Distributed Data Intelligence Platform

An MSN-inspired Content Intelligence & Analytics Platform exploring how heterogeneous content metadata and engagement events can become reliable analytical data products.

**Current status: M1 Day 4 end-to-end RSS/Atom batch ingestion implemented.** The Java 21 Maven module includes a strict JSON source catalog, sequential batch runner, executable CLI, bounded raw retrieval, S3 persistence, and synthetic offline tests. Two live batches across NASA, Guardian and xkcd passed local Bronze read-back verification. A pinned local SeaweedFS Compose configuration and opt-in integration tests are provided; SeaweedFS 4.48 upload/read-back, integrity, retries, and restart persistence have passed local integration validation. Parsing, scheduled jobs, and downstream processing remain planned.

## Engineering objectives

- Ingest heterogeneous sources through batch and streaming paths.
- Transform data with Apache Spark into Bronze, Silver, and Gold layers.
- Demonstrate data quality, replay, idempotency, and observable processing.
- Serve SQL analytics and specialized downstream consumers.
- Build a reproducible local environment and measure performance before making scaling claims.
- Validate selected workloads temporarily on AWS only after explicit approval.

## Planned technology stack

| Area | Candidates |
| --- | --- |
| Services and ingestion | Java 21, Spring Boot; Python where appropriate |
| Distributed processing | Apache Spark, PySpark, Spark SQL |
| Operational storage | PostgreSQL |
| Event transport | Kafka or Redpanda (selection deferred) |
| Object storage | SeaweedFS locally (ADR 0002 accepted); Amazon S3 for approved cloud validation |
| Data formats | JSON, Parquet; Iceberg deferred |
| SQL analytics | DuckDB locally; Athena as a cloud candidate |
| Frontend | React, TypeScript |
| Local orchestration | Docker Compose |

Most entries remain directions. Java ingestion now uses Maven and AWS SDK v2; the SeaweedFS 4.48 local integration and restart checks have passed. No cloud compatibility guarantee is claimed.

## Planned high-level pipeline

```text
Content metadata / publisher exports --> Batch ingestion ------+
Engagement events --------------------> Event transport -------+--> Bronze (raw)
                                                                       |
                                                               Spark transformations
                                                                       |
                                                                 Silver (validated)
                                                                       |
                                                               Spark aggregations
                                                                       |
                                                                  Gold (curated)
                                                                       |
                                                    SQL analytics / APIs / dashboard
```

Batch processing comes first; streaming is introduced incrementally. Consumers may also read Silver where their use case requires record-level data.

## Repository layout

| Path | Purpose |
| --- | --- |
| `config/` | Public RSS/Atom source catalog |
| `docs/` | Charter, architecture, decision records, and future benchmarks |
| `ingestion/` | Java 21 Maven module: RSS retrieval, batch CLI and Bronze S3 storage |
| `processing/` | Future Spark transformations and data quality checks |
| `services/` | Future APIs and application services |
| `frontend/` | Future analytics UI |
| `infrastructure/local/` | Pinned SeaweedFS Compose configuration |
| `infrastructure/aws/` | Reserved for explicitly approved cloud validation |
| `tests/` | Future integration and end-to-end tests |
| `scripts/` | Repository checks and future operational helpers |

Empty directories contain `.gitkeep` placeholders so the layout survives a future clone.

## Milestone roadmap

| Milestone | Planned outcome |
| --- | --- |
| M0 | Repository foundation and development conventions (M0.1 and M0.3 implemented) |
| M1 | Source contracts and reproducible local environment |
| M2 | Batch ingestion into Bronze |
| M3 | Spark transformations into Silver |
| M4 | Gold models and SQL analytics |
| M5 | Streaming ingestion and incremental processing |
| M6 | Consumer APIs and analytics dashboard |
| M7 | Reliability, observability, and automated validation |
| M8 | Reproducible benchmarks and optimization |
| M9 | Optional, approved temporary AWS validation and teardown |

M0.1 and M0.3 are implemented. M1 includes RSS/Atom retrieval, Bronze storage, and the first executable batch workflow. The broader M2–M9 roadmap remains planned; M9 is outside the local MVP.

## Getting started

Start with the [charter](docs/charter.md), [architecture proposal](docs/architecture/overview.md), and [agent guidelines](AGENTS.md). The ingestion library requires JDK 21 and Maven 3.9.16 or compatible Maven 3.9+. Run `mvn -B -ntp -f ingestion/pom.xml verify` or `bash scripts/test-ingestion.sh`. See the [RSS/Atom contract](docs/architecture/rss-ingestion.md), [Bronze contract](docs/architecture/bronze-storage.md), and [local SeaweedFS setup](docs/local-seaweedfs.md). `.env.example` documents required local-only configuration; credentials stay in ignored `.env`. No application service or scheduler is introduced.

Run the first batch after exporting the local `.env` settings:

```sh
set -a
. ./.env
set +a
bash scripts/run-batch.sh config/rss-sources.json
```

See [batch architecture, data policy, CLI and live results](docs/architecture/batch-ingestion.md). GitHub contains code, public configuration, synthetic fixtures, tests and documentation; SeaweedFS contains actual publisher snapshots and ingestion metadata. Bronze preserves raw bytes; article parsing and normalization remain future Silver work.

Record consequential choices using the [ADR process](docs/adr/README.md). This project is inspired by a content portal domain and is not affiliated with MSN or Microsoft.

## Documentation checks (M0.3)

[CI](.github/workflows/ci.yml) runs on pull requests targeting `main` and pushes to `main`, with read-only repository permissions. It checks Markdown formatting, local documentation links, repository hygiene, and Maven Java 21 tests. Default CI needs no storage service or AWS credentials.

From the repository root, with Git, Bash, Node.js 24.x, and npm installed:

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm run check
```

Run checks separately with `npm run lint:markdown` or `npm run check:hygiene`. The npm manifest contains development tooling only; it does not introduce a frontend or application. Exact tool versions and their dependency graph are recorded in `package-lock.json`. Installation requires npm registry access; the checks run offline afterward.

Markdown checks use [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2) and [markdownlint-rule-relative-links](https://github.com/theoludwig/markdownlint-rule-relative-links). Default formatting rules apply except line length, preserving the existing long prose and tables. Non-ignored Markdown files are checked, including new files before staging. Markdown links and images are checked for existing local targets and Markdown heading fragments. External URL availability and HTML links are not checked; this keeps CI independent of third-party website availability.

The hygiene script checks the entire tracked working tree and new non-ignored files for Git whitespace errors (trailing whitespace, extra blank lines at EOF, and spaces before indentation tabs) and conflict markers. It also rejects tracked files matching ignore rules, including force-added local data or environment files. This is not a secret scanner or full EditorConfig validator; sensitive content in otherwise allowed files still requires review.

## Project Intent & AI Assistance Disclosure

### Project Intent

The Distributed Data Intelligence Platform is an independent,
non-commercial engineering and learning project developed to
explore, implement, and demonstrate modern distributed data
processing architectures.

The project is inspired by large-scale content aggregation and
analytics systems, but it is not intended to reproduce any
existing commercial platform.

Its primary objectives are to:

- Gain practical experience with distributed data processing,
  streaming architectures, data lakes, and cloud technologies.
- Apply established software engineering principles to the design
  and implementation of an end-to-end data platform.
- Explore architectural alternatives, engineering tradeoffs,
  performance characteristics, and operational challenges.
- Demonstrate technical capabilities through working implementations,
  automated tests, documented decisions, and measurable benchmarks.
- Maintain a publicly accessible portfolio of engineering work,
  including both successful approaches and lessons learned.

The emphasis is on engineering understanding, correctness,
reliability, maintainability, and measurable results rather
than simply integrating numerous technologies.

### Use of AI-Assisted Development

This project is being planned, designed, and implemented with
assistance from AI tools, primarily OpenAI ChatGPT and Codex.

AI assistance may be used for:

- Requirements analysis and project planning.
- Architecture discussions and evaluation of design alternatives.
- Code generation, refactoring, and implementation support.
- Test development and debugging.
- Documentation and technical explanations.
- Code reviews and identification of potential improvements.

AI-generated recommendations and implementations are treated
as inputs to the engineering process, not as authoritative
or automatically correct solutions.

Architectural decisions, implementation acceptance, correctness
validation, performance evaluation, and overall project direction
remain the responsibility of the project author.

The intention is to use AI as an engineering productivity and
learning tool while maintaining a clear understanding of the
systems, technologies, algorithms, and tradeoffs involved.

### Engineering Transparency

The repository aims to maintain a transparent record of development
through Git history, architecture decision records (ADRs), tests,
documentation, and performance benchmarks.

Planned functionality will be distinguished from implemented and
validated capabilities.

Performance and scalability claims will be supported by reproducible
measurements wherever practical.

This project represents an ongoing engineering learning process
rather than a production-ready commercial platform.

## Local Silver prototype

The [M1 Day 5 Silver guide](docs/architecture/silver-processing.md) documents the proposed article observation contract, isolated PySpark setup, offline tests and local Parquet demonstration. It processes only the existing fictional fixtures; SeaweedFS Silver integration remains future work.
