# Gold JDBC feasibility experiment

**M6.1 proof of concept only. Not a production API or verified Gold consumer.** Successful CLI responses are labeled `UNVERIFIED_POC`. No Spring Boot, HTTP server, frontend, database file or background worker is included.

See the [consumer integration guide](../../docs/architecture/consumer-integration.md) for exact setup, offline fixtures, tests, mappings, failure cases and integrity gaps. [ADR 0005](../../docs/adr/0005-gold-consumer-integration.md) remains **Proposed**; it recommends preserving the existing Python verifier for M6.2 rather than deploying this partial reader.

The only new runtime capability is `org.duckdb:duckdb_jdbc:1.4.5.0`. JSON parsing uses the existing repository's Jackson 2.22.3 baseline, and tests use its JUnit 6.1.3 baseline. Java 21 and Maven 3.9+ are required for the experiment. Generation of synthetic completed publications additionally uses the existing Python/Spark development dependencies; JDBC queries themselves do not.
