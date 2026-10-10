# Coding agent guidelines

- Develop local-first. The MVP must run without AWS access or cloud credentials.
- Prefer simple, maintainable solutions with clear boundaries.
- Avoid unnecessary dependencies and services; explain why new ones are needed.
- Implement changes incrementally within the requested milestone.
- Include appropriate tests for functional changes and report what was actually run. Documentation-only changes need consistency checks, not application scaffolding.
- Document consequential architectural decisions in `docs/adr/`.
- Evaluate reasonable alternatives and explain significant tradeoffs.
- Do not treat proposed decisions as accepted without user approval.
- Avoid modifying accepted ADRs silently; supersede them when
  architectural decisions change substantially.
- Do not introduce AWS infrastructure without explicit user approval. Approval must cover scope, cost limits, lifetime, and teardown before provisioning.
- Keep local data, generated outputs, and build artifacts out of Git.
- Do not commit or push unless explicitly instructed.

## Data Engineering Principles

- Design ingestion and processing with idempotency in mind.
- Account for duplicate, missing, malformed, and late-arriving data.
- Define clear data contracts and validation rules.
- Avoid silent data loss; invalid records must be handled explicitly.
- Consider event ordering, retry behavior, and replay where applicable.
- Document assumptions about delivery and processing guarantees.
- Prefer deterministic transformations that can be tested independently.

## Reproducibility and Performance

- Pin or constrain dependency and runtime versions appropriately.
- Externalize environment-specific configuration.
- Prefer reproducible local setup and teardown procedures.
- Avoid hardcoding machine-specific paths or credentials.
- Measure performance before making optimization claims.
- Document benchmark environments, workloads, and methodology.
- Distinguish local measurements, AWS measurements, and
  theoretical scalability estimates.

## Implementation Discipline

- Define acceptance criteria before implementing substantial features.
- Prefer small changes that can be independently tested.
- Use existing libraries and frameworks instead of reinventing
  established infrastructure.
- Handle errors explicitly and provide actionable diagnostics.
- Include meaningful logging without exposing sensitive information.
- Update relevant documentation when behavior or architecture changes.
- Report incomplete work, known limitations, and unverified assumptions.

