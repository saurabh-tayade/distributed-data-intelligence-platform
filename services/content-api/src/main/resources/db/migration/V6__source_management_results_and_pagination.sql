-- Existing evidence/identities are unchanged. New source sequence is immutable under V5's source guard.
ALTER TABLE sources ADD COLUMN source_seq bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE sources ADD CONSTRAINT source_sequence_unique UNIQUE(source_seq);
-- B1-era bindings may lack snapshots; they are never fabricated or rewritten.
ALTER TABLE command_bindings ADD COLUMN response_data bytea
    CHECK(response_data IS NULL OR valid_object_bytes(response_data,65536));
-- Private reviewed-endpoint identity evidence; no URL is stored in public configuration/audit.
-- Legacy rows remain unqualified rather than fabricating their original catalog evidence.
ALTER TABLE source_revisions ADD COLUMN endpoint_id uuid;
ALTER TABLE source_revisions ADD COLUMN endpoint_sha256 sha256_hex;
ALTER TABLE source_revisions ADD CONSTRAINT endpoint_evidence_pair CHECK((endpoint_id IS NULL)=(endpoint_sha256 IS NULL));
CREATE INDEX source_endpoint_evidence_idx ON source_revisions(endpoint_id) WHERE endpoint_id IS NOT NULL;
