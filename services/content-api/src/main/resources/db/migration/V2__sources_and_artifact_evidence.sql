CREATE TABLE sources (
    product_id product_id NOT NULL, source_id source_key NOT NULL,
    profile_family varchar(32) NOT NULL CHECK (profile_family IN ('rss-atom-v1','request-jsonl-v2','request-parquet-v1')),
    current_revision integer NOT NULL CHECK (current_revision BETWEEN 1 AND 1000), enabled boolean NOT NULL,
    row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(product_id,source_id),
    CHECK ((product_id = 'content-intelligence') = (profile_family = 'rss-atom-v1'))
);
CREATE TABLE source_revisions (
    product_id product_id NOT NULL, source_id source_key NOT NULL, revision integer NOT NULL CHECK (revision BETWEEN 1 AND 1000),
    display_name text NOT NULL CHECK (octet_length(display_name) BETWEEN 1 AND 128),
    config_version smallint NOT NULL CHECK (config_version=1),
    configuration_bytes bytea NOT NULL CHECK (valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK (configuration_sha256 = encode(sha256(configuration_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), actor varchar(32) NOT NULL CHECK(actor='local-operator'),
    PRIMARY KEY(product_id,source_id,revision),
    FOREIGN KEY(product_id,source_id) REFERENCES sources ON DELETE RESTRICT
);
-- Atomic initial source + revision insertion: final membership must hold at commit.
ALTER TABLE sources ADD CONSTRAINT current_source_revision_fk
    FOREIGN KEY(product_id,source_id,current_revision) REFERENCES source_revisions(product_id,source_id,revision)
    DEFERRABLE INITIALLY DEFERRED;
CREATE TABLE artifacts (
    artifact_id uuid PRIMARY KEY, kind varchar(32) NOT NULL CHECK(kind IN ('RELIABILITY_BATCH_V2','PUBLISHER_REFERENCE','SERVICE_REFERENCE','SILVER','GOLD','CONTENT','RELIABILITY_V2')),
    product_id product_id NOT NULL, contract_version integer NOT NULL CHECK(contract_version > 0),
    processor_revision integer NOT NULL CHECK(processor_revision > 0), native_identity varchar(512) NOT NULL CHECK(octet_length(native_identity) BETWEEN 1 AND 512),
    storage_binding_id token_key NOT NULL REFERENCES storage_bindings ON DELETE RESTRICT,
    relative_address relative_address NOT NULL,
    evidence_bytes bytea NOT NULL CHECK(valid_object_bytes(evidence_bytes,16777216)),
    evidence_sha256 sha256_hex NOT NULL CHECK(evidence_sha256 = encode(sha256(evidence_bytes),'hex')),
    verified_at timestamptz NOT NULL, UNIQUE(storage_binding_id,relative_address), UNIQUE(artifact_id,product_id),
    FOREIGN KEY(storage_binding_id,product_id) REFERENCES storage_bindings(binding_id,product)
);
CREATE TABLE artifact_dependencies (
    artifact_id uuid NOT NULL REFERENCES artifacts ON DELETE RESTRICT,
    role token_key NOT NULL, ordinal integer NOT NULL CHECK(ordinal BETWEEN 0 AND 127),
    dependency_artifact_id uuid NOT NULL REFERENCES artifacts ON DELETE RESTRICT,
    PRIMARY KEY(artifact_id,role,ordinal), UNIQUE(artifact_id,role,dependency_artifact_id),
    CHECK(artifact_id <> dependency_artifact_id)
);
CREATE INDEX artifacts_native_identity_idx ON artifacts(kind,native_identity);
-- Serialize dependency-edge insertion against the coordinator in B2; this check also rejects cycles.
CREATE FUNCTION reject_artifact_cycle() RETURNS trigger LANGUAGE plpgsql SET search_path=ddip_management,pg_catalog AS $$
BEGIN
    IF EXISTS (WITH RECURSIVE chain(id) AS (
        SELECT NEW.dependency_artifact_id UNION
        SELECT d.dependency_artifact_id FROM artifact_dependencies d JOIN chain c ON d.artifact_id=c.id
    ) SELECT 1 FROM chain WHERE id=NEW.artifact_id) THEN
        RAISE EXCEPTION USING MESSAGE='ARTIFACT_DEPENDENCY_CYCLE', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER artifact_cycle BEFORE INSERT ON artifact_dependencies FOR EACH ROW EXECUTE FUNCTION reject_artifact_cycle();
