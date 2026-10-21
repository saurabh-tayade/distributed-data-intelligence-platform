-- Canonical bytes are supplied by the fixed application encoder, never by JSONB serialization.
CREATE DOMAIN sha256_hex AS varchar(64) CHECK (VALUE ~ '^[0-9a-f]{64}$');
CREATE DOMAIN product_id AS varchar(32) CHECK (VALUE IN ('content-intelligence','application-reliability'));
CREATE DOMAIN source_key AS varchar(128) CHECK (VALUE ~ '^[a-z0-9][a-z0-9_-]{0,127}$');
CREATE DOMAIN token_key AS varchar(128) CHECK (VALUE ~ '^[A-Za-z0-9_-]{1,128}$');
CREATE DOMAIN relative_address AS varchar(1024) CHECK (
    VALUE <> '' AND VALUE !~ ':' AND VALUE !~ '(^/|(^|/)\.\.?(/|$)|//|\\)' AND VALUE !~ '[[:cntrl:]]');
CREATE FUNCTION valid_object_bytes(b bytea, max_bytes integer) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    RETURN octet_length(b) BETWEEN 2 AND max_bytes AND json_typeof(convert_from(b,'UTF8')::json) = 'object';
EXCEPTION WHEN OTHERS THEN RETURN false;
END $$;

CREATE TABLE storage_bindings (
    binding_id token_key PRIMARY KEY, product product_id NOT NULL,
    role varchar(32) NOT NULL CHECK (role IN ('BRONZE','BATCH','REFERENCE','SILVER','GOLD','CONTENT','RELIABILITY','CONTROL','STAGING','TEMP')),
    configuration_bytes bytea NOT NULL CHECK (valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK (configuration_sha256 = encode(sha256(configuration_bytes),'hex')),
    installation_id uuid NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(binding_id,product)
);
CREATE TABLE execution_profiles (
    profile_id token_key PRIMARY KEY,
    configuration_bytes bytea NOT NULL CHECK (valid_object_bytes(configuration_bytes,65536)),
    configuration_sha256 sha256_hex NOT NULL CHECK (configuration_sha256 = encode(sha256(configuration_bytes),'hex')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(profile_id,configuration_sha256)
);
CREATE TABLE execution_profile_storage (
    profile_id token_key NOT NULL REFERENCES execution_profiles ON DELETE RESTRICT,
    binding_id token_key NOT NULL REFERENCES storage_bindings ON DELETE RESTRICT,
    PRIMARY KEY(profile_id,binding_id)
);
CREATE TABLE worker_instances (
    instance_id uuid PRIMARY KEY, installation_id uuid NOT NULL,
    execution_profile_id token_key NOT NULL REFERENCES execution_profiles ON DELETE RESTRICT,
    profile_sha256 sha256_hex NOT NULL, host_binding_sha256 sha256_hex NOT NULL,
    pid bigint NOT NULL CHECK (pid > 0), process_started_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(), heartbeat_at timestamptz NOT NULL,
    status varchar(16) NOT NULL CHECK (status IN ('READY','STOPPING','RECOVERY_ONLY','STOPPED')),
    row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    UNIQUE(instance_id,installation_id),
    FOREIGN KEY(execution_profile_id,profile_sha256) REFERENCES execution_profiles(profile_id,configuration_sha256)
);
CREATE TABLE coordinator (
    singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1), installation_id uuid NOT NULL UNIQUE,
    mode varchar(16) NOT NULL CHECK (mode IN ('READY','STOPPING','RECOVERY_ONLY')),
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0), active_attempt_id uuid,
    owner_instance_id uuid, heartbeat_at timestamptz,
    row_version bigint NOT NULL DEFAULT 0 CHECK (row_version >= 0),
    CHECK ((active_attempt_id IS NULL) = (owner_instance_id IS NULL)),
    CHECK (active_attempt_id IS NULL OR heartbeat_at IS NOT NULL),
    FOREIGN KEY(owner_instance_id,installation_id) REFERENCES worker_instances(instance_id,installation_id) ON DELETE RESTRICT
);
