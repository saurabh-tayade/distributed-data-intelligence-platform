CREATE FUNCTION reject_evidence_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING MESSAGE='IMMUTABLE_EVIDENCE', ERRCODE='23514'; END $$;
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['storage_bindings','execution_profiles','execution_profile_storage','source_revisions','artifacts',
        'artifact_dependencies','run_sources','run_inputs','stage_artifacts','command_bindings','audit_events'] LOOP
        EXECUTE format('CREATE TRIGGER immutable_evidence BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_evidence_mutation()',t);
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;
-- JSONB here compares row identity only; it never defines canonical bytes or a digest.
CREATE FUNCTION guard_mutable_row() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE allowed text[];
BEGIN
    IF TG_OP <> 'UPDATE' THEN RAISE EXCEPTION USING MESSAGE='HISTORY_DELETE_FORBIDDEN', ERRCODE='23514'; END IF;
    allowed := TG_ARGV;
    IF (to_jsonb(NEW) - allowed) IS DISTINCT FROM (to_jsonb(OLD) - allowed) THEN
        RAISE EXCEPTION USING MESSAGE='IMMUTABLE_IDENTITY', ERRCODE='23514';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION USING MESSAGE='ROW_VERSION_REQUIRED', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER coordinator_guard BEFORE UPDATE OR DELETE ON coordinator FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('mode','epoch','active_attempt_id','owner_instance_id','heartbeat_at','row_version');
CREATE TRIGGER worker_guard BEFORE UPDATE OR DELETE ON worker_instances FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('heartbeat_at','status','row_version');
CREATE TRIGGER source_guard BEFORE UPDATE OR DELETE ON sources FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('current_revision','enabled','updated_at','row_version');
CREATE TRIGGER run_guard BEFORE UPDATE OR DELETE ON runs FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('state','current_stage','current_attempt_id','row_version','finished_at','result_artifact_id','outcome_code','counts_bytes','retry_eligible');
CREATE TRIGGER attempt_guard BEFORE UPDATE OR DELETE ON attempts FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('state','finished_at','outcome_code','row_version');
CREATE TRIGGER stage_guard BEFORE UPDATE OR DELETE ON stage_invocations FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('state','command_address','command_sha256','result_address','result_sha256','child_pid','child_started_at','child_instance_id','started_at','finished_at','outcome_code','counts_bytes','row_version');
CREATE TRIGGER rss_guard BEFORE UPDATE OR DELETE ON rss_deliveries FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('state','metadata_sha256','receipt_bytes','row_version');
CREATE TRIGGER control_guard BEFORE UPDATE OR DELETE ON controls FOR EACH ROW EXECUTE FUNCTION guard_mutable_row('state','reason','applied_at','row_version');
DO $$ DECLARE t text; BEGIN
    FOREACH t IN ARRAY ARRAY['coordinator','worker_instances','sources','runs','attempts','stage_invocations','rss_deliveries','controls'] LOOP
        EXECUTE format('CREATE TRIGGER no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION reject_evidence_mutation()',t);
    END LOOP;
END $$;

CREATE FUNCTION guard_terminal_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_TABLE_NAME='attempts' AND OLD.state IN ('SUCCEEDED','FAILED','CANCELLED','INTERRUPTED')) OR
       (TG_TABLE_NAME='stage_invocations' AND OLD.state IN ('VERIFIED','FAILED')) OR
       (TG_TABLE_NAME='controls' AND OLD.state IN ('APPLIED','TOO_LATE','REJECTED')) OR
       (TG_TABLE_NAME='runs' AND OLD.state='SUCCEEDED') THEN
        RAISE EXCEPTION USING MESSAGE='TERMINAL_OUTCOME_IMMUTABLE', ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER terminal_outcome BEFORE UPDATE ON attempts FOR EACH ROW EXECUTE FUNCTION guard_terminal_outcome();
CREATE TRIGGER terminal_outcome BEFORE UPDATE ON stage_invocations FOR EACH ROW EXECUTE FUNCTION guard_terminal_outcome();
CREATE TRIGGER terminal_outcome BEFORE UPDATE ON controls FOR EACH ROW EXECUTE FUNCTION guard_terminal_outcome();
CREATE TRIGGER terminal_outcome BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION guard_terminal_outcome();
