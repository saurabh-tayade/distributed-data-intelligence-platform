-- Content-only recovery boundary. No RESUME command or dispatch transition is introduced.
ALTER TABLE runs DROP CONSTRAINT runs_state_check;
ALTER TABLE runs ADD CONSTRAINT runs_state_check CHECK(state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','SUCCEEDED','FAILED','CANCELLED','RECOVERY_REQUIRED','PAUSED'));
ALTER TABLE runs ADD CONSTRAINT content_pause_only CHECK(state <> 'PAUSED' OR (pipeline='CONTENT_RSS_V1' AND current_attempt_id IS NOT NULL AND result_artifact_id IS NULL AND NOT retry_eligible));
