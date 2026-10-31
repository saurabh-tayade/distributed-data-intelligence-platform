-- V8 is retained unchanged. RESUME needs a distinct durable control/audit identity.
ALTER TABLE controls DROP CONSTRAINT controls_type_check;
ALTER TABLE controls ADD CONSTRAINT controls_type_check CHECK(type IN ('CANCEL','RETRY','RECONCILE','RESUME'));
