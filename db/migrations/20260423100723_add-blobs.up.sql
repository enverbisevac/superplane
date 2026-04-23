BEGIN;

CREATE TABLE blobs (
  id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
  organization_id    uuid        NOT NULL,
  scope_type         TEXT        NOT NULL,
  canvas_id          uuid        NULL,
  node_id            TEXT        NULL,
  execution_id       uuid        NULL,
  path               TEXT        NOT NULL,
  object_key         TEXT        NOT NULL,
  size_bytes         BIGINT      NOT NULL DEFAULT 0,
  content_type       TEXT        NULL,
  status             TEXT        NOT NULL DEFAULT 'pending',
  created_by_user_id uuid        NULL,
  created_at         TIMESTAMPTZ NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL,

  CONSTRAINT blobs_pkey
    PRIMARY KEY (id),

  CONSTRAINT blobs_organization_id_fkey
    FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE,

  CONSTRAINT blobs_canvas_id_fkey
    FOREIGN KEY (canvas_id) REFERENCES workflows(id) ON DELETE CASCADE,

  CONSTRAINT blobs_execution_id_fkey
    FOREIGN KEY (execution_id) REFERENCES workflow_node_executions(id) ON DELETE CASCADE,

  CONSTRAINT blobs_scope_type_check
    CHECK (scope_type IN ('org', 'canvas', 'node', 'execution')),

  CONSTRAINT blobs_status_check
    CHECK (status IN ('pending', 'ready')),

  CONSTRAINT blobs_scope_ids_check CHECK (
       (scope_type = 'org'       AND canvas_id IS NULL     AND node_id IS NULL     AND execution_id IS NULL)
    OR (scope_type = 'canvas'    AND canvas_id IS NOT NULL AND node_id IS NULL     AND execution_id IS NULL)
    OR (scope_type = 'node'      AND canvas_id IS NOT NULL AND node_id IS NOT NULL AND execution_id IS NULL)
    OR (scope_type = 'execution' AND execution_id IS NOT NULL)
  )
);

CREATE UNIQUE INDEX idx_blobs_org_path_unique
  ON blobs (organization_id, path)
  WHERE scope_type = 'org' AND status = 'ready';

CREATE UNIQUE INDEX idx_blobs_canvas_path_unique
  ON blobs (organization_id, canvas_id, path)
  WHERE scope_type = 'canvas' AND status = 'ready';

CREATE UNIQUE INDEX idx_blobs_node_path_unique
  ON blobs (organization_id, canvas_id, node_id, path)
  WHERE scope_type = 'node' AND status = 'ready';

CREATE UNIQUE INDEX idx_blobs_execution_path_unique
  ON blobs (organization_id, execution_id, path)
  WHERE scope_type = 'execution' AND status = 'ready';

CREATE UNIQUE INDEX idx_blobs_object_key_unique
  ON blobs (object_key);

CREATE INDEX idx_blobs_org_scope
  ON blobs (organization_id, scope_type, created_at DESC);

CREATE INDEX idx_blobs_canvas
  ON blobs (organization_id, canvas_id, created_at DESC)
  WHERE canvas_id IS NOT NULL;

CREATE INDEX idx_blobs_node
  ON blobs (organization_id, canvas_id, node_id, created_at DESC)
  WHERE node_id IS NOT NULL;

CREATE INDEX idx_blobs_execution
  ON blobs (organization_id, execution_id, created_at DESC)
  WHERE execution_id IS NOT NULL;

COMMIT;
