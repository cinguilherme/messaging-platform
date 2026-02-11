CREATE TABLE IF NOT EXISTS attachments (
  attachment_id UUID PRIMARY KEY,
  conversation_id UUID NOT NULL,
  uploader_id UUID NOT NULL,
  object_key TEXT NOT NULL UNIQUE,
  mime_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  referenced_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS attachments_conversation_idx ON attachments (conversation_id);
CREATE INDEX IF NOT EXISTS attachments_expires_idx ON attachments (expires_at);
