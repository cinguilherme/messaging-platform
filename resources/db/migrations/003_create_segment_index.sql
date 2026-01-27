CREATE TABLE IF NOT EXISTS segment_index (
  conversation_id UUID NOT NULL,
  seq_start BIGINT NOT NULL,
  seq_end BIGINT NOT NULL,
  object_key TEXT NOT NULL,
  byte_size BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (conversation_id, seq_start)
);
