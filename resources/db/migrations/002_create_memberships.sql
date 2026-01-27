CREATE TABLE IF NOT EXISTS memberships (
  conversation_id UUID NOT NULL,
  user_id UUID NOT NULL,
  role TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (conversation_id, user_id)
);
