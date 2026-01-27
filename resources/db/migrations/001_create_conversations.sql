CREATE TABLE IF NOT EXISTS conversations (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  type TEXT NOT NULL,
  title TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
