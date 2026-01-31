CREATE TABLE IF NOT EXISTS user_profiles (
  user_id UUID PRIMARY KEY,
  username TEXT NULL,
  first_name TEXT NULL,
  last_name TEXT NULL,
  avatar_url TEXT NULL,
  email TEXT NULL,
  enabled BOOLEAN NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_profiles_username_idx ON user_profiles (username);
CREATE INDEX IF NOT EXISTS user_profiles_email_idx ON user_profiles (email);
