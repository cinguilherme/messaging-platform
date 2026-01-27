CREATE INDEX IF NOT EXISTS memberships_user_id_idx ON memberships (user_id);
CREATE INDEX IF NOT EXISTS segment_index_conv_end_idx ON segment_index (conversation_id, seq_end);
