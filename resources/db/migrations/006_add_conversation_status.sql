-- resources/db/migrations/006_add_conversation_status.sql
ALTER TABLE conversations
ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'active';
