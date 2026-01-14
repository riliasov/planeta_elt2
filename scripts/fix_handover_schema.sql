-- Create webapp schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS webapp;

-- Create tasks table
CREATE TABLE IF NOT EXISTS webapp.tasks (
    id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    title text NOT NULL,
    description text,
    status text DEFAULT 'new' NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    assignee_id text
);

-- Enable RLS (Good practice, even if we open it up for now)
ALTER TABLE webapp.tasks ENABLE ROW LEVEL SECURITY;

-- Grants for authenticated users (WebApp)
GRANT USAGE ON SCHEMA webapp TO authenticated;
GRANT ALL ON TABLE webapp.tasks TO authenticated;

-- Policy to allow all access for authenticated (for now, based on "fix error 500" priority)
-- Drop existing policy if exists to avoid error
DROP POLICY IF EXISTS "Enable all access for authenticated users" ON webapp.tasks;

CREATE POLICY "Enable all access for authenticated users"
ON webapp.tasks
FOR ALL
TO authenticated
USING (true)
WITH CHECK (true);

-- Ensure service_role has full access
GRANT ALL ON TABLE webapp.tasks TO service_role;
