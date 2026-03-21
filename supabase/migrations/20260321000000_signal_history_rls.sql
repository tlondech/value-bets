-- ── 1. Enable RLS — active/trialing subscribers only get full data ──────────
ALTER TABLE signal_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY "active_subscribers_read" ON signal_history
  FOR SELECT USING (
    auth.uid() IN (
      SELECT user_id FROM subscriptions
      WHERE status IN ('active', 'trialing')
    )
  );

-- ── 2. Teaser RPC — SECURITY DEFINER bypasses RLS ───────────────────────────
--    Returns all columns but nulls out the three sensitive numerical fields.
--    Any authenticated user may call this.

CREATE OR REPLACE FUNCTION get_teaser_signals()
RETURNS SETOF json
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT (to_json(sh)::jsonb || '{"odds":null,"true_prob":null,"ev":null}'::jsonb)::json
  FROM signal_history sh
  WHERE settled = false
    AND kickoff > NOW()
  ORDER BY kickoff ASC;
$$;

-- Only authenticated users may call the teaser function
REVOKE EXECUTE ON FUNCTION get_teaser_signals() FROM PUBLIC;
GRANT  EXECUTE ON FUNCTION get_teaser_signals() TO authenticated;
