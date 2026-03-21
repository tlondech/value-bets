-- Teaser update: expose true_prob and ev so locked users can see the model's
-- conviction, while odds and outcome_label remain hidden.
CREATE OR REPLACE FUNCTION get_teaser_signals()
RETURNS SETOF json
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT (to_json(sh)::jsonb || '{"odds":null,"outcome_label":null}'::jsonb)::json
  FROM signal_history sh
  WHERE settled = false
    AND kickoff > NOW()
  ORDER BY kickoff ASC;
$$;
