-- Returns the highest-odds settled win per sport in the last 7 days.
-- Football falls back through worldcup → ucl → epl → any league.
-- Accessible to unauthenticated (anon) users for the auth screen showcase.

CREATE OR REPLACE FUNCTION get_showcase_signals()
RETURNS json LANGUAGE sql SECURITY DEFINER AS $$
  WITH ranked AS (
    SELECT
      sport, league_key, league_name, stage,
      home_team, away_team, home_canonical, away_canonical,
      home_crest, away_crest, home_rank, away_rank,
      kickoff, settled_at, outcome, outcome_label,
      odds, true_prob, ev,
      actual_home_score, actual_away_score, score_detail, surface,
      ROW_NUMBER() OVER (
        PARTITION BY sport
        ORDER BY
          CASE
            WHEN sport = 'football' THEN
              CASE league_key
                WHEN 'worldcup' THEN 1
                WHEN 'ucl'      THEN 2
                WHEN 'epl'      THEN 3
                ELSE 4
              END
            ELSE 0
          END ASC,
          odds DESC
      ) AS rn
    FROM signal_history
    WHERE result = 'hit'
      AND settled = true
      AND settled_at >= NOW() - INTERVAL '7 days'
  )
  SELECT json_build_object(
    'football',   (SELECT row_to_json(r) FROM ranked r WHERE sport = 'football'   AND rn = 1),
    'basketball', (SELECT row_to_json(r) FROM ranked r WHERE sport = 'basketball' AND rn = 1),
    'tennis',     (SELECT row_to_json(r) FROM ranked r WHERE sport = 'tennis'     AND rn = 1)
  );
$$;

REVOKE EXECUTE ON FUNCTION get_showcase_signals() FROM PUBLIC;
GRANT  EXECUTE ON FUNCTION get_showcase_signals() TO anon;
GRANT  EXECUTE ON FUNCTION get_showcase_signals() TO authenticated;
