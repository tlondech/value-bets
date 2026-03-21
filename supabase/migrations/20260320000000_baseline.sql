-- Baseline: recreates the two production tables for local development.
-- Tables were originally created via the Supabase SQL editor; this migration
-- captures them so `supabase start` can apply all subsequent migrations cleanly.

CREATE TABLE IF NOT EXISTS public.subscriptions (
  user_id                uuid        NOT NULL,
  stripe_customer_id     text        NOT NULL,
  stripe_sub_id          text        NULL,
  status                 text        NOT NULL DEFAULT 'inactive',
  current_period_end     timestamptz NULL,
  trial_used             boolean     NOT NULL DEFAULT false,
  CONSTRAINT subscriptions_pkey PRIMARY KEY (user_id),
  CONSTRAINT subscriptions_stripe_customer_id_key UNIQUE (stripe_customer_id),
  CONSTRAINT subscriptions_user_id_fkey FOREIGN KEY (user_id)
    REFERENCES auth.users (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.signal_history (
  id                bigint         GENERATED ALWAYS AS IDENTITY NOT NULL,
  recorded_date     date           NOT NULL,
  league_key        text           NOT NULL,
  league_name       text           NOT NULL,
  home_team         text           NOT NULL,
  away_team         text           NOT NULL,
  home_canonical    text           NULL,
  away_canonical    text           NULL,
  kickoff           timestamptz    NOT NULL,
  stage             text           NULL,
  outcome           text           NOT NULL,
  outcome_label     text           NOT NULL,
  odds              numeric(6,3)   NOT NULL,
  true_prob         numeric(7,4)   NOT NULL,
  ev                numeric(7,4)   NOT NULL,
  settled           boolean        NOT NULL DEFAULT false,
  result            text           NULL,
  actual_home_score integer        NULL,
  actual_away_score integer        NULL,
  settled_at        timestamptz    NULL,
  created_at        timestamptz    NOT NULL DEFAULT now(),
  home_rank         integer        NULL,
  away_rank         integer        NULL,
  home_form         jsonb          NULL,
  away_form         jsonb          NULL,
  home_crest        text           NULL,
  away_crest        text           NULL,
  home_rest_days    integer        NULL,
  away_rest_days    integer        NULL,
  h2h_used          boolean        NULL,
  is_second_leg     boolean        NULL,
  agg_home          integer        NULL,
  agg_away          integer        NULL,
  leg1_result       jsonb          NULL,
  team_news         jsonb          NULL,
  sport             text           NOT NULL DEFAULT 'football',
  handicap_line     numeric        NULL,
  surface           text           NULL,
  bookmaker_link    text           NULL,
  score_detail      text           NULL,
  home_seed         integer        NULL,
  away_seed         integer        NULL,
  CONSTRAINT signal_history_pkey PRIMARY KEY (id),
  CONSTRAINT uq_signal_history UNIQUE (kickoff, home_team, away_team, outcome),
  CONSTRAINT signal_history_sport_check CHECK (
    sport = ANY (ARRAY['football','basketball','tennis'])
  )
);

CREATE INDEX IF NOT EXISTS idx_bet_history_kickoff  ON public.signal_history (kickoff);
CREATE INDEX IF NOT EXISTS idx_bet_history_settled  ON public.signal_history (settled);
CREATE INDEX IF NOT EXISTS idx_bet_history_recorded ON public.signal_history (recorded_date);
