-- Fix: RLS policy now also enforces current_period_end for trialing subscriptions.
-- Previously, a user whose trial had elapsed but whose Stripe webhook hadn't yet
-- updated status to 'past_due' could still read signal_history rows.
DROP POLICY IF EXISTS "active_subscribers_read" ON signal_history;

CREATE POLICY "active_subscribers_read" ON signal_history
  FOR SELECT USING (
    auth.uid() IN (
      SELECT user_id FROM subscriptions
      WHERE status = 'active'
         OR (status = 'trialing' AND current_period_end > now())
    )
  );
