import Stripe from "https://esm.sh/stripe@14?target=deno";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2?target=deno";

const stripe = new Stripe(Deno.env.get("STRIPE_SECRET_KEY")!, {
  httpClient: Stripe.createFetchHttpClient(),
  apiVersion: "2024-04-10",
});

const supabase = createClient(
  Deno.env.get("SUPABASE_URL")!,
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
);

const STRIPE_PRICE_ID = Deno.env.get("STRIPE_PRICE_ID")!;
const FRONTEND_URL    = Deno.env.get("FRONTEND_URL")!;

Deno.serve(async (req) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Authorization, Content-Type, apikey, x-client-info",
      },
    });
  }

  // Verify JWT and get the caller's user
  const authHeader = req.headers.get("Authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    return json({ error: "Unauthorized" }, 401);
  }
  const token = authHeader.slice(7);
  const { data: { user }, error: authError } = await supabase.auth.getUser(token);
  if (authError || !user) return json({ error: "Unauthorized" }, 401);

  // Look up or create this user's subscription row
  const { data: subRow } = await supabase
    .from("subscriptions")
    .select("stripe_customer_id, trial_used")
    .eq("user_id", user.id)
    .single();

  // Look up or create Stripe Customer
  let stripeCustomerId: string;
  if (subRow?.stripe_customer_id) {
    stripeCustomerId = subRow.stripe_customer_id;
  } else {
    const customer = await stripe.customers.create({
      email: user.email,
      metadata: { supabase_user_id: user.id },
    });
    stripeCustomerId = customer.id;

    // Persist the customer ID early so webhook can match it back
    await supabase.from("subscriptions").upsert({
      user_id:            user.id,
      stripe_customer_id: stripeCustomerId,
      status:             "inactive",
      trial_used:         false,
    }, { onConflict: "user_id" });
  }

  const trialUsed = subRow?.trial_used ?? false;

  // Build Checkout session
  const sessionParams: Stripe.Checkout.SessionCreateParams = {
    mode:                       "subscription",
    customer:                   stripeCustomerId,
    client_reference_id:        user.id,
    payment_method_collection:  "always", // card required even during trial
    line_items: [{ price: STRIPE_PRICE_ID, quantity: 1 }],
    success_url: `${FRONTEND_URL}?checkout=success`,
    cancel_url:  `${FRONTEND_URL}?checkout=cancel`,
  };

  // Offer 14-day trial only if they haven't used one before
  if (!trialUsed) {
    sessionParams.subscription_data = { trial_period_days: 14 };
  }

  const session = await stripe.checkout.sessions.create(sessionParams);

  return json({ url: session.url });
});

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type":                "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  });
}
