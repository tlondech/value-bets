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

Deno.serve(async (req) => {
  const signature = req.headers.get("stripe-signature");
  if (!signature) return new Response("Missing signature", { status: 400 });

  const body = await req.text();

  let event: Stripe.Event;
  try {
    event = await stripe.webhooks.constructEventAsync(
      body,
      signature,
      Deno.env.get("STRIPE_WEBHOOK_SECRET")!,
    );
  } catch (err) {
    console.error("Webhook signature verification failed:", err);
    return new Response("Invalid signature", { status: 400 });
  }

  try {
    switch (event.type) {
      case "checkout.session.completed": {
        const session = event.data.object as Stripe.Checkout.Session;
        const userId          = session.client_reference_id;
        const stripeCustomerId = session.customer as string;
        const stripeSubId     = session.subscription as string;

        if (!userId || !stripeCustomerId) break;

        // Retrieve the subscription to get current period and status
        const sub = await stripe.subscriptions.retrieve(stripeSubId);

        await supabase.from("subscriptions").upsert({
          user_id:            userId,
          stripe_customer_id: stripeCustomerId,
          stripe_sub_id:      stripeSubId,
          status:             sub.status,
          current_period_end: new Date(sub.current_period_end * 1000).toISOString(),
          trial_used:         true, // consumed regardless of outcome
        }, { onConflict: "user_id" });

        console.log(`checkout.session.completed — user=${userId} status=${sub.status}`);
        break;
      }

      case "customer.subscription.updated": {
        const sub = event.data.object as Stripe.Subscription;
        await supabase
          .from("subscriptions")
          .update({
            stripe_sub_id:      sub.id,
            status:             sub.status,
            current_period_end: new Date(sub.current_period_end * 1000).toISOString(),
          })
          .eq("stripe_customer_id", sub.customer as string);

        console.log(`subscription.updated — customer=${sub.customer} status=${sub.status}`);
        break;
      }

      case "customer.subscription.deleted": {
        const sub = event.data.object as Stripe.Subscription;
        await supabase
          .from("subscriptions")
          .update({ status: "canceled", stripe_sub_id: sub.id })
          .eq("stripe_customer_id", sub.customer as string);

        console.log(`subscription.deleted — customer=${sub.customer}`);
        break;
      }

      default:
        console.log(`Unhandled event type: ${event.type}`);
    }
  } catch (err) {
    console.error("Error handling event:", err);
    return new Response("Internal error", { status: 500 });
  }

  return new Response(JSON.stringify({ received: true }), {
    headers: { "Content-Type": "application/json" },
  });
});
