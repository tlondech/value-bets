// ── Supabase client ────────────────────────────────────────────
// Uses the ESM build so it can be imported as a module.
// The UMD <script> tag in index.html must be replaced with this import.
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm";

export const TRIAL_DAYS = 14;

const isLocal = ["localhost", "127.0.0.1"].includes(window.location.hostname);
export const SUPABASE_URL = isLocal
  ? "http://127.0.0.1:54321"
  : "https://uteiydpfxybtjzmdvsgc.supabase.co";
const SUPABASE_KEY = isLocal
  ? "sb_publishable_ACJWlzQHlZjBrEguHvfOxg_3BJgxAaH"
  : "sb_publishable_v2PxaiIgMqJs404xFYlYsg_S_3aHvTo";

export const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: true },
});
