// ── Supabase client ────────────────────────────────────────────
// Uses the ESM build so it can be imported as a module.
// The UMD <script> tag in index.html must be replaced with this import.
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm";

export const SUPABASE_URL = "https://uteiydpfxybtjzmdvsgc.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InV0ZWl5ZHBmeHlidGp6bWR2c2djIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM0MTY1ODYsImV4cCI6MjA4ODk5MjU4Nn0.bAIiJpwEjNdZojaVBd6-hnQA_lH9CgO65kKbihyGsGw";

export const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: true },
});
