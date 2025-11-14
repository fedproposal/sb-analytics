// worker.js — sb-analytics (Cloudflare Workers + Hyperdrive + CORS)
import { Client } from "pg";

/* ---------------- CORS helpers ---------------- */
function parseAllowed(env) {
  const raw = env.ALLOWED_ORIGINS || env.CORS_ALLOW_ORIGINS || "";
  return raw
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);
}

function isAllowedOrigin(origin, env) {
  if (!origin) return true; // direct tab open (no Origin) -> allow
  const list = parseAllowed(env);
  if (list.length === 0) return true;          // no config -> allow
  if (list.includes("*")) return true;         // wildcard -> allow
  return list.includes(origin);                // exact match only
}

function corsHeaders(origin, allow) {
  const h = new Headers({
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
    "Vary": "Origin"
  });
  if (allow && origin) h.set("Access-Control-Allow-Origin", origin);
  // if no Origin (e.g., curl), do not set ACAO — not needed
  return h;
}

function withCORS(resp, origin, allow) {
  const h = corsHeaders(origin, allow);
  const out = new Headers(resp.headers);
  for (const [k, v] of h) out.set(k, v);
  return new Response(resp.body, { status: resp.status, headers: out });
}

/* ---------------- Worker ---------------- */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const allowed = isAllowedOrigin(origin, env);

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin, allowed) });
    }

    // Gate by allowed origins (send 403, not 404, with CORS so browser can see it)
    if (!allowed) {
      return withCORS(
        new Response(JSON.stringify({ ok: false, error: "Forbidden origin" }), {
          status: 403,
          headers: { "Content-Type": "application/json" }
        }),
        origin,
        true
      );
    }

    // All endpoints live under /sb/*
    if (!url.pathname.startsWith("/sb/")) {
      return withCORS(new Response("Not found", { status: 404 }), origin, true);
    }

    // Health check
    if (url.pathname === "/sb/health") {
      return withCORS(
        new Response(JSON.stringify({ ok: true, db: true }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        }),
        origin,
        true
      );
    }

    // Data endpoint used by the card
    if (url.pathname === "/sb/agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(1, Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10)));

      const client = new Client({
        connectionString: env.HYPERDRIVE.connectionString,
        ssl: { rejectUnauthorized: false }
      });

      try {
        await client.connect();

        const sql = `
          SELECT agency, sb_share_pct, dollars_total
          FROM public.sb_agency_share
          WHERE fiscal_year = $1
          ORDER BY dollars_total DESC
          LIMIT $2
        `;

        const { rows } = await client.query(sql, [fy, limit]);

        return withCORS(
          new Response(
            JSON.stringify({
              ok: true,
              rows: rows.map(r => ({
                agency: r.agency,
                sb_share_pct:
                  typeof r.sb_share_pct === "number"
                    ? r.sb_share_pct
                    : Number(r.sb_share_pct),
                dollars_total:
                  typeof r.dollars_total === "number"
                    ? r.dollars_total
                    : Number(r.dollars_total)
              }))
            }),
            { status: 200, headers: { "Content-Type": "application/json" } }
          ),
          origin,
          true
        );
      } catch (e) {
        return withCORS(
          new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          }),
          origin,
          true
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    // Fallback
    return withCORS(new Response("Not found", { status: 404 }), origin, true);
  }
};
