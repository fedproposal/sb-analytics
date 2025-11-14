// worker.js — sb-analytics
import { Client } from "pg";

// Build CORS headers; echoes the caller Origin if allowed, else "*"
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);

  // if no list configured, allow everyone for GETs
  const allow =
    list.length === 0 || list.includes("*")
      ? origin || "*"
      : list.includes(origin)
      ? origin
      : list[0];

  return {
    "Access-Control-Allow-Origin": allow || "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
    "Vary": "Origin"
  };
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const baseHeaders = cors(origin, env);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: baseHeaders });
    }

    if (!url.pathname.startsWith("/sb/")) {
      return new Response("Not found", { status: 404, headers: baseHeaders });
    }

    // Simple health endpoint for debugging
    if (url.pathname === "/sb/health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...baseHeaders, "Content-Type": "application/json" }
      });
    }

    // Data endpoint used by the card
    if (url.pathname === "/sb/agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10))
      );

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
        ctx.waitUntil(client.end());

        return new Response(
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
          { status: 200, headers: { ...baseHeaders, "Content-Type": "application/json" } }
        );
      } catch (e) {
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          { status: 500, headers: { ...baseHeaders, "Content-Type": "application/json" } }
        );
      }
    }

    return new Response("Not found", { status: 404, headers: baseHeaders });
  }
};
