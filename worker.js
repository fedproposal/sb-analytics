import { Client } from "pg";

function parseIntSafe(v, d) {
  const n = parseInt(v ?? "", 10);
  return Number.isNaN(n) ? d : n;
}
function cors(env, req) {
  const origin = req.headers.get("origin") || "";
  const allowList = (env.ALLOWED_ORIGINS || "").split(/\s+/).filter(Boolean);
  const allow = allowList.includes(origin) ? origin : "*";
  return {
    "access-control-allow-origin": allow,
    "access-control-allow-methods": "GET,OPTIONS",
    "access-control-allow-headers": "content-type",
    "content-type": "application/json; charset=utf-8"
  };
}
function j(env, req, body, init = {}) {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: { ...cors(env, req), ...(init.headers || {}) }
  });
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    if (req.method === "OPTIONS") return new Response(null, { headers: cors(env, req) });

    // Health check
    if (url.pathname === "/_health") {
      try {
        const c = new Client({ connectionString: env.HYPERDRIVE.connectionString });
        await c.connect();
        const r = await c.query("SELECT 1 as ok");
        ctx.waitUntil(c.end());
        return j(env, req, { ok: true, result: r.rows[0] });
      } catch (e) {
        return j(env, req, { ok: false, error: String(e) }, { status: 500 });
      }
    }

    // GET /sb/agency-share?fy=2026&limit=12
    if (url.pathname === "/sb/agency-share") {
      const fy = parseIntSafe(url.searchParams.get("fy"), NaN);
      const limit = Math.max(1, Math.min(100, parseIntSafe(url.searchParams.get("limit"), 12)));
      if (!Number.isInteger(fy)) return j(env, req, { ok: false, error: "Missing or bad ?fy" }, { status: 400 });

      const view = env.VIEW_SB_AGENCY_SHARE || "public.sb_agency_share";
      const sql = `
        SELECT agency, sb_share_pct, dollars_total
        FROM ${view}
        WHERE fiscal_year = $1
        ORDER BY dollars_total DESC
        LIMIT $2;
      `;

      try {
        const c = new Client({ connectionString: env.HYPERDRIVE.connectionString });
        await c.connect();
        const r = await c.query(sql, [fy, limit]);
        ctx.waitUntil(c.end());
        return j(env, req, { ok: true, rows: r.rows });
      } catch (e) {
        return j(env, req, { ok: false, error: String(e) }, { status: 500 });
      }
    }

    return new Response("Not found", { status: 404 });
  }
};
