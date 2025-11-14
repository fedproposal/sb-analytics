import { Client } from "pg";

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/sb/_health") {
      try {
        const c = new Client({ connectionString: env.HYPERDRIVE.connectionString });
        await c.connect();
        await c.query("select 1");
        ctx.waitUntil(c.end());
        return Response.json({ ok: true, db: true });
      } catch (e) {
        return Response.json({ ok: false, error: String(e) }, { status: 500 });
      }
    }

    // /sb/agency-share?fy=2026&limit=12
    if (url.pathname === "/sb/agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(1, Math.min(100, parseInt(url.searchParams.get("limit") || "12", 10)));

      const sql = `
        select agency, sb_share_pct, dollars_total
        from public.sb_agency_share
        where fiscal_year = $1
        order by dollars_total desc
        limit $2
      `;

      const client = new Client({ connectionString: env.HYPERDRIVE.connectionString });
      await client.connect();
      try {
        const { rows } = await client.query(sql, [fy, limit]);
        return Response.json({ ok: true, rows });
      } catch (e) {
        return Response.json({ ok: false, error: String(e) }, { status: 500 });
      } finally {
        ctx.waitUntil(client.end());
      }
    }

    return new Response("Not found", { status: 404 });
  }
};
