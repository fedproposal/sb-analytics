// worker.js — sb-analytics (root + /sb path support)
import { Client } from "pg";

// CORS helper
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);
  const allow =
    list.length === 0 || list.includes("*")
      ? origin || "*"
      : list.includes(origin)
      ? origin
      : list[0];
  return {
    "Access-Control-Allow-Origin": allow || "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
    Vary: "Origin",
  };
}

// Helper: create PG client
function createClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const headers = cors(origin, env);

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    // Normalize path: strip an optional leading `/sb`
    // "/sb/agency-share" -> "/agency-share"
    // "/agency-share"    -> "/agency-share"
    // "/sb/ai/search"    -> "/ai/search"
    // "/sb/ai/ask"       -> "/ai/ask"
    const path = url.pathname.replace(/^\/sb(\/|$)/, "/");

    // Simple health
    if (path === "/health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    // ---------- AI SEARCH (forecasts + SAM) ----------
    // GET https://api.fedproposal.com/sb/ai/search?q=...&source=forecast|sam_notice&limit=...
    if (path === "/ai/search" && request.method === "GET") {
      const q = (url.searchParams.get("q") || "").trim();
      let source = (url.searchParams.get("source") || "").trim();

      // Default to forecasts if not provided
      if (!source) source = "forecast";

      const limitParam = parseInt(url.searchParams.get("limit") || "20", 10);
      const limit = Number.isFinite(limitParam)
        ? Math.min(Math.max(limitParam, 1), 50)
        : 20;

      if (!q) {
        return new Response(
          JSON.stringify({ ok: false, error: "Missing q parameter" }),
          {
            status: 400,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      // For now, don't allow direct USAspending search (too big / slow without indexing).
      if (source === "usaspending_contract_award") {
        return new Response(
          JSON.stringify({
            ok: false,
            error:
              "Direct USAspending AI search is not enabled yet. Try source=forecast or source=sam_notice.",
          }),
          {
            status: 400,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      // Choose which AI view to query based on source
      let table = "ai_forecasts_v1";
      if (source === "sam_notice") {
        table = "ai_sam_notices_v1";
      } else {
        source = "forecast";
      }

      const client = createClient(env);

      try {
        await client.connect();

        const sql = `
          SELECT
            doc_id,
            source,
            doc_date,
            agency,
            naics_code,
            LEFT(doc_text, 4000) AS doc_text
          FROM ${table}
          WHERE doc_text ILIKE '%' || $1::text || '%'
          ORDER BY doc_date DESC NULLS LAST
          LIMIT $2::int
        `;

        const { rows } = await client.query(sql, [q, limit]);

        ctx.waitUntil(client.end());

        return new Response(
          JSON.stringify({
            ok: true,
            query: { q, source, limit },
            rows,
          }),
          {
            status: 200,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      } catch (e) {
        try {
          await client.end();
        } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "search failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }
    }

    // ---------- AI ASK (uses the same views + OpenAI) ----------
    // POST https://api.fedproposal.com/sb/ai/ask
    // body: { question: string, source?: "forecast" | "sam_notice", limit?: number }
    if (path === "/ai/ask" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch {
        body = {};
      }

      const question = (body.question || "").trim();
      let source = (body.source || "forecast").trim();
      const limitParam = parseInt(String(body.limit || "10"), 10);
      const limit = Number.isFinite(limitParam)
        ? Math.min(Math.max(limitParam, 1), 10)
        : 10;

      if (!question) {
        return new Response(
          JSON.stringify({ ok: false, error: "Missing question" }),
          {
            status: 400,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      if (!env.OPENAI_API_KEY) {
        return new Response(
          JSON.stringify({
            ok: false,
            error:
              "OPENAI_API_KEY is not configured on this worker. Ask AI backend is not ready yet.",
          }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      // For now, restrict to forecast or sam_notice
      let table = "ai_forecasts_v1";
      if (source === "sam_notice") {
        table = "ai_sam_notices_v1";
      } else {
        source = "forecast";
      }

      const client = createClient(env);

      try {
        await client.connect();

        const sql = `
          SELECT
            doc_id,
            source,
            doc_date,
            agency,
            naics_code,
            LEFT(doc_text, 2000) AS doc_text
          FROM ${table}
          WHERE doc_text ILIKE '%' || $1::text || '%'
          ORDER BY doc_date DESC NULLS LAST
          LIMIT $2::int
        `;

        // Use the *question* itself as the keyword term for now
        const { rows } = await client.query(sql, [question, limit]);
        ctx.waitUntil(client.end());

        // Build context text for the AI
        const contextPieces = rows.map((r, idx) => {
          return [
            `Record ${idx + 1}:`,
            `  doc_id: ${r.doc_id}`,
            `  source: ${r.source}`,
            r.agency ? `  agency: ${r.agency}` : "",
            r.naics_code ? `  naics_code: ${r.naics_code}` : "",
            `  content: ${r.doc_text}`,
          ]
            .filter(Boolean)
            .join("\n");
        });

        const context =
          contextPieces.length > 0
            ? contextPieces.join("\n\n---\n\n")
            : "No matching records were found in the database.";

        // Call OpenAI
        const messages = [
          {
            role: "system",
            content:
              "You are a helpful federal government contracts research assistant. " +
              "You answer questions using the data records provided. " +
              "When you reference specific data, mention the doc_id and source (forecast or sam_notice). " +
              "If the data is not available, say that you don't know rather than guessing.",
          },
          {
            role: "user",
            content:
              `User question:\n${question}\n\n` +
              `Relevant data records from the database:\n${context}`,
          },
        ];

        const aiRes = await fetch(
          "https://api.openai.com/v1/chat/completions",
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${env.OPENAI_API_KEY}`,
            },
            body: JSON.stringify({
              model: "gpt-4.1-mini",
              messages,
              temperature: 0.2,
            }),
          }
        );

        if (!aiRes.ok) {
          const errText = await aiRes.text();
          return new Response(
            JSON.stringify({
              ok: false,
              error: `OpenAI error: HTTP ${aiRes.status} ${errText}`,
            }),
            {
              status: 500,
              headers: { ...headers, "Content-Type": "application/json" },
            }
          );
        }

        const aiJson = await aiRes.json();
        const answer =
          aiJson?.choices?.[0]?.message?.content ||
          "I was not able to generate an answer.";

        return new Response(
          JSON.stringify({
            ok: true,
            source,
            question,
            answer,
            refs: rows.map(r => ({
              doc_id: r.doc_id,
              source: r.source,
              doc_date: r.doc_date,
              agency: r.agency,
              naics_code: r.naics_code,
            })),
          }),
          {
            status: 200,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      } catch (e) {
        try {
          await client.end();
        } catch {}
        return new Response(
          JSON.stringify({
            ok: false,
            error: e?.message || "ask-ai failed",
          }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }
    }

    // ---------- SB agency share endpoint (donut) ----------
    if (path === "/agency-share" && request.method === "GET") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10))
      );

      const client = createClient(env);

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

        const data = rows.map(r => ({
          agency: r.agency,
          sb_share_pct:
            typeof r.sb_share_pct === "number"
              ? r.sb_share_pct
              : Number(r.sb_share_pct),
          dollars_total:
            typeof r.dollars_total === "number"
              ? r.dollars_total
              : Number(r.dollars_total),
        }));

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json" },
        });
      } catch (e) {
        try {
          await client.end();
        } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }
    }

    return new Response("Not found", { status: 404, headers });
  },
};
