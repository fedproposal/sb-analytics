// worker.js — sb-analytics
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
    Vary: "Origin",
  };
}

const SB_API_BASE = "https://api.fedproposal.com/sb";

// ---------- Helpers ----------

async function makePgClient(env) {
  const client = new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

async function callOpenAI(env, prompt, data) {
  const apiKey = env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is not configured in Cloudflare env");
  }

  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      temperature: 0.2,
      max_tokens: 500,
      messages: [
        {
          role: "system",
          content:
            "You are a federal contracts analyst for a business development team. " +
            "You receive structured USAspending-style JSON for a single contract award, " +
            "plus its modification history and any subawards. " +
            "Write a concise, plain-English analysis that a small-business capture manager can act on.",
        },
        {
          role: "user",
          content: [
            {
              type: "text",
              text:
                prompt +
                "\n\nHere is the contract data as JSON. Do NOT echo the raw JSON, just reason over it.\n",
            },
            {
              type: "input_json",
              input_json: data,
            },
          ],
        },
      ],
    }),
  });

  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(json?.error?.message || `OpenAI HTTP ${res.status}`);
  }
  const text =
    json.choices?.[0]?.message?.content ||
    "No analysis text returned from model.";
  return text;
}

// ---------- Main Worker ----------

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const headers = cors(origin, env);

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    const path = url.pathname.replace(/^\/sb(\/|$)/, "/");

    // Simple health
    if (path === "/health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    // ==========================
    //  SB Agency Share (existing)
    // ==========================
    if (path === "/agency-share" && request.method === "GET") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10))
      );

      const client = await makePgClient(env);

      try {
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

    // ============================================
    //  NEW: /contracts/insights  (POST, JSON body)
    // ============================================
    if (path === "/contracts/insights" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch {
        return new Response(
          JSON.stringify({ ok: false, error: "Invalid JSON body" }),
          {
            status: 400,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      const piid = String(body.piid || "").trim();
      if (!piid) {
        return new Response(
          JSON.stringify({ ok: false, error: "Missing piid" }),
          {
            status: 400,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      const client = await makePgClient(env);

      try {
        // Main award row (latest action for this PIID)
        const awardRes = await client.query(
          `
          SELECT
            award_id_piid,
            award_type,
            awarding_agency_name,
            awarding_office_name,
            funding_agency_name,
            naics_code,
            naics_description,
            recipient_name,
            recipient_uei,
            recipient_duns,
            federal_action_obligation_num,
            current_total_value_of_award_num,
            potential_total_value_of_award_num,
            total_dollars_obligated_num,
            pop_start_date,
            pop_current_end_date,
            pop_potential_end_date,
            action_date,
            action_fy
          FROM public.usaspending_awards_v1
          WHERE award_id_piid = $1
          ORDER BY action_date DESC
          LIMIT 1
        `,
          [piid]
        );

        if (!awardRes.rows.length) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: `No award found for PIID ${piid}`,
            }),
            {
              status: 404,
              headers: { ...headers, "Content-Type": "application/json" },
            }
          );
        }

        const award = awardRes.rows[0];

        // Mod history (all actions for this PIID)
        const modsRes = await client.query(
          `
          SELECT
            action_date,
            action_fy,
            federal_action_obligation_num,
            total_dollars_obligated_num,
            current_total_value_of_award_num
          FROM public.usaspending_awards_v1
          WHERE award_id_piid = $1
          ORDER BY action_date ASC
          LIMIT 50
        `,
          [piid]
        );

        const mods = modsRes.rows || [];

        // Optional: subawards (adjust column names if needed)
        let subs = [];
        try {
          const subRes = await client.query(
            `
            SELECT
              award_id_piid,
              subaward_amount_num,
              subaward_recipient_name,
              subaward_action_date
            FROM public.usaspending_contract_subawards
            WHERE award_id_piid = $1
            ORDER BY subaward_action_date ASC
            LIMIT 50
          `,
            [piid]
          );
          subs = subRes.rows || [];
        } catch (e) {
          // If the table/columns don't exist yet, just ignore subawards.
          subs = [];
        }

        ctx.waitUntil(client.end());

        // Build compact data blob to send to OpenAI
        const aiData = {
          award,
          mods,
          subs,
        };

        const prompt =
          "Using the contract award data, modification history, and any subawards, " +
          "write a concise 2–3 paragraph analysis for a small-business capture manager.\n\n" +
          "Cover at least:\n" +
          "1) Who the incumbent is (name, and whether it *appears* to be small or large if evident).\n" +
          "2) Rough size of the contract (obligated and potential value) and remaining time in the period of performance.\n" +
          "3) Any interesting modification patterns (many mods? large funding spikes? apparent bridge/extension?).\n" +
          "4) When the recompete window is likely (base/option dates; if PoP end is within 12–24 months, call that out).\n" +
          "5) Any obvious capture angles or risks for a challenger (e.g., heavy incumbent concentration, lots of subs, etc.).\n\n" +
          "Do NOT guess specific NAICS size standards or certification status. If something is not in the data, say so plainly.";

        const summaryText = await callOpenAI(env, prompt, aiData);

        return new Response(
          JSON.stringify({
            ok: true,
            piid,
            summary: summaryText,
            meta: {
              hasMods: !!mods.length,
              modsCount: mods.length,
              subsCount: subs.length,
            },
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
            error: e?.message || "contract insights failed",
          }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }
    }

    // Fallback
    return new Response("Not found", { status: 404, headers });
  },
};
