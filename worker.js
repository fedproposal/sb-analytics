// worker.js — sb-analytics
import { Client } from "pg";

// ---------- CORS helper ----------
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "")
    .split(",")
    .map((s) => s.trim())
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

// ---------- DB client via Hyperdrive ----------
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  });
}

/**
 * ========================= COLUMN MAPPING =========================
 *
 * These names are taken directly from:
 *   public.usaspending_awards_v1
 */
const USA_TABLE = "public.usaspending_awards_v1";

const COL = {
  // Date the current period of performance ends (DATE)
  END_DATE: "pop_current_end_date",

  // NAICS code (TEXT)
  NAICS: "naics_code",

  // Awarding agency name (TEXT)
  AGENCY: "awarding_agency_name",

  // Awarding sub-agency / component (TEXT)
  SUB_AGENCY: "awarding_sub_agency_name",

  // PIID / contract id (TEXT)
  PIID: "award_id_piid",

  // Award key / identifier (TEXT)
  AWARD_ID: "award_key",

  // Use *potential* total value (base + all options) as "Value"
  VALUE: "potential_total_value_of_award_num",
};
/* ================================================================= */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const headers = cors(origin, env);

    // Normalize path segments once for all route checks
    const segments = url.pathname.split("/").filter(Boolean);
    // e.g. "/sb/expiring-contracts" -> ["sb","expiring-contracts"]
    const last = segments[segments.length - 1] || "";
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : "";

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    // ---------- Health ----------
    // e.g. GET /sb/health
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    /* =============================================================
     * 0) Agencies list (for dropdown / datalist)
     *    GET /sb/agencies
     *    Returns top-level + sub-agencies + offices (distinct)
     * =========================================================== */
    if (last === "agencies") {
      const client = makeClient(env);
      try {
        await client.connect();
        const sql = `
          SELECT DISTINCT name
          FROM (
            SELECT awarding_agency_name      AS name
            FROM public.usaspending_awards_v1
            WHERE awarding_agency_name IS NOT NULL

            UNION

            SELECT awarding_sub_agency_name AS name
            FROM public.usaspending_awards_v1
            WHERE awarding_sub_agency_name IS NOT NULL

            UNION

            SELECT awarding_office_name     AS name
            FROM public.usaspending_awards_v1
            WHERE awarding_office_name IS NOT NULL
          ) AS x
          WHERE name IS NOT NULL
          ORDER BY name
          LIMIT 400;
        `;
        const { rows } = await client.query(sql);

        return new Response(
          JSON.stringify({ ok: true, rows }), // [{ name: "Department of Defense" }, { name: "U.S. Special Operations Command" }, ...]
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
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      } finally {
        try {
          await client.end();
        } catch {}
      }
    }

    /* =============================================================
     * 1) SB Agency Share (existing)
     *    GET /sb/agency-share?fy=2026&limit=12
     *    (or any route whose last segment is `agency-share`)
     * =========================================================== */
    if (last === "agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10);
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10))
      );

      const client = makeClient(env);

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

        const data = rows.map((r) => ({
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

    /* =============================================================
     * 2) Expiring Contracts (Neon-backed)
     *    GET /sb/expiring-contracts?naics=541519&agency=VA&window_days=180&limit=50
     *    (or any route whose last segment is `expiring-contracts`)
     * =========================================================== */
    if (last === "expiring-contracts") {
      const naicsParam = (url.searchParams.get("naics") || "").trim();
      const agencyFilter = (url.searchParams.get("agency") || "").trim();
      const windowDays = Math.max(
        1,
        Math.min(365, parseInt(url.searchParams.get("window_days") || "180", 10))
      );
      const limit = Math.max(
        1,
        Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10))
      );

      const naicsList =
        naicsParam.length > 0
          ? naicsParam
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          : [];

      const client = makeClient(env);

      try {
        await client.connect();

        const sql = `
          SELECT
            ${COL.PIID}       AS piid,
            ${COL.AWARD_ID}   AS award_key,
            ${COL.AGENCY}     AS agency,
            ${COL.NAICS}      AS naics,
            ${COL.END_DATE}   AS end_date,
            ${COL.VALUE}      AS value
          FROM ${USA_TABLE}
          WHERE
            ${COL.END_DATE} >= CURRENT_DATE
            AND ${COL.END_DATE} < CURRENT_DATE + $1::int
            AND (
              $2::text IS NULL
              OR ${COL.AGENCY}     ILIKE '%' || $2 || '%'
              OR ${COL.SUB_AGENCY} ILIKE '%' || $2 || '%'
            )
            AND (
              $3::text[] IS NULL
              OR ${COL.NAICS} = ANY($3)
            )
          ORDER BY ${COL.END_DATE} ASC
          LIMIT $4
        `;

        const params = [
          windowDays, // $1
          agencyFilter || null, // $2
          naicsList.length ? naicsList : null, // $3
          limit, // $4
        ];

        const { rows } = await client.query(sql, params);
        ctx.waitUntil(client.end());

        const data = rows.map((r) => ({
          piid: r.piid,
          award_key: r.award_key,
          agency: r.agency,
          naics: r.naics,
          end_date: r.end_date,
          // potential_total_value_of_award_num already chosen in COL.VALUE
          value: typeof r.value === "number" ? r.value : Number(r.value),
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

    /* =============================================================
     * 3) Contract insights (AI)
     *    POST /sb/contracts/insights  { piid: "HC102825F0042" }
     *    Matches any path that ends with "/contracts/insights"
     * =========================================================== */
    if (
      request.method === "POST" &&
      url.pathname.toLowerCase().endsWith("/contracts/insights")
    ) {
      if (!env.OPENAI_API_KEY) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: "OPENAI_API_KEY is not configured for sb-analytics.",
          }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      }

      const client = makeClient(env);
      try {
        const body = await request.json().catch(() => ({}));
        const piid = String(body.piid || "").trim().toUpperCase();
        if (!piid) {
          return new Response(
            JSON.stringify({ ok: false, error: "Missing piid" }),
            {
              status: 400,
              headers: { ...headers, "Content-Type": "application/json" },
            }
          );
        }

        await client.connect();

        // Pull contract snapshot plus aggregated subaward footprint
        const sql = `
          SELECT
            a.award_id_piid,
            a.awarding_agency_name,
            a.awarding_sub_agency_name,
            a.awarding_office_name,
            a.recipient_name,
            a.naics_code,
            a.naics_description,
            a.pop_start_date,
            a.pop_current_end_date,
            a.pop_potential_end_date,
            a.current_total_value_of_award_num,
            a.potential_total_value_of_award_num,
            a.total_dollars_obligated_num,

            s.subaward_count,
            s.distinct_sub_recipients,
            s.total_subaward_amount,
            s.sample_sub_names
          FROM public.usaspending_awards_v1 AS a
          LEFT JOIN public.fp_contract_incumbent_subs_agg AS s
            ON s.prime_piid = a.award_id_piid
          WHERE a.award_id_piid = $1
          ORDER BY a.pop_current_end_date DESC
          LIMIT 1
        `;
        const { rows } = await client.query(sql, [piid]);

        if (!rows.length) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: "No award found for that PIID.",
            }),
            {
              status: 404,
              headers: { ...headers, "Content-Type": "application/json" },
            }
          );
        }

        const a = rows[0];
        const subCount = Number(a.subaward_count || 0);
        const distinctSubs = Number(a.distinct_sub_recipients || 0);
        const subsAmount = Number(a.total_subaward_amount || 0);
        const sampleSubs = Array.isArray(a.sample_sub_names)
          ? a.sample_sub_names.filter(Boolean)
          : [];

        const subsSummary =
          subCount > 0
            ? `Subaward footprint: approximately ${subCount} subawards to ${distinctSubs} distinct subrecipients, totaling about $${subsAmount.toLocaleString("en-US", { maximumFractionDigits: 0 })}. Sample subcontractors: ${sampleSubs
                .slice(0, 5)
                .join(", ")}.`
            : "No subcontract award data was found for this PIID in the subaward dataset.";

        // Build a compact prompt – includes subs
        const prompt = `
You are helping a small federal contractor quickly understand a single contract and its subcontracting footprint.

Contract snapshot:
- PIID: ${a.award_id_piid}
- Awarding agency: ${a.awarding_agency_name || "—"}
- Sub-agency / office: ${a.awarding_sub_agency_name || "—"} / ${
          a.awarding_office_name || "—"
        }
- Prime contractor (incumbent): ${a.recipient_name || "—"}
- NAICS: ${a.naics_code || "—"} – ${a.naics_description || "—"}
- Period of performance: ${a.pop_start_date || "—"} to ${
          a.pop_current_end_date || "—"
        } (potential: ${a.pop_potential_end_date || "—"})
- Total obligated to date: ${a.total_dollars_obligated_num || 0}
- Current value (base + exercised options): ${
          a.current_total_value_of_award_num || 0
        }
- Ceiling (base + all options): ${
          a.potential_total_value_of_award_num || 0
        }

Subcontracting summary:
- ${subsSummary}

In 4–6 bullet points, explain in plain language:
1) Where this contract is in its lifecycle (early / mid / near end).
2) How heavily used it appears based on obligations vs ceiling.
3) What the subcontracting picture suggests (e.g., prime doing most work vs meaningful subs, any opportunities to partner with likely small-business subs).
4) What a small business should consider if they want to compete on the recompete or position as a teaming partner.

Keep it under 220 words, concise, and non-technical.
        `.trim();

        const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${env.OPENAI_API_KEY}`,
          },
          body: JSON.stringify({
            model: "gpt-4.1-mini",
            messages: [
              {
                role: "system",
                content:
                  "You are a federal contracts analyst helping small businesses interpret contract and subcontracting data.",
              },
              { role: "user", content: prompt },
            ],
            max_tokens: 400,
          }),
        });

        const aiText = await aiRes.text();
        let aiJson = {};
        try {
          aiJson = aiText ? JSON.parse(aiText) : {};
        } catch {
          throw new Error(
            "AI response was not valid JSON: " + aiText.slice(0, 120)
          );
        }

        const summary =
          aiJson.choices?.[0]?.message?.content?.trim() ||
          "AI produced no summary.";

        return new Response(
          JSON.stringify({ ok: true, summary }),
          {
            status: 200,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: e?.message || "AI insight failed",
          }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        );
      } finally {
        try {
          await client.end();
        } catch {}
      }
    }

    // ---------- Fallback ----------
    return new Response("Not found", { status: 404, headers });
  },
};
