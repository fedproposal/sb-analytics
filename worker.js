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

// Helper to format dollars for the AI prompt (rough, human-readable)
function fmtMoney(n) {
  const num = Number(n || 0);
  if (!isFinite(num) || num <= 0) return "$0";
  if (num >= 1e9) return `$${(num / 1e9).toFixed(1)}B`;
  if (num >= 1e6) return `$${(num / 1e6).toFixed(1)}M`;
  if (num >= 1e3) return `$${(num / 1e3).toFixed(1)}K`;
  return `$${num.toFixed(0)}`;
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

    // Path segments (handles /sb/... and /prod/sb/...)
    const segments = url.pathname.split("/").filter(Boolean);
    const last = segments[segments.length - 1] || "";
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : "";

    /* =============================================================
     * Health: GET /sb/health
     * =========================================================== */
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    /* =============================================================
     * Agencies list (for dropdown / datalist)
     *   GET /sb/agencies
     *   Returns top-level + sub-agencies + offices
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
        ctx.waitUntil(client.end());

        return new Response(
          JSON.stringify({ ok: true, rows }), // [{ name: "U.S. Special Operations Command" }, ...]
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } catch (e) {
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }
    }

    /* =============================================================
     * 1) SB Agency Share (existing)
     *    GET /sb/agency-share?fy=2026&limit=12
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
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }
    }

    /* =============================================================
     * 2) Expiring Contracts (Neon-backed)
     *    GET /sb/expiring-contracts?naics=541519&agency=...&window_days=180&limit=50
     *
     *    NOTE: to keep performance reasonable on Neon (view), we use
     *    equality on agency instead of ILIKE contains.
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
            ${COL.PIID}     AS piid,
            ${COL.AWARD_ID} AS award_key,
            ${COL.AGENCY}   AS agency,
            ${COL.NAICS}    AS naics,
            ${COL.END_DATE} AS end_date,
            ${COL.VALUE}    AS value
          FROM ${USA_TABLE}
          WHERE
            ${COL.END_DATE} >= CURRENT_DATE
            AND ${COL.END_DATE} < CURRENT_DATE + $1::int
            AND (
              $2::text IS NULL
              OR ${COL.AGENCY}    = $2
              OR ${COL.SUB_AGENCY} = $2
            )
            AND (
              $3::text[] IS NULL
              OR ${COL.NAICS} = ANY($3)
            )
          ORDER BY ${COL.END_DATE} ASC
          LIMIT $4
        `;

        const params = [
          windowDays,                        // $1
          agencyFilter || null,              // $2
          naicsList.length ? naicsList : null, // $3
          limit,                             // $4
        ];

        const { rows } = await client.query(sql, params);
        ctx.waitUntil(client.end());

        const data = rows.map((r) => ({
          piid: r.piid,
          award_key: r.award_key,
          agency: r.agency,
          naics: r.naics,
          end_date: r.end_date,
          value: typeof r.value === "number" ? r.value : Number(r.value),
        }));

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json" },
        });
      } catch (e) {
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }
    }

    /* =============================================================
     * 3) Contract insights (AI + subs)
     *    POST /sb/contracts/insights
     *    Body: { piid: "HC102825F0042" }
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
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }

      const client = makeClient(env);
      try {
        const body = await request.json().catch(() => ({}));
        const piid = String(body.piid || "").trim().toUpperCase();
        if (!piid) {
          return new Response(
            JSON.stringify({ ok: false, error: "Missing piid" }),
            { status: 400, headers: { ...headers, "Content-Type": "application/json" } }
          );
        }

        await client.connect();

        // ---- 3a. Main contract snapshot ----
        const awardSql = `
          SELECT
            award_key,
            award_id_piid,
            awarding_agency_name,
            awarding_sub_agency_name,
            awarding_office_name,
            recipient_name,
            naics_code,
            naics_description,
            pop_start_date,
            pop_current_end_date,
            pop_potential_end_date,
            current_total_value_of_award_num,
            potential_total_value_of_award_num,
            total_dollars_obligated_num
          FROM public.usaspending_awards_v1
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC
          LIMIT 1
        `;
        const { rows: awardRows } = await client.query(awardSql, [piid]);

        if (!awardRows.length) {
          return new Response(
            JSON.stringify({ ok: false, error: "No award found for that PIID." }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } }
          );
        }

        const a = awardRows[0];

        // ---- 3b. Subaward snapshot directly from subawards table ----
        const subsSql = `
          SELECT
            COUNT(*)                          AS subaward_count,
            COUNT(DISTINCT subawardee_uei)    AS distinct_sub_recipients,
            COALESCE(SUM(subaward_amount),0)  AS total_subaward_amount,
            ARRAY_REMOVE(
              ARRAY_AGG(DISTINCT subawardee_name),
              NULL
            )                                 AS sample_sub_names
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1
        `;
        const { rows: subsRows } = await client.query(subsSql, [piid]);
        const subsRow = subsRows[0] || {};

        const subsSummary = {
          count: Number(subsRow.subaward_count || 0),
          distinctRecipients: Number(subsRow.distinct_sub_recipients || 0),
          totalAmount: Number(subsRow.total_subaward_amount || 0),
          top: Array.isArray(subsRow.sample_sub_names)
            ? subsRow.sample_sub_names.slice(0, 5)
            : [],
        };

        const burnPct =
          Number(a.potential_total_value_of_award_num || 0) > 0
            ? Number(a.total_dollars_obligated_num || 0) /
              Number(a.potential_total_value_of_award_num || 1)
            : null;

        const subsLine =
          subsSummary.count > 0
            ? `Subcontracts reported: ${subsSummary.count} actions, ${subsSummary.distinctRecipients} distinct subs, about ${fmtMoney(
                subsSummary.totalAmount
              )} total. Top subs: ${subsSummary.top.join(", ")}.`
            : "No subcontract awards are publicly reported for this PIID (data may be incomplete).";

        // ---- 3c. Build AI prompt ----
        const prompt = `
You are helping a small federal contractor quickly understand a single contract and where there may be opportunity.

Prime contract snapshot:
- PIID: ${a.award_id_piid}
- Awarding agency: ${a.awarding_agency_name || "—"}
- Sub-agency / office: ${a.awarding_sub_agency_name || "—"} / ${
          a.awarding_office_name || "—"
        }
- Prime recipient: ${a.recipient_name || "—"}
- NAICS: ${a.naics_code || "—"} – ${a.naics_description || "—"}
- Period of performance: ${a.pop_start_date || "—"} to ${
          a.pop_current_end_date || "—"
        } (potential through ${a.pop_potential_end_date || "—"})
- Total obligated to date: ${fmtMoney(a.total_dollars_obligated_num)}
- Current value (base + exercised options): ${fmtMoney(
          a.current_total_value_of_award_num
        )}
- Ceiling (base + all options): ${fmtMoney(
          a.potential_total_value_of_award_num
        )}
- Estimated burn vs ceiling: ${
          burnPct === null ? "unknown" : `${(burnPct * 100).toFixed(1)}% used`
        }

Subcontractor snapshot (from USAspending; reporting is often incomplete):
- ${subsLine}

In 4–6 concise bullet points, explain:

1) Where this contract is in its lifecycle (early / mid / near end / completed).
2) How heavily used it is (based on obligation vs ceiling and period of performance).
3) What the subcontractor footprint suggests about teaming or competition (mention specific subs by name when helpful).
4) What a small business should think about to position for the recompete or to subcontract/partner on follow-on work.

Keep the tone practical and non-technical. Avoid repeating the same phrasing across bullets.
        `.trim();

        // ---- 3d. Call OpenAI ----
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
                  "You are a federal contracts analyst helping small businesses spot opportunities in expiring contracts.",
              },
              { role: "user", content: prompt },
            ],
            max_tokens: 380,
          }),
        });

        const aiText = await aiRes.text();
        let aiJson = {};
        try {
          aiJson = aiText ? JSON.parse(aiText) : {};
        } catch {
          throw new Error(
            "AI response was not valid JSON: " + aiText.slice(0, 160)
          );
        }

        const summary =
          aiJson.choices?.[0]?.message?.content?.trim() ||
          "AI produced no summary.";

        const disclaimer =
          "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete.";

        return new Response(
          JSON.stringify({
            ok: true,
            summary,
            subs: subsSummary,
            disclaimer,
          }),
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: e?.message || "AI insight failed",
          }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
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
