// worker.js — sb-analytics
import { Client } from "pg"

// ---------- CORS helper ----------
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)

  const allow =
    list.length === 0 || list.includes("*")
      ? origin || "*"
      : list.includes(origin)
      ? origin
      : list[0]

  return {
    "Access-Control-Allow-Origin": allow || "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Max-Age": "86400",
    "Cache-Control": "no-store",
    Vary: "Origin",
  }
}

// ---------- DB client via Hyperdrive ----------
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  })
}

/**
 * ========================= COLUMN MAPPING =========================
 * These names are taken directly from: public.usaspending_awards_v1
 */
const USA_TABLE = "public.usaspending_awards_v1"

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
}
/* ================================================================= */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)
    const origin = request.headers.get("Origin") || ""
    const headers = cors(origin, env)

    // Normalize path segments once for all route checks
    const segments = url.pathname.split("/").filter(Boolean)
    // e.g. "/sb/contracts/insights" -> ["sb","contracts","insights"]
    const last = segments[segments.length - 1] || ""
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : ""

    // ---------- Preflight ----------
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers })
    }

    // ---------- Health ----------
    // e.g. GET /sb/health
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      })
    }

    /* =============================================================
     * 0) Agencies list (for dropdown / datalist)
     *    GET /sb/agencies
     *    Returns top-level + sub-agencies + offices
     * =========================================================== */
    if (last === "agencies") {
      const client = makeClient(env)
      try {
        await client.connect()
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
        `
        const { rows } = await client.query(sql)

        return new Response(JSON.stringify({ ok: true, rows }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try {
          await client.end()
        } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        )
      } finally {
        try {
          await client.end()
        } catch {}
      }
    }

    /* =============================================================
     * 1) SB Agency Share
     *    GET /sb/agency-share?fy=2026&limit=12
     * =========================================================== */
    if (last === "agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10)
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10))
      )

      const client = makeClient(env)

      try {
        await client.connect()
        const sql = `
          SELECT agency, sb_share_pct, dollars_total
          FROM public.sb_agency_share
          WHERE fiscal_year = $1
          ORDER BY dollars_total DESC
          LIMIT $2
        `
        const { rows } = await client.query(sql, [fy, limit])
        ctx.waitUntil(client.end())

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
        }))

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try {
          await client.end()
        } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        )
      }
    }

    /* =============================================================
     * 2) Expiring Contracts
     *    GET /sb/expiring-contracts?naics=541519&agency=VA&window_days=180&limit=50
     * =========================================================== */
    if (last === "expiring-contracts") {
      const naicsParam = (url.searchParams.get("naics") || "").trim()
      const agencyFilter = (url.searchParams.get("agency") || "").trim()
      const windowDays = Math.max(
        1,
        Math.min(
          365,
          parseInt(url.searchParams.get("window_days") || "180", 10)
        )
      )
      const limit = Math.max(
        1,
        Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10))
      )

      const naicsList =
        naicsParam.length > 0
          ? naicsParam
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          : []

      const client = makeClient(env)

      try {
        await client.connect()

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
              OR ${COL.AGENCY}     ILIKE '%' || $2 || '%'
              OR ${COL.SUB_AGENCY} ILIKE '%' || $2 || '%'
            )
            AND (
              $3::text[] IS NULL
              OR ${COL.NAICS} = ANY($3)
            )
          ORDER BY ${COL.END_DATE} ASC
          LIMIT $4
        `

        const params = [
          windowDays, // $1
          agencyFilter || null, // $2
          naicsList.length ? naicsList : null, // $3
          limit, // $4
        ]

        const { rows } = await client.query(sql, params)
        ctx.waitUntil(client.end())

        const data = rows.map((r) => ({
          piid: r.piid,
          award_key: r.award_key,
          agency: r.agency,
          naics: r.naics,
          end_date: r.end_date,
          // potential_total_value_of_award_num already chosen in COL.VALUE
          value: typeof r.value === "number" ? r.value : Number(r.value),
        }))

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try {
          await client.end()
        } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          {
            status: 500,
            headers: { ...headers, "Content-Type": "application/json" },
          }
        )
      }
    }

    /* =============================================================
     * 3) Contract insights (AI + subcontractors)
     *    POST /sb/contracts/insights  { piid: "HC102825F0042" }
     * =========================================================== */
    if (
      request.method === "POST" &&
      last === "insights" &&
      secondLast === "contracts"
    ) {
      if (!env.OPENAI_API_KEY) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: "OPENAI_API_KEY is not configured for sb-analytics.",
          }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        )
      }

      const client = makeClient(env)
      try {
        const body = await request.json().catch(() => ({}))
        const piid = String(body.piid || "").trim().toUpperCase()
        if (!piid) {
          return new Response(
            JSON.stringify({ ok: false, error: "Missing piid" }),
            { status: 400, headers: { ...headers, "Content-Type": "application/json" } }
          )
        }

        await client.connect()

        // 3a) Base award snapshot
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
        `
        const { rows: awardRows } = await client.query(awardSql, [piid])

        if (!awardRows.length) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: "No award found for that PIID.",
            }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } }
          )
        }

        const a = awardRows[0]

        // 3b) Subaward footprint for this prime (direct from subawards table)
        const subsParams = [a.award_id_piid, a.award_key]

        const subsSummarySql = `
          SELECT
            COUNT(*) AS subaward_count,
            COUNT(DISTINCT subawardee_uei) AS distinct_sub_recipients,
            COALESCE(SUM(subaward_amount),0) AS total_subaward_amount
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1
             OR prime_award_unique_key = $2
        `
        const subsTopSql = `
          SELECT
            subawardee_name,
            subawardee_uei,
            subaward_amount
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1
             OR prime_award_unique_key = $2
          ORDER BY subaward_amount DESC NULLS LAST
          LIMIT 10
        `

        const { rows: subsSummaryRows } = await client.query(
          subsSummarySql,
          subsParams
        )
        const { rows: subsTopRows } = await client.query(subsTopSql, subsParams)

        const subsSummaryRow = subsSummaryRows[0] || {
          subaward_count: 0,
          distinct_sub_recipients: 0,
          total_subaward_amount: 0,
        }

        const subs = {
          count: Number(subsSummaryRow.subaward_count || 0),
          distinctRecipients: Number(
            subsSummaryRow.distinct_sub_recipients || 0
          ),
          totalAmount: Number(subsSummaryRow.total_subaward_amount || 0),
          top: subsTopRows.map((r) => ({
            name: r.subawardee_name || null,
            uei: r.subawardee_uei || null,
            amount:
              typeof r.subaward_amount === "number"
                ? r.subaward_amount
                : Number(r.subaward_amount || 0),
          })),
        }

        const topNames = subs.top
          .map((s) => s.name)
          .filter(Boolean)
          .slice(0, 3)
          .join(", ")

        const subsBullet =
          subs.count > 0
            ? `- USAspending reports about ${subs.count} subcontract awards to ~${subs.distinctRecipients} recipients (approx. $${subs.totalAmount.toLocaleString("en-US", { maximumFractionDigits: 0 })} total). Notable subs include: ${topNames ||
                "names unavailable"}. Note: primes are not required to report all subs, so this list may be incomplete.`
            : `- USAspending does not list any subcontract awards for this PIID. Primes may still have unreported subcontractors.`

        // 3c) Build AI prompt
        const prompt = `
You are helping a small federal contractor quickly understand a single contract.

Contract snapshot:
- PIID: ${a.award_id_piid}
- Awarding agency: ${a.awarding_agency_name || "—"}
- Sub-agency / office: ${a.awarding_sub_agency_name || "—"} / ${a.awarding_office_name || "—"}
- Prime recipient: ${a.recipient_name || "—"}
- NAICS: ${a.naics_code || "—"} – ${a.naics_description || "—"}
- Period of performance: ${a.pop_start_date || "—"} to ${a.pop_current_end_date || "—"} (potential: ${a.pop_potential_end_date || "—"})
- Total obligated: ${a.total_dollars_obligated_num || 0}
- Current value (base + exercised): ${a.current_total_value_of_award_num || 0}
- Ceiling (base + all options): ${a.potential_total_value_of_award_num || 0}

Subcontractor footprint:
${subsBullet}

In 3–5 bullet points, explain:
1) Where this contract is in its lifecycle (early / mid / near end).
2) How heavily used it appears based on obligation vs ceiling.
3) What a small business should consider if they want to compete on the recompete or as a teaming partner (including whether teaming with any listed subs might make sense).

Keep it under 180 words, concise, and non-technical.
        `.trim()

        // 3d) Call OpenAI
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
                  "You are a federal contracts analyst helping small businesses.",
              },
              { role: "user", content: prompt },
            ],
            max_tokens: 350,
          }),
        })

        const aiText = await aiRes.text()
        let aiJson = {}
        try {
          aiJson = aiText ? JSON.parse(aiText) : {}
        } catch {
          throw new Error(
            "AI response was not valid JSON: " + aiText.slice(0, 120)
          )
        }

        const summary =
          aiJson.choices?.[0]?.message?.content?.trim() ||
          "AI produced no summary."

        const disclaimer =
          "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete."

        return new Response(
          JSON.stringify({ ok: true, summary, subs, disclaimer }),
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        )
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "AI insight failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        )
      } finally {
        try {
          await client.end()
        } catch {}
      }
    }

    // ---------- Fallback ----------
    return new Response("Not found", { status: 404, headers })
  },
}
