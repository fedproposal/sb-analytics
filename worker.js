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
 *
 * Table:
 *   public.usaspending_awards_v1
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

    // ---------- Preflight ----------
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers })
    }

    // ---------- Normalize path segments ----------
    // Works for:
    //   /sb/expiring-contracts
    //   /prod/sb/expiring-contracts
    //   /sb/contracts/insights
    //   /sb/agencies
    const segments = url.pathname.split("/").filter(Boolean)
    const last = segments[segments.length - 1] || ""
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : ""

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
            FROM ${USA_TABLE}
            WHERE awarding_agency_name IS NOT NULL

            UNION

            SELECT awarding_sub_agency_name AS name
            FROM ${USA_TABLE}
            WHERE awarding_sub_agency_name IS NOT NULL

            UNION

            SELECT awarding_office_name     AS name
            FROM ${USA_TABLE}
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
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
        )
      } finally {
        try {
          await client.end()
        } catch {}
      }
    }

    /* =============================================================
     * 1) SB Agency Share (existing)
     *    GET /sb/agency-share?fy=2026&limit=12
     *    (or any route whose last segment is `agency-share`)
     * =========================================================== */
    if (last === "agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10)
      const limit = Math.max(
        1,
        Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10)),
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
          },
        )
      }
    }

    /* =============================================================
     * 2) Expiring Contracts (Neon-backed)
     *    GET /sb/expiring-contracts?naics=541519&agency=VA&window_days=180&limit=50
     *    (or any route whose last segment is `expiring-contracts`)
     *
     *    IMPORTANT: agency filter now uses *exact* matches against
     *    agency / sub-agency / office names to avoid slow `%...%`
     *    scans and timeouts.
     * =========================================================== */
    if (last === "expiring-contracts") {
      const naicsParam = (url.searchParams.get("naics") || "").trim()
      const agencyFilter = (url.searchParams.get("agency") || "").trim()
      const windowDays = Math.max(
        1,
        Math.min(365, parseInt(url.searchParams.get("window_days") || "180", 10)),
      )
      const limit = Math.max(
        1,
        Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10)),
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
              OR ${COL.AGENCY}      = $2
              OR ${COL.SUB_AGENCY}  = $2
              OR awarding_office_name = $2
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
          value:
            typeof r.value === "number"
              ? r.value
              : Number(r.value ?? null),
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
          },
        )
      }
    }

    /* =============================================================
     * 3) Contract insights (AI + subcontracts)
     *    POST /sb/contracts/insights  { piid: "HC102825F0042" }
     *    Matches any path that ends with "/contracts/insights"
     *
     *    - Pulls core award data from usaspending_awards_v1
     *    - Pulls subcontract data from usaspending_contract_subawards
     *    - Computes lifecycle & burn in code so AI can’t contradict it
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
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
        )
      }

      const client = makeClient(env)
      try {
        const body = await request.json().catch(() => ({}))
        const piid = String(body.piid || "").trim().toUpperCase()
        if (!piid) {
          return new Response(
            JSON.stringify({ ok: false, error: "Missing piid" }),
            { status: 400, headers: { ...headers, "Content-Type": "application/json" } },
          )
        }

        await client.connect()

        // ---- 3a. Core award snapshot ----
        const awardSql = `
          SELECT
            award_id_piid,
            awarding_agency_name,
            awarding_sub_agency_name,
            awarding_office_name,
            recipient_name,
            recipient_uei,
            naics_code,
            naics_description,
            pop_start_date,
            pop_current_end_date,
            pop_potential_end_date,
            current_total_value_of_award_num,
            potential_total_value_of_award_num,
            total_dollars_obligated_num
          FROM ${USA_TABLE}
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC
          LIMIT 1
        `
        const awardRes = await client.query(awardSql, [piid])

        if (!awardRes.rows.length) {
          return new Response(
            JSON.stringify({ ok: false, error: "No award found for that PIID." }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } },
          )
        }

        const a = awardRes.rows[0]

        const toNumber = (x) =>
          typeof x === "number" ? x : x == null ? null : Number(x)

        const obligated = toNumber(a.total_dollars_obligated_num) ?? 0
        const currentValue =
          toNumber(a.current_total_value_of_award_num) ??
          toNumber(a.potential_total_value_of_award_num) ??
          0
        const ceiling = toNumber(a.potential_total_value_of_award_num) ?? currentValue

        // ---- 3b. Lifecycle and window in code ----
        const today = new Date()

        const parseDate = (d) => (d ? new Date(d) : null)
        const startDate = parseDate(a.pop_start_date)
        const currentEnd = parseDate(a.pop_current_end_date)
        const potentialEnd = parseDate(a.pop_potential_end_date)

        // Use potential end if available, otherwise current end
        const endForLifecycle = potentialEnd || currentEnd

        let lifecycleStage = "unknown"
        let lifecycleLabel = "Lifecycle insight limited"
        let windowLabel = "Window unknown"
        let timeElapsedPct = null
        let burnPct = null

        if (startDate && endForLifecycle && endForLifecycle > startDate) {
          const totalMs = endForLifecycle.getTime() - startDate.getTime()
          const clampedNow = Math.min(
            Math.max(today.getTime(), startDate.getTime()),
            endForLifecycle.getTime(),
          )
          const elapsedMs = clampedNow - startDate.getTime()
          timeElapsedPct = Math.round((elapsedMs / totalMs) * 100)

          if (today < startDate) {
            lifecycleStage = "not_started"
            lifecycleLabel = "Not started yet"
            windowLabel = "Window not opened"
          } else if (today > endForLifecycle) {
            lifecycleStage = "complete"
            lifecycleLabel = "Performance complete"
            windowLabel = "Window passed"
          } else if (timeElapsedPct < 25) {
            lifecycleStage = "early"
            lifecycleLabel = "Early stage"
            windowLabel = "In performance window"
          } else if (timeElapsedPct < 75) {
            lifecycleStage = "mid"
            lifecycleLabel = "Mid-stage"
            windowLabel = "In performance window"
          } else {
            lifecycleStage = "late"
            lifecycleLabel = "Late / near end"
            windowLabel = "In performance window"
          }
        }

        if (ceiling && ceiling > 0) {
          burnPct = Math.round((obligated / ceiling) * 100)
        }

        // ---- 3c. Subcontracts for this PIID ----
        const subsSql = `
          SELECT
            subawardee_name,
            subawardee_uei,
            subaward_amount
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1
        `
        const subsRes = await client.query(subsSql, [piid])
        const subsRaw = subsRes.rows || []

        // Aggregate by (name, uei)
        const subMap = new Map()
        for (const row of subsRaw) {
          const name = row.subawardee_name || "(Unnamed subrecipient)"
          const uei = row.subawardee_uei || null
          const amt = toNumber(row.subaward_amount) || 0
          const key = `${uei || "NOUEI"}|${name}`
          const prev = subMap.get(key) || { name, uei, amount: 0 }
          prev.amount += amt
          subMap.set(key, prev)
        }

        const subsAgg = Array.from(subMap.values())
        subsAgg.sort((a, b) => (b.amount || 0) - (a.amount || 0))

        const subCount = subsRaw.length
        const distinctRecipients = subsAgg.length
        const totalSubAmount = subsAgg.reduce((sum, s) => sum + (s.amount || 0), 0)

        const topSubs = subsAgg.slice(0, 5)

        let primeVsSubsPct = null
        let largestSubPct = null

        if (obligated > 0 && totalSubAmount > 0) {
          const subPct = Math.min(100, (totalSubAmount / obligated) * 100)
          primeVsSubsPct = {
            prime: Math.round(100 - subPct),
            subs: Math.round(subPct),
          }
        }

        if (totalSubAmount > 0 && topSubs.length > 0) {
          largestSubPct = Math.round((topSubs[0].amount / totalSubAmount) * 100)
        }

        const disclaimer =
          "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete."

        // ---- 3d. Build structured context for the UI ----
        const primary = {
          piid: a.award_id_piid,
          agency: a.awarding_agency_name || null,
          subAgency: a.awarding_sub_agency_name || null,
          office: a.awarding_office_name || null,
          primeName: a.recipient_name || null,
          primeUei: a.recipient_uei || null,
          naicsCode: a.naics_code || null,
          naicsDescription: a.naics_description || null,
          popStartDate: a.pop_start_date || null,
          popCurrentEndDate: a.pop_current_end_date || null,
          popPotentialEndDate: a.pop_potential_end_date || null,
          obligated,
          currentValue,
          ceiling,
        }

        const lifecycle = {
          stage: lifecycleStage,
          label: lifecycleLabel,
          windowLabel,
          timeElapsedPct,
          burnPct,
          primeVsSubsPct,
          largestSubPct,
        }

        const subs = {
          count: subCount,
          distinctRecipients,
          totalAmount: totalSubAmount,
          top: topSubs,
        }

        // ---- 3e. AI summary (uses lifecycle + subs, but doesn't own them) ----
        const burnText =
          burnPct == null
            ? "Burn vs. ceiling could not be determined."
            : `Approximately ${burnPct}% of the contract ceiling is obligated (≈$${Math.round(
                obligated,
              ).toLocaleString("en-US")} of ≈$${Math.round(
                ceiling,
              ).toLocaleString("en-US")}).`

        const lifecycleText =
          lifecycleStage === "not_started"
            ? "The contract has not yet started."
            : lifecycleStage === "complete"
            ? "The contract’s period of performance appears to be complete."
            : lifecycleStage === "early"
            ? "The contract is in its early stage of performance."
            : lifecycleStage === "mid"
            ? "The contract is mid-way through its performance period."
            : lifecycleStage === "late"
            ? "The contract is late in its performance period (near the end)."
            : "Lifecycle stage is limited due to incomplete dates."

        const subsText =
          subCount === 0
            ? "No subcontract awards are publicly reported for this contract; teaming may require direct outreach to the prime."
            : `There are ${subCount} reported subawards to ${distinctRecipients} unique recipients, totaling about $${Math.round(
                totalSubAmount,
              ).toLocaleString("en-US")}.`

        const topSubText =
          topSubs.length === 0
            ? ""
            : `Top reported subs include ${topSubs
                .slice(0, 3)
                .map((s) => `${s.name} (UEI ${s.uei || "unknown"})`)
                .join(", ")}.`

        const prompt = `
You are helping a small federal contractor quickly understand a single contract and how to position for a recompete or subcontracting role.

Treat the lifecycle, burn %, and subcontracting figures below as correct. Do not contradict them.

Contract snapshot:
- PIID: ${primary.piid}
- Awarding agency: ${primary.agency || "—"}
- Component / office: ${primary.subAgency || "—"} / ${primary.office || "—"}
- Prime: ${primary.primeName || "—"} (UEI: ${primary.primeUei || "unknown"})
- NAICS: ${primary.naicsCode || "—"} – ${primary.naicsDescription || "—"}
- Period of performance: ${primary.popStartDate || "—"} to ${
          primary.popCurrentEndDate || "—"
        } (potential: ${primary.popPotentialEndDate || "—"})
- Lifecycle: ${lifecycle.label} (time elapsed ≈${
          timeElapsedPct == null ? "unknown" : timeElapsedPct + "%"
        })
- Burn vs ceiling: ${burnText}
- Subcontracting footprint: ${subsText} ${topSubText}

Write 4–5 bullet points that:
1) Explain where this contract is in its lifecycle and what that means tactically.
2) Interpret how heavily used the vehicle is based on burn vs ceiling (and whether there is room left for work).
3) Describe what the subcontracting pattern suggests (no subs, concentrated with a few, or spread across many) and how that affects teaming strategy.
4) Give concrete next actions for a small business (e.g., which offices or primes/subs to talk to, what to research, and when in the likely recompete window to act).

Avoid generic advice like "build relationships" unless you tie it specifically to the agency, office, prime, or named subs above.
Keep it under 200 words, concise, and non-technical.
        `.trim()

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
                  "You are a federal contracts analyst helping small businesses interpret USAspending and subcontract data.",
              },
              { role: "user", content: prompt },
            ],
            max_tokens: 400,
          }),
        })

        const aiText = await aiRes.text()
        let aiJson = {}
        try {
          aiJson = aiText ? JSON.parse(aiText) : {}
        } catch {
          throw new Error("AI response was not valid JSON: " + aiText.slice(0, 160))
        }

        const summary =
          aiJson.choices?.[0]?.message?.content?.trim() ||
          "AI produced no summary."

        return new Response(
          JSON.stringify({
            ok: true,
            summary,
            primary,
            lifecycle,
            subs,
            disclaimer,
          }),
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } },
        )
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "AI insight failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } },
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
