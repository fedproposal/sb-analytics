// worker.js — sb-analytics
import { Client } from "pg"

/* ========================= CORS ========================= */
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

// Re-attach CORS headers to cached responses
function withCors(res, headers) {
  const h = new Headers(res.headers)
  for (const [k, v] of Object.entries(headers || {})) h.set(k, v)
  return new Response(res.body, { status: res.status, headers: h })
}

/* ========================= DB client (Hyperdrive) ========================= */
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  })
}

/* ========================= Column mapping ========================= */
const USA_TABLE = "public.usaspending_awards_v1"

const COL = {
  END_DATE: "pop_current_end_date",
  NAICS: "naics_code",
  AGENCY: "awarding_agency_name",
  SUB_AGENCY: "awarding_sub_agency_name",
  PIID: "award_id_piid",
  AWARD_ID: "award_key",
  VALUE: "potential_total_value_of_award_num",
}

/* ========================= Optional SAM helper ========================= */
async function fetchVendorWebsiteByUEI(uei, env) {
  const key = env.SAM_API_KEY
  if (!key || !uei) return null
  try {
    const u = new URL("https://api.sam.gov/entity-information/v2/entities")
    u.searchParams.set("ueiSAM", uei)
    u.searchParams.set("api_key", key)
    const r = await fetch(u.toString(), { cf: { cacheTtl: 86400, cacheEverything: true } })
    if (!r.ok) return null
    const j = await r.json()
    const ent =
      j?.entityRegistration ||
      (Array.isArray(j?.entities) ? j.entities[0] : null) ||
      (Array.isArray(j?.results) ? j.results[0] : null) ||
      null
    const website =
      ent?.coreData?.businessInformation?.url ||
      ent?.coreData?.generalInformation?.corporateUrl ||
      ent?.coreData?.generalInformation?.url ||
      null
    return website && typeof website === "string" ? website.trim() : null
  } catch {
    return null
  }
}

/* ========================= Worker ========================= */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)
    const origin = request.headers.get("Origin") || ""
    const headers = cors(origin, env)

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers })
    }

    // Path parsing
    const segments = url.pathname.split("/").filter(Boolean)
    const last = segments[segments.length - 1] || ""
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : ""

    /* ========================= Health ========================= */
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      })
    }

    /* ========================= Agencies (cached 24h) =========================
     * GET /sb/agencies
     */
    if (last === "agencies") {
      const cache = caches.default
      const cacheKey = new Request(url.toString(), request)
      const cached = await cache.match(cacheKey)
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=86400, stale-while-revalidate=604800" },
        )
      }

      const client = makeClient(env)
      try {
        await client.connect()
        const sql = `
          SELECT DISTINCT name
          FROM (
            SELECT awarding_agency_name      AS name FROM ${USA_TABLE} WHERE awarding_agency_name IS NOT NULL
            UNION
            SELECT awarding_sub_agency_name AS name FROM ${USA_TABLE} WHERE awarding_sub_agency_name IS NOT NULL
            UNION
            SELECT awarding_office_name     AS name FROM ${USA_TABLE} WHERE awarding_office_name IS NOT NULL
          ) x
          WHERE name IS NOT NULL
          ORDER BY name
          LIMIT 400;
        `
        const { rows } = await client.query(sql)

        const res = new Response(JSON.stringify({ ok: true, rows }), {
          status: 200,
          headers: {
            ...headers,
            "Content-Type": "application/json",
            "Cache-Control": "public, s-maxage=86400, stale-while-revalidate=604800",
          },
        })
        ctx.waitUntil(cache.put(cacheKey, res.clone()))
        return res
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= SB Agency Share =========================
     * GET /sb/agency-share?fy=2026&limit=12
     */
    if (last === "agency-share") {
      const fy = parseInt(url.searchParams.get("fy") || "2026", 10)
      const limit = Math.max(1, Math.min(50, parseInt(url.searchParams.get("limit") || "12", 10)))
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
          sb_share_pct: typeof r.sb_share_pct === "number" ? r.sb_share_pct : Number(r.sb_share_pct),
          dollars_total: typeof r.dollars_total === "number" ? r.dollars_total : Number(r.dollars_total),
        }))

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      }
    }

    /* ========================= Vendor Summary =========================
     * GET /sb/vendor-summary?uei=XXXX&agency=Dept%20of%20Defense
     */
    if (last === "vendor-summary") {
      const uei = (url.searchParams.get("uei") || "").trim()
      const agencyFilter = (url.searchParams.get("agency") || "").trim()

      if (!uei) {
        return new Response(JSON.stringify({ ok: false, error: "Missing uei parameter." }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        })
      }

      const client = makeClient(env)
      try {
        await client.connect()

        const vendorSql = `
          SELECT recipient_name
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
          ORDER BY total_dollars_obligated_num DESC NULLS LAST
          LIMIT 1
        `
        const vendorRes = await client.query(vendorSql, [uei])
        const vendorName = vendorRes.rows[0]?.recipient_name || null

        const summarySql = `
          SELECT
            fiscal_year,
            COUNT(*)                                 AS awards,
            SUM(total_dollars_obligated_num)         AS obligated,
            SUM(potential_total_value_of_award_num)  AS ceiling
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL
              OR awarding_agency_name      = $2
              OR awarding_sub_agency_name  = $2
              OR awarding_office_name      = $2
            )
          GROUP BY fiscal_year
          ORDER BY fiscal_year DESC
        `
        const summaryRes = await client.query(summarySql, [uei, agencyFilter || null])

        const byYear = (summaryRes.rows || []).map((r) => ({
          fiscalYear: r.fiscal_year,
          awards: Number(r.awards || 0),
          obligated: typeof r.obligated === "number" ? r.obligated : Number(r.obligated || 0),
          ceiling: typeof r.ceiling === "number" ? r.ceiling : Number(r.ceiling || 0),
        }))

        const totals = byYear.reduce(
          (acc, y) => {
            acc.awards += y.awards
            acc.obligated += y.obligated
            acc.ceiling += y.ceiling
            return acc
          },
          { awards: 0, obligated: 0, ceiling: 0 },
        )

        return new Response(JSON.stringify({
          ok: true,
          vendor: { uei, name: vendorName },
          agencyFilter: agencyFilter || null,
          totals,
          byYear,
        }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Expiring Contracts (cached 5m) =========================
     * GET /sb/expiring-contracts?naics=541519&agency=VA&window_days=180&limit=50
     */
    if (last === "expiring-contracts") {
      const naicsParam   = (url.searchParams.get("naics") || "").trim()
      const agencyFilter = (url.searchParams.get("agency") || "").trim()
      const windowDays   = Math.max(1, Math.min(365, parseInt(url.searchParams.get("window_days") || "180", 10)))
      const limit        = Math.max(1, Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10)))

      const naicsList =
        naicsParam.length > 0
          ? naicsParam.split(",").map((s) => s.trim()).filter(Boolean)
          : []

      const cache = caches.default
      const cacheKey = new Request(url.toString(), request)
      const cached = await cache.match(cacheKey)
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=300, stale-while-revalidate=86400" },
        )
      }

      const client = makeClient(env)
      try {
        await client.connect()
        try { await client.query("SET statement_timeout = '55s'") } catch {}

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
              OR ${COL.AGENCY}        = $2
              OR ${COL.SUB_AGENCY}    = $2
              OR awarding_office_name = $2
            )
            AND (
              $3::text[] IS NULL
              OR ${COL.NAICS} = ANY($3)
            )
          ORDER BY ${COL.END_DATE} ASC
          LIMIT $4
        `
        const params = [windowDays, agencyFilter || null, naicsList.length ? naicsList : null, limit]
        const { rows } = await client.query(sql, params)
        ctx.waitUntil(client.end())

        const data = rows.map((r) => ({
          piid: r.piid,
          award_key: r.award_key,
          agency: r.agency,
          naics: r.naics,
          end_date: r.end_date,
          value: typeof r.value === "number" ? r.value : Number(r.value ?? null),
        }))

        const res = new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200,
          headers: {
            ...headers,
            "Content-Type": "application/json",
            "Cache-Control": "public, s-maxage=300, stale-while-revalidate=86400",
          },
        })
        ctx.waitUntil(cache.put(cacheKey, res.clone()))
        return res
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      }
    }

    /* ========================= Vendor Awards (FY fallback) =========================
     * GET /sb/vendor-awards?uei=XXXX&agency=...&years=5&limit=100
     */
    if (last === "vendor-awards") {
      const uei   = (url.searchParams.get("uei") || "").trim()
      const agency = (url.searchParams.get("agency") || "").trim()
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years") || "5", 10)))
      const limit = Math.max(1, Math.min(300, parseInt(url.searchParams.get("limit") || "100", 10)))

      if (!uei) {
        return new Response(JSON.stringify({ ok: false, error: "Missing uei" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        })
      }

      const client = makeClient(env)
      try {
        await client.connect()
        try { await client.query("SET statement_timeout = '15000'") } catch {}

        const schema = USA_TABLE.includes(".") ? USA_TABLE.split(".")[0] : "public"
        const table  = USA_TABLE.includes(".") ? USA_TABLE.split(".")[1] : USA_TABLE
        const colsRes = await client.query(
          `SELECT column_name FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2`,
          [schema, table]
        )
        const have = new Set((colsRes.rows || []).map(r => String(r.column_name).toLowerCase()))
        const has = (c) => have.has(String(c).toLowerCase())

        let fyExpr = null
        if (has("fiscal_year")) {
          fyExpr = "fiscal_year"
        } else if (has("action_date_fiscal_year")) {
          fyExpr = "action_date_fiscal_year"
        } else if (has("action_date")) {
          fyExpr = `CASE WHEN EXTRACT(MONTH FROM action_date)::int >= 10
                        THEN EXTRACT(YEAR FROM action_date)::int + 1
                        ELSE EXTRACT(YEAR FROM action_date)::int
                   END`
        } else if (has("pop_current_end_date")) {
          fyExpr = `CASE WHEN EXTRACT(MONTH FROM pop_current_end_date)::int >= 10
                        THEN EXTRACT(YEAR FROM pop_current_end_date)::int + 1
                        ELSE EXTRACT(YEAR FROM pop_current_end_date)::int
                   END`
        } else if (has("period_of_performance_current_end_date")) {
          fyExpr = `CASE WHEN EXTRACT(MONTH FROM period_of_performance_current_end_date)::int >= 10
                        THEN EXTRACT(YEAR FROM period_of_performance_current_end_date)::int + 1
                        ELSE EXTRACT(YEAR FROM period_of_performance_current_end_date)::int
                   END`
        } else {
          fyExpr = "EXTRACT(YEAR FROM CURRENT_DATE)::int"
        }

        const setAsideExpr = (() => {
          const c = ["type_of_set_aside","type_set_aside","set_aside"].filter(has)
          return c.length ? `COALESCE(${c.map(n=>`${n}::text`).join(",")})` : "NULL"
        })()

        const vehicleExpr = (() => {
          const c = ["idv_type","idv_type_of_award","contract_vehicle","contract_award_type","award_type"].filter(has)
          return c.length ? `COALESCE(${c.map(n=>`${n}::text`).join(",")})` : "NULL"
        })()

        const sql = `
          SELECT *
          FROM (
            SELECT
              award_id_piid                               AS piid,
              (${fyExpr})                                 AS fiscal_year,
              awarding_agency_name                        AS agency,
              awarding_sub_agency_name                    AS sub_agency,
              awarding_office_name                        AS office,
              naics_code                                  AS naics,
              ${setAsideExpr}                             AS set_aside,
              ${vehicleExpr}                              AS vehicle,
              total_dollars_obligated_num                 AS obligated,
              pop_current_end_date                        AS pop_end
            FROM ${USA_TABLE}
            WHERE recipient_uei = $1
              AND (
                $2::text IS NULL
                OR awarding_agency_name      = $2
                OR awarding_sub_agency_name  = $2
                OR awarding_office_name      = $2
              )
          ) q
          WHERE q.fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
          ORDER BY q.fiscal_year DESC, q.pop_end DESC NULLS LAST, q.piid DESC
          LIMIT $4
        `
        const params = [uei, agency || null, years, limit]
        const { rows } = await client.query(sql, params)
        ctx.waitUntil(client.end())

        const data = (rows || []).map(r => ({
          piid: r.piid,
          fiscal_year: Number(r.fiscal_year),
          agency: r.agency, sub_agency: r.sub_agency, office: r.office,
          naics: r.naics,
          set_aside: r.set_aside || null,
          vehicle: r.vehicle || null,
          obligated: typeof r.obligated === "number" ? r.obligated : Number(r.obligated || 0),
        }))

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      }
    }

    /* ========================= Contract Insights (AI + subs) =========================
     * POST /sb/contracts/insights  { piid: "..." }
     */
    if (request.method === "POST" && url.pathname.toLowerCase().endsWith("/contracts/insights")) {
      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok: false, error: "OPENAI_API_KEY is not configured for sb-analytics." }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      }

      const client = makeClient(env)
      try {
        const body = await request.json().catch(() => ({}))
        const piid = String(body.piid || "").trim().toUpperCase()
        if (!piid) {
          return new Response(JSON.stringify({ ok: false, error: "Missing piid" }), {
            status: 400, headers: { ...headers, "Content-Type": "application/json" },
          })
        }

        await client.connect()

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
          return new Response(JSON.stringify({ ok: false, error: "No award found for that PIID." }), {
            status: 404, headers: { ...headers, "Content-Type": "application/json" },
          })
        }

        const a = awardRes.rows[0]
        const toNumber = (x) => (typeof x === "number" ? x : x == null ? null : Number(x))

        const obligated = toNumber(a.total_dollars_obligated_num) ?? 0
        const currentValue =
          toNumber(a.current_total_value_of_award_num) ??
          toNumber(a.potential_total_value_of_award_num) ?? 0
        const ceiling = toNumber(a.potential_total_value_of_award_num) ?? currentValue

        const today = new Date()
        const parseDate = (d) => (d ? new Date(d) : null)
        const startDate = parseDate(a.pop_start_date)
        const currentEnd = parseDate(a.pop_current_end_date)
        const potentialEnd = parseDate(a.pop_potential_end_date)
        const endForLifecycle = potentialEnd || currentEnd

        let lifecycleStage = "unknown"
        let lifecycleLabel = "Lifecycle insight limited"
        let windowLabel = "Window unknown"
        let timeElapsedPct = null
        let burnPct = null

        if (startDate && endForLifecycle && endForLifecycle > startDate) {
          const totalMs = endForLifecycle.getTime() - startDate.getTime()
          const clampedNow = Math.min(Math.max(today.getTime(), startDate.getTime()), endForLifecycle.getTime())
          const elapsedMs = clampedNow - startDate.getTime()
          timeElapsedPct = Math.round((elapsedMs / totalMs) * 100)

          if (today < startDate) {
            lifecycleStage = "not_started"; lifecycleLabel = "Not started yet"; windowLabel = "Window not opened"
          } else if (today > endForLifecycle) {
            lifecycleStage = "complete"; lifecycleLabel = "Performance complete"; windowLabel = "Window passed"
          } else if (timeElapsedPct < 25) {
            lifecycleStage = "early"; lifecycleLabel = "Early stage"; windowLabel = "In performance window"
          } else if (timeElapsedPct < 75) {
            lifecycleStage = "mid"; lifecycleLabel = "Mid-stage"; windowLabel = "In performance window"
          } else {
            lifecycleStage = "late"; lifecycleLabel = "Late / near end"; windowLabel = "In performance window"
          }
        }

        if (ceiling && ceiling > 0) burnPct = Math.round((obligated / ceiling) * 100)

        const subsSql = `
          SELECT subawardee_name, subawardee_uei, subaward_amount
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1
        `
        const subsRes = await client.query(subsSql, [piid])
        const subsRaw = subsRes.rows || []

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
        const subsAgg = Array.from(subMap.values()).sort((a, b) => (b.amount || 0) - (a.amount || 0))
        const subCount = subsRaw.length
        const distinctRecipients = subsAgg.length
        const totalSubAmount = subsAgg.reduce((sum, s) => sum + (s.amount || 0), 0)
        const topSubs = subsAgg.slice(0, 5)

        let primeVsSubsPct = null
        let largestSubPct = null
        if (obligated > 0 && totalSubAmount > 0) {
          const subPct = Math.min(100, (totalSubAmount / obligated) * 100)
          primeVsSubsPct = { prime: Math.round(100 - subPct), subs: Math.round(subPct) }
        }
        if (totalSubAmount > 0 && topSubs.length > 0) {
          largestSubPct = Math.round((topSubs[0].amount / totalSubAmount) * 100)
        }

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

        const website = await fetchVendorWebsiteByUEI(primary.primeUei, env)
        if (website) primary.website = website

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

        const burnText =
          burnPct == null
            ? "Burn vs. ceiling could not be determined."
            : `Approximately ${burnPct}% of the contract ceiling is obligated (≈$${Math.round(obligated).toLocaleString("en-US")} of ≈$${Math.round(ceiling).toLocaleString("en-US")}).`

        const subsText =
          subCount === 0
            ? "No subcontract awards are publicly reported for this contract; teaming may require direct outreach to the prime."
            : `There are ${subCount} reported subawards to ${distinctRecipients} unique recipients, totaling about $${Math.round(totalSubAmount).toLocaleString("en-US")}.`

        const topSubText =
          topSubs.length === 0
            ? ""
            : `Top reported subs include ${topSubs.slice(0, 3).map((s) => `${s.name} (UEI ${s.uei || "unknown"})`).join(", ")}.`

        const prompt = `
You are helping a small federal contractor quickly understand a single contract and how to position for a recompete or subcontracting role.
Treat the lifecycle, burn %, and subcontracting figures below as correct.

Contract snapshot:
- PIID: ${primary.piid}
- Awarding agency: ${primary.agency || "—"}
- Component / office: ${primary.subAgency || "—"} / ${primary.office || "—"}
- Prime: ${primary.primeName || "—"} (UEI: ${primary.primeUei || "unknown"})${website ? ` — Website: ${website}` : ""}
- NAICS: ${primary.naicsCode || "—"} – ${primary.naicsDescription || "—"}
- Period of performance: ${primary.popStartDate || "—"} to ${primary.popCurrentEndDate || "—"} (potential: ${primary.popPotentialEndDate || "—"})
- Lifecycle stage: ${lifecycle.stage} (${lifecycle.label}, time elapsed ≈${timeElapsedPct == null ? "unknown" : timeElapsedPct + "%"})
- ${burnText}
- Subcontracting footprint: ${subsText} ${topSubText}

Write 4–6 bullets that:
- state the lifecycle stage & what that means for capture timing,
- explain the Fit Score drivers for UEI {MY_UEI}: NAICS match, awards at this org, socio-economic match to prior set-asides, and incumbent presence,
- give 3 specific next actions to raise the score (teaming, intel calls, vehicles, set-aside alignment),
- keep it under 170 words, no fluff.
When data is missing, say “unknown” once and move on.
        `.trim()

        const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.OPENAI_API_KEY}` },
          body: JSON.stringify({
            model: "gpt-4.1-mini",
            messages: [
              { role: "system", content: "You are a federal contracts analyst helping small businesses interpret USAspending and subcontract data." },
              { role: "user", content: prompt },
            ],
            max_tokens: 400,
          }),
        })

        const aiText = await aiRes.text()
        let aiJson = {}
        try { aiJson = aiText ? JSON.parse(aiText) : {} } catch {
          throw new Error("AI response was not valid JSON: " + aiText.slice(0, 160))
        }
        const summary = aiJson.choices?.[0]?.message?.content?.trim() || "AI produced no summary."

        return new Response(JSON.stringify({ ok: true, summary, primary, lifecycle, subs, disclaimer:
          "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete."
        }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e?.message || "AI insight failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= MY ENTITY =========================
     * GET /sb/my-entity?uei=XXXX
     */
    if (last === "my-entity") {
      const uei = (url.searchParams.get("uei") || "").trim().toUpperCase()
      if (!uei) {
        return new Response(JSON.stringify({ ok:false, error:"Missing uei parameter." }), {
          status:400, headers:{ ...headers, "Content-Type":"application/json" }
        })
      }

      const client = makeClient(env)
      try {
        await client.connect()

        // Best-effort name (from awards)
        const nameRes = await client.query(
          `SELECT recipient_name
           FROM ${USA_TABLE}
           WHERE recipient_uei = $1
           ORDER BY total_dollars_obligated_num DESC NULLS LAST
           LIMIT 1`, [uei]
        )
        const name = nameRes.rows[0]?.recipient_name || null

        // NAICS set (from awards)
        const naicsRes = await client.query(
          `SELECT DISTINCT naics_code
           FROM ${USA_TABLE}
           WHERE recipient_uei = $1 AND naics_code IS NOT NULL
           LIMIT 200`, [uei]
        )
        const naics = (naicsRes.rows || []).map(r => r.naics_code).filter(Boolean)

        // Optional: socio-economic categories from your SAM proxy
        let smallBizCategories = []
        try {
          if (env.SAM_PROXY_URL) {
            const samUrl = `${env.SAM_PROXY_URL.replace(/\/+$/, "")}/entity?uei=${encodeURIComponent(uei)}`
            const samRes = await fetch(samUrl)
            if (samRes.ok) {
              const samJson = await samRes.json().catch(() => null)
              const cats =
                (Array.isArray(samJson?.categories) && samJson.categories) ||
                (samJson?.entity?.socioEconomicCategories || [])
              smallBizCategories = Array.from(new Set((cats || []).filter(Boolean)))
            }
          }
        } catch {}

        return new Response(JSON.stringify({
          ok: true,
          entity: { uei, name, naics, smallBizCategories },
        }), {
          status: 200,
          headers: { ...headers, "Content-Type": "application/json", "Cache-Control": "public, s-maxage=86400" },
        })
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message || "query failed" }), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ===================== Bid/No-Bid (UEI + PIID) =====================
     * GET /sb/bid-nobid?piid=HC102825F0042&uei=MKA4F1KQSSB5&years=5
     */
    if (last === "bid-nobid") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase()
      const uei  = (url.searchParams.get("uei")  || "").trim().toUpperCase()
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years") || "5", 10)))
      if (!piid || !uei) {
        return new Response(JSON.stringify({ ok:false, error:"Missing piid or uei" }),
          { status:400, headers:{ ...headers, "Content-Type":"application/json" }})
      }

      const client = makeClient(env)
      try {
        await client.connect()

        // discover FY column
        const schema = USA_TABLE.includes(".") ? USA_TABLE.split(".")[0] : "public"
        const table  = USA_TABLE.includes(".") ? USA_TABLE.split(".")[1] : USA_TABLE
        const cols = await client.query(
          `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
          [schema, table]
        )
        const have = new Set((cols.rows||[]).map(r=>String(r.column_name).toLowerCase()))
        const has  = (c) => have.has(String(c).toLowerCase())
        const fyExpr = has("fiscal_year") ? "fiscal_year"
          : has("action_date_fiscal_year") ? "action_date_fiscal_year"
          : has("action_date") ? `CASE WHEN EXTRACT(MONTH FROM action_date)::int>=10
                                   THEN EXTRACT(YEAR FROM action_date)::int+1
                                   ELSE EXTRACT(YEAR FROM action_date)::int END`
          : `EXTRACT(YEAR FROM CURRENT_DATE)::int`

        // award snapshot
        const award = await client.query(`
          SELECT award_id_piid, naics_code, naics_description,
                 awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                 recipient_uei, recipient_name,
                 total_dollars_obligated_num AS obligated,
                 potential_total_value_of_award_num AS ceiling,
                 pop_current_end_date
          FROM ${USA_TABLE}
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC NULLS LAST
          LIMIT 1`, [piid])
        if (!award.rows.length) {
          return new Response(JSON.stringify({ ok:false, error:"PIID not found" }),
            { status:404, headers:{ ...headers, "Content-Type":"application/json" }})
        }
        const A = award.rows[0]
        const orgName = A.awarding_sub_agency_name || A.awarding_office_name || A.awarding_agency_name
        const naics = A.naics_code

        // my SAM profile (via our own route)
        const myEntRes = await fetch(`${url.origin}/sb/my-entity?uei=${encodeURIComponent(uei)}`)
        const myEntJson = await myEntRes.json().catch(()=>({}))
        const myNAICS = (myEntJson?.entity?.naics || []).filter(Boolean)
        const mySocio = (myEntJson?.entity?.socio || myEntJson?.entity?.smallBizCategories || []).filter(Boolean)

        // my awards at this org
        const myAwards = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[uei, orgName, years])

        // incumbent strength at this org
        const inc = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[A.recipient_uei, orgName, years])

        // price context: percentiles
        const dist = await client.query(`
          WITH base AS (
            SELECT total_dollars_obligated_num AS obligated
            FROM ${USA_TABLE}
            WHERE naics_code = $1
              AND (
                $2::text IS NULL OR awarding_agency_name=$2
                OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
              )
              AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
              AND total_dollars_obligated_num IS NOT NULL
          )
          SELECT
            percentile_cont(0.25) WITHIN GROUP (ORDER BY obligated)::float8 AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY obligated)::float8 AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY obligated)::float8 AS p75
          FROM base
        `,[naics, orgName, years])
        const P = dist.rows[0] || { p25:null, p50:null, p75:null }

        // set-aside tendency
        const setAside = await client.query(`
          SELECT COUNT(*) FILTER (WHERE COALESCE(type_of_set_aside, idv_type_of_award) IS NOT NULL)::int AS known,
                 COUNT(*)::int AS total,
                 MAX(COALESCE(type_of_set_aside, idv_type_of_award)) AS example_set_aside
          FROM ${USA_TABLE}
          WHERE naics_code=$1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[naics, orgName, years])

        // scoring helpers
        const toNum = (x)=> typeof x === "number"? x : x==null? 0 : Number(x)||0
        const meCnt = toNum(myAwards.rows[0]?.cnt)
        const meObl = toNum(myAwards.rows[0]?.obligated)
        const incCnt = toNum(inc.rows[0]?.cnt)
        const incObl = toNum(inc.rows[0]?.obligated)
        const today = new Date()
        const dEnd = A.pop_current_end_date ? new Date(A.pop_current_end_date) : null
        const daysToEnd = dEnd ? Math.round((dEnd.getTime() - today.getTime())/86400000) : null
        const burn = A.ceiling && A.ceiling>0 ? Math.round((toNum(A.obligated)/toNum(A.ceiling))*100) : null

        // Technical fit 1–5
        const tech =
          myNAICS.includes(naics) ? 5 :
          myNAICS.some((c)=> c && c.slice(0,3)===String(naics).slice(0,3)) ? 4 : 2

        // Past performance 1–5
        const pp = meCnt>=3 || meObl>=2_000_000 ? 5 : meCnt>=1 ? 3 : 1

        // Staffing proxy 1–5 (via local award size distribution)
        let staffing = 3
        if (P.p50) {
          const contractSize = toNum(A.obligated||A.ceiling||0)
          staffing = contractSize <= P.p50 ? 5 : (contractSize <= (P.p75||P.p50) ? 4 : 2)
        }

        // Schedule risk 1–5
        let sched = 3
        if (daysToEnd!=null && burn!=null) {
          if (daysToEnd>180 && burn<70) sched=5
          else if (daysToEnd<60 || burn>90) sched=2
          else sched=3
        }

        // Compliance 1–5 (set-aside tendency vs my socio)
        const knownSA = toNum(setAside.rows[0]?.known)
        const example = (setAside.rows[0]?.example_set_aside||"").toUpperCase()
        const haveMatch = (tag)=> (mySocio||[]).some((s)=> String(s).toUpperCase().includes(tag))
        let comp = 3
        if (knownSA>0) {
          if (example.includes("SDVOSB") && haveMatch("SDVOSB")) comp=5
          else if (example.includes("WOSB") && haveMatch("WOSB")) comp=5
          else if (example.includes("HUB") && haveMatch("HUB")) comp=5
          else if (example.includes("8(A)") && (haveMatch("8(A)")||haveMatch("8A"))) comp=5
          else comp=2
        }

        // Price competitiveness 1–5
        let price = 3
        if (P.p25 && P.p50 && P.p75) {
          const val = toNum(A.obligated||A.ceiling||0)
          if (val<=P.p25) price=4
          else if (val<=P.p50) price=5
          else if (val<=P.p75) price=3
          else price=2
        }

        // Customer intimacy 1–5
        const intimacy = meCnt>=3 ? 5 : meCnt===2 ? 4 : meCnt===1 ? 3 : 1

        // Competitive Intel 1–5
        const compIntel = incCnt===0 ? 5 : incCnt<=2 ? 4 : incCnt<=5 ? 3 : 2

        // Weighted %
        const W = { tech:24, pp:20, staff:12, sched:8, compliance:8, price:8, intimacy:10, intel:10 }
        const pct = ( (W.tech*(tech/5)) + (W.pp*(pp/5)) + (W.staff*(staffing/5)) + (W.sched*(sched/5)) +
                      (W.compliance*(comp/5)) + (W.price*(price/5)) + (W.intimacy*(intimacy/5)) + (W.intel*(compIntel/5)) )
        const weighted = Math.round(pct*10)/10
        const decision = weighted>=80 ? "bid" : (weighted>=65 ? "conditional" : "no_bid")

        // Heat map + “how to raise score”
        const heat = [
          { level: incCnt>5 ? "High":"Med-High", reason: `Incumbent has ${incCnt} awards / $${Math.round(incObl).toLocaleString()} at ${orgName}` },
          { level: myNAICS.includes(naics) ? "Low":"Med-High", reason: myNAICS.includes(naics) ? "Exact NAICS match" : "No exact NAICS in SAM" },
          { level: meCnt>0 ? "Medium":"High", reason: meCnt>0 ? `You have ${meCnt} awards at this org` : "No awards at this org" },
        ]
        const improve = []
        if (!myNAICS.includes(naics)) improve.push(`Add NAICS ${naics} to SAM (or team with a prime holding it).`)
        if (meCnt===0) improve.push(`Pursue micro-tasking/teaming at ${orgName} to build a reference quickly.`)
        if (knownSA>0 && comp<5) improve.push(`Align socio-economic category with prior set-aside pattern (${example || "varied"}).`)
        if ((P.p50||0)>0 && (toNum(A.obligated||A.ceiling||0)>(P.p75||P.p50))) improve.push("Propose lean staffing/price to land below local median.")

        return new Response(JSON.stringify({
          ok:true,
          inputs:{ piid, uei, org:orgName, naics },
          criteria:[
            { name:"Technical Fit", weight:24, score:tech, reason: myNAICS.includes(naics) ? "Exact NAICS match" : (myNAICS.some((c)=>c && c.slice(0,3)===String(naics).slice(0,3)) ? "Related NAICS family" : "No NAICS match") },
            { name:"Relevant Experience / Past Performance", weight:20, score:pp, reason:`Your awards at this org: ${meCnt}; $${Math.round(meObl).toLocaleString()}` },
            { name:"Staffing & Key Personnel", weight:12, score:staffing, reason:"Proxy via local award size distribution" },
            { name:"Schedule / ATO Timeline Risk", weight:8, score:sched, reason:`Days to end: ${daysToEnd ?? "unknown"}; burn: ${burn ?? "unknown"}%` },
            { name:"Compliance", weight:8, score:comp, reason: (setAside.rows[0]?.example_set_aside || "Set-aside").toString() },
            { name:"Price Competitiveness", weight:8, score:price, reason:"Position vs NAICS@org percentiles" },
            { name:"Customer Intimacy", weight:10, score:intimacy, reason:`Your awards at this org: ${meCnt}` },
            { name:"Competitive Intelligence", weight:10, score:compIntel, reason:`Incumbent awards: ${incCnt}` },
          ],
          weighted_percent: weighted,
          decision,
          heatmap: heat,
          improve_now: improve
        }), { status:200, headers:{ ...headers, "Content-Type":"application/json" }})
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message||"bid-nobid failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* =============================================================
     * One-click Bid/No-Bid memo
     * GET /sb/bid-nobid-memo?piid=...&uei=...&years=5
     * =========================================================== */
    if (last === "bid-nobid-memo") {
      const piid  = (url.searchParams.get("piid") || "").trim().toUpperCase()
      const uei   = (url.searchParams.get("uei")  || "").trim().toUpperCase()
      const years = (url.searchParams.get("years")|| "5").trim()
      if (!piid || !uei) {
        return new Response(JSON.stringify({ ok:false, error:"Missing piid or uei" }), {
          status: 400, headers: { ...headers, "Content-Type":"application/json" }
        })
      }

      // call the internal scorer
      const bnbURL = new URL(request.url)
      bnbURL.pathname = "/sb/bid-nobid"
      bnbURL.search = `?piid=${encodeURIComponent(piid)}&uei=${encodeURIComponent(uei)}&years=${encodeURIComponent(years)}`
      const bnbRes = await fetch(bnbURL.toString(), { headers: { "Accept":"application/json" } })
      const bnbTxt = await bnbRes.text()
      let bnb = {}
      try { bnb = bnbTxt ? JSON.parse(bnbTxt) : {} } catch {}
      if (!bnbRes.ok || bnb?.ok === false) {
        return new Response(JSON.stringify({ ok:false, error: bnb?.error || "bid-nobid failed" }), {
          status: 500, headers: { ...headers, "Content-Type":"application/json" }
        })
      }

      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok:false, error:"OPENAI_API_KEY not set" }), {
          status: 500, headers: { ...headers, "Content-Type":"application/json" }
        })
      }

      const matrixLines = (bnb.criteria || []).map((c) =>
        `${c.name} | weight ${c.weight}% | score ${c.score}/5 | ${c.reason || ""}`.trim()
      ).join("\n")

      const prompt = `
You are the Bid/No-Bid Decision GPT for federal capture. Use the scored matrix below to produce:
1) A decision (Bid / Conditional / No-Bid) with percent.
2) A 5–7 line executive memo referencing the actual numbers (NAICS match, org history $, socio-econ vs set-aside, incumbent strength, schedule/burn, price percentile).
3) A 5-line risk heat map (emojis only: 🟢🟡🟠🔴).
4) A 10–14 day capture plan (bullets).
5) 6–10 precise CO questions.

Inputs:
- PIID: ${bnb?.inputs?.piid || piid}
- Org: ${bnb?.inputs?.org || "—"}
- NAICS: ${bnb?.inputs?.naics || "—"}
- Decision: ${bnb?.decision || "—"} (${bnb?.weighted_percent ?? "—"}%)

Matrix:
${matrixLines}

Keep it concise and specific—no fluff.
      `.trim()

      const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type":"application/json", "Authorization":`Bearer ${env.OPENAI_API_KEY}` },
        body: JSON.stringify({
          model: "gpt-4.1-mini",
          messages: [
            { role: "system", content: "You are a federal capture strategist." },
            { role: "user", content: prompt }
          ],
          temperature: 0.3,
          max_tokens: 900
        })
      })

      const aiTxt = await aiRes.text()
      let memo = ""
      try { memo = JSON.parse(aiTxt).choices?.[0]?.message?.content?.trim() || "" }
      catch { memo = aiTxt.slice(0, 3000) }

      return new Response(JSON.stringify({ ok:true, ...bnb, memo }), {
        status: 200, headers: { ...headers, "Content-Type":"application/json" }
      })
    }

    // Fallback
    return new Response("Not found", { status: 404, headers })
  },
}
