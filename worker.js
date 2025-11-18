// worker.js — sb-analytics (v2)
// Uses public.usaspending_awards_v2

import { Client } from "pg"

/* ========================= CORS ========================= */
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "")
    .split(",").map(s => s.trim()).filter(Boolean)
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

function withCors(res, headers) {
  const h = new Headers(res.headers)
  for (const [k, v] of Object.entries(headers || {})) h.set(k, v)
  return new Response(res.body, { status: res.status, headers: h })
}

/* ========================= DB client ========================= */
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  })
}

/* ========================= Helpers ========================= */
function fyNowUTC() {
  const d = new Date()
  const m = d.getUTCMonth() + 1
  const y = d.getUTCFullYear()
  return m >= 10 ? y + 1 : y
}

/* ========================= Column mapping ========================= */
const USA_TABLE = "public.usaspending_awards_fast" // <-- fast materialized view

const COL = {
  END_DATE: "pop_current_end_date",
  NAICS: "naics_code",
  AGENCY: "awarding_agency_name",
  SUB_AGENCY: "awarding_sub_agency_name",
  OFFICE: "awarding_office_name",
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
    const j = await r.json().catch(() => null)
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

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers })
    }

    const segments = url.pathname.split("/").filter(Boolean)
    const last = segments[segments.length - 1] || ""

    /* ========================= Health ========================= */
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      })
    }

    /* ========================= Agencies (cached 24h) ========================= */
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
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= SB Agency Share ========================= */
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
        const data = rows.map(r => ({
          agency: r.agency,
          sb_share_pct: Number(r.sb_share_pct),
          dollars_total: Number(r.dollars_total),
        }))
        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Vendor Summary ========================= */
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
          obligated: Number(r.obligated || 0),
          ceiling: Number(r.ceiling || 0),
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
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Expiring Contracts (cached 5m) ========================= */
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
              OR ${COL.OFFICE}        = $2
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
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Vendor Awards (FY window) =========================
     * GET /sb/vendor-awards?uei=XXXX&agency=...&years=5&limit=100
     * now returns: title, extent_competed, number_of_offers_received, type_of_set_aside, idv_type_of_award
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

        const sql = `
          SELECT
            award_id_piid           AS piid,
            fiscal_year,
            awarding_agency_name    AS agency,
            awarding_sub_agency_name AS sub_agency,
            awarding_office_name    AS office,
            naics_code              AS naics,
            type_of_set_aside       AS set_aside,
            idv_type_of_award       AS vehicle,
            title,
            extent_competed,
            number_of_offers_received,
            total_dollars_obligated_num AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL
              OR awarding_agency_name      = $2
              OR awarding_sub_agency_name  = $2
              OR awarding_office_name      = $2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
          ORDER BY fiscal_year DESC, pop_current_end_date DESC NULLS LAST, piid DESC
          LIMIT $4
        `
        const { rows } = await client.query(sql, [uei, agency || null, years, limit])

        const data = (rows || []).map(r => ({
          piid: r.piid,
          fiscal_year: Number(r.fiscal_year),
          agency: r.agency,
          sub_agency: r.sub_agency,
          office: r.office,
          naics: r.naics,
          set_aside: r.set_aside || null,
          vehicle: r.vehicle || null,
          title: r.title || null,
          extent_competed: r.extent_competed || null,
          number_of_offers_received: r.number_of_offers_received || null,
          obligated: Number(r.obligated || 0),
        }))

        return new Response(JSON.stringify({ ok: true, rows: data }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e?.message || "query failed" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Teaming Suggestions (NEW) =========================
     * GET/POST /sb/teaming-suggestions?piid=...&naics=...&org=...&years=3&limit=5&exclude_ueis=UEI1,UEI2
     */
    if (last === "teaming-suggestions") {
      // accept both GET query and POST JSON body
      let p = {}
      if (request.method === "GET") {
        p = Object.fromEntries(url.searchParams.entries())
      } else {
        try { p = await request.json() } catch { p = {} }
      }

      const piid  = String(p.piid || "").trim().toUpperCase()
      const naics = String(p.naics || "").trim()
      const org   = String(p.org || "").trim()
      const years = Math.max(1, Number(p.years ?? 3) || 3)
      const limit = Math.max(1, Number(p.limit ?? 5) || 5)
      const excludeUEIs = String(p.exclude_ueis || "")
        .split(",").map(s => s.trim().toUpperCase()).filter(Boolean)

      if (!naics || !org) {
        return new Response(JSON.stringify({ ok:false, error:"Missing required params: naics, org" }), {
          status: 400, headers: { ...headers, "Content-Type":"application/json" }
        })
      }

      const fyMin = fyNowUTC() - years + 1
      const orgLike = `%${org}%`

      const client = makeClient(env)
      try {
        await client.connect()

        // Try to exclude the incumbent prime (recipient_uei) for the given PIID
        if (piid) {
          try {
            const inc = await client.query(
              `SELECT DISTINCT recipient_uei FROM ${USA_TABLE} WHERE award_id_piid = $1 LIMIT 1`, [piid]
            )
            const incUei = (inc?.rows?.[0]?.recipient_uei || "").toUpperCase()
            if (incUei) excludeUEIs.push(incUei)
          } catch {}
        }
        const excludeParam = (excludeUEIs.length ? excludeUEIs : [""]).map(s => s.toUpperCase())

        const sql = `
WITH pool AS (
  SELECT LOWER(TRIM(recipient_uei)) AS uei,
         COALESCE(NULLIF(recipient_name,''),'—') AS name,
         SUM(total_dollars_obligated_num) AS obligated
  FROM ${USA_TABLE}
  WHERE fiscal_year >= $1
    AND (naics_code = $2 OR LEFT(naics_code,3) = LEFT($2,3))
    AND (
      awarding_sub_agency_name ILIKE $3 OR
      awarding_office_name     ILIKE $3 OR
      awarding_agency_name     ILIKE $3
    )
    AND COALESCE(UPPER(recipient_uei),'') <> ALL($4::text[])
  GROUP BY recipient_uei, recipient_name
),
enriched AS (
  SELECT
    p.uei, p.name, p.obligated,
    (
      SELECT s.type_of_set_aside
      FROM (
        SELECT COALESCE(NULLIF(type_of_set_aside,''),'NONE') AS type_of_set_aside,
               COUNT(*) AS c
        FROM ${USA_TABLE} a
        WHERE a.recipient_uei = p.uei
          AND a.fiscal_year   >= $1
          AND (
            a.awarding_sub_agency_name ILIKE $3 OR
            a.awarding_office_name     ILIKE $3 OR
            a.awarding_agency_name     ILIKE $3
          )
        GROUP BY 1
        ORDER BY c DESC NULLS LAST
        LIMIT 1
      ) s
    ) AS set_aside,
    COALESCE((
      SELECT json_agg(json_build_object(
        'piid', a.award_id_piid,
        'fiscal_year', a.fiscal_year,
        'naics', a.naics_code,
        'title', a.title,
        'obligated', a.total_dollars_obligated_num
      ) ORDER BY a.fiscal_year DESC, a.total_dollars_obligated_num DESC)
      FROM (
        SELECT award_id_piid, fiscal_year, naics_code, title, total_dollars_obligated_num
        FROM ${USA_TABLE} a
        WHERE a.recipient_uei = p.uei
          AND a.fiscal_year   >= $1
          AND (
            a.awarding_sub_agency_name ILIKE $3 OR
            a.awarding_office_name     ILIKE $3 OR
            a.awarding_agency_name     ILIKE $3
          )
        ORDER BY fiscal_year DESC, total_dollars_obligated_num DESC
        LIMIT 6
      ) a
    ), '[]'::json) AS recent_awards
  FROM pool p
)
SELECT UPPER(uei) AS uei,
       name,
       COALESCE(set_aside,'—') AS set_aside,
       obligated,
       recent_awards,
       NULL::text AS website,
       NULL::json AS contact
FROM enriched
ORDER BY obligated ASC NULLS FIRST
LIMIT $5;
        `
        const params = [fyMin, naics, orgLike, excludeParam, limit]
        const { rows } = await client.query(sql, params)

        return new Response(JSON.stringify({ ok:true, rows }), {
          status: 200, headers: { ...headers, "Content-Type":"application/json" }
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message || "teaming-suggestions failed" }), {
          status: 500, headers: { ...headers, "Content-Type":"application/json" }
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Contract Insights (AI + subs) =========================
     * POST /sb/contracts/insights  { piid: "..." }
     * Primary now includes: type_of_set_aside, number_of_offers_received, title, extent_competed
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
            total_dollars_obligated_num,
            type_of_set_aside,
            number_of_offers_received,
            extent_competed,
            title
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
        const currentValue = toNumber(a.current_total_value_of_award_num) ?? toNumber(a.potential_total_value_of_award_num) ?? 0
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
          // NEW for donuts:
          type_of_set_aside: a.type_of_set_aside || null,
          number_of_offers_received: a.number_of_offers_received || null,
          extent_competed: a.extent_competed || null,
          title: a.title || null,
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

    /* ========================= MY ENTITY ========================= */
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

        const nameRes = await client.query(
          `SELECT recipient_name
           FROM ${USA_TABLE}
           WHERE recipient_uei = $1
           ORDER BY total_dollars_obligated_num DESC NULLS LAST
           LIMIT 1`, [uei]
        )
        const name = nameRes.rows[0]?.recipient_name || null

        const naicsRes = await client.query(
          `SELECT DISTINCT naics_code
           FROM ${USA_TABLE}
           WHERE recipient_uei = $1 AND naics_code IS NOT NULL
           LIMIT 200`, [uei]
        )
        const naics = (naicsRes.rows || []).map(r => r.naics_code).filter(Boolean)

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
        return new Response(JSON.stringify({ ok:false, error:e?.message || "query failed" }), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= Bid/No-Bid (UEI + PIID) ========================= */
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

        // Award snapshot
        const award = await client.query(`
          SELECT award_id_piid, naics_code, naics_description,
                 awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                 recipient_uei, recipient_name,
                 total_dollars_obligated_num AS obligated,
                 potential_total_value_of_award_num AS ceiling,
                 pop_current_end_date,
                 type_of_set_aside
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

        // My entity NAICS + socio
        const myEntRes = await fetch(`${url.origin}/sb/my-entity?uei=${encodeURIComponent(uei)}`, { headers: { Accept: "application/json" }})
        const myEntJson = await myEntRes.json().catch(()=>({}))
        const myNAICS = (myEntJson?.entity?.naics || []).filter(Boolean)
        const mySocio = (myEntJson?.entity?.smallBizCategories || []).filter(Boolean)

        // My awards at this org
        const myAwards = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[uei, orgName, years])

        // Incumbent presence at this org
        const inc = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[A.recipient_uei, orgName, years])

        // Local market distribution for NAICS@org
        const dist = await client.query(`
          WITH base AS (
            SELECT total_dollars_obligated_num AS obligated
            FROM ${USA_TABLE}
            WHERE naics_code = $1
              AND (
                $2::text IS NULL OR awarding_agency_name=$2
                OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
              )
              AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
              AND total_dollars_obligated_num IS NOT NULL
          )
          SELECT
            percentile_cont(0.25) WITHIN GROUP (ORDER BY obligated)::float8 AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY obligated)::float8 AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY obligated)::float8 AS p75
          FROM base
        `,[naics, orgName, years])
        const P = dist.rows[0] || { p25:null, p50:null, p75:null }

        // Historic set-aside tendency for this NAICS@org
        const setAside = await client.query(`
          SELECT COUNT(*) FILTER (WHERE type_of_set_aside IS NOT NULL AND type_of_set_aside <> '')::int AS known,
                 COUNT(*)::int AS total,
                 MAX(type_of_set_aside) AS example_set_aside
          FROM ${USA_TABLE}
          WHERE naics_code=$1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[naics, orgName, years])

        // Scoring
        const toNum = (x)=> typeof x === "number"? x : x==null? 0 : Number(x)||0
        const meCnt = toNum(myAwards.rows[0]?.cnt)
        const meObl = toNum(myAwards.rows[0]?.obligated)
        const incCnt = toNum(inc.rows[0]?.cnt)
        const incObl = toNum(inc.rows[0]?.obligated)
        const today = new Date()
        const dEnd = A.pop_current_end_date ? new Date(A.pop_current_end_date) : null
        const daysToEnd = dEnd ? Math.round((dEnd.getTime() - today.getTime())/86400000) : null
        const burn = A.ceiling && A.ceiling>0 ? Math.round((toNum(A.obligated)/toNum(A.ceiling))*100) : null

        const tech =
          myNAICS.includes(naics) ? 5 :
          myNAICS.some(c=> c?.slice(0,3)===String(naics).slice(0,3)) ? 4 : 2

        const pp = meCnt>=3 || meObl>=2_000_000 ? 5 : meCnt>=1 ? 3 : 1

        let staffing = 3
        if (P.p50) {
          const contractSize = toNum(A.obligated||A.ceiling||0)
          staffing = contractSize <= P.p50 ? 5 : (contractSize <= (P.p75||P.p50) ? 4 : 2)
        }

        let sched = 3
        if (daysToEnd!=null && burn!=null) {
          if (daysToEnd>180 && burn<70) sched=5
          else if (daysToEnd<60 || burn>90) sched=2
          else sched=3
        }

        const knownSA = toNum(setAside.rows[0]?.known)
        const example = (setAside.rows[0]?.example_set_aside||"").toUpperCase()
        const haveMatch = (tag)=> mySocio.some(s=> String(s).toUpperCase().includes(tag))
        let comp = 3
        if (knownSA>0) {
          if (example.includes("SDVOSB") && haveMatch("SDVOSB")) comp=5
          else if (example.includes("WOSB") && haveMatch("WOSB")) comp=5
          else if (example.includes("HUB") && haveMatch("HUB")) comp=5
          else if (example.includes("8(A)") && (haveMatch("8(A)")||haveMatch("8A"))) comp=5
          else comp=2
        }

        let price = 3
        if (P.p25 && P.p50 && P.p75) {
          const val = toNum(A.obligated||A.ceiling||0)
          if (val<=P.p25) price=4
          else if (val<=P.p50) price=5
          else if (val<=P.p75) price=3
          else price=2
        }

        const intimacy = meCnt>=3 ? 5 : meCnt===2 ? 4 : meCnt===1 ? 3 : 1
        const compIntel = incCnt===0 ? 5 : incCnt<=2 ? 4 : incCnt<=5 ? 3 : 2

        const W = { tech:24, pp:20, staff:12, sched:8, compliance:8, price:8, intimacy:10, intel:10 }
        const weighted = Math.round((
          W.tech*(tech/5) + W.pp*(pp/5) + W.staff*(staffing/5) + W.sched*(sched/5) +
          W.compliance*(comp/5) + W.price*(price/5) + W.intimacy*(intimacy/5) + W.intel*(compIntel/5)
        ) * 10) / 10
        const decision = weighted>=80 ? "bid" : (weighted>=65 ? "conditional" : "no_bid")

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
            { name:"Technical Fit", weight:24, score:tech, reason: myNAICS.includes(naics) ? "Exact NAICS match" : myNAICS.some(c=>c?.slice(0,3)===String(naics).slice(0,3)) ? "Related NAICS family" : "No NAICS match" },
            { name:"Relevant Experience / Past Performance", weight:20, score:pp, reason:`Your awards at this org: ${meCnt}; $${Math.round(meObl).toLocaleString()}` },
            { name:"Staffing & Key Personnel", weight:12, score:staffing, reason:"Proxy via local award size distribution" },
            { name:"Schedule / ATO Timeline Risk", weight:8, score:sched, reason:`Days to end: ${daysToEnd ?? "unknown"}; burn: ${burn ?? "unknown"}%` },
            { name:"Compliance", weight:8, score:comp, reason: knownSA? `Historic set-aside pattern: ${example||"varied"}`:"Set-aside unknown" },
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
        return new Response(JSON.stringify({ ok:false, error:e?.message||"bid-nobid failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      } finally {
        try { await client.end() } catch {}
      }
    }

    /* ========================= One-click Bid/No-Bid memo =========================
     * GET /sb/bid-nobid-memo?piid=...&uei=...&years=5
     */
    if (last === "bid-nobid-memo") {
      const piid  = (url.searchParams.get("piid") || "").trim().toUpperCase()
      const uei   = (url.searchParams.get("uei")  || "").trim().toUpperCase()
      const years = (url.searchParams.get("years")|| "5").trim()
      if (!piid || !uei) {
        return new Response(JSON.stringify({ ok:false, error:"Missing piid or uei" }), {
          status: 400, headers: { ...headers, "Content-Type":"application/json" }
        })
      }

      // Call our internal scorer on the SAME origin
      const scorerUrl = new URL(url.toString())
      scorerUrl.pathname = "/sb/bid-nobid"
      scorerUrl.search = `?piid=${encodeURIComponent(piid)}&uei=${encodeURIComponent(uei)}&years=${encodeURIComponent(years)}`
      const bnbRes = await fetch(scorerUrl.toString(), { headers: { "Accept":"application/json" } })
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
        headers: {
          "Content-Type":"application/json",
          "Authorization":`Bearer ${env.OPENAI_API_KEY}`
        },
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
