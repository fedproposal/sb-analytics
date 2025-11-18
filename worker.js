// worker.js — sb-analytics (updated)
import { Client } from "pg"

/* =============== CORS ================== */
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

/* =============== DB ================== */
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
  })
}

/* =============== Columns / Tables ================== */
// IMPORTANT: we now point at v2 with your new columns.
const USA_TABLE = "public.usaspending_awards_v2"
const COL = {
  END_DATE: "pop_current_end_date",
  NAICS: "naics_code",
  AGENCY: "awarding_agency_name",
  SUB_AGENCY: "awarding_sub_agency_name",
  PIID: "award_id_piid",
  AWARD_ID: "award_key",
  VALUE: "potential_total_value_of_award_num",
}

/* =============== Optional SAM helper ================== */
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
    const contact =
      ent?.pointsOfContact?.[0] || ent?.coreData?.businessTypes?.[0] || null
    return {
      website: website && typeof website === "string" ? website.trim() : null,
      contact: contact
        ? {
            name: [contact?.firstName, contact?.lastName].filter(Boolean).join(" ") || null,
            email: contact?.email || null,
            phone: contact?.phone || null,
          }
        : null,
    }
  } catch { return null }
}

/* =============== Worker ================== */
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

    /* ---- health ---- */
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true, db: true }), {
        status: 200, headers: { ...headers, "Content-Type": "application/json" },
      })
    }

    /* ---- agencies (cached) ---- */
    if (last === "agencies") {
      const cache = caches.default
      const cacheKey = new Request(url.toString(), request)
      const cached = await cache.match(cacheKey)
      if (cached) return withCors(cached, { ...headers, "Cache-Control":"public, s-maxage=86400, stale-while-revalidate=604800" })

      const client = makeClient(env)
      try {
        await client.connect()
        const sql = `
          SELECT DISTINCT name FROM (
            SELECT awarding_agency_name      AS name FROM ${USA_TABLE} WHERE awarding_agency_name IS NOT NULL
            UNION
            SELECT awarding_sub_agency_name AS name FROM ${USA_TABLE} WHERE awarding_sub_agency_name IS NOT NULL
            UNION
            SELECT awarding_office_name     AS name FROM ${USA_TABLE} WHERE awarding_office_name IS NOT NULL
          ) x
          WHERE name IS NOT NULL
          ORDER BY name
          LIMIT 400;`
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
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed"}), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- expiring-contracts (cached 5m) ---- */
    if (last === "expiring-contracts") {
      const naicsParam   = (url.searchParams.get("naics") || "").trim()
      const agencyFilter = (url.searchParams.get("agency") || "").trim()
      const windowDays   = Math.max(1, Math.min(365, parseInt(url.searchParams.get("window_days") || "180", 10)))
      const limit        = Math.max(1, Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10)))

      const naicsList =
        naicsParam.length > 0
          ? naicsParam.split(",").map(s => s.trim()).filter(Boolean)
          : []

      const cache = caches.default
      const cacheKey = new Request(url.toString(), request)
      const cached = await cache.match(cacheKey)
      if (cached) return withCors(cached, { ...headers, "Cache-Control":"public, s-maxage=300, stale-while-revalidate=86400" })

      const client = makeClient(env)
      try {
        await client.connect()
        await client.query("SET statement_timeout = '55s'")
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
          LIMIT $4`
        const params = [windowDays, agencyFilter || null, naicsList.length ? naicsList : null, limit]
        const { rows } = await client.query(sql, params)
        const data = rows.map(r => ({
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
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed"}), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- my-entity ---- */
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

        // optional SAM website + contact
        let smallBizCategories = []
        let website = null, contact = null
        try {
          const sam = await fetchVendorWebsiteByUEI(uei, env)
          website = sam?.website || null
          contact = sam?.contact || null
        } catch {}

        return new Response(JSON.stringify({
          ok: true,
          entity: { uei, name, naics, smallBizCategories, website, contact },
        }), {
          status:200,
          headers:{ ...headers, "Content-Type":"application/json", "Cache-Control":"public, s-maxage=86400" },
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed"}), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- vendor-awards (includes title + extent_competed) ---- */
    if (last === "vendor-awards") {
      const uei   = (url.searchParams.get("uei") || "").trim()
      const agency = (url.searchParams.get("agency") || "").trim()
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years") || "5", 10)))
      const limit = Math.max(1, Math.min(300, parseInt(url.searchParams.get("limit") || "100", 10)))
      if (!uei) {
        return new Response(JSON.stringify({ ok:false, error:"Missing uei" }), {
          status:400, headers:{ ...headers, "Content-Type":"application/json" }
        })
      }
      const client = makeClient(env)
      try {
        await client.connect()
        await client.query(`SET statement_timeout='15s'`)

        // detect FY field
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
        if (has("fiscal_year")) fyExpr = "fiscal_year"
        else fyExpr = `EXTRACT(YEAR FROM CURRENT_DATE)::int`

        const sql = `
          SELECT
            award_id_piid                               AS piid,
            (${fyExpr})                                 AS fiscal_year,
            awarding_agency_name                        AS agency,
            awarding_sub_agency_name                    AS sub_agency,
            awarding_office_name                        AS office,
            naics_code                                  AS naics,
            type_of_set_aside                           AS set_aside,
            COALESCE(idv_type_of_award, award_type)     AS vehicle,
            extent_competed                              AS extent_competed,
            COALESCE(title, transaction_description, prime_award_base_transaction_description) AS title,
            total_dollars_obligated_num                 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
          ORDER BY (${fyExpr}) DESC, pop_current_end_date DESC NULLS LAST, piid DESC
          LIMIT $4`
        const { rows } = await client.query(sql, [uei, agency || null, years, limit])
        const data = (rows || []).map(r => ({
          piid: r.piid,
          fiscal_year: Number(r.fiscal_year),
          agency: r.agency, sub_agency: r.sub_agency, office: r.office,
          naics: r.naics,
          set_aside: r.set_aside || null,
          vehicle: r.vehicle || null,
          extent_competed: r.extent_competed || null,
          title: r.title || null,
          obligated: typeof r.obligated === "number" ? r.obligated : Number(r.obligated || 0),
        }))
        return new Response(JSON.stringify({ ok:true, rows:data }), {
          status:200, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed"}), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- contracts/insights (adds new quick-facts fields) ---- */
    if (request.method === "POST" && url.pathname.toLowerCase().endsWith("/contracts/insights")) {
      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok:false, error:"OPENAI_API_KEY is not configured for sb-analytics." }), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      }
      const body = await request.json().catch(()=>({}))
      const piid = String(body.piid || "").trim().toUpperCase()
      if (!piid) {
        return new Response(JSON.stringify({ ok:false, error:"Missing piid" }), {
          status:400, headers:{ ...headers, "Content-Type":"application/json" }
        })
      }

      const client = makeClient(env)
      try {
        await client.connect()
        const awardSql = `
          SELECT
            award_id_piid,
            awarding_agency_name,
            awarding_sub_agency_name,
            awarding_office_name,
            recipient_name,
            recipient_uei,
            recipient_doing_business_as_name,
            cage_code,
            naics_code,
            naics_description,
            pop_start_date,
            pop_current_end_date,
            pop_potential_end_date,
            current_total_value_of_award_num,
            potential_total_value_of_award_num,
            total_dollars_obligated_num,
            extent_competed,
            type_of_set_aside,
            number_of_offers_received,
            COALESCE(title, transaction_description, prime_award_base_transaction_description) AS title
          FROM ${USA_TABLE}
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC NULLS LAST
          LIMIT 1`
        const awardRes = await client.query(awardSql, [piid])
        if (!awardRes.rows.length) {
          return new Response(JSON.stringify({ ok:false, error:"No award found for that PIID." }), {
            status:404, headers:{ ...headers, "Content-Type":"application/json" }
          })
        }
        const a = awardRes.rows[0]
        const toNum = (x) => (typeof x === "number" ? x : x==null ? null : Number(x))

        const obligated    = toNum(a.total_dollars_obligated_num) ?? 0
        const currentValue = toNum(a.current_total_value_of_award_num) ?? 0
        const ceiling      = toNum(a.potential_total_value_of_award_num) ?? currentValue

        const today = new Date()
        const parseDate = (d) => (d ? new Date(d) : null)
        const startDate   = parseDate(a.pop_start_date)
        const currentEnd  = parseDate(a.pop_current_end_date)
        const potentialEnd= parseDate(a.pop_potential_end_date)
        const endForLife  = potentialEnd || currentEnd

        let lifecycleStage = "unknown", lifecycleLabel = "Lifecycle insight limited", windowLabel="Window unknown", timeElapsedPct=null, burnPct=null
        if (startDate && endForLife && endForLife > startDate) {
          const total = endForLife.getTime() - startDate.getTime()
          const nowClamped = Math.min(Math.max(today.getTime(), startDate.getTime()), endForLife.getTime())
          timeElapsedPct = Math.round((nowClamped - startDate.getTime())/total*100)
          if (today < startDate) { lifecycleStage="not_started"; lifecycleLabel="Not started yet"; windowLabel="Window not opened" }
          else if (today > endForLife) { lifecycleStage="complete"; lifecycleLabel="Performance complete"; windowLabel="Window passed" }
          else if (timeElapsedPct < 25) { lifecycleStage="early"; lifecycleLabel="Early stage"; windowLabel="In performance window" }
          else if (timeElapsedPct < 75) { lifecycleStage="mid"; lifecycleLabel="Mid-stage"; windowLabel="In performance window" }
          else { lifecycleStage="late"; lifecycleLabel="Late / near end"; windowLabel="In performance window" }
        }
        if (ceiling && ceiling > 0) burnPct = Math.round((obligated/ceiling)*100)

        // subawards summary
        const subsSql = `
          SELECT subawardee_name, subawardee_uei, subaward_amount
          FROM public.usaspending_contract_subawards
          WHERE prime_award_piid = $1`
        const subsRes = await client.query(subsSql, [piid])
        const subsRaw = subsRes.rows || []
        const subMap = new Map()
        for (const row of subsRaw) {
          const name = row.subawardee_name || "(Unnamed subrecipient)"
          const uei  = row.subawardee_uei || null
          const amt  = toNum(row.subaward_amount) || 0
          const key  = `${uei || "NOUEI"}|${name}`
          const prev = subMap.get(key) || { name, uei, amount: 0 }
          prev.amount += amt
          subMap.set(key, prev)
        }
        const subsAgg = Array.from(subMap.values()).sort((a,b) => (b.amount||0)-(a.amount||0))
        const totalSubAmount = subsAgg.reduce((s,x)=>s+(x.amount||0),0)
        const primeVsSubsPct = obligated>0 && totalSubAmount>0
          ? { prime: Math.round(100 - Math.min(100,totalSubAmount/obligated*100)),
              subs:  Math.round(Math.min(100,totalSubAmount/obligated*100)) }
          : null
        const largestSubPct = totalSubAmount>0 && subsAgg[0] ? Math.round((subsAgg[0].amount/totalSubAmount)*100) : null

        const primary = {
          piid: a.award_id_piid,
          agency: a.awarding_agency_name || null,
          subAgency: a.awarding_sub_agency_name || null,
          office: a.awarding_office_name || null,
          primeName: a.recipient_name || null,
          primeUei: a.recipient_uei || null,
          dbaName: a.recipient_doing_business_as_name || null,
          cageCode: a.cage_code || null,
          naicsCode: a.naics_code || null,
          naicsDescription: a.naics_description || null,
          popStartDate: a.pop_start_date || null,
          popCurrentEndDate: a.pop_current_end_date || null,
          popPotentialEndDate: a.pop_potential_end_date || null,
          title: a.title || null,
          extentCompeted: a.extent_competed || null,
          typeOfSetAside: a.type_of_set_aside || null,
          numberOfOffers: a.number_of_offers_received || null,
          obligated, currentValue, ceiling,
        }

        const websiteInfo = await fetchVendorWebsiteByUEI(primary.primeUei, env)
        if (websiteInfo?.website) primary.website = websiteInfo.website

        const lifecycle = {
          stage: lifecycleStage, label: lifecycleLabel, windowLabel, timeElapsedPct, burnPct, primeVsSubsPct, largestSubPct,
        }
        const subs = { count: subsRaw.length, distinctRecipients: subsAgg.length, totalAmount: totalSubAmount, top: subsAgg.slice(0,5) }

        // Short AI bullets (unchanged)
        const prompt = `
You are helping a small federal contractor quickly understand a single contract and how to position for a recompete or subcontracting role.
Treat the lifecycle, burn %, and subcontracting figures below as correct.

Contract snapshot:
- PIID: ${primary.piid}
- Awarding agency: ${primary.agency || "—"}
- Component / office: ${primary.subAgency || "—"} / ${primary.office || "—"}
- Prime: ${primary.primeName || "—"} (UEI: ${primary.primeUei || "unknown"})${primary.website ? ` — Website: ${primary.website}` : ""}
- NAICS: ${primary.naicsCode || "—"} – ${primary.naicsDescription || "—"}
- Period of performance: ${primary.popStartDate || "—"} to ${primary.popCurrentEndDate || "—"} (potential: ${primary.popPotentialEndDate || "—"})
- Lifecycle: time elapsed ≈${lifecycle.timeElapsedPct == null ? "unknown" : lifecycle.timeElapsedPct + "%"}, burn ≈${lifecycle.burnPct == null ? "unknown" : lifecycle.burnPct + "%"}

Write 4–6 bullets with next actions to raise capture score; max 170 words.
`.trim()

        const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
          method: "POST",
          headers: { "Content-Type":"application/json", Authorization:`Bearer ${env.OPENAI_API_KEY}` },
          body: JSON.stringify({ model:"gpt-4.1-mini", messages:[
            { role:"system", content:"You are a federal contracts analyst helping small businesses interpret USAspending and subcontract data." },
            { role:"user", content: prompt },
          ], max_tokens: 400 })
        })
        const aiText = await aiRes.text()
        let aiJson = {}; try { aiJson = aiText ? JSON.parse(aiText) : {} } catch {}
        const summary = aiJson?.choices?.[0]?.message?.content?.trim() || "—"

        return new Response(JSON.stringify({
          ok: true, summary, primary, lifecycle, subs,
          disclaimer: "Subcontractor data is sourced from USAspending and may be incomplete."
        }), { status:200, headers:{ ...headers, "Content-Type":"application/json" }})
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message||"AI insight failed" }), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- bid-nobid & memo (unchanged logic except v2 table) ---- */
    // ... (KEEP your existing /sb/bid-nobid and /sb/bid-nobid-memo blocks exactly as in your latest worker,
    // they already reference type_of_set_aside and USA_TABLE above now points to v2.)

    /* ---- Teaming suggestions (NEW) ----
     * GET /sb/teaming-suggestions?piid=...&naics=541519&org=NASA&years=3&limit=5&exclude_ueis=UEI1,UEI2
     */
    if (last === "teaming-suggestions") {
      const piid  = (url.searchParams.get("piid") || "").trim().toUpperCase()
      const naics = (url.searchParams.get("naics") || "").trim()
      const org   = (url.searchParams.get("org") || "").trim()
      const years = Math.max(1, Math.min(5, parseInt(url.searchParams.get("years") || "3", 10)))
      const limit = Math.max(1, Math.min(10, parseInt(url.searchParams.get("limit") || "5", 10)))
      const exclude = new Set(((url.searchParams.get("exclude_ueis") || "").split(",").map(s=>s.trim().toUpperCase()).filter(Boolean)))

      const client = makeClient(env)
      try {
        await client.connect()

        // incumbent UEI (exclude)
        if (piid) {
          const incRes = await client.query(`SELECT recipient_uei FROM ${USA_TABLE} WHERE award_id_piid=$1 LIMIT 1`, [piid])
          const inc = incRes.rows[0]?.recipient_uei?.toUpperCase()
          if (inc) exclude.add(inc)
        }

        // candidate pool within org + NAICS family
        const sql = `
          WITH base AS (
            SELECT
              recipient_uei AS uei,
              recipient_name AS name,
              type_of_set_aside AS set_aside,
              naics_code AS naics,
              COALESCE(title, transaction_description, prime_award_base_transaction_description) AS title,
              total_dollars_obligated_num AS obligated,
              fiscal_year
            FROM ${USA_TABLE}
            WHERE (${naics ? "naics_code = $1 OR LEFT(naics_code,3) = LEFT($1,3)" : "TRUE"})
              AND (
                $2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
              )
              AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
              AND recipient_uei IS NOT NULL
          ),
          agg AS (
            SELECT
              uei, MIN(name) AS name,
              SUM(COALESCE(obligated,0)) AS obligated,
              MAX(set_aside) AS set_aside
            FROM base
            GROUP BY uei
          )
          SELECT * FROM agg
          WHERE uei <> ALL($4::text[])
          ORDER BY obligated ASC NULLS LAST
          LIMIT $5`
        const excludes = Array.from(exclude)
        const { rows } = await client.query(sql, [naics || null, org || null, years, excludes, limit])

        // enrich each with recent awards (evidence) + website/contact
        const out = []
        for (const r of rows) {
          const awardsRes = await client.query(
            `SELECT award_id_piid AS piid, fiscal_year, naics_code AS naics,
                    COALESCE(title, transaction_description, prime_award_base_transaction_description) AS title,
                    total_dollars_obligated_num AS obligated
             FROM ${USA_TABLE}
             WHERE recipient_uei=$1
               AND (
                 $2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
               )
               AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
             ORDER BY fiscal_year DESC, pop_current_end_date DESC NULLS LAST
             LIMIT 5`,
            [r.uei, org || null, years]
          )
          let website = null, contact = null
          try {
            const sam = await fetchVendorWebsiteByUEI(r.uei, env)
            website = sam?.website || null
            contact = sam?.contact || null
          } catch {}
          out.push({
            uei: r.uei,
            name: r.name,
            set_aside: r.set_aside || null,
            obligated: typeof r.obligated === "number" ? r.obligated : Number(r.obligated || 0),
            website, contact,
            recent_awards: (awardsRes.rows || []).map(a => ({
              piid: a.piid, fiscal_year: a.fiscal_year, naics: a.naics, title: a.title, obligated: a.obligated
            })),
          })
        }

        return new Response(JSON.stringify({ ok:true, rows: out }), {
          status:200, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message||"teaming failed"}), {
          status:500, headers:{ ...headers, "Content-Type":"application/json" }
        })
      } finally { try { await client.end() } catch {} }
    }

    /* ---- fallback ---- */
    return new Response("Not found", { status: 404, headers })
  },
}
