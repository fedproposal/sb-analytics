// worker.js — sb-analytics (final)
import { Client } from "pg"

/* ---------- CORS ---------- */
function cors(origin, env) {
  const list = (env?.CORS_ALLOW_ORIGINS || "").split(",").map(s=>s.trim()).filter(Boolean)
  const allow = list.length === 0 || list.includes("*") ? (origin || "*") : (list.includes(origin) ? origin : list[0])
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
  for (const [k,v] of Object.entries(headers||{})) h.set(k,v)
  return new Response(res.body, { status: res.status, headers: h })
}

/* ---------- DB ---------- */
function makeClient(env) {
  return new Client({ connectionString: env.HYPERDRIVE.connectionString, ssl: { rejectUnauthorized: false } })
}

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

/* ---------- helpers ---------- */
async function getColumns(client, tableFqn) {
  const schema = tableFqn.includes(".") ? tableFqn.split(".")[0] : "public"
  const table  = tableFqn.includes(".") ? tableFqn.split(".")[1] : tableFqn
  const r = await client.query(
    `SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2`,
    [schema, table]
  )
  return new Set((r.rows||[]).map(x => String(x.column_name).toLowerCase()))
}
function coalesceExpr(names, have, castText=true) {
  const present = names.filter(n => have.has(n.toLowerCase()))
  if (!present.length) return "NULL"
  const segs = present.map(n => castText ? `${n}::text` : n)
  return `COALESCE(${segs.join(",")})`
}

/* ---------- optional SAM website ---------- */
async function fetchVendorWebsiteByUEI(uei, env) {
  const key = env.SAM_API_KEY
  if (!key || !uei) return null
  try {
    const u = new URL("https://api.sam.gov/entity-information/v2/entities")
    u.searchParams.set("ueiSAM", uei)
    u.searchParams.set("api_key", key)
    const r = await fetch(u.toString(), { cf:{ cacheTtl:86400, cacheEverything:true } })
    if (!r.ok) return null
    const j = await r.json()
    const ent = j?.entityRegistration || j?.entities?.[0] || j?.results?.[0] || null
    const website =
      ent?.coreData?.businessInformation?.url ||
      ent?.coreData?.generalInformation?.corporateUrl ||
      ent?.coreData?.generalInformation?.url || null
    return typeof website === "string" ? website.trim() : null
  } catch { return null }
}

/* ===================================================================== */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)
    const origin = request.headers.get("Origin") || ""
    const headers = cors(origin, env)
    if (request.method === "OPTIONS") return new Response(null, { status:204, headers })

    const segs = url.pathname.split("/").filter(Boolean)
    const last = segs[segs.length-1] || ""

    /* ---------- health ---------- */
    if (last === "health") {
      return new Response(JSON.stringify({ ok:true, db:true }), {
        status:200, headers:{ ...headers, "Content-Type":"application/json" }
      })
    }

    /* ---------- agencies (cached) ---------- */
    if (last === "agencies") {
      const cache = caches.default
      const key = new Request(url.toString(), request)
      const cached = await cache.match(key)
      if (cached) return withCors(cached, { ...headers, "Cache-Control":"public, s-maxage=86400, stale-while-revalidate=604800" })

      const client = makeClient(env)
      try {
        await client.connect()
        const sql = `
          SELECT DISTINCT name FROM (
            SELECT awarding_agency_name      AS name FROM ${USA_TABLE} WHERE awarding_agency_name IS NOT NULL
            UNION SELECT awarding_sub_agency_name AS name FROM ${USA_TABLE} WHERE awarding_sub_agency_name IS NOT NULL
            UNION SELECT awarding_office_name     AS name FROM ${USA_TABLE} WHERE awarding_office_name IS NOT NULL
          ) x WHERE name IS NOT NULL ORDER BY name LIMIT 400;
        `
        const { rows } = await client.query(sql)
        const res = new Response(JSON.stringify({ ok:true, rows }), {
          status:200, headers:{ ...headers, "Content-Type":"application/json",
            "Cache-Control":"public, s-maxage=86400, stale-while-revalidate=604800" }
        })
        ctx.waitUntil(cache.put(key, res.clone()))
        return res
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      } finally { try { await client.end() } catch {} }
    }

    /* ---------- expiring contracts ---------- */
    if (last === "expiring-contracts") {
      const naicsParam = (url.searchParams.get("naics")||"").trim()
      const agencyFilter = (url.searchParams.get("agency")||"").trim()
      const windowDays = Math.max(1, Math.min(365, parseInt(url.searchParams.get("window_days")||"180",10)))
      const limit = Math.max(1, Math.min(200, parseInt(url.searchParams.get("limit")||"100",10)))

      const naicsList = naicsParam ? naicsParam.split(",").map(s=>s.trim()).filter(Boolean) : []

      const cache = caches.default
      const key = new Request(url.toString(), request)
      const cached = await cache.match(key)
      if (cached) return withCors(cached, { ...headers, "Cache-Control":"public, s-maxage=300, stale-while-revalidate=86400" })

      const client = makeClient(env)
      try {
        await client.connect()
        try { await client.query("SET statement_timeout='55s'") } catch {}

        const sql = `
          SELECT ${COL.PIID} AS piid, ${COL.AWARD_ID} AS award_key, ${COL.AGENCY} AS agency,
                 ${COL.NAICS} AS naics, ${COL.END_DATE} AS end_date, ${COL.VALUE} AS value
          FROM ${USA_TABLE}
          WHERE ${COL.END_DATE} >= CURRENT_DATE
            AND ${COL.END_DATE} < CURRENT_DATE + $1::int
            AND (
              $2::text IS NULL OR ${COL.AGENCY}=$2 OR ${COL.SUB_AGENCY}=$2 OR awarding_office_name=$2
            )
            AND ($3::text[] IS NULL OR ${COL.NAICS} = ANY($3))
          ORDER BY ${COL.END_DATE} ASC
          LIMIT $4
        `
        const { rows } = await client.query(sql, [windowDays, agencyFilter || null, naicsList.length ? naicsList : null, limit])
        const data = rows.map(r => ({
          piid: r.piid, award_key: r.award_key, agency: r.agency,
          naics: r.naics, end_date: r.end_date,
          value: typeof r.value === "number" ? r.value : Number(r.value ?? null),
        }))
        const res = new Response(JSON.stringify({ ok:true, rows:data }), {
          status:200, headers:{ ...headers, "Content-Type":"application/json",
            "Cache-Control":"public, s-maxage=300, stale-while-revalidate=86400" }
        })
        ctx.waitUntil(cache.put(key, res.clone()))
        return res
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      }
    }

    /* ---------- my-entity ---------- */
    if (last === "my-entity") {
      const uei = (url.searchParams.get("uei")||"").trim().toUpperCase()
      if (!uei) return new Response(JSON.stringify({ ok:false, error:"Missing uei parameter." }),
        { status:400, headers:{ ...headers, "Content-Type":"application/json" }})
      const client = makeClient(env)
      try {
        await client.connect()
        const nameRes = await client.query(
          `SELECT recipient_name FROM ${USA_TABLE}
           WHERE recipient_uei=$1
           ORDER BY total_dollars_obligated_num DESC NULLS LAST LIMIT 1`,
           [uei]
        )
        const naicsRes = await client.query(
          `SELECT DISTINCT naics_code FROM ${USA_TABLE}
           WHERE recipient_uei=$1 AND naics_code IS NOT NULL LIMIT 200`,
           [uei]
        )
        let smallBizCategories = []
        try {
          if (env.SAM_PROXY_URL) {
            const u = `${env.SAM_PROXY_URL.replace(/\/+$/,"")}/entity?uei=${encodeURIComponent(uei)}`
            const r = await fetch(u)
            if (r.ok) {
              const j = await r.json().catch(()=>null)
              const cats = j?.categories || j?.entity?.socioEconomicCategories || []
              smallBizCategories = Array.from(new Set((cats||[]).filter(Boolean)))
            }
          }
        } catch {}
        return new Response(JSON.stringify({
          ok:true,
          entity:{
            uei,
            name: nameRes.rows[0]?.recipient_name || null,
            naics: (naicsRes.rows||[]).map(r=>r.naics_code).filter(Boolean),
            smallBizCategories,
          }
        }), { status:200, headers:{ ...headers, "Content-Type":"application/json", "Cache-Control":"public, s-maxage=86400" }})
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message||"query failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      } finally { try { await client.end() } catch {} }
    }

    /* ---------- vendor-awards (w/ title) ---------- */
/* =============================================================
 * Vendor awards list (last N fiscal years, filtered by agency)
 *    GET /sb/vendor-awards?uei=XXXX&agency=Dept%20of%20Defense&years=5&limit=100
 * Now returns: piid, fiscal_year, agency/sub/office, naics, set_aside, vehicle,
 *              title, extent_competed, obligated
 * =========================================================== */
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

    // Column discovery against the view (v2)
    const schema = USA_TABLE.includes(".") ? USA_TABLE.split(".")[0] : "public"
    const table  = USA_TABLE.includes(".") ? USA_TABLE.split(".")[1] : USA_TABLE
    const colsRes = await client.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2`,
      [schema, table]
    )
    const have = new Set((colsRes.rows || []).map(r => String(r.column_name).toLowerCase()))
    const has = (c) => have.has(String(c).toLowerCase())

    // Fiscal-year expression (robust)
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
    } else {
      fyExpr = "EXTRACT(YEAR FROM CURRENT_DATE)::int" // last-resort
    }

    // Set-aside/vehicle expressions (work across schemas)
    const setAsideExpr = (() => {
      const c = ["type_of_set_aside","type_set_aside","set_aside","set_aside_any"].filter(has)
      return c.length ? `COALESCE(${c.map(n=>`${n}::text`).join(",")})` : "NULL"
    })()

    const vehicleExpr = (() => {
      const c = ["idv_type","idv_type_of_award","contract_vehicle","contract_award_type","award_type"].filter(has)
      return c.length ? `COALESCE(${c.map(n=>`${n}::text`).join(",")})` : "NULL"
    })()

    // New fields: title + extent_competed (with graceful fallbacks)
    const titleExpr = (() => {
      const c = ["title","transaction_description","prime_award_base_transaction_description"].filter(has)
      return c.length ? `COALESCE(${c.map(n=>`${n}::text`).join(",")})` : "NULL"
    })()

    const extentExpr = has("extent_competed")
      ? "extent_competed::text"
      : (has("extent_competed_code") ? "extent_competed_code::text" : "NULL")

    // Query
    const sql = `
      SELECT *
      FROM (
        SELECT
          award_id_piid                                   AS piid,
          (${fyExpr})                                     AS fiscal_year,
          awarding_agency_name                            AS agency,
          awarding_sub_agency_name                        AS sub_agency,
          awarding_office_name                            AS office,
          naics_code                                      AS naics,
          ${setAsideExpr}                                 AS set_aside,
          ${vehicleExpr}                                  AS vehicle,
          ${titleExpr}                                    AS title,
          ${extentExpr}                                   AS extent_competed,
          total_dollars_obligated_num                     AS obligated,
          pop_current_end_date                            AS pop_end
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
      title: r.title || null,
      extent_competed: r.extent_competed || null,
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

    /* ---------- contract insights (AI) ---------- */
    if (request.method === "POST" && url.pathname.toLowerCase().endsWith("/contracts/insights")) {
      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok:false, error:"OPENAI_API_KEY is not configured for sb-analytics." }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      }
      const client = makeClient(env)
      try {
        const body = await request.json().catch(()=> ({}))
        const piid = String(body.piid || "").trim().toUpperCase()
        if (!piid) return new Response(JSON.stringify({ ok:false, error:"Missing piid" }),
          { status:400, headers:{ ...headers, "Content-Type":"application/json" }})
        await client.connect()

        const awardSql = `
          SELECT award_id_piid, awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                 recipient_name, recipient_uei, naics_code, naics_description,
                 pop_start_date, pop_current_end_date, pop_potential_end_date,
                 current_total_value_of_award_num, potential_total_value_of_award_num, total_dollars_obligated_num
          FROM ${USA_TABLE} WHERE award_id_piid=$1
          ORDER BY pop_current_end_date DESC LIMIT 1
        `
        const aRes = await client.query(awardSql, [piid])
        if (!aRes.rows.length) {
          return new Response(JSON.stringify({ ok:false, error:"No award found for that PIID." }),
            { status:404, headers:{ ...headers, "Content-Type":"application/json" }})
        }
        const a = aRes.rows[0]
        const toNum = (x)=> typeof x==="number" ? x : (x==null? null : Number(x))
        const obligated = toNum(a.total_dollars_obligated_num) ?? 0
        const currentValue =
          toNum(a.current_total_value_of_award_num) ??
          toNum(a.potential_total_value_of_award_num) ?? 0
        const ceiling = toNum(a.potential_total_value_of_award_num) ?? currentValue

        const today = new Date()
        const sd = a.pop_start_date ? new Date(a.pop_start_date) : null
        const cend = a.pop_current_end_date ? new Date(a.pop_current_end_date) : null
        const pend = a.pop_potential_end_date ? new Date(a.pop_potential_end_date) : null
        const end = pend || cend
        let stage = "unknown", label="Lifecycle insight limited", windowLabel="Window unknown", timePct=null, burnPct=null
        if (sd && end && end>sd) {
          const total = end.getTime()-sd.getTime()
          const elapsed = Math.min(Math.max(today.getTime(), sd.getTime()), end.getTime()) - sd.getTime()
          timePct = Math.round((elapsed/total)*100)
          if (today<sd) { stage="not_started"; label="Not started yet"; windowLabel="Window not opened" }
          else if (today>end) { stage="complete"; label="Performance complete"; windowLabel="Window passed" }
          else if (timePct<25) { stage="early"; label="Early stage"; windowLabel="In performance window" }
          else if (timePct<75) { stage="mid"; label="Mid-stage"; windowLabel="In performance window" }
          else { stage="late"; label="Late / near end"; windowLabel="In performance window" }
        }
        if (ceiling && ceiling>0) burnPct = Math.round((obligated/ceiling)*100)

        const subsRes = await client.query(
          `SELECT subawardee_name, subawardee_uei, subaward_amount
           FROM public.usaspending_contract_subawards WHERE prime_award_piid=$1`, [piid]
        )
        const subsRaw = subsRes.rows || []
        const subMap = new Map()
        for (const row of subsRaw) {
          const name = row.subawardee_name || "(Unnamed subrecipient)"
          const uei = row.subawardee_uei || null
          const amt = toNum(row.subaward_amount) || 0
          const key = `${uei || "NOUEI"}|${name}`
          const prev = subMap.get(key) || { name, uei, amount:0 }
          prev.amount += amt
          subMap.set(key, prev)
        }
        const subsAgg = Array.from(subMap.values()).sort((a,b)=> (b.amount||0)-(a.amount||0))
        const subCount = subsRaw.length
        const distinctRecipients = subsAgg.length
        const totalSubAmount = subsAgg.reduce((s,x)=> s+(x.amount||0), 0)
        const topSubs = subsAgg.slice(0,5)

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
          obligated, currentValue, ceiling,
        }
        const website = await fetchVendorWebsiteByUEI(primary.primeUei, env)
        if (website) primary.website = website

        const lifecycle = {
          stage, label, windowLabel, timeElapsedPct: timePct, burnPct,
          primeVsSubsPct: (obligated>0 && totalSubAmount>0)
            ? { prime: Math.round(100 - Math.min(100,(totalSubAmount/obligated)*100)), subs: Math.round(Math.min(100,(totalSubAmount/obligated)*100)) }
            : null,
          largestSubPct: (totalSubAmount>0 && topSubs.length>0) ? Math.round((topSubs[0].amount/totalSubAmount)*100) : null,
        }
        const subs = { count: subCount, distinctRecipients, totalAmount: totalSubAmount, top: topSubs }

        const burnText = burnPct==null
          ? "Burn vs. ceiling could not be determined."
          : `Approximately ${burnPct}% of ceiling is obligated (≈$${Math.round(obligated).toLocaleString()} of ≈$${Math.round(ceiling).toLocaleString()}).`
        const subsText = subCount===0
          ? "No subcontract awards are publicly reported for this contract."
          : `There are ${subCount} reported subawards to ${distinctRecipients} recipients, totaling about $${Math.round(totalSubAmount).toLocaleString()}.`

        const prompt = `
You are helping a small federal contractor interpret a contract snapshot and what to do next.
Use the given numbers as ground truth.

PIID: ${primary.piid}
Agency: ${primary.agency || "—"} / ${primary.subAgency || "—"} / ${primary.office || "—"}
Prime: ${primary.primeName || "—"} (UEI ${primary.primeUei || "unknown"})${website ? ` — ${website}` : ""}
NAICS: ${primary.naicsCode || "—"} — ${primary.naicsDescription || "—"}
PoP: ${primary.popStartDate || "—"} → ${primary.popCurrentEndDate || "—"} (potential: ${primary.popPotentialEndDate || "—"})
Lifecycle: ${lifecycle.stage} (${lifecycle.label}) time≈${lifecycle.timeElapsedPct ?? "?"}%
${burnText}
Subs: ${subsText}

Write 4–6 crisp bullets on capture timing and actionable next steps. 170 words max.
        `.trim()

        const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
          method:"POST",
          headers:{ "Content-Type":"application/json", Authorization:`Bearer ${env.OPENAI_API_KEY}` },
          body: JSON.stringify({ model:"gpt-4.1-mini", messages:[
            { role:"system", content:"You are a federal contracts analyst." },
            { role:"user", content: prompt }
          ], max_tokens:400 })
        })
        const aiText = await aiRes.text()
        let aiJson = {}
        try { aiJson = aiText ? JSON.parse(aiText) : {} } catch { throw new Error("AI response not JSON: "+aiText.slice(0,160)) }
        const summary = aiJson.choices?.[0]?.message?.content?.trim() || "AI produced no summary."

        return new Response(JSON.stringify({
          ok:true, summary, primary, lifecycle, subs,
          disclaimer: "Subcontractor data is sourced from USAspending and may be incomplete."
        }), { status:200, headers:{ ...headers, "Content-Type":"application/json" }})
      } catch (e) {
        return new Response(JSON.stringify({ ok:false, error:e?.message||"AI insight failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      }
    }

    /* ---------- bid/no-bid (auto-scorer) ---------- */
    if (last === "bid-nobid") {
      const piid = (url.searchParams.get("piid")||"").trim().toUpperCase()
      const uei  = (url.searchParams.get("uei") ||"").trim().toUpperCase()
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years")||"5",10)))
      if (!piid || !uei) return new Response(JSON.stringify({ ok:false, error:"Missing piid or uei" }),
        { status:400, headers:{ ...headers, "Content-Type":"application/json" }})

      const client = makeClient(env)
      try {
        await client.connect()
        const have = await getColumns(client, USA_TABLE)

        // FY expression
        let fyExpr = "EXTRACT(YEAR FROM CURRENT_DATE)::int"
        if (have.has("fiscal_year")) fyExpr = "fiscal_year"
        else if (have.has("action_date_fiscal_year")) fyExpr = "action_date_fiscal_year"
        else if (have.has("action_date")) {
          fyExpr = `CASE WHEN EXTRACT(MONTH FROM action_date)::int>=10
                    THEN EXTRACT(YEAR FROM action_date)::int+1 ELSE EXTRACT(YEAR FROM action_date)::int END`
        } else if (have.has("pop_current_end_date")) {
          fyExpr = `CASE WHEN EXTRACT(MONTH FROM pop_current_end_date)::int>=10
                    THEN EXTRACT(YEAR FROM pop_current_end_date)::int+1 ELSE EXTRACT(YEAR FROM pop_current_end_date)::int END`
        }

        // award snapshot
        const aRes = await client.query(`
          SELECT award_id_piid, naics_code, naics_description,
                 awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                 recipient_uei, recipient_name,
                 total_dollars_obligated_num AS obligated,
                 potential_total_value_of_award_num AS ceiling,
                 pop_current_end_date
          FROM ${USA_TABLE}
          WHERE award_id_piid=$1
          ORDER BY pop_current_end_date DESC NULLS LAST
          LIMIT 1`, [piid])
        if (!aRes.rows.length) {
          return new Response(JSON.stringify({ ok:false, error:"PIID not found" }),
            { status:404, headers:{ ...headers, "Content-Type":"application/json" }})
        }
        const A = aRes.rows[0]
        const orgName = A.awarding_sub_agency_name || A.awarding_office_name || A.awarding_agency_name
        const naics = A.naics_code

        // my-entity NAICS/socio
        const entRes = await fetch(`${url.origin}/sb/my-entity?uei=${encodeURIComponent(uei)}`)
        const entJson = await entRes.json().catch(()=> ({}))
        const myNAICS = (entJson?.entity?.naics || []).filter(Boolean)
        const mySocio = (entJson?.entity?.smallBizCategories || entJson?.entity?.socio || []).filter(Boolean)

        // my awards
        const myRes = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei=$1
            AND ($2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2)
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[uei, orgName, years])

        // incumbent awards at org
        const incRes = await client.query(`
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${USA_TABLE}
          WHERE recipient_uei=$1
            AND ($2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2)
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[A.recipient_uei, orgName, years])

        // market distribution for NAICS@org
        const distRes = await client.query(`
          WITH base AS (
            SELECT total_dollars_obligated_num AS obligated
            FROM ${USA_TABLE}
            WHERE naics_code=$1
              AND ($2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2)
              AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
              AND total_dollars_obligated_num IS NOT NULL
          )
          SELECT
            percentile_cont(0.25) WITHIN GROUP (ORDER BY obligated)::float8 AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY obligated)::float8 AS p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY obligated)::float8 AS p75
          FROM base
        `,[naics, orgName, years])
        const P = distRes.rows[0] || { p25:null, p50:null, p75:null }

        // set-aside tendency (SAFE!)
        const setAsideExpr = coalesceExpr(["type_of_set_aside","type_set_aside","set_aside"], have, true)
        const saRes = await client.query(`
          SELECT COUNT(*) FILTER (WHERE ${setAsideExpr} IS NOT NULL)::int AS known,
                 COUNT(*)::int AS total,
                 MAX(${setAsideExpr}) AS example_set_aside
          FROM ${USA_TABLE}
          WHERE naics_code=$1
            AND ($2::text IS NULL OR awarding_agency_name=$2 OR awarding_sub_agency_name=$2 OR awarding_office_name=$2)
            AND (${fyExpr}) >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        `,[naics, orgName, years])

        // scoring
        const toNum = (x)=> typeof x==="number" ? x : Number(x||0)
        const meCnt = toNum(myRes.rows[0]?.cnt), meObl = toNum(myRes.rows[0]?.obligated)
        const incCnt = toNum(incRes.rows[0]?.cnt), incObl = toNum(incRes.rows[0]?.obligated)
        const today = new Date()
        const dEnd = A.pop_current_end_date ? new Date(A.pop_current_end_date) : null
        const daysToEnd = dEnd ? Math.round((dEnd.getTime() - today.getTime())/86400000) : null
        const burn = A.ceiling && A.ceiling>0 ? Math.round((toNum(A.obligated)/toNum(A.ceiling))*100) : null

        const tech =
          myNAICS.includes(naics) ? 5 :
          myNAICS.some(c=> (c||"").slice(0,3) === String(naics||"").slice(0,3)) ? 4 : 2

        const pp = (meCnt>=3 || meObl>=2_000_000) ? 5 : (meCnt>=1 ? 3 : 1)

        let staffing = 3
        if (P.p50) {
          const contractSize = toNum(A.obligated || A.ceiling || 0)
          staffing = contractSize <= P.p50 ? 5 : (contractSize <= (P.p75||P.p50) ? 4 : 2)
        }

        let sched = 3
        if (daysToEnd!=null && burn!=null) {
          if (daysToEnd>180 && burn<70) sched=5
          else if (daysToEnd<60 || burn>90) sched=2
        }

        const knownSA = toNum(saRes.rows[0]?.known)
        const example = (saRes.rows[0]?.example_set_aside || "").toUpperCase()
        const haveMatch = (tag)=> mySocio.some(s=> (s||"").toUpperCase().includes(tag))
        let comp = 3
        if (knownSA>0) {
          if ((example.includes("SDVOSB") && haveMatch("SDVOSB")) ||
              (example.includes("WOSB")  && haveMatch("WOSB"))  ||
              (example.includes("HUB")   && haveMatch("HUB"))   ||
              (example.includes("8(A)")  && (haveMatch("8(A)") || haveMatch("8A")))) comp = 5
          else comp = 2
        }

        let price = 3
        if (P.p25 && P.p50 && P.p75) {
          const val = toNum(A.obligated || A.ceiling || 0)
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
        ) * 100) / 100
        const decision = weighted>=80 ? "bid" : (weighted>=65 ? "conditional" : "no_bid")

        const heat = [
          { level: incCnt>5 ? "High":"Med-High", reason:`Incumbent has ${incCnt} awards / $${Math.round(incObl).toLocaleString()} at ${orgName}` },
          { level: myNAICS.includes(naics) ? "Low":"Med-High", reason: myNAICS.includes(naics) ? "Exact NAICS match" : "No exact NAICS in SAM" },
          { level: meCnt>0 ? "Medium":"High", reason: meCnt>0 ? `You have ${meCnt} awards at this org` : "No awards at this org" },
        ]
        const improve = []
        if (!myNAICS.includes(naics)) improve.push(`Add NAICS ${naics} in SAM or team with a holder.`)
        if (meCnt===0) improve.push(`Pursue micro-tasking/teaming at ${orgName} to build a reference.`)
        if (knownSA>0 && comp<5) improve.push(`Align socio-economic status to prior set-aside pattern (${example || "varied"}).`)

        return new Response(JSON.stringify({
          ok:true,
          inputs:{ piid, uei, org:orgName, naics },
          criteria:[
            { name:"Technical Fit", weight:24, score:tech, reason: myNAICS.includes(naics) ? "Exact NAICS match" : (myNAICS.some(c=> (c||"").slice(0,3)===String(naics||"").slice(0,3)) ? "Related NAICS family" : "No NAICS match") },
            { name:"Relevant Experience / Past Performance", weight:20, score:pp, reason:`Your awards at this org: ${meCnt}; $${Math.round(meObl).toLocaleString()}` },
            { name:"Staffing & Key Personnel", weight:12, score:staffing, reason:"Proxy via local award size distribution" },
            { name:"Schedule / ATO Timeline Risk", weight:8, score:sched, reason:`Days to end: ${daysToEnd ?? "unknown"}; burn: ${burn ?? "unknown"}%` },
            { name:"Compliance", weight:8, score:comp, reason: knownSA ? `Historic set-aside example: ${example||"—"}` : "Set-aside pattern unknown" },
            { name:"Price Competitiveness", weight:8, score:price, reason:"Position vs NAICS@org percentiles" },
            { name:"Customer Intimacy", weight:10, score:intimacy, reason:`Your awards at this org: ${meCnt}` },
            { name:"Competitive Intelligence", weight:10, score:compIntel, reason:`Incumbent awards: ${incCnt}` },
          ],
          weighted_percent: weighted,
          decision, heatmap: heat, improve_now: improve
        }), { status:200, headers:{ ...headers, "Content-Type":"application/json" }})
      } catch (e) {
        try { await client.end() } catch {}
        return new Response(JSON.stringify({ ok:false, error:e?.message||"bid-nobid failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      } finally { try { await client.end() } catch {} }
    }

    /* ---------- bid-nobid-memo ---------- */
    if (last === "bid-nobid-memo") {
      const piid  = (url.searchParams.get("piid")||"").trim().toUpperCase()
      const uei   = (url.searchParams.get("uei") ||"").trim().toUpperCase()
      const years = (url.searchParams.get("years")||"5").trim()
      if (!piid || !uei) return new Response(JSON.stringify({ ok:false, error:"Missing piid or uei" }),
        { status:400, headers:{ ...headers, "Content-Type":"application/json" }})
      const bnbURL = new URL(request.url)
      bnbURL.pathname = "/sb/bid-nobid"
      bnbURL.search = `?piid=${encodeURIComponent(piid)}&uei=${encodeURIComponent(uei)}&years=${encodeURIComponent(years)}`
      const bRes = await fetch(bnbURL.toString(), { headers:{ "Accept":"application/json" } })
      const bTxt = await bRes.text()
      let bnb = {}; try { bnb = bTxt ? JSON.parse(bTxt) : {} } catch {}
      if (!bRes.ok || bnb?.ok === false) {
        return new Response(JSON.stringify({ ok:false, error: bnb?.error || "bid-nobid failed" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      }
      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok:false, error:"OPENAI_API_KEY not set" }),
          { status:500, headers:{ ...headers, "Content-Type":"application/json" }})
      }
      const matrixLines = (bnb.criteria||[]).map(c =>
        `${c.name} | weight ${c.weight}% | score ${c.score}/5 | ${c.reason||""}`.trim()
      ).join("\n")
      const prompt = `
You are the Bid/No-Bid Decision GPT. Using the scored matrix, write:
1) Decision with percent (Bid / Conditional / No-Bid).
2) A 5–7 line executive memo referencing specific numbers (NAICS match, org history $, socio-econ & set-aside, incumbent strength, schedule/burn, price percentile).
3) A 5-line emoji risk heat map (🟢🟡🟠🔴).
4) A 10–14 day capture plan (bullets).
5) 6–10 precise CO questions.

PIID: ${bnb?.inputs?.piid || piid}
Org: ${bnb?.inputs?.org || "—"} | NAICS: ${bnb?.inputs?.naics || "—"}
Decision: ${bnb?.decision || "—"} (${bnb?.weighted_percent ?? "—"}%)

Matrix:
${matrixLines}
      `.trim()
      const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
        method:"POST",
        headers:{ "Content-Type":"application/json", Authorization:`Bearer ${env.OPENAI_API_KEY}` },
        body: JSON.stringify({
          model:"gpt-4.1-mini",
          messages:[
            { role:"system", content:"You are a federal capture strategist." },
            { role:"user", content: prompt }
          ],
          temperature:0.3, max_tokens:900
        })
      })
      const aiTxt = await aiRes.text()
      let memo = ""; try { memo = JSON.parse(aiTxt).choices?.[0]?.message?.content?.trim() || "" } catch { memo = aiTxt.slice(0,3000) }
      return new Response(JSON.stringify({ ok:true, ...bnb, memo }), {
        status:200, headers:{ ...headers, "Content-Type":"application/json" }
      })
    }

    /* ---------- fallback ---------- */
    return new Response("Not found", { status:404, headers })
  }
}
