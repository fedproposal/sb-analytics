// worker.js — sb-analytics (DB-first, no direct USAspending from Worker)
// Uses Neon/Hyperdrive views you created:
//   - fp.contract_txn_min_v1           (mods/transactions + comments)
//   - fp.contract_award_summary_v      (award summary/meta)
//
// Notes:
// - CORS-friendly, edge-cached responses
// - Keeps existing routes (agencies, expiring-contracts, vendor-awards, insights, etc.)

import { Client } from "pg";

/* =====================================================================
   C O R S
   ===================================================================== */
function cors(origin, env) {
  const list = (env && env.CORS_ALLOW_ORIGINS ? env.CORS_ALLOW_ORIGINS : "")
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
    Vary: "Origin",
  };
}
function withCors(res, headers) {
  const h = new Headers(res.headers);
  for (const [k, v] of Object.entries(headers || {})) h.set(k, v);
  return new Response(res.body, { status: res.status, headers: h });
}

/* =====================================================================
   D A T A B A S E
   ===================================================================== */
function makeClient(env) {
  return new Client({
    connectionString: env.HYPERDRIVE.connectionString,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 15000,
  });
}

// These two were used by some routes; keep them if your fast view exists.
// They are NOT used by /sb/usa-contract in this version.
const TBL_FAST = "public.usaspending_awards_fast";
const TBL_V2   = "public.usaspending_awards_v2";

/** Probe the materialized view safely; ok if you don’t have it. */
async function preferFast(client) {
  try {
    const r = await client.query(
      `SELECT ispopulated
       FROM pg_matviews
       WHERE schemaname='public' AND matviewname='usaspending_awards_fast'
       LIMIT 1`
    );
    const populated =
      r.rows && r.rows[0] && (r.rows[0].ispopulated === true || r.rows[0].ispopulated === "t");
    if (!populated) return false;

    await client.query(`SET LOCAL statement_timeout = '1500ms'`);
    await client.query(`SELECT 1 FROM ${TBL_FAST} LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}

/** Prefer FAST when available; otherwise fall back to v2. */
async function queryPreferringFast(client, mkSQL, params) {
  const useFast = await preferFast(client);
  const sql = mkSQL(useFast ? TBL_FAST : TBL_V2);
  try {
    return await client.query(sql, params);
  } catch (e) {
    if (useFast) return await client.query(mkSQL(TBL_V2), params);
    throw e;
  }
}

/* =====================================================================
   U T I L S
   ===================================================================== */
async function fetchJSON(u, init) {
  const r = await fetch(u, init);
  const t = await r.text();
  let j = {};
  try {
    j = t ? JSON.parse(t) : {};
  } catch {
    throw new Error("Non-JSON response: " + t.slice(0, 160));
  }
  if (!r.ok || (j && j.ok === false)) throw new Error((j && j.error) || `HTTP ${r.status}`);
  return j;
}

async function fetchVendorWebsiteByUEI(uei, env) {
  const key = env.SAM_API_KEY;
  if (!key || !uei) return null;
  try {
    const u = new URL("https://api.sam.gov/entity-information/v2/entities");
    u.searchParams.set("ueiSAM", uei);
    u.searchParams.set("api_key", key);
    const r = await fetch(u.toString(), { cf: { cacheTtl: 86400, cacheEverything: true } });
    if (!r.ok) return null;
    const j = await r.json().catch(() => null);
    const ent =
      j && j.entityRegistration
        ? j.entityRegistration
        : Array.isArray(j && j.entities)
        ? j.entities[0]
        : Array.isArray(j && j.results)
        ? j.results[0]
        : null;
    const website =
      ent && ent.coreData && ent.coreData.businessInformation && ent.coreData.businessInformation.url
        ? ent.coreData.businessInformation.url
        : ent && ent.coreData && ent.coreData.generalInformation && ent.coreData.generalInformation.corporateUrl
        ? ent.coreData.generalInformation.corporateUrl
        : ent && ent.coreData && ent.coreData.generalInformation && ent.coreData.generalInformation.url
        ? ent.coreData.generalInformation.url
        : null;
    return website && typeof website === "string" ? website.trim() : null;
  } catch {
    return null;
  }
}

/* Normalize SBA/SAM category strings into canonical tags the scorer expects. */
function normalizeSocioTags(input) {
  const arr = Array.isArray(input)
    ? input
    : String(input || "")
        .split(/[;,|]/)
        .map((s) => s.trim())
        .filter(Boolean);
  const out = new Set();
  for (const raw of arr) {
    const s = String(raw || "").toUpperCase();
    if (!s) continue;
    if (/\b8\s*\(?A\)?/.test(s) || s.includes("8(A)")) out.add("8(A)");
    if (s.includes("SDVOSB") || s.includes("SERVICE-DISABLED")) out.add("SDVOSB");
    if (s.includes("WOSB") || s.includes("WOMEN")) out.add("WOSB");
    if (s.includes("HUBZONE")) out.add("HUBZONE");
    if (s.includes("VOSB") && !s.includes("SDVOSB")) out.add("VOSB");
    if (s.includes("SMALL BUSINESS")) out.add("SMALL BUSINESS");
  }
  return Array.from(out);
}

/* =====================================================================
   W O R K E R   E N T R Y
   ===================================================================== */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const headers = cors(origin, env);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    const segments = url.pathname.split("/").filter(Boolean);
    const last = segments[segments.length - 1] || "";

    /* -------------------- health -------------------- */
    if (last === "health") {
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    /* -------------------- agencies (cached 24h) -------------------- */
    if (last === "agencies") {
      const cache = caches.default;
      const cacheKey = new Request(url.toString(), request);
      const cached = await cache.match(cacheKey);
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=86400, stale-while-revalidate=604800" }
        );
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);
        const mkSQL = (t) => `
          SELECT DISTINCT name FROM (
            SELECT awarding_agency_name      AS name FROM ${t} WHERE awarding_agency_name IS NOT NULL
            UNION
            SELECT awarding_sub_agency_name AS name FROM ${t} WHERE awarding_sub_agency_name IS NOT NULL
            UNION
            SELECT awarding_office_name     AS name FROM ${t} WHERE awarding_office_name IS NOT NULL
          ) x WHERE name IS NOT NULL
          ORDER BY name LIMIT 400`;
        const { rows } = await queryPreferringFast(client, mkSQL, []);
        const res = new Response(JSON.stringify({ ok: true, rows }), {
          status: 200,
          headers: {
            ...headers,
            "Content-Type": "application/json",
            "Cache-Control": "public, s-maxage=86400, stale-while-revalidate=604800",
          },
        });
        ctx.waitUntil(cache.put(cacheKey, res.clone()));
        return res;
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- expiring-contracts (cached 5m) -------------------- */
    if (last === "expiring-contracts") {
      const naicsParam = (url.searchParams.get("naics") || "").trim();
      const agencyFilter = (url.searchParams.get("agency") || "").trim();
      const windowDays = Math.max(1, Math.min(365, parseInt(url.searchParams.get("window_days") || "180", 10)));
      const limit = Math.max(1, Math.min(200, parseInt(url.searchParams.get("limit") || "100", 10)));
      const naicsList = naicsParam ? naicsParam.split(",").map((s) => s.trim()).filter(Boolean) : [];

      const cache = caches.default;
      const cacheKey = new Request(url.toString(), request);
      const cached = await cache.match(cacheKey);
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=300, stale-while-revalidate=86400" }
        );
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);
       const mkSQL = (table) => `
  WITH base AS (
    SELECT
      award_id_piid                         AS piid,
      award_key                             AS award_key,
      awarding_agency_name                  AS agency,
      naics_code                            AS naics,
      /* NEW: incumbent name */
    COALESCE(NULLIF(recipient_name,''), NULL) AS incumbent,
      pop_current_end_date                  AS end_date,
      potential_total_value_of_award_num    AS value,
      recipient_name,
      recipient_uei
    FROM ${table}
    WHERE pop_current_end_date >= CURRENT_DATE
      AND pop_current_end_date < CURRENT_DATE + $1::int
      AND (
        $2::text IS NULL
        OR awarding_agency_name     = $2
        OR awarding_sub_agency_name = $2
        OR awarding_office_name     = $2
      )
      AND (
        $3::text[] IS NULL
        OR naics_code = ANY($3)
      )
  )
  SELECT
    b.piid,
    b.award_key,
    b.agency,
    b.naics,
    b.end_date,
    b.value,
    /* incumbent name + UEI with backfill from any non-null row for same PIID */
    COALESCE(
      b.recipient_name,
      (SELECT b2.recipient_name
         FROM ${table} b2
        WHERE b2.award_id_piid = b.piid
          AND b2.recipient_name IS NOT NULL
        ORDER BY COALESCE(b2.action_date, b2.pop_current_end_date) DESC NULLS LAST
        LIMIT 1)
    ) AS incumbent,
    COALESCE(
      b.recipient_uei,
      (SELECT b2.recipient_uei
         FROM ${table} b2
        WHERE b2.award_id_piid = b.piid
          AND b2.recipient_uei IS NOT NULL
        ORDER BY COALESCE(b2.action_date, b2.pop_current_end_date) DESC NULLS LAST
        LIMIT 1)
    ) AS incumbent_uei
  FROM base b
  ORDER BY b.end_date ASC
  LIMIT $4`;
        const params = [windowDays, agencyFilter || null, naicsList.length ? naicsList : null, limit];
        const { rows } = await queryPreferringFast(client, mkSQL, params);

        const res = new Response(JSON.stringify({ ok: true, rows }), {
          status: 200,
          headers: {
            ...headers,
            "Content-Type": "application/json",
            "Cache-Control": "public, s-maxage=300, stale-while-revalidate=86400",
          },
        });
        ctx.waitUntil(cache.put(cacheKey, res.clone()));
        return res;
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- vendor-awards (left pane list) -------------------- */
    if (last === "vendor-awards") {
      const uei = (url.searchParams.get("uei") || "").trim();
      const agency = (url.searchParams.get("agency") || "").trim();
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years") || "5", 10)));
      const limit = Math.max(1, Math.min(300, parseInt(url.searchParams.get("limit") || "100", 10)));
      if (!uei) {
        return new Response(JSON.stringify({ ok: false, error: "Missing uei" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }

      const mkSQL = (t) => `
        SELECT
          award_id_piid            AS piid,
          fiscal_year,
          awarding_agency_name     AS agency,
          awarding_sub_agency_name AS sub_agency,
          awarding_office_name     AS office,
          naics_code               AS naics,
          type_of_set_aside        AS set_aside,
          idv_type_of_award        AS vehicle,
          title,
          extent_competed,
          number_of_offers_received,
          total_dollars_obligated_num AS obligated
        FROM ${t}
        WHERE recipient_uei = $1
          AND (
            $2::text IS NULL
            OR awarding_agency_name      = $2
            OR awarding_sub_agency_name  = $2
            OR awarding_office_name      = $2
          )
          AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
        ORDER BY fiscal_year DESC, pop_current_end_date DESC NULLS LAST, piid DESC
        LIMIT $4`;

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);
        const { rows } = await queryPreferringFast(client, mkSQL, [uei, agency || null, years, limit]);
        return new Response(JSON.stringify({ ok: true, rows }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        });
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- contracts/insights (POST, cached 10m) -------------------- */
    if (request.method === "POST" && url.pathname.toLowerCase().endsWith("/contracts/insights")) {
      const bodyTxt = await request.clone().text();
      let piid = "";
      try { piid = (JSON.parse(bodyTxt).piid || "").trim().toUpperCase(); } catch {}
      if (!piid) {
        return new Response(JSON.stringify({ ok: false, error: "Missing piid" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }
      const cache = caches.default;
      const cacheKey = new Request(url.toString() + "::" + piid, request);
      const cached = await cache.match(cacheKey);
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=600, stale-while-revalidate=86400" }
        );
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);

        const mkSQL = (t) => `
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
          FROM ${t}
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC
          LIMIT 1`;

        const aRes = await queryPreferringFast(client, mkSQL, [piid]);
        if (!aRes.rows.length) {
          return new Response(JSON.stringify({ ok: false, error: "No award found for that PIID." }), {
            status: 404, headers: { ...headers, "Content-Type": "application/json" },
          });
        }
        const a = aRes.rows[0];
        const num = (x) => (typeof x === "number" ? x : x == null ? null : Number(x));
        const obligated = num(a.total_dollars_obligated_num) ?? 0;
        const current = num(a.current_total_value_of_award_num);
        const ceiling = num(a.potential_total_value_of_award_num) ?? current ?? 0;

        const start = a.pop_start_date ? new Date(a.pop_start_date) : null;
        const end = a.pop_potential_end_date
          ? new Date(a.pop_potential_end_date)
          : a.pop_current_end_date
          ? new Date(a.pop_current_end_date)
          : null;

        let burnPct = null, stage = "unknown", label = "Lifecycle insight limited", windowLabel = "Window unknown";
        if (ceiling && ceiling > 0) burnPct = Math.round((obligated / ceiling) * 100);
        if (start && end && end > start) {
          const now = Date.now();
          const t = end.getTime() - start.getTime();
          const e = Math.min(Math.max(now, start.getTime()), end.getTime()) - start.getTime();
          const pct = Math.round((e / t) * 100);
          if (now < start.getTime()) { stage = "not_started"; label = "Not started yet"; windowLabel = "Window not opened"; }
          else if (now > end.getTime()) { stage = "complete"; label = "Performance complete"; windowLabel = "Window passed"; }
          else if (pct < 25) { stage = "early"; label = "Early stage"; windowLabel = "In performance window"; }
          else if (pct < 75) { stage = "mid"; label = "Mid-stage"; windowLabel = "In performance window"; }
          else { stage = "late"; label = "Late / near end"; windowLabel = "In performance window"; }
        }

        // Subs (best-effort)
        let subs = { count: 0, distinctRecipients: 0, totalAmount: 0, top: [] };
        try {
          const s = await client.query(
            `SELECT subawardee_name, subawardee_uei, subaward_amount
             FROM public.usaspending_contract_subawards
             WHERE prime_award_piid = $1`,
            [piid]
          );
          const map = new Map();
          for (const r of s.rows || []) {
            const name = r.subawardee_name || "(Unnamed subrecipient)";
            const key = `${r.subawardee_uei || "NOUEI"}|${name}`;
            const prev = map.get(key) || { name, uei: r.subawardee_uei || null, amount: 0 };
            prev.amount += Number(r.subaward_amount || 0);
            map.set(key, prev);
          }
          const agg = Array.from(map.values()).sort((a, b) => (b.amount || 0) - (a.amount || 0));
          subs = {
            count: s.rowCount || 0,
            distinctRecipients: agg.length,
            totalAmount: agg.reduce((sum, x) => sum + (x.amount || 0), 0),
            top: agg.slice(0, 5),
          };
        } catch {}

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
          currentValue: current ?? (ceiling || null),
          ceiling,
          type_of_set_aside: a.type_of_set_aside || null,
          number_of_offers_received: a.number_of_offers_received || null,
          extent_competed: a.extent_competed || null,
          title: a.title || null,
        };
        const website = await fetchVendorWebsiteByUEI(primary.primeUei, env);
        if (website) primary.website = website;

        const lifecycle = {
          stage, label, windowLabel,
          timeElapsedPct: null, burnPct,
          primeVsSubsPct: null, largestSubPct: null,
        };

        const res = new Response(
          JSON.stringify({
            ok: true,
            primary,
            lifecycle,
            subs,
            disclaimer:
              "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete.",
          }),
          {
            status: 200,
            headers: {
              ...headers,
              "Content-Type": "application/json",
              "Cache-Control": "public, s-maxage=600, stale-while-revalidate=86400",
            },
          }
        );
        ctx.waitUntil(cache.put(cacheKey, res.clone()));
        return res;
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "insights failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- usa-contract (DB-backed, resilient) -------------------- */
    if (last === "usa-contract") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase();
      if (!piid) {
        return new Response(JSON.stringify({ ok: false, error: "Missing piid" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }

      // Edge cache (15m)
      const cache = caches.default;
      const cacheKey = new Request(url.toString(), request);
      const cached = await cache.match(cacheKey);
      if (cached) {
        return withCors(
          cached,
          { ...headers, "Cache-Control": "public, s-maxage=900, stale-while-revalidate=86400" }
        );
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);

        // Summary/meta
        const sumSQL = `
          SELECT
            piid,
            pop_start          AS pop_start,
            pop_current_end    AS pop_current_end,
            pop_potential_end  AS pop_potential_end,
            current_total_value_of_award_num      AS current_total_value_of_award,
            potential_total_value_of_award_num    AS potential_total_value_of_award
          FROM fp.contract_award_summary_v
          WHERE piid = $1
          LIMIT 1`;
        const sumRes = await client.query(sumSQL, [piid]);
        if (!sumRes.rows.length) {
          return new Response(
            JSON.stringify({ ok: false, error: `No award found for PIID ${piid}` }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } }
          );
        }
        const s = sumRes.rows[0];

        // Transactions/mods (+ comments)
        const txSQL = `
          SELECT action_date, obligation, modification_number, action_type, transaction_description
          FROM fp.contract_txn_min_v1
          WHERE piid = $1
          ORDER BY action_date ASC
          LIMIT 5000`;
        const txRes = await client.query(txSQL, [piid]);

        const spendPoints = (txRes.rows || []).map((r) => ({
          date: r.action_date ? String(r.action_date) : null,
          obligation: Number(r.obligation || 0),
          mod: r.modification_number || "",
          type: r.action_type || "",
          description: r.transaction_description || "",
        }));

        const payload = {
          ok: true,
          piid,
          award_id: null, // not available from DB views; keep null for shape compatibility
          meta: {
            pop_start: s.pop_start || null,
            pop_current_end: s.pop_current_end || null,
            pop_potential_end: s.pop_potential_end || null,
            current_total_value_of_award: s.current_total_value_of_award ?? null,
            potential_total_value_of_award: s.potential_total_value_of_award ?? null,
          },
          spendPoints,
        };

        const res = new Response(JSON.stringify(payload), {
          status: 200,
          headers: {
            ...headers,
            "Content-Type": "application/json",
            "Cache-Control": "public, s-maxage=900, stale-while-revalidate=86400",
          },
        });
        ctx.waitUntil(cache.put(cacheKey, res.clone()));
        return res;
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "usa-contract failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- contract-summary (alias -> usa-contract.meta) -------------------- */
    if (last === "contract-summary") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase();
      if (!piid) {
        return new Response(JSON.stringify({ ok: false, error: "Missing piid" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }
      const r = await fetch(`${url.origin}/sb/usa-contract?piid=${encodeURIComponent(piid)}`);
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j || j.ok !== true) {
        return new Response(
          JSON.stringify({ ok: false, error: (j && j.error) || "lookup failed" }),
          { status: r.status || 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }
      const m = j.meta || {};
      return new Response(
        JSON.stringify({
          ok: true,
          award_id: j.award_id || null,
          pop_start: m.pop_start || null,
          pop_current_end: m.pop_current_end || null,
          pop_potential_end: m.pop_potential_end || null,
          current_total_value_of_award: m.current_total_value_of_award ?? null,
          potential_total_value_of_award: m.potential_total_value_of_award ?? null,
        }),
        { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
      );
    }

    /* -------------------- contracts/activity (alias -> usa-contract.spendPoints) -------------------- */
    if (segments.length >= 2 && segments[segments.length - 2] === "contracts" && last === "activity") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase();
      if (!piid) {
        return new Response(JSON.stringify({ ok: false, error: "Missing piid" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }
      const r = await fetch(`${url.origin}/sb/usa-contract?piid=${encodeURIComponent(piid)}`);
      const j = await r.json().catch(() => ({}));
      if (!r.ok || !j || j.ok !== true) {
        return new Response(
          JSON.stringify({ ok: false, error: (j && j.error) || "activity failed" }),
          { status: r.status || 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }
      const points = (j.spendPoints || []).map((p) => ({
        date: p.date,
        federal_action_obligation: p.obligation,
        modification_number: p.mod,
        action_type: p.type || null,
        transaction_description: p.description || null,
      }));
      return new Response(JSON.stringify({ ok: true, points, results: points }), {
        status: 200, headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    /* -------------------- my-entity (SBA view first; SAM fallback; normalized tags) -------------------- */
    if (last === "my-entity") {
      const uei = (url.searchParams.get("uei") || "").trim().toUpperCase();
      if (!uei) {
        return new Response(JSON.stringify({ ok: false, error: "Missing uei parameter." }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);

        // Pull award-derived name + NAICS (fast view preferred)
        const mkName = (t) => `SELECT recipient_name FROM ${t}
                                WHERE recipient_uei = $1
                                ORDER BY total_dollars_obligated_num DESC NULLS LAST
                                LIMIT 1`;
        const mkNaics = (t) => `SELECT DISTINCT naics_code FROM ${t}
                                WHERE recipient_uei = $1 AND naics_code IS NOT NULL
                                LIMIT 200`;
        const nameRes  = await queryPreferringFast(client, mkName,  [uei]);
        const naicsRes = await queryPreferringFast(client, mkNaics, [uei]);

        let name  = (nameRes.rows[0] && nameRes.rows[0].recipient_name) || null;
        let naics = (naicsRes.rows || []).map((r) => r.naics_code).filter(Boolean);

        // SBA view (preferred for socio-econ + website + richer NAICS if present)
        let sbaRow = null;
        try {
          const sba = await client.query(
            `SELECT uei, business_name, website,
                    capabilities_narrative, capabilities_statement_link,
                    smallbiz_categories, naics_codes, active_sba_certifications_raw
               FROM sba.smallbiz_v
              WHERE upper(uei) = upper($1)
              LIMIT 1`,
            [uei]
          );
          sbaRow = sba.rows[0] || null;
        } catch {
          /* ok if SBA schema not present */
        }

        let smallBizCategories = [];
        let website = null;

        if (sbaRow) {
          name = sbaRow.business_name || name;
          website = sbaRow.website || null;

          // Prefer parsed NAICS from SBA
          if (Array.isArray(sbaRow.naics_codes) && sbaRow.naics_codes.length) {
            naics = sbaRow.naics_codes.filter(Boolean);
          }

          // Normalize socio tags
          const rawCats = Array.isArray(sbaRow.smallbiz_categories)
            ? sbaRow.smallbiz_categories
            : sbaRow.active_sba_certifications_raw;
          smallBizCategories = normalizeSocioTags(rawCats);
        }

        // If still missing tags, hit SAM proxy (if configured) and normalize
        if (!smallBizCategories.length && env.SAM_PROXY_URL) {
          try {
            const p = `${env.SAM_PROXY_URL.replace(/\/+$/, "")}/entity?uei=${encodeURIComponent(uei)}`;
            const j = await fetchJSON(p);
            const cats =
              (Array.isArray(j?.categories) && j.categories) ||
              j?.entity?.socioEconomicCategories ||
              [];
            smallBizCategories = normalizeSocioTags(cats);
          } catch {}
        }

        // If no website yet, try SAM direct (API key)
        if (!website) {
          website = await fetchVendorWebsiteByUEI(uei, env);
        }

        return new Response(
          JSON.stringify({
            ok: true,
            entity: { uei, name, naics, smallBizCategories, website: website || null },
          }),
          {
            status: 200,
            headers: {
              ...headers,
              "Content-Type": "application/json",
              "Cache-Control": "public, s-maxage=86400",
            },
          }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- bid-nobid (prefers actual set-aside; compares to SBA tags) -------------------- */
    if (last === "bid-nobid") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase();
      const uei  = (url.searchParams.get("uei")  || "").trim().toUpperCase();
      const years = Math.max(1, Math.min(10, parseInt(url.searchParams.get("years") || "5", 10)));
      if (!piid || !uei) {
        return new Response(
          JSON.stringify({ ok: false, error: "Missing piid or uei" }),
          { status: 400, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);

        const mkAward = (t) => `
          SELECT award_id_piid, naics_code, naics_description,
                 awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                 recipient_uei, recipient_name,
                 total_dollars_obligated_num AS obligated,
                 potential_total_value_of_award_num AS ceiling,
                 pop_current_end_date,
                 type_of_set_aside,
                 number_of_offers_received
          FROM ${t}
          WHERE award_id_piid = $1
          ORDER BY pop_current_end_date DESC NULLS LAST
          LIMIT 1`;

        const award = await queryPreferringFast(client, mkAward, [piid]);
        if (!award.rows.length) {
          return new Response(
            JSON.stringify({ ok: false, error: "PIID not found" }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } }
          );
        }
        const A = award.rows[0];
        const orgName = A.awarding_sub_agency_name || A.awarding_office_name || A.awarding_agency_name;
        const naics = A.naics_code;
        const offersRaw = (A.number_of_offers_received == null ? "" : String(A.number_of_offers_received)).trim();
        const offersNum = offersRaw && !Number.isNaN(Number(offersRaw)) ? Number(offersRaw) : null;
        const actualSA  = (A.type_of_set_aside || "").toString().toUpperCase();

        // Pull your SBA-enriched entity (includes smallBizCategories + naics)
        const myEnt = await fetchJSON(`${url.origin}/sb/my-entity?uei=${encodeURIComponent(uei)}`).catch(() => ({}));
        const myNAICS = (myEnt && myEnt.entity && myEnt.entity.naics) || [];
        const mySocio = (myEnt && myEnt.entity && myEnt.entity.smallBizCategories) || [];

        const mkMyAwards = (t) => `
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${t}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)`;
        const myAwards = await queryPreferringFast(client, mkMyAwards, [uei, orgName, years]);

        const mkInc = (t) => `
          SELECT COUNT(*)::int AS cnt, COALESCE(SUM(total_dollars_obligated_num),0)::float8 AS obligated
          FROM ${t}
          WHERE recipient_uei = $1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)`;
        const inc = await queryPreferringFast(client, mkInc, [A.recipient_uei, orgName, years]);

        const mkDist = (t) => `
          WITH base AS (
            SELECT total_dollars_obligated_num AS obligated
            FROM ${t}
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
          FROM base`;
        const dist = await queryPreferringFast(client, mkDist, [naics, orgName, years]);
        const P = (dist && dist.rows && dist.rows[0]) || { p25: null, p50: null, p75: null };

        const toNum = (x) => (typeof x === "number" ? x : x == null ? 0 : Number(x) || 0);
        const meCnt = toNum(myAwards.rows[0] && myAwards.rows[0].cnt);
        const meObl = toNum(myAwards.rows[0] && myAwards.rows[0].obligated);
        const incCnt = toNum(inc.rows[0] && inc.rows[0].cnt);
        const today = new Date();
        const dEnd = A.pop_current_end_date ? new Date(A.pop_current_end_date) : null;
        const daysToEnd = dEnd ? Math.round((dEnd.getTime() - today.getTime()) / 86400000) : null;
        const burn = A.ceiling && A.ceiling > 0 ? Math.round((toNum(A.obligated) / toNum(A.ceiling)) * 100) : null;

        // Scoring
        const tech = myNAICS.includes(naics)
          ? 5
          : myNAICS.some((c) => (c || "").slice(0, 3) === String(naics).slice(0, 3))
          ? 4
          : 2;
        const pp = meCnt >= 3 || meObl >= 2000000 ? 5 : meCnt >= 1 ? 3 : 1;

        let staffing = 3;
        if (P.p50) {
          const contractSize = toNum(A.obligated || A.ceiling || 0);
          staffing = contractSize <= P.p50 ? 5 : contractSize <= (P.p75 || P.p50) ? 4 : 2;
        }
        let sched = 3;
        if (daysToEnd != null && burn != null) {
          if (daysToEnd > 180 && burn < 70) sched = 5;
          else if (daysToEnd < 60 || burn > 90) sched = 2;
          else sched = 3;
        }

        const haveMatch = (tag) => (mySocio || []).some((s) => String(s).toUpperCase().includes(tag));
        // Prefer the award's actual set-aside; if blank, fall back to NAICS@org sample
        const mkSA = (t) => `
          SELECT MAX(type_of_set_aside) AS example_set_aside
          FROM ${t}
          WHERE naics_code=$1
            AND (
              $2::text IS NULL OR awarding_agency_name=$2
              OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
            )
            AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)`;
        const histSA = await queryPreferringFast(client, mkSA, [naics, orgName, years]);
        const histExample = ((histSA.rows[0] && histSA.rows[0].example_set_aside) || "").toUpperCase();
        const saUsed = actualSA || histExample;

        let compliance = 3;
        if (saUsed) {
          if (saUsed.includes("8(A)"))                     compliance = haveMatch("8(A)")    ? 5 : 2;
          else if (saUsed.includes("SDVOSB") || saUsed.includes("SERVICE-DISABLED"))
                                                           compliance = haveMatch("SDVOSB") ? 5 : 2;
          else if (saUsed.includes("WOSB") || saUsed.includes("WOMEN"))
                                                           compliance = haveMatch("WOSB")   ? 5 : 2;
          else if (saUsed.includes("HUBZONE"))             compliance = haveMatch("HUBZONE")? 5 : 2;
          else if (saUsed.includes("SMALL"))               compliance = 4; // general SB set-aside
          else                                             compliance = 3;
        }

        // Price proxy
        let price = 3;
        if (P.p25 && P.p50 && P.p75) {
          const val = toNum(A.obligated || A.ceiling || 0);
          if (val <= P.p25) price = 4;
          else if (val <= P.p50) price = 5;
          else if (val <= P.p75) price = 3;
          else price = 2;
        }

        // Competitive intel: include incumbent awards and offers (if present)
        let intel = incCnt === 0 ? 5 : incCnt <= 2 ? 4 : incCnt <= 5 ? 3 : 2;
        if (offersNum != null) {
          if (offersNum >= 10) intel = Math.min(intel, 2);
          else if (offersNum >= 5) intel = Math.min(intel, 3);
        }

        const W = { tech: 24, pp: 20, staff: 12, sched: 8, compliance: 12, price: 8, intimacy: 8, intel: 8 };
        const intimacy = meCnt >= 3 ? 5 : meCnt === 2 ? 4 : meCnt === 1 ? 3 : 1;

        const weighted =
          Math.round(
            (W.tech * (tech / 5) +
              W.pp * (pp / 5) +
              W.staff * (staffing / 5) +
              W.sched * (sched / 5) +
              W.compliance * (compliance / 5) +
              W.price * (price / 5) +
              W.intimacy * (intimacy / 5) +
              W.intel * (intel / 5)) *
              10
          ) / 10;
        const decision = weighted >= 80 ? "bid" : weighted >= 65 ? "conditional" : "no_bid";

        return new Response(
          JSON.stringify({
            ok: true,
            inputs: { piid, uei, org: orgName, naics, set_aside: saUsed }, // for memo
            criteria: [
              { name: "Technical Fit", weight: W.tech, score: tech, reason: myNAICS.includes(naics) ? "Exact NAICS match" : myNAICS.some((c) => (c || "").slice(0, 3) === String(naics).slice(0, 3)) ? "Related NAICS family" : "No NAICS match" },
              { name: "Relevant Experience / Past Performance", weight: W.pp, score: pp, reason: `Your awards at this org: ${meCnt}; $${Math.round(meObl).toLocaleString()}` },
              { name: "Staffing & Key Personnel", weight: W.staff, score: staffing, reason: "Proxy via local award size distribution" },
              { name: "Schedule / ATO Timeline Risk", weight: W.sched, score: sched, reason: `Days to end: ${daysToEnd ?? "unknown"}; burn: ${burn ?? "unknown"}%` },
              { name: "Compliance (Set-Aside Eligibility)", weight: W.compliance, score: compliance, reason: saUsed ? `Set-aside: ${saUsed}; your tags: ${(mySocio || []).join(", ") || "none"}` : "Set-aside unknown" },
              { name: "Price Competitiveness", weight: W.price, score: price, reason: "Position vs NAICS@org percentiles" },
              { name: "Customer Intimacy", weight: W.intimacy, score: intimacy, reason: `Your awards at this org: ${meCnt}` },
              { name: "Competitive Intelligence", weight: W.intel, score: intel, reason: `Incumbent awards: ${incCnt}${offersNum!=null ? `; offers: ${offersNum}` : ""}` },
            ],
            weighted_percent: weighted,
            decision,
          }),
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "bid-nobid failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- bid-nobid-memo -------------------- */
    if (last === "bid-nobid-memo") {
      const piid = (url.searchParams.get("piid") || "").trim().toUpperCase();
      const uei = (url.searchParams.get("uei") || "").trim().toUpperCase();
      const years = (url.searchParams.get("years") || "5").trim();

      if (!piid || !uei) {
        return new Response(JSON.stringify({ ok: false, error: "Missing piid or uei" }), {
          status: 400, headers: { ...headers, "Content-Type": "application/json" },
        });
      }

      const scorer = new URL(url.toString());
      scorer.pathname = "/sb/bid-nobid";
      scorer.search = `?piid=${encodeURIComponent(piid)}&uei=${encodeURIComponent(uei)}&years=${encodeURIComponent(years)}`;
      const j = await fetchJSON(scorer.toString()).catch((e) => ({ ok: false, error: e && e.message }));

      if (!j || j.ok !== true) {
        return new Response(
          JSON.stringify({ ok: false, error: (j && j.error) || "bid-nobid failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      }

      if (!env.OPENAI_API_KEY) {
        return new Response(JSON.stringify({ ok: false, error: "OPENAI_API_KEY not set" }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        });
      }

      const inputs = j.inputs || {};
      const orgName = (inputs.org || "—").toString();
      const naics = (inputs.naics || "—").toString();
      const setAsideRaw = (inputs.set_aside || inputs.setAside || inputs.setAsideType || "NONE").toString().toUpperCase();
      const decision = (j.decision || "—").toString();
      const pct = String(j.weighted_percent ?? "—");

      const NON_MENTION_SETASIDES = new Set([
        "NONE", "N/A", "FULL AND OPEN", "FULL & OPEN",
        "FULL AND OPEN COMPETITION", "UNRESTRICTED",
      ]);

      const lines = (j.criteria || [])
        .map((c) => `${c.name} | weight ${c.weight}% | score ${c.score}/5 | ${c.reason || ""}`)
        .join("\n");

      const prompt = `
You are the Bid/No-Bid Decision GPT for federal capture. Follow the rules exactly.

GOAL
Return:
1) A decision line: BID / CONDITIONAL / NO-BID with percent.
2) A 5–7 line executive memo that references ONLY the provided facts and scores.

FACTS
- PIID: ${inputs.piid || piid}
- Org: ${orgName}
- NAICS: ${naics}
- Set-aside: ${setAsideRaw}
- Model Decision: ${decision} (${pct}%)

SCORED MATRIX (source of truth for reasoning)
${lines}

HARD RULES (must follow)
- Do NOT invent details. If a fact is not present above, do not mention it.
- Mention set-aside ONLY if it is explicitly a socio-economic restriction (e.g., 8(a), WOSB/EWOSB, SDVOSB, HUBZone, Small Business). 
- If set-aside is one of: ${Array.from(NON_MENTION_SETASIDES).join(", ")} then do NOT mention any set-aside or socio-economic eligibility at all.
- Never state Women-Owned, 8(a), HUBZone, SDVOSB, or Small Business unless set-aside explicitly includes it.
- Reference numeric items (scores or amounts) that appear in the matrix or inputs (e.g., NAICS match, awards $, burn %, days to end, price percentile, incumbent strength).
- Keep the memo concise (5–7 lines), specific, and free of fluff.

OUTPUT FORMAT
Decision: <BID / CONDITIONAL / NO-BID> (<percent>%)

Executive Memo:
<5–7 lines using only the facts and matrix>
      `.trim();

      const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${env.OPENAI_API_KEY}` },
        body: JSON.stringify({
          model: "gpt-4.1-mini",
          messages: [
            { role: "system", content: "You are a federal capture strategist. Obey all hard rules and never invent facts." },
            { role: "user", content: prompt },
          ],
          temperature: 0.2,
          max_tokens: 900,
        }),
      });

      const txt = await aiRes.text();
      let memo = "";
      try { memo = JSON.parse(txt).choices[0].message.content.trim() || ""; }
      catch { memo = txt.slice(0, 3000); }

      return new Response(JSON.stringify({ ok: true, ...j, memo }), {
        status: 200, headers: { ...headers, "Content-Type": "application/json" },
      });
    }

    /* -------------------- teaming-suggestions -------------------- */
    if (last === "teaming-suggestions") {
      let qp = {};
      if (request.method === "POST") {
        try { qp = await request.json(); } catch { qp = {}; }
      } else {
        const params = new URL(request.url).searchParams;
        qp = Object.fromEntries(params.entries());
      }
      const piid = (qp.piid || "").toString().trim().toUpperCase();
      const naics = (qp.naics || "").toString().trim();
      const org = (qp.org || "").toString().trim();
      const years = Math.max(1, Math.min(10, parseInt(qp.years || "3", 10)));
      const limit = Math.max(3, Math.min(10, parseInt(qp.limit || "5", 10)));
      const exclude = String(qp.exclude_ueis || "")
        .split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);

      if (piid) {
        const client0 = makeClient(env);
        try {
          await client0.connect();
          await client0.query(`SET statement_timeout = '20s'`);
          const mkInc = (t) => `SELECT recipient_uei FROM ${t} WHERE award_id_piid=$1 LIMIT 1`;
          const r0 = await queryPreferringFast(client0, mkInc, [piid]);
          const incUEI = r0.rows[0] && r0.rows[0].recipient_uei && r0.rows[0].recipient_uei.toUpperCase();
          if (incUEI) exclude.push(incUEI);
        } catch {} finally {
          try { await client0.end(); } catch {}
        }
      }

      const client = makeClient(env);
      try {
        await client.connect();
        await client.query(`SET statement_timeout = '20s'`);

        const mkSQL = (t) => `
          WITH base AS (
            SELECT
              recipient_uei  AS uei,
              recipient_name AS name,
              COUNT(*)       AS awards,
              COALESCE(SUM(total_dollars_obligated_num),0) AS obligated
            FROM ${t}
            WHERE ($1::text IS NULL OR naics_code = $1)
              AND (
                $2::text IS NULL OR awarding_agency_name=$2
                OR awarding_sub_agency_name=$2 OR awarding_office_name=$2
              )
              AND fiscal_year >= EXTRACT(YEAR FROM CURRENT_DATE)::int - ($3::int - 1)
              AND recipient_uei IS NOT NULL
            GROUP BY recipient_uei, recipient_name
          )
          SELECT * FROM base
          WHERE NOT (upper(uei) = ANY($4))
          ORDER BY obligated ASC NULLS LAST, awards DESC
          LIMIT $5`;

        const params = [naics || null, org || null, years, exclude.length ? exclude : ["__NONE__"], limit * 2];
        const base = await queryPreferringFast(client, mkSQL, params);

        const out = [];
        for (const b of base.rows) {
          const mkA = (t) => `
            SELECT award_id_piid AS piid, fiscal_year, naics_code AS naics,
                   title, total_dollars_obligated_num AS obligated
            FROM ${t}
            WHERE recipient_uei = $1
              AND ($2::text IS NULL OR naics_code=$2)
              AND (
                $3::text IS NULL OR awarding_agency_name=$3
                OR awarding_sub_agency_name=$3 OR awarding_office_name=$3
              )
            ORDER BY fiscal_year DESC, pop_current_end_date DESC NULLS LAST
            LIMIT 3`;
          const a = await queryPreferringFast(client, mkA, [b.uei, naics || null, org || null]);
          const website = await fetchVendorWebsiteByUEI(b.uei, env);
          out.push({
            uei: b.uei,
            name: b.name,
            set_aside: null,
            obligated: Number(b.obligated || 0),
            recent_awards: (a.rows || []).map((r) => ({
              piid: r.piid,
              fiscal_year: Number(r.fiscal_year),
              naics: r.naics,
              title: r.title,
              obligated: Number(r.obligated || 0),
            })),
            website,
            contact: null,
          });
          if (out.length >= limit) break;
        }

        return new Response(JSON.stringify({ ok: true, rows: out }), {
          status: 200, headers: { ...headers, "Content-Type": "application/json" },
        });
      } catch (e) {
        return new Response(
          JSON.stringify({ ok: false, error: (e && e.message) || "teaming failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* -------------------- SAM.gov Opportunities proxy -------------------- */
    const SAM_ALLOWED_TYPES = new Set([
      "Solicitation",
      "Combined Synopsis/Solicitation",
      "Presolicitation",
      "Sources Sought",
      "Award Notice",
      "Special Notice",
    ]);
    function coerceTypes(input) {
      let types = Array.isArray(input) ? input : String(input || "").split(",");
      types = types.map((s) => String(s || "").trim()).filter(Boolean).filter((t) => SAM_ALLOWED_TYPES.has(t));
      if (types.length === 0) types = ["Solicitation"];
      return types;
    }
    function coerceNaics(input) {
      let arr = Array.isArray(input) ? input : String(input || "").split(/[\,\s]+/);
      return Array.from(
        new Set(arr.map((s) => String(s).replace(/\D+/g, "")).filter((s) => s.length >= 2 && s.length <= 6))
      );
    }
    function fmtMDY(d) {
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const yyyy = d.getFullYear();
      return `${mm}/${dd}/${yyyy}`;
    }
    function isMDY(s) { return /^\d{2}\/\d{2}\/\d{4}$/.test(String(s || "")); }
    function buildSamURL(env, body) {
      const u = new URL("https://api.sam.gov/prod/opportunities/v2/search");
      if (!env.SAM_API_KEY) throw new Error("SAM_API_KEY is not configured");
      u.searchParams.set("api_key", env.SAM_API_KEY);

      const days = Math.max(1, Math.min(365, parseInt(body.windowDays || "15", 10)));
      const now = new Date();
      const from = new Date(now.getTime() - days * 86400000);

      const postedFrom = isMDY(body.postedFrom) ? body.postedFrom : isMDY(body.posted_from) ? body.posted_from : fmtMDY(from);
      const postedTo   = isMDY(body.postedTo)   ? body.postedTo   : isMDY(body.posted_to)   ? body.posted_to   : fmtMDY(now);

      u.searchParams.set("postedFrom", postedFrom);
      u.searchParams.set("postedTo", postedTo);

      for (const t of coerceTypes(body.noticeTypes || body.type || body.noticeType)) {
        u.searchParams.append("noticeType", t);
      }
      for (const code of coerceNaics(body.naics || body.naicsCode || body.naicsCodes)) {
        u.searchParams.append("naics", code);
      }

      const q   = body.q || body.keyword || body.keywords || body.search || body.searchText || body.searchTerm || "";
      const org = body.agency_contains || body.agency || body.agencyName || body.organization || body.org || body.fullParentPathName || "";
      const mergedKeyword = [q, org].map((s) => String(s || "").trim()).filter(Boolean).join(" ");
      if (mergedKeyword) u.searchParams.set("keyword", mergedKeyword);

      const setAsideRaw = body.setAside || body.setAsideCodes || body.typeOfSetAside || body.type_of_set_aside;
      if (setAsideRaw) {
        const val = Array.isArray(setAsideRaw) ? setAsideRaw[0] : setAsideRaw;
        const code = String(val || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
        if (code) u.searchParams.set("setAsideCode", code);
      }

      const limit  = Math.max(1, Math.min(100, parseInt(body.limit  || "25", 10)));
      const offset = Math.max(0, parseInt(body.offset || "0", 10));
      u.searchParams.set("limit", String(limit));
      u.searchParams.set("offset", String(offset));
      u.searchParams.set("sort", "modifiedDate");
      u.searchParams.set("order", "desc");

      return u;
    }

    if (segments[0] === "opportunities" && last === "search") {
      try {
        let body = {};
        if (request.method === "POST") {
          const txt = await request.text();
          try { body = JSON.parse(txt); } catch { body = {}; }
        } else {
          body = Object.fromEntries(new URL(request.url).searchParams.entries());
        }

        const samURL = buildSamURL(env, body);
        const r = await fetch(samURL.toString(), {
          cf: { cacheTtl: 900, cacheEverything: true },
          headers: { Accept: "application/json" },
        });
        const rawText = await r.text();

        const passHeaders = {
          ...headers,
          "Content-Type": "application/json",
          "Cache-Control": "public, s-maxage=900, stale-while-revalidate=86400",
          "x-sam-url": samURL.toString(),
        };

        if (r.status >= 400 && r.status < 500) {
          return new Response(
            JSON.stringify({
              ok: false,
              status: r.status,
              hint:
                r.status === 400
                  ? "SAM rejected the parameters. Check date format (MM/dd/yyyy), notice types, and window <= 365 days."
                  : r.status === 401 || r.status === 403
                  ? "Check SAM_API_KEY on the Worker."
                  : "Upstream client error.",
              upstream: rawText,
            }),
            { status: 400, headers: passHeaders }
          );
        }

        let normalized;
        try {
          const j = rawText ? JSON.parse(rawText) : {};
          const dataAny = j.opportunitiesData || j.searchResults || j.data || j.results || [];
          const opportunitiesData = Array.isArray(dataAny)
            ? dataAny
            : Array.isArray(dataAny.opportunitiesData)
            ? dataAny.opportunitiesData
            : [];
          const totalRecords = j.totalRecords || j.total || (Array.isArray(opportunitiesData) ? opportunitiesData.length : 0);
          const limit = Number(j.limit || body.limit || 25) || 25;
          const offset = Number(j.offset || body.offset || 0) || 0;
          normalized = { totalRecords, limit, offset, opportunitiesData };
        } catch {
          normalized = { totalRecords: 0, limit: 25, offset: 0, opportunitiesData: [] };
        }

        return new Response(JSON.stringify(normalized), { status: 200, headers: passHeaders });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: String((e && e.message) || e) }), {
          status: 500, headers: { ...headers, "Content-Type": "application/json" },
        });
      }
    }

    // default 404
    return new Response("Not found", { status: 404, headers });
  },
};
