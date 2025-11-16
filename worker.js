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

    // Preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers });
    }

    // ---------- Normalize path by segments ----------
    // Works for:
    //   /sb/expiring-contracts
    //   /prod/sb/expiring-contracts
    //   /sb/contracts/insights
    //   /sb/agencies
    const segments = url.pathname.split("/").filter(Boolean);
    const last = segments[segments.length - 1] || "";
    const secondLast = segments.length > 1 ? segments[segments.length - 2] : "";

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
     *    Returns top-level + sub-agencies + offices (distinct names)
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
          JSON.stringify({ ok: true, rows }), // [{ name: "Department of Defense" }, ...]
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } catch (e) {
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "query failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    /* =============================================================
     * 1) SB Agency Share
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
     * 3) Contract insights (AI + subs + snapshot)
     *    POST /sb/contracts/insights  { piid: "HC102825F0042" }
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

        // 3a. Prime contract snapshot
        const primeSql = `
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
        const { rows: primeRows } = await client.query(primeSql, [piid]);

        if (!primeRows.length) {
          return new Response(
            JSON.stringify({ ok: false, error: "No award found for that PIID." }),
            { status: 404, headers: { ...headers, "Content-Type": "application/json" } }
          );
        }

        const a = primeRows[0];

        const snapshot = {
          obligated: Number(a.total_dollars_obligated_num ?? 0),
          currentValue: Number(a.current_total_value_of_award_num ?? 0),
          ceiling: Number(a.potential_total_value_of_award_num ?? 0),
          popStart: a.pop_start_date,
          popCurrentEnd: a.pop_current_end_date,
          popPotentialEnd: a.pop_potential_end_date,
        };

        const primeAwardKey = a.award_key;
        const primePiid = a.award_id_piid;

        // 3b. Subcontract footprint for this prime
        // We aggregate per subrecipient so we can show top subs.
        const subsSql = `
          SELECT
            COALESCE(subawardee_name, '') AS subawardee_name,
            COALESCE(subawardee_uei, '')  AS subawardee_uei,
            SUM(subaward_amount)          AS total_amount,
            COUNT(*)                      AS action_count
          FROM public.usaspending_contract_subawards
          WHERE
            (prime_award_piid = $1 OR prime_award_unique_key = $2)
            AND subaward_amount IS NOT NULL
          GROUP BY
            COALESCE(subawardee_name, ''),
            COALESCE(subawardee_uei, '')
        `;
        const { rows: subsRows } = await client.query(subsSql, [
          primePiid,
          primeAwardKey,
        ]);

        let subs = null;

        if (subsRows.length) {
          let totalAmount = 0;
          let totalActions = 0;
          const recipients = new Set();
          const top = [];

          for (const r of subsRows) {
            const name = r.subawardee_name || "Unknown";
            const uei = r.subawardee_uei || null;
            const amount =
              typeof r.total_amount === "number"
                ? r.total_amount
                : Number(r.total_amount || 0);
            const actions =
              typeof r.action_count === "number"
                ? r.action_count
                : Number(r.action_count || 0);

            totalAmount += amount;
            totalActions += actions;
            recipients.add(uei || name);

            top.push({ name, uei, amount });
          }

          top.sort((a, b) => (b.amount || 0) - (a.amount || 0));

          subs = {
            count: totalActions,
            distinctRecipients: recipients.size,
            totalAmount,
            top: top.slice(0, 10),
          };
        } else {
          subs = {
            count: 0,
            distinctRecipients: 0,
            totalAmount: 0,
            top: [],
          };
        }

        const disclaimer =
          "Subcontractor data is sourced from USAspending. Primes are not required to report every subcontract, so this list may be incomplete.";

        // 3c. Build AI prompt (use snapshot + subs summary)
        const subSummaryLine =
          subs && subs.count > 0
            ? `There are ${subs.count} reported subaward actions to ${subs.distinctRecipients} unique recipients, totaling about $${subs.totalAmount.toFixed(
                0
              )}.`
            : "No subcontract awards are publicly reported for this PIID, or reporting is incomplete.";

        const prompt = `
You are helping a small federal contractor quickly understand a single contract and the subcontracting landscape.

Prime contract snapshot:
- PIID: ${a.award_id_piid}
- Awarding agency: ${a.awarding_agency_name || "—"}
- Sub-agency / office: ${a.awarding_sub_agency_name || "—"} / ${a.awarding_office_name || "—"}
- Recipient (prime): ${a.recipient_name || "—"}
- NAICS: ${a.naics_code || "—"} – ${a.naics_description || "—"}
- Period of performance: ${a.pop_start_date || "—"} to ${a.pop_current_end_date || "—"} (potential: ${a.pop_potential_end_date || "—"})
- Total obligated: ${a.total_dollars_obligated_num || 0}
- Current value (base + exercised): ${a.current_total_value_of_award_num || 0}
- Ceiling (base + all options): ${a.potential_total_value_of_award_num || 0}

Subcontract footprint:
- ${subSummaryLine}

In 4–6 bullet points, explain in plain language:
1) Where this contract is in its lifecycle (early / mid / near end / completed) based on the dates.
2) How heavily used it appears (obligation vs ceiling).
3) What the subcontracting picture suggests (lots of subs, concentrated in one sub, or none reported).
4) What a small business should do next if they want to compete on the recompete OR team as a sub, being specific about timing and next steps.

Keep it under 200 words, concise, and non-technical.
        `.trim();

        // 3d. Call OpenAI
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
                  "You are a federal contracts analyst helping small businesses understand expiring contracts and subcontracting opportunities.",
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
            "AI response was not valid JSON: " + aiText.slice(0, 160)
          );
        }

        const summary =
          aiJson.choices?.[0]?.message?.content?.trim() ||
          "AI produced no summary.";

        return new Response(
          JSON.stringify({
            ok: true,
            summary,
            snapshot,
            subs,
            disclaimer,
          }),
          { status: 200, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } catch (e) {
        try { await client.end(); } catch {}
        return new Response(
          JSON.stringify({ ok: false, error: e?.message || "AI insight failed" }),
          { status: 500, headers: { ...headers, "Content-Type": "application/json" } }
        );
      } finally {
        try { await client.end(); } catch {}
      }
    }

    // ---------- Fallback ----------
    return new Response("Not found", { status: 404, headers });
  },
};
