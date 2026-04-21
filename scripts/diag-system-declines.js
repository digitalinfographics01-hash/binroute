/**
 * Diagnose system declines vs real declines.
 *
 * Strategy:
 * 1. Find pairs where:
 *    - A declined order on a Main campaign (e.g., 130, 98, 284)
 *    - Followed within 5 minutes by an attempt on the corresponding PP campaign
 *    - Same contact_id, same product_id
 * 2. For each such pair, dump the Main decline's decline_reason / decline_reason_details / decline_category / prepaid flag
 * 3. Aggregate to find the dominant "system routing" decline reasons
 * 4. Compare to overall decline reason distribution to spot the system patterns
 */
const { initDb, querySql } = require('../src/db/connection');

// Main → PP pairings we care about (from earlier analysis)
const MAIN_PP_PAIRS = [
  { main: 130, pp: 132 },  // Viraflexx ME OAS
  { main: 98,  pp: 100 },  // Viraflexx ME JB
  { main: 284, pp: 286 },  // Eternal Lumi Serum
  { main: 221, pp: 223 },  // Eternal Lumi Serum (other)
  { main: 297, pp: 298 },  // E-XceL ME
  { main: 102, pp: 104 },  // Viraflexx PreW JB
  { main: 134, pp: 136 },  // Viraflexx PreW OAS
];

(async () => {
  await initDb();

  console.log('='.repeat(80));
  console.log('PART 1: Decline reasons on Main declines that were followed by PP attempts');
  console.log('='.repeat(80));

  const allRoutingDeclines = [];
  const reasonCounts = {};
  const detailsCounts = {};
  const categoryCounts = {};

  for (const pair of MAIN_PP_PAIRS) {
    // Find Main declines whose contact had a PP attempt for same product within 5 minutes
    const pairs = querySql(`
      SELECT
        m.order_id as main_order_id,
        m.decline_reason as main_reason,
        m.decline_reason_details as main_details,
        m.decline_category as main_category,
        m.prepaid as main_prepaid,
        m.acquisition_date as main_date,
        p.order_id as pp_order_id,
        p.acquisition_date as pp_date,
        m.contact_id,
        JSON_EXTRACT(m.product_ids, '$[0]') as product_id,
        ROUND((julianday(p.acquisition_date) - julianday(m.acquisition_date)) * 86400) as seconds_diff
      FROM orders m
      JOIN orders p
        ON p.client_id = m.client_id
        AND p.contact_id = m.contact_id
        AND p.contact_id IS NOT NULL AND p.contact_id != 0
        AND JSON_EXTRACT(p.product_ids, '$[0]') = JSON_EXTRACT(m.product_ids, '$[0]')
        AND p.campaign_id = ?
        AND p.acquisition_date > m.acquisition_date
        AND (julianday(p.acquisition_date) - julianday(m.acquisition_date)) * 86400 < 300
      WHERE m.client_id = 1
        AND m.campaign_id = ?
        AND m.order_status = 7
      LIMIT 50
    `, [pair.pp, pair.main]);

    if (pairs.length === 0) continue;

    console.log();
    console.log('Main ' + pair.main + ' → PP ' + pair.pp + ': ' + pairs.length + ' samples');
    pairs.forEach(p => {
      const reason = p.main_reason || '(none)';
      reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
      detailsCounts[p.main_details || '(none)'] = (detailsCounts[p.main_details || '(none)'] || 0) + 1;
      categoryCounts[p.main_category || '(none)'] = (categoryCounts[p.main_category || '(none)'] || 0) + 1;
      allRoutingDeclines.push(p);
    });

    // Show first 3 samples per pair
    pairs.slice(0, 3).forEach(p => {
      console.log('  Main order ' + p.main_order_id + ' (declined ' + p.seconds_diff + 's before PP retry):');
      console.log('    decline_reason:         ' + (p.main_reason || '(none)'));
      console.log('    decline_reason_details: ' + (p.main_details || '(none)').substring(0, 100));
      console.log('    decline_category:       ' + (p.main_category || '(none)'));
      console.log('    prepaid flag:           ' + p.main_prepaid);
    });
  }

  console.log();
  console.log('='.repeat(80));
  console.log('PART 2: Aggregated decline_reasons across all routing-decline samples');
  console.log('='.repeat(80));
  console.log('Total samples: ' + allRoutingDeclines.length);
  console.log();
  console.log('Top decline_reason values:');
  Object.entries(reasonCounts).sort((a, b) => b[1] - a[1]).slice(0, 20).forEach(([r, n]) => {
    console.log('  ' + String(n).padStart(5) + '  ' + r.substring(0, 80));
  });
  console.log();
  console.log('Top decline_category values:');
  Object.entries(categoryCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).forEach(([r, n]) => {
    console.log('  ' + String(n).padStart(5) + '  ' + r);
  });
  console.log();
  console.log('Top decline_reason_details (truncated to 100 chars):');
  Object.entries(detailsCounts).sort((a, b) => b[1] - a[1]).slice(0, 15).forEach(([r, n]) => {
    console.log('  ' + String(n).padStart(5) + '  ' + r.substring(0, 100));
  });

  // Part 3: Compare to overall decline reason distribution
  console.log();
  console.log('='.repeat(80));
  console.log('PART 3: Overall decline_reason distribution for comparison (Kytsan, top 25)');
  console.log('='.repeat(80));
  const overallReasons = querySql(`
    SELECT COALESCE(decline_reason, '(none)') as reason, COUNT(*) as n
    FROM orders
    WHERE client_id = 1 AND order_status = 7
    GROUP BY reason
    ORDER BY n DESC
    LIMIT 25
  `);
  overallReasons.forEach(r => {
    console.log('  ' + String(r.n).padStart(7) + '  ' + r.reason.substring(0, 80));
  });

  // Part 4: Specific reason → percentage that triggers a PP retry
  console.log();
  console.log('='.repeat(80));
  console.log('PART 4: For each decline_reason on Main campaigns, what % triggered PP retry?');
  console.log('='.repeat(80));
  console.log('(Only showing reasons with >100 declines on Main campaigns)');
  console.log();
  const reasonAnalysis = querySql(`
    SELECT
      COALESCE(m.decline_reason, '(none)') as reason,
      COUNT(*) as total_main_declines,
      SUM(CASE WHEN EXISTS(
        SELECT 1 FROM orders p
        WHERE p.client_id = m.client_id
          AND p.contact_id = m.contact_id
          AND p.contact_id IS NOT NULL AND p.contact_id != 0
          AND JSON_EXTRACT(p.product_ids, '$[0]') = JSON_EXTRACT(m.product_ids, '$[0]')
          AND p.campaign_id IN (132, 100, 286, 223, 298, 104, 136)
          AND p.acquisition_date > m.acquisition_date
          AND (julianday(p.acquisition_date) - julianday(m.acquisition_date)) * 86400 < 300
      ) THEN 1 ELSE 0 END) as routed_to_pp
    FROM orders m
    WHERE m.client_id = 1
      AND m.order_status = 7
      AND m.campaign_id IN (130, 98, 284, 221, 297, 102, 134)
    GROUP BY reason
    HAVING total_main_declines >= 50
    ORDER BY (routed_to_pp * 1.0 / total_main_declines) DESC, total_main_declines DESC
  `);

  console.log('decline_reason'.padEnd(60) + '  total  routed  pct');
  console.log('-'.repeat(80));
  reasonAnalysis.forEach(r => {
    const pct = ((r.routed_to_pp / r.total_main_declines) * 100).toFixed(1);
    console.log(
      r.reason.substring(0, 58).padEnd(60) + '  ' +
      String(r.total_main_declines).padStart(5) + '  ' +
      String(r.routed_to_pp).padStart(6) + '  ' +
      pct + '%'
    );
  });

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
