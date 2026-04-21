/**
 * VCT (client 6) transaction classifier.
 *
 * Campaign-based classification with product-level reprocessing detection.
 *
 * Phase 1 — per-order rules (no context needed):
 *   1. is_test / is_internal_test                → excluded
 *   2. NULL customer_id                          → anonymous_decline
 *   3. $0 orders                                 → subscription_trigger
 *   4. Decline salvage camps (37,49,7,12)        → decline_salvage / decline_salvage_rebill / decline_salvage_retry
 *   5. Membership camps (3,11,60,4)              → membership / membership_rebill / membership_retry
 *   6. Cycle 1+, retry 0                         → main_rebill / upsell_rebill
 *   7. Cycle 1+, retry 1+                        → rebill_retry / upsell_rebill_retry
 *
 * Phase 2 — cycle 0 initials (needs customer context):
 *   Group by (customer_id, campaign_id, main_product_id), sorted by order_id:
 *   - First order for that combo                 → main_initial / upsell_initial
 *   - Subsequent order, same product             → initial_reprocessing / upsell_reprocessing
 *   - Subsequent order, different product         → main_initial / upsell_initial (repeat customer, new product)
 *
 * Usage:
 *   node scripts/classify-vct.js              # dry run
 *   node scripts/classify-vct.js --apply      # write to DB
 */
const { initDb, runSql, querySql } = require('../src/db/connection');

const CLIENT_ID = 6;
const DRY_RUN = !process.argv.includes('--apply');

// Upsell campaigns — all sell "Ultimate Protection (Lifetime Warranty + Shipping Protection)"
// Verified via API product lookup 2026-04-20: every one of these sells the same product
const UPSELL_CAMPS = new Set([
  // Original (from Apr 17 exploration)
  29, 73, 41, 64, 77, 13, 86, 88, 100, 101, 102, 98, 79, 82, 84,
  // Newly confirmed (Apr 20 — product names verified via v2 API)
  75, 90, 92, 95, 97, 104, 106, 108, 122, 136, 138, 140, 142, 144, 146,
  148, 152, 154, 156, 158, 162, 164, 166, 168, 170, 172, 174, 176,
]);

// "Ultimate Protection" product IDs — if order has this product, it's upsell regardless of campaign
// Extracted from all confirmed upsell campaigns 2026-04-20
const UPSELL_PRODUCT_IDS = new Set([
  568,2808,2917,7062,9411,13035,13459,14879,14992,17933,18336,22595,23510,24180,
  24241,24242,24561,24562,24563,24564,24565,24566,24571,24572,24573,25161,25162,
  28332,28335,28354,28355,28361,28380,28383,28384,28409,28715,28740,28879,29218,
  30881,30883,31830,31856,32018,32019,32517,32682,32992,33402,33782,34015,34016,
  34042,34047,34048,34106,34358,34433,34562,34570,34787,34792,34938,34992,35161,
  35695,36161,40115,42624,43020,
]);

// Decline salvage campaigns (product names: Initial Declines SS, ODECLINE, Decline Upsell, BL Error)
const DECLINE_SALVAGE_CAMPS = new Set([37, 49, 7, 12]);

// Membership/VIP/Rewards campaigns
const MEMBERSHIP_CAMPS = new Set([3, 11, 60, 4]);

function classifyPhase1(order) {
  const { customer_id, order_total, billing_cycle, retry_attempt,
    campaign_id, main_product_id, is_test, is_internal_test, is_test_cc } = order;

  if (is_test || is_internal_test || is_test_cc) return 'excluded';
  if (customer_id == null) return 'anonymous_decline';

  // Upsell detection: campaign-based OR product-based (Ultimate Protection)
  const isUpsell = UPSELL_CAMPS.has(campaign_id) || UPSELL_PRODUCT_IDS.has(main_product_id);
  if (isUpsell) {
    if (billing_cycle === 0) return null; // → Phase 2 (upsell_initial or upsell_reprocessing)
    if (retry_attempt > 0) return 'upsell_rebill_retry';
    return 'upsell_rebill';
  }

  if (order_total === 0 || order_total === '0' || order_total === 0.0) return 'subscription_trigger';

  if (DECLINE_SALVAGE_CAMPS.has(campaign_id)) {
    if (billing_cycle === 0) return 'decline_salvage';
    if (retry_attempt > 0) return 'decline_salvage_retry';
    return 'decline_salvage_rebill';
  }

  if (MEMBERSHIP_CAMPS.has(campaign_id)) {
    if (billing_cycle === 0) return 'membership';
    if (retry_attempt > 0) return 'membership_retry';
    return 'membership_rebill';
  }

  if (billing_cycle > 0) {
    if (retry_attempt > 0) return 'rebill_retry';
    return 'main_rebill';
  }

  // Cycle 0 with customer_id and real $ — needs Phase 2
  return null;
}

(async () => {
  await initDb();

  console.log(`VCT Classifier — ${DRY_RUN ? 'DRY RUN' : 'APPLYING'}`);
  console.log('Loading orders...');

  const orders = querySql(`
    SELECT order_id, customer_id, order_total, billing_cycle, retry_attempt,
           campaign_id, main_product_id, is_test, COALESCE(is_internal_test, 0) as is_internal_test,
           COALESCE(is_test_cc, 0) as is_test_cc, derived_product_role
    FROM orders WHERE client_id = ?
    ORDER BY order_id
  `, [CLIENT_ID]);

  console.log(`Loaded ${orders.length} orders`);

  const results = new Map(); // order_id → role

  // Phase 1
  const cycle0Orders = [];
  let phase1Count = 0;

  for (const o of orders) {
    const role = classifyPhase1(o);
    if (role !== null) {
      results.set(o.order_id, role);
      phase1Count++;
    } else {
      cycle0Orders.push(o);
    }
  }

  console.log(`Phase 1: ${phase1Count} orders classified`);
  console.log(`Phase 2: ${cycle0Orders.length} cycle-0 initials need context analysis`);

  // Phase 2 — group by (customer_id, campaign_id, main_product_id)
  // Track which (customer, campaign, product) combos we've seen
  const seen = new Set(); // "customer:campaign:product"

  let initialCount = 0, reprocessCount = 0;

  for (const o of cycle0Orders) {
    const isUpsell = UPSELL_CAMPS.has(o.campaign_id) || UPSELL_PRODUCT_IDS.has(o.main_product_id);
    const productKey = o.customer_id + ':' + o.campaign_id + ':' + (o.main_product_id || 'null');

    if (seen.has(productKey)) {
      // Same customer, same campaign, same product — reprocessing
      results.set(o.order_id, isUpsell ? 'upsell_reprocessing' : 'initial_reprocessing');
      reprocessCount++;
    } else {
      // First time seeing this combo — true initial
      results.set(o.order_id, isUpsell ? 'upsell_initial' : 'main_initial');
      seen.add(productKey);
      initialCount++;
    }
  }

  console.log(`Phase 2: ${initialCount} true initials, ${reprocessCount} reprocessing (same customer+campaign+product)`);

  // Build stats
  const stats = {};
  const changes = [];

  for (const o of orders) {
    const role = results.get(o.order_id);
    stats[role] = (stats[role] || 0) + 1;
    if (o.derived_product_role !== role) {
      changes.push({ order_id: o.order_id, old: o.derived_product_role, new: role });
    }
  }

  console.log('\n=== Classification Distribution ===');
  const sorted = Object.entries(stats).sort((a, b) => b[1] - a[1]);
  let total = 0;
  for (const [role, count] of sorted) {
    total += count;
    console.log(`  ${role.padEnd(28)} ${String(count).padStart(9)}  (${(100 * count / orders.length).toFixed(1)}%)`);
  }
  console.log(`  ${'TOTAL'.padEnd(28)} ${String(total).padStart(9)}`);

  console.log(`\nChanges needed: ${changes.length} / ${orders.length} orders`);

  if (changes.length > 0) {
    const sampleChanges = {};
    for (const c of changes) {
      const key = `${c.old || 'NULL'} → ${c.new}`;
      sampleChanges[key] = (sampleChanges[key] || 0) + 1;
    }
    console.log('\nChange summary:');
    Object.entries(sampleChanges).sort((a, b) => b[1] - a[1]).forEach(([key, count]) => {
      console.log(`  ${count.toString().padStart(9)}  ${key}`);
    });
  }

  if (!DRY_RUN && changes.length > 0) {
    console.log('\nApplying changes in batches...');
    const BATCH_SIZE = 50000;
    let applied = 0;
    for (let i = 0; i < changes.length; i += BATCH_SIZE) {
      const batch = changes.slice(i, i + BATCH_SIZE);
      runSql('BEGIN TRANSACTION');
      for (const c of batch) {
        runSql('UPDATE orders SET derived_product_role = ? WHERE client_id = ? AND order_id = ?',
          [c.new, CLIENT_ID, c.order_id]);
      }
      runSql('COMMIT');
      applied += batch.length;
      console.log(`  ${applied} / ${changes.length}`);
    }
    console.log(`Done — ${applied} orders updated.`);
  } else if (DRY_RUN) {
    console.log('\nDry run — no changes written. Run with --apply to write.');
  }
})().catch(e => { console.error('FAIL:', e); process.exit(1); });
