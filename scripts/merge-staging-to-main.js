/**
 * Merge staging DB into main DB after backfill validation.
 *
 * Steps:
 *   1. Validate staging data (counts, NULL checks, samples)
 *   2. ATTACH staging DB to main
 *   3. INSERT OR IGNORE from staging.orders into main orders
 *   4. WAL checkpoint
 *   5. Verify final count
 *
 * Usage:
 *   node scripts/merge-staging-to-main.js --dry-run   # validate only
 *   node scripts/merge-staging-to-main.js              # merge
 */
const Database = require('better-sqlite3');
const path = require('path');

const MAIN_DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');
const STAGING_DB_PATH = path.join(__dirname, '..', 'data', 'backfill_staging.db');
const DRY_RUN = process.argv.includes('--dry-run');
const CLIENT_ID = 6;

console.log(`=== Merge Staging → Main ${DRY_RUN ? '(DRY RUN)' : ''} ===\n`);

// ─── Step 1: Validate staging ───────────────────────────────────

const staging = new Database(STAGING_DB_PATH, { readonly: true });

const stagingCount = staging.prepare('SELECT COUNT(*) as n FROM orders').get().n;
const stagingClients = staging.prepare('SELECT client_id, COUNT(*) as n FROM orders GROUP BY client_id').all();
const nullOrderId = staging.prepare('SELECT COUNT(*) as n FROM orders WHERE order_id IS NULL').get().n;
const nullCustomer = staging.prepare('SELECT COUNT(*) as n FROM orders WHERE customer_id IS NULL').get().n;
const nullDateCreated = staging.prepare('SELECT COUNT(*) as n FROM orders WHERE date_created IS NULL').get().n;
const dateRange = staging.prepare('SELECT MIN(date_created) as oldest, MAX(date_created) as newest FROM orders').get();
const campaignBreakdown = staging.prepare(`
  SELECT campaign_id, COUNT(*) as n FROM orders GROUP BY campaign_id ORDER BY n DESC LIMIT 15
`).all();

console.log('Staging DB validation:');
console.log(`  Total orders: ${stagingCount}`);
console.log(`  Client breakdown:`, stagingClients);
console.log(`  NULL order_id: ${nullOrderId}`);
console.log(`  NULL customer_id: ${nullCustomer} (anonymous declines expected)`);
console.log(`  NULL date_created: ${nullDateCreated}`);
console.log(`  Date range: ${dateRange.oldest} → ${dateRange.newest}`);
console.log(`  Top campaigns:`, campaignBreakdown.map(c => `camp ${c.campaign_id}: ${c.n}`).join(', '));

if (stagingCount === 0) {
  console.log('\nStaging DB is empty. Nothing to merge.');
  process.exit(0);
}

if (nullOrderId > 0) {
  console.log('\nERROR: Found orders with NULL order_id. Aborting.');
  process.exit(1);
}

// Sample a few orders
console.log('\nSample orders:');
const samples = staging.prepare('SELECT order_id, customer_id, campaign_id, order_total, order_status, date_created FROM orders ORDER BY RANDOM() LIMIT 5').all();
samples.forEach(s => console.log(`  order ${s.order_id} | cust ${s.customer_id} | camp ${s.campaign_id} | $${s.order_total} | status ${s.order_status} | ${s.date_created}`));

staging.close();

if (DRY_RUN) {
  console.log('\n=== DRY RUN — no changes made ===');
  console.log('Run without --dry-run to merge.');
  process.exit(0);
}

// ─── Step 2: Merge ──────────────────────────────────────────────

console.log('\nOpening main DB...');
const main = new Database(MAIN_DB_PATH);
main.pragma('journal_mode = WAL');
main.pragma('synchronous = NORMAL');
main.pragma('busy_timeout = 30000');

const beforeCount = main.prepare('SELECT COUNT(*) as n FROM orders WHERE client_id = ?').get(CLIENT_ID).n;
console.log(`Main DB before: ${beforeCount} orders (client ${CLIENT_ID})`);

console.log(`\nATTACHing staging DB...`);
main.exec(`ATTACH DATABASE '${STAGING_DB_PATH}' AS staging`);

// Count how many are truly new (not already in main)
const newCount = main.prepare(`
  SELECT COUNT(*) as n FROM staging.orders s
  WHERE NOT EXISTS (SELECT 1 FROM main.orders m WHERE m.client_id = s.client_id AND m.order_id = s.order_id)
`).get().n;
const dupeCount = stagingCount - newCount;

console.log(`  New orders to insert: ${newCount}`);
console.log(`  Already in main (dupes): ${dupeCount}`);

if (newCount === 0) {
  console.log('\nAll staging orders already exist in main. Nothing to merge.');
  main.exec('DETACH DATABASE staging');
  main.close();
  process.exit(0);
}

console.log(`\nInserting ${newCount} orders...`);

// Get column list from staging (excluding any autoincrement id)
const cols = staging ? null : null; // staging is closed, use hardcoded list
const columnList = `client_id, order_id, customer_id, contact_id, is_anonymous_decline,
  campaign_id, gateway_id, gateway_descriptor,
  cc_first_6, cc_type, order_status, order_total,
  decline_reason, decline_reason_details,
  acquisition_date, date_created, billing_cycle, is_cascaded, retry_attempt,
  is_recurring, tx_type, product_ids, ancestor_id,
  billing_country, billing_state, ip_address,
  prepaid, prepaid_match,
  email_address, preserve_gateway,
  is_chargeback, chargeback_date, is_refund, refund_amount, refund_date,
  is_void, void_amount, void_date, amount_refunded_to_date,
  click_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, device_category,
  created_by, billing_model_id, billing_model_name, offer_id, subscription_id,
  coupon_id, coupon_discount_amount, decline_salvage_discount_percent, rebill_discount_percent,
  stop_after_next_rebill, on_hold, hold_date, order_confirmed,
  parent_id, child_id, is_in_trial, order_subtotal, shipping_total, tax_total,
  c1, c2, c3, affid,
  time_stamp, is_test_cc, retry_date, tracking_number, shipping_date,
  billing_first_name, billing_last_name, billing_street_address, billing_street_address2, billing_company_name, billing_state_id,
  first_name, last_name, customers_telephone,
  shipping_first_name, shipping_last_name, shipping_street_address, shipping_street_address2, shipping_company_name,
  shipping_city, shipping_country, shipping_state, shipping_state_id, shipping_postcode, shipping_method_name, shipping_id,
  cc_orig_first_6, cc_orig_last_4,
  check_account_last_4, check_routing_last_4, check_ssn_last_4, check_transitnum,
  main_product_id, main_product_quantity, upsell_product_id, upsell_product_quantity,
  next_subscription_product, next_subscription_product_id, is_any_product_recurring, shippable,
  aid, opt, sub_affiliate, created_by_user_name, credit_applied, promo_code,
  current_rebill_discount_percent, order_confirmed_date, order_sales_tax, order_sales_tax_amount, shipping_amount, on_hold_by,
  is_rma, rma_number, rma_reason, return_reason,
  consent_required, consent_received, order_customer_types, website_received, website_sent, ip_address_lookup,
  employee_notes, system_notes, custom_fields`;

const result = main.exec(`
  INSERT OR IGNORE INTO main.orders (${columnList})
  SELECT ${columnList} FROM staging.orders
`);

// ─── Step 3: Verify ─────────────────────────────────────────────

const afterCount = main.prepare('SELECT COUNT(*) as n FROM orders WHERE client_id = ?').get(CLIENT_ID).n;
const inserted = afterCount - beforeCount;

console.log(`\nMain DB after: ${afterCount} orders (client ${CLIENT_ID})`);
console.log(`Inserted: ${inserted} new orders (expected ${newCount})`);

if (inserted !== newCount) {
  console.log('WARNING: Inserted count does not match expected. Some may have conflicted on other constraints.');
}

// WAL checkpoint
console.log('\nCheckpointing WAL...');
main.pragma('wal_checkpoint(PASSIVE)');

main.exec('DETACH DATABASE staging');
main.close();

console.log(`\n=== MERGE COMPLETE === ${inserted} orders added to main DB`);
console.log('Staging DB preserved at:', STAGING_DB_PATH);
console.log('You can delete it after verifying: rm', STAGING_DB_PATH);
