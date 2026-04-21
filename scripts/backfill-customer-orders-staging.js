/**
 * Backfill missing orders into a STAGING DB (never touches main DB).
 *
 * Two customer groups:
 *   1. Broken ancestor/parent links — orders reference IDs not in our DB
 *   2. Decline salvage customers (camps 37,49,7,12) — need full history for timing-based linking
 *
 * Uses batch order_view (25 IDs per call) for ~6x speedup over single-order fetches.
 *
 * Flow:
 *   1. Read main DB (read-only) → build customer list + existing order sets
 *   2. customer_view per customer → find missing order IDs
 *   3. Batch order_view → fetch missing orders → write to staging DB
 *   4. After completion: run merge-staging-to-main.js to copy validated data
 *
 * Usage:
 *   node scripts/backfill-customer-orders-staging.js
 */
const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const CLIENT_ID = 6;
const BATCH_SIZE = 25; // order_view sweet spot: 25 IDs per call
const CONCURRENCY = 5; // process 5 customers in parallel
const LOG_FILE = '/opt/binroute/logs/backfill-staging.log';
const CHECKPOINT_FILE = '/opt/binroute/checkpoint-backfill-staging.json';
const MAIN_DB_PATH = path.join(__dirname, '..', 'data', 'binroute.db');
const STAGING_DB_PATH = path.join(__dirname, '..', 'data', 'backfill_staging.db');
const MAX_WAL_MB = 200;
const MIN_DISK_GB = 5;
const DECLINE_SALVAGE_CAMPS = [37, 49, 7, 12];

const sleep = ms => new Promise(r => setTimeout(r, ms));

function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

function checkDiskSafety() {
  const walFile = STAGING_DB_PATH + '-wal';
  try {
    const walStats = fs.statSync(walFile);
    const walMB = walStats.size / (1024 * 1024);
    if (walMB > MAX_WAL_MB) {
      log(`SAFETY ABORT: Staging WAL is ${walMB.toFixed(0)} MB (limit ${MAX_WAL_MB} MB)`);
      process.exit(1);
    }
  } catch (e) {}

  try {
    const dfOut = execSync('df --output=avail /opt/binroute 2>/dev/null || df /opt/binroute', { encoding: 'utf8' });
    const lines = dfOut.trim().split('\n');
    const match = lines[lines.length - 1].match(/(\d+)/);
    if (match) {
      const freeGB = parseInt(match[1]) / (1024 * 1024);
      if (freeGB < MIN_DISK_GB) {
        log(`SAFETY ABORT: Only ${freeGB.toFixed(1)} GB free disk (limit ${MIN_DISK_GB} GB)`);
        process.exit(1);
      }
    }
  } catch (e) {}
}

// ─── Staging DB setup ───────────────────────────────────────────

function initStagingDb() {
  const db = new Database(STAGING_DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('cache_size = -32000');
  db.pragma('busy_timeout = 10000');

  db.exec(`
    CREATE TABLE IF NOT EXISTS orders (
      client_id INTEGER NOT NULL,
      order_id INTEGER NOT NULL,
      customer_id INTEGER,
      contact_id INTEGER,
      is_anonymous_decline INTEGER DEFAULT 0,
      campaign_id INTEGER,
      gateway_id INTEGER,
      gateway_descriptor TEXT,
      cc_first_6 TEXT,
      cc_type TEXT,
      order_status INTEGER,
      order_total REAL DEFAULT 0,
      decline_reason TEXT,
      decline_reason_details TEXT,
      acquisition_date TEXT,
      date_created TEXT,
      billing_cycle TEXT DEFAULT '0',
      is_cascaded INTEGER DEFAULT 0,
      retry_attempt TEXT DEFAULT '0',
      is_recurring TEXT DEFAULT '0',
      tx_type TEXT,
      product_ids TEXT,
      ancestor_id INTEGER,
      billing_country TEXT,
      billing_state TEXT,
      ip_address TEXT,
      prepaid TEXT DEFAULT '0',
      prepaid_match TEXT DEFAULT 'No',
      email_address TEXT,
      preserve_gateway INTEGER DEFAULT 0,
      is_chargeback INTEGER DEFAULT 0,
      chargeback_date TEXT,
      is_refund INTEGER DEFAULT 0,
      refund_amount REAL DEFAULT 0,
      refund_date TEXT,
      is_void INTEGER DEFAULT 0,
      void_amount REAL DEFAULT 0,
      void_date TEXT,
      amount_refunded_to_date REAL DEFAULT 0,
      click_id TEXT,
      utm_source TEXT,
      utm_medium TEXT,
      utm_campaign TEXT,
      utm_content TEXT,
      utm_term TEXT,
      device_category TEXT,
      created_by TEXT,
      billing_model_id INTEGER,
      billing_model_name TEXT,
      offer_id INTEGER,
      subscription_id TEXT,
      coupon_id TEXT,
      coupon_discount_amount REAL DEFAULT 0,
      decline_salvage_discount_percent REAL DEFAULT 0,
      rebill_discount_percent REAL DEFAULT 0,
      stop_after_next_rebill INTEGER DEFAULT 0,
      on_hold INTEGER DEFAULT 0,
      hold_date TEXT,
      order_confirmed TEXT,
      parent_id INTEGER,
      child_id INTEGER,
      is_in_trial INTEGER DEFAULT 0,
      order_subtotal REAL DEFAULT 0,
      shipping_total REAL DEFAULT 0,
      tax_total REAL DEFAULT 0,
      c1 TEXT, c2 TEXT, c3 TEXT, affid TEXT,
      time_stamp TEXT,
      is_test_cc INTEGER DEFAULT 0,
      retry_date TEXT,
      tracking_number TEXT,
      shipping_date TEXT,
      billing_first_name TEXT,
      billing_last_name TEXT,
      billing_street_address TEXT,
      billing_street_address2 TEXT,
      billing_company_name TEXT,
      billing_state_id TEXT,
      first_name TEXT,
      last_name TEXT,
      customers_telephone TEXT,
      shipping_first_name TEXT,
      shipping_last_name TEXT,
      shipping_street_address TEXT,
      shipping_street_address2 TEXT,
      shipping_company_name TEXT,
      shipping_city TEXT,
      shipping_country TEXT,
      shipping_state TEXT,
      shipping_state_id TEXT,
      shipping_postcode TEXT,
      shipping_method_name TEXT,
      shipping_id TEXT,
      cc_orig_first_6 TEXT,
      cc_orig_last_4 TEXT,
      check_account_last_4 TEXT,
      check_routing_last_4 TEXT,
      check_ssn_last_4 TEXT,
      check_transitnum TEXT,
      main_product_id INTEGER,
      main_product_quantity INTEGER,
      upsell_product_id INTEGER,
      upsell_product_quantity INTEGER,
      next_subscription_product TEXT,
      next_subscription_product_id INTEGER,
      is_any_product_recurring INTEGER DEFAULT 0,
      shippable INTEGER DEFAULT 0,
      aid TEXT, opt TEXT, sub_affiliate TEXT,
      created_by_user_name TEXT,
      credit_applied REAL DEFAULT 0,
      promo_code TEXT,
      current_rebill_discount_percent REAL DEFAULT 0,
      order_confirmed_date TEXT,
      order_sales_tax REAL DEFAULT 0,
      order_sales_tax_amount REAL DEFAULT 0,
      shipping_amount REAL DEFAULT 0,
      on_hold_by TEXT,
      is_rma INTEGER DEFAULT 0,
      rma_number TEXT,
      rma_reason TEXT,
      return_reason TEXT,
      consent_required INTEGER DEFAULT 0,
      consent_received INTEGER DEFAULT 0,
      order_customer_types TEXT,
      website_received TEXT,
      website_sent TEXT,
      ip_address_lookup TEXT,
      employee_notes TEXT,
      system_notes TEXT,
      custom_fields TEXT,
      UNIQUE(client_id, order_id)
    )
  `);

  return db;
}

// ─── Main ───────────────────────────────────────────────────────

(async () => {
  // Open main DB read-only, staging DB read-write
  const mainDb = new Database(MAIN_DB_PATH, { readonly: true });
  const stagingDb = initStagingDb();

  // Load StickyClient
  const StickyClient = require('../src/api/sticky-client');
  const clientRow = mainDb.prepare('SELECT sticky_base_url, sticky_username, sticky_password FROM clients WHERE id = ?').get(CLIENT_ID);
  const client = new StickyClient({ baseUrl: clientRow.sticky_base_url, username: clientRow.sticky_username, password: clientRow.sticky_password });

  // ─── Build customer list ──────────────────────────────────────

  log('Building customer list...');

  // Group 1: broken ancestor/parent links
  const brokenLinkCusts = mainDb.prepare(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = ? AND customer_id IS NOT NULL AND (
      (ancestor_id IS NOT NULL AND ancestor_id != order_id
       AND ancestor_id NOT IN (SELECT order_id FROM orders WHERE client_id = ?))
      OR
      (parent_id IS NOT NULL AND parent_id != order_id
       AND parent_id NOT IN (SELECT order_id FROM orders WHERE client_id = ?))
    )
  `).all(CLIENT_ID, CLIENT_ID, CLIENT_ID).map(r => r.customer_id);

  log(`Group 1 (broken links): ${brokenLinkCusts.length} customers`);

  // Group 2: decline salvage customers not already in group 1
  const brokenSet = new Set(brokenLinkCusts);
  const salvageCusts = mainDb.prepare(`
    SELECT DISTINCT customer_id FROM orders
    WHERE client_id = ? AND campaign_id IN (${DECLINE_SALVAGE_CAMPS.join(',')})
    AND customer_id IS NOT NULL
  `).all(CLIENT_ID).map(r => r.customer_id).filter(c => !brokenSet.has(c));

  log(`Group 2 (salvage, new): ${salvageCusts.length} customers`);

  const allCustomers = [...brokenLinkCusts, ...salvageCusts];
  log(`Total: ${allCustomers.length} customers`);

  // ─── Load checkpoint ──────────────────────────────────────────

  let checkpoint = { done: [], stats: { customers: 0, fetched: 0, saved: 0, skipped: 0, errors: 0, apiCalls: 0 } };
  try { checkpoint = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8')); } catch (e) {}
  if (!checkpoint.stats) checkpoint.stats = { customers: 0, fetched: 0, saved: 0, skipped: 0, errors: 0, apiCalls: 0 };
  if (!checkpoint.stats.apiCalls) checkpoint.stats.apiCalls = 0;

  const doneSet = new Set(checkpoint.done);
  const pending = allCustomers.filter(c => !doneSet.has(c));
  log(`${doneSet.size} already done, ${pending.length} remaining`);

  let { customers: custDone, fetched, saved, skipped, errors, apiCalls } = checkpoint.stats;
  let consecutiveErrors = 0;

  // ─── Prepare staging insert ───────────────────────────────────

  const insertStmt = stagingDb.prepare(`
    INSERT OR IGNORE INTO orders (
      client_id, order_id, customer_id, contact_id, is_anonymous_decline,
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
      employee_notes, system_notes, custom_fields
    ) VALUES (${Array(134).fill('?').join(',')})
  `);

  function insertOrder(order) {
    insertStmt.run(
      CLIENT_ID, order.order_id, order.customer_id, order.contact_id, order.is_anonymous_decline,
      order.campaign_id, order.gateway_id, order.gateway_descriptor,
      order.cc_first_6, order.cc_type, order.order_status, order.order_total,
      order.decline_reason, order.decline_reason_details,
      order.acquisition_date, order.date_created, order.billing_cycle, order.is_cascaded, order.retry_attempt,
      order.is_recurring, order.tx_type, order.product_ids, order.ancestor_id,
      order.billing_country, order.billing_state, order.ip_address,
      order.prepaid || '0', order.prepaid_match || 'No',
      order.email_address, order.preserve_gateway,
      order.is_chargeback, order.chargeback_date, order.is_refund, order.refund_amount, order.refund_date,
      order.is_void, order.void_amount, order.void_date, order.amount_refunded_to_date,
      order.click_id, order.utm_source, order.utm_medium, order.utm_campaign, order.utm_content, order.utm_term, order.device_category,
      order.created_by, order.billing_model_id, order.billing_model_name, order.offer_id, order.subscription_id,
      order.coupon_id, order.coupon_discount_amount, order.decline_salvage_discount_percent, order.rebill_discount_percent,
      order.stop_after_next_rebill, order.on_hold, order.hold_date, order.order_confirmed,
      order.parent_id, order.child_id, order.is_in_trial, order.order_subtotal, order.shipping_total, order.tax_total,
      order.c1, order.c2, order.c3, order.affid,
      order.time_stamp, order.is_test_cc, order.retry_date, order.tracking_number, order.shipping_date,
      order.billing_first_name, order.billing_last_name, order.billing_street_address, order.billing_street_address2, order.billing_company_name, order.billing_state_id,
      order.first_name, order.last_name, order.customers_telephone,
      order.shipping_first_name, order.shipping_last_name, order.shipping_street_address, order.shipping_street_address2, order.shipping_company_name,
      order.shipping_city, order.shipping_country, order.shipping_state, order.shipping_state_id, order.shipping_postcode, order.shipping_method_name, order.shipping_id,
      order.cc_orig_first_6, order.cc_orig_last_4,
      order.check_account_last_4, order.check_routing_last_4, order.check_ssn_last_4, order.check_transitnum,
      order.main_product_id, order.main_product_quantity, order.upsell_product_id, order.upsell_product_quantity,
      order.next_subscription_product, order.next_subscription_product_id, order.is_any_product_recurring, order.shippable,
      order.aid, order.opt, order.sub_affiliate, order.created_by_user_name, order.credit_applied, order.promo_code,
      order.current_rebill_discount_percent, order.order_confirmed_date, order.order_sales_tax, order.order_sales_tax_amount, order.shipping_amount, order.on_hold_by,
      order.is_rma, order.rma_number, order.rma_reason, order.return_reason,
      order.consent_required, order.consent_received, order.order_customer_types, order.website_received, order.website_sent, order.ip_address_lookup,
      order.employee_notes, order.system_notes, order.custom_fields
    );
  }

  // ─── Process single customer (returns result object) ─────────

  async function processCustomer(cid) {
    const result = { cid, saved: 0, fetched: 0, skipped: 0, errors: 0, apiCalls: 0, mainSize: 0, apiSize: 0 };

    try {
      // Step 1: customer_view → full order list
      const cv = await client._post('customer_view', { customer_id: cid });
      result.apiCalls++;

      if (!cv || cv.response_code !== '100' || !cv.order_list) {
        result.skipped++;
        return result;
      }

      // Normalize order_list — API returns string for single-order customers
      const apiOrderIds = Array.isArray(cv.order_list)
        ? cv.order_list.map(String)
        : [String(cv.order_list)];
      result.apiSize = apiOrderIds.length;

      // Step 2: diff against BOTH main DB and staging DB
      const existingMain = new Set(
        mainDb.prepare('SELECT order_id FROM orders WHERE client_id = ? AND customer_id = ?')
          .all(CLIENT_ID, cid).map(r => String(r.order_id))
      );
      const existingStaging = new Set(
        stagingDb.prepare('SELECT order_id FROM orders WHERE client_id = ? AND customer_id = ?')
          .all(CLIENT_ID, cid).map(r => String(r.order_id))
      );
      result.mainSize = existingMain.size;
      const missing = apiOrderIds.filter(id => !existingMain.has(id) && !existingStaging.has(id));

      if (missing.length === 0) return result;

      // Step 3: batch order_view — 25 IDs per call, no manual sleep (rate limiter handles it)
      const ordersToInsert = [];
      for (let b = 0; b < missing.length; b += BATCH_SIZE) {
        const batch = missing.slice(b, b + BATCH_SIZE);
        try {
          const data = await client._post('order_view', { order_id: batch.join(',') });
          result.apiCalls++;

          if (!data || data.response_code !== '100') {
            result.errors += batch.length;
            continue;
          }

          const orders = data.data && typeof data.data === 'object' ? data.data : {};
          const rawOrders = Object.values(orders).filter(o => o && o.order_id);
          result.fetched += rawOrders.length;

          for (const raw of rawOrders) {
            ordersToInsert.push(client.normalizeOrder(raw));
          }

          // Track IDs that weren't in the response
          const returnedIds = new Set(rawOrders.map(o => String(o.order_id)));
          result.skipped += batch.filter(id => !returnedIds.has(id)).length;

        } catch (e) {
          result.errors += batch.length;
        }
      }

      // Insert all orders for this customer in one transaction
      if (ordersToInsert.length > 0) {
        const insertBatch = stagingDb.transaction((orders) => {
          for (const order of orders) {
            insertOrder(order);
          }
        });
        insertBatch(ordersToInsert);
        result.saved = ordersToInsert.length;
      }

    } catch (e) {
      result.errors++;
      result.error = e.message;
    }

    return result;
  }

  // ─── Process customers with concurrency ─────────────────────

  let processed = 0;
  for (let i = 0; i < pending.length; i += CONCURRENCY) {
    const chunk = pending.slice(i, i + CONCURRENCY);

    // Process chunk in parallel
    const results = await Promise.all(chunk.map(cid => processCustomer(cid)));

    // Aggregate results (sequential — DB writes already done in transactions)
    for (const r of results) {
      checkpoint.done.push(r.cid);
      custDone++;
      fetched += r.fetched;
      saved += r.saved;
      skipped += r.skipped;
      errors += r.errors;
      apiCalls += r.apiCalls;

      if (r.saved > 0) {
        log(`  customer ${r.cid}: ${r.saved}/${r.fetched} saved (had ${r.mainSize} in main, API has ${r.apiSize})`);
      }
      if (r.error) {
        log(`  customer ${r.cid}: ERROR ${r.error}`);
      }
    }

    processed += chunk.length;

    // Progress + checkpoint + safety every 100 customers
    if (processed % 100 < CONCURRENCY) {
      saveCheckpoint();
      stagingDb.pragma('wal_checkpoint(PASSIVE)');
      checkDiskSafety();
      const stagingCount = stagingDb.prepare('SELECT COUNT(*) as n FROM orders').get().n;
      const walFile = STAGING_DB_PATH + '-wal';
      const walMB = fs.existsSync(walFile) ? (fs.statSync(walFile).size / (1024 * 1024)).toFixed(1) : '0';
      log(`Progress: ${processed}/${pending.length} customers | staging=${stagingCount} | fetched=${fetched} saved=${saved} skipped=${skipped} errors=${errors} apiCalls=${apiCalls} | WAL=${walMB}MB`);
    }
  }

  // Final checkpoint + cleanup
  saveCheckpoint();
  stagingDb.pragma('wal_checkpoint(TRUNCATE)');

  const finalCount = stagingDb.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  log(`\nDONE! ${pending.length} customers processed.`);
  log(`Staging DB: ${finalCount} orders in ${STAGING_DB_PATH}`);
  log(`Stats: fetched=${fetched} saved=${saved} skipped=${skipped} errors=${errors} apiCalls=${apiCalls}`);
  log(`\nNext step: node scripts/merge-staging-to-main.js`);

  stagingDb.close();
  mainDb.close();
  process.exit(0);

  function saveCheckpoint() {
    checkpoint.stats = { customers: custDone, fetched, saved, skipped, errors, apiCalls };
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint));
  }
})().catch(e => { console.error('FATAL:', e); process.exit(1); });
