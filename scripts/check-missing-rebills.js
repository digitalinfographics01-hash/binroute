#!/usr/bin/env node
/**
 * Daily Missing Rebills Check
 *
 * Compares approved orders in BinRoute DB against Flow Optix
 * (straight_sale_continuity_orders) to find orders that should
 * have rebills scheduled but don't.
 *
 * Runs on the server after daily Kytsan import.
 * Can also be run manually:
 *   node scripts/check-missing-rebills.js              — console report
 *   node scripts/check-missing-rebills.js --days=14    — custom lookback
 *
 * When required as a module (from scheduler), exports runMissingRebillsCheck().
 */

const mysql = require('mysql2/promise');
const path = require('path');
const fs = require('fs');

const FLOW_OPTIX = {
  host: '206.71.148.36',
  user: 'omg',
  password: 'HuGeJOGlLCWgdVoivAIBImtAq',
  database: 'whitelabel'
};

const CLIENT_ID = 1;
const EXCLUDE_CAMPAIGNS = [304]; // Shipping insurance — one-time sale
const REPORT_DIR = path.join(__dirname, '..', 'data', 'rebill-reports');

async function runMissingRebillsCheck(lookbackDays = 14) {
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - lookbackDays);
  const startStr = startDate.toISOString().split('T')[0];
  const today = new Date().toISOString().split('T')[0];

  console.log(`[RebillCheck] Date range: ${startStr} to ${today}`);

  // Step 1: Get all non-test order IDs from Flow Optix
  console.log('[RebillCheck] Querying Flow Optix DB...');
  let foConn;
  try {
    foConn = await mysql.createConnection(FLOW_OPTIX);
    const [foRows] = await foConn.query(
      'SELECT DISTINCT order_id FROM straight_sale_continuity_orders WHERE is_test = 0'
    );
    var foOrderIds = new Set(foRows.map(r => String(r.order_id)));
    console.log(`[RebillCheck] Flow Optix has ${foOrderIds.size} scheduled rebill orders`);
  } finally {
    if (foConn) await foConn.end();
  }

  // Step 2: Get approved orders from local BinRoute DB
  console.log('[RebillCheck] Querying local BinRoute DB...');
  const Database = require('better-sqlite3');
  const dbPath = path.join(__dirname, '..', 'data', 'binroute.db');
  const db = new Database(dbPath, { readonly: true });

  const serverOrders = db.prepare(`
    SELECT order_id, ancestor_id, customer_id, campaign_id, gateway_id,
      order_status, order_total, tx_type, billing_cycle, main_product_id,
      first_name, last_name, email_address, cc_first_6, cc_type,
      billing_country, acquisition_date, subscription_id
    FROM orders
    WHERE client_id = ?
      AND order_status IN (2, 6, 8)
      AND is_test = 0 AND is_internal_test = 0 AND is_test_cc = 0
      AND acquisition_date >= ?
  `).all(CLIENT_ID, startStr);

  db.close();
  console.log(`[RebillCheck] BinRoute has ${serverOrders.length} approved orders`);

  // Step 3: Find missing
  const excludeSet = new Set(EXCLUDE_CAMPAIGNS.map(String));
  const missing = serverOrders.filter(o =>
    !foOrderIds.has(String(o.order_id)) && !excludeSet.has(String(o.campaign_id))
  );

  // Step 4: Build report
  const byCampaign = {};
  let totalRevenue = 0;
  missing.forEach(o => {
    const key = `${o.campaign_id}|${o.tx_type}`;
    if (!byCampaign[key]) byCampaign[key] = { campaign_id: o.campaign_id, tx_type: o.tx_type, count: 0, revenue: 0 };
    byCampaign[key].count++;
    const amt = parseFloat(o.order_total) || 0;
    byCampaign[key].revenue += amt;
    totalRevenue += amt;
  });

  console.log(`[RebillCheck] Approved: ${serverOrders.length} | In Flow Optix: ${serverOrders.length - missing.length} | MISSING: ${missing.length}`);

  if (missing.length > 0) {
    console.log(`[RebillCheck] Missing revenue: $${totalRevenue.toFixed(2)}`);
    console.log('[RebillCheck] Breakdown:');
    Object.values(byCampaign).sort((a, b) => b.count - a.count).forEach(g => {
      console.log(`  Campaign ${g.campaign_id} (${g.tx_type}): ${g.count} missing, $${g.revenue.toFixed(2)}`);
    });
  }

  // Step 5: Save report to disk
  if (!fs.existsSync(REPORT_DIR)) fs.mkdirSync(REPORT_DIR, { recursive: true });

  const report = {
    date: today,
    lookbackDays,
    startDate: startStr,
    approvedCount: serverOrders.length,
    inFlowOptix: serverOrders.length - missing.length,
    missingCount: missing.length,
    missingRevenue: totalRevenue,
    breakdown: Object.values(byCampaign).sort((a, b) => b.count - a.count),
    missingOrders: missing.map(o => ({
      order_id: o.order_id,
      campaign_id: o.campaign_id,
      tx_type: o.tx_type,
      order_total: o.order_total,
      first_name: o.first_name,
      last_name: o.last_name,
      email_address: o.email_address,
      main_product_id: o.main_product_id,
      acquisition_date: o.acquisition_date,
      subscription_id: o.subscription_id
    }))
  };

  const reportPath = path.join(REPORT_DIR, `missing-rebills-${today}.json`);
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log(`[RebillCheck] Report saved: ${reportPath}`);

  return report;
}

// --- CLI mode ---
if (require.main === module) {
  const args = process.argv.slice(2);
  const daysArg = args.find(a => a.startsWith('--days='));
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 14;

  runMissingRebillsCheck(days)
    .then(report => {
      if (report.missingCount === 0) {
        console.log('\n[RebillCheck] All orders have rebills scheduled.');
      } else {
        console.log(`\n[RebillCheck] ${report.missingCount} orders need to be added to Flow Optix.`);
        console.log('\nMissing order IDs:');
        report.missingOrders.forEach(o => {
          console.log(`  ${o.order_id} | ${o.first_name} ${o.last_name} | camp ${o.campaign_id} | ${o.tx_type} | $${o.order_total} | ${o.acquisition_date}`);
        });
      }
    })
    .catch(err => {
      console.error('[RebillCheck] Error:', err.message);
      process.exit(1);
    });
}

module.exports = { runMissingRebillsCheck };
