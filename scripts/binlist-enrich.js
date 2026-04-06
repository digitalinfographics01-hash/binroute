/**
 * BIN enrichment via binlist.net API.
 * Looks up BINs that have no entry in bin_lookup and inserts results.
 *
 * Usage: node scripts/binlist-enrich.js [clientId]
 * Runs with 6s delay between requests to respect rate limits.
 */
const fs = require('fs');
const path = require('path');
const { initDb, querySql, runSql, saveDb, closeDb } = require('../src/db/connection');
const { initializeDatabase } = require('../src/db/schema');

const DELAY_MS = 10000;
const PROGRESS_INTERVAL = 50;

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function lookupBin(bin, attempt = 1) {
  const axios = require('axios');
  try {
    const res = await axios.get(`https://lookup.binlist.net/${bin}`, {
      headers: { 'Accept-Version': '3' },
      timeout: 10000,
    });
    return res.data;
  } catch (err) {
    if (err.response && err.response.status === 404) return null;
    if (err.response && err.response.status === 429) {
      if (attempt >= 3) {
        console.log(`  [Rate limited] ${bin} — giving up after 3 attempts`);
        return null;
      }
      const backoff = 30000 * attempt;
      console.log(`  [Rate limited] ${bin} — waiting ${backoff/1000}s (attempt ${attempt}/3)...`);
      await sleep(backoff);
      return lookupBin(bin, attempt + 1);
    }
    return null;
  }
}

function normalizeType(typeStr) {
  if (!typeStr) return null;
  const t = typeStr.toUpperCase().trim();
  if (t === 'CREDIT') return 'CREDIT';
  if (t === 'DEBIT' || t === 'PREPAID') return 'DEBIT';
  return t;
}

async function run() {
  const clientId = parseInt(process.argv[2], 10) || 1;

  await initializeDatabase();
  console.log('[BinList Enrich] Starting BIN enrichment for client', clientId);

  // Get unmatched BINs ordered by order count DESC
  const bins = querySql(`
    SELECT DISTINCT o.cc_first_6 as bin,
      COUNT(*) as order_count,
      MAX(o.cc_type) as cc_type
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.is_test = 0 AND b.bin IS NULL
      AND o.cc_first_6 IS NOT NULL AND o.cc_first_6 != ''
    GROUP BY o.cc_first_6
    ORDER BY order_count DESC
  `);

  console.log(`[BinList Enrich] ${bins.length} BINs to look up`);
  console.log(`[BinList Enrich] Estimated time: ~${Math.ceil(bins.length * DELAY_MS / 60000)} minutes`);
  console.log();

  let found = 0;
  let notFound = 0;
  let errors = 0;

  for (let i = 0; i < bins.length; i++) {
    const { bin, cc_type } = bins[i];

    // Progress report
    if (i > 0 && i % PROGRESS_INTERVAL === 0) {
      console.log(`[BinList Enrich] Progress: ${i}/${bins.length} (${found} found, ${notFound} not found, ${errors} errors)`);
      saveDb();
    }

    const data = await lookupBin(bin);

    if (data && (data.bank || data.scheme || data.type)) {
      const issuerBank = (data.bank && data.bank.name) ? data.bank.name.toUpperCase() : null;
      const cardBrand = data.scheme ? data.scheme.toUpperCase() : (cc_type ? cc_type.toUpperCase() : null);
      const cardType = normalizeType(data.type);
      const cardLevel = data.brand || null;
      const isPrepaid = data.prepaid ? 1 : 0;

      runSql(
        `INSERT INTO bin_lookup (bin, issuer_bank, card_brand, card_type, card_level, is_prepaid, source)
         VALUES (?, ?, ?, ?, ?, ?, 'binlist_api')`,
        [bin, issuerBank, cardBrand, cardType, cardLevel, isPrepaid]
      );
      found++;
    } else {
      // Insert placeholder so we don't re-lookup
      runSql(
        `INSERT INTO bin_lookup (bin, source) VALUES (?, 'binlist_not_found')`,
        [bin]
      );
      notFound++;
    }

    // Rate limit delay (skip on last)
    if (i < bins.length - 1) {
      await sleep(DELAY_MS);
    }
  }

  // Final save
  saveDb();

  console.log();
  console.log(`[BinList Enrich] Complete!`);
  console.log(`  Found:     ${found}`);
  console.log(`  Not found: ${notFound}`);
  console.log(`  Errors:    ${errors}`);
  console.log(`  Total:     ${bins.length}`);

  closeDb();
}

run().catch(err => {
  console.error('[BinList Enrich] Fatal error:', err);
  closeDb();
  process.exit(1);
});
