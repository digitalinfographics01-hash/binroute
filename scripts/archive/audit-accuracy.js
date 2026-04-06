/**
 * Final accuracy audit — validates all engine outputs against raw SQL.
 */
const { initDb, querySql, closeDb } = require('../src/db/connection');

const TEST_BINS = "'144444'";
const PREPAID_EXCL = "(o.decline_reason IS NULL OR o.decline_reason != 'Prepaid Credit Cards Are Not Accepted')";

(async () => {
  await initDb();

  console.log('================================================================');
  console.log('  FINAL ACCURACY AUDIT — All Engines');
  console.log('================================================================');

  // ── MAIN INITIALS ──
  console.log('\n── MAIN INITIALS ──');
  const natMain = querySql(`
    SELECT COUNT(*) as total, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as approved
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${TEST_BINS})
      AND ${PREPAID_EXCL}
  `)[0];
  const cascMain = querySql(`
    SELECT COUNT(*) as c
    FROM orders o
    JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = 1 AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND o.derived_product_role = 'main_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${TEST_BINS})
      AND ${PREPAID_EXCL}
  `)[0];

  const initRow = querySql("SELECT result_json FROM analytics_cache WHERE output_type = 'flow-optix-v2-initials'")[0];
  const initData = JSON.parse(initRow.result_json);
  const mCards = initData.main.cards;
  const mEngTotal = mCards.reduce((s, c) => s + c.totalAttempts, 0);
  const mEngApp = mCards.reduce((s, c) => s + c.totalApproved, 0);
  const mExpTotal = natMain.total + cascMain.c;

  console.log('  SQL: natural=' + natMain.total + ' + cascade=' + cascMain.c + ' = ' + mExpTotal + ' (app=' + natMain.approved + ')');
  console.log('  Engine: total=' + mEngTotal + ' app=' + mEngApp);
  console.log('  ' + (mExpTotal === mEngTotal && natMain.approved === mEngApp ? 'EXACT MATCH ✓' : 'DIFF ✗ total=' + (mEngTotal - mExpTotal) + ' app=' + (mEngApp - natMain.approved)));

  // Per-bank spot check
  const topBanks = ['SUTTON BANK', 'CAPITAL ONE, NATIONAL ASSOCIATION', 'JPMORGAN CHASE BANK N.A.', 'WELLS FARGO BANK, NATIONAL ASSOCIATION', 'DISCOVER ISSUER'];
  console.log('\n  Per-bank spot check:');
  for (const bank of topBanks) {
    const nat = querySql(`
      SELECT COUNT(*) as t, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as a
      FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      WHERE o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
        AND g.exclude_from_analysis = 0 AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
        AND b.issuer_bank = ?`, [bank])[0];
    const casc = querySql(`
      SELECT COUNT(*) as c FROM orders o
      JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = 1 AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
        AND o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_test = 0 AND o.is_internal_test = 0 AND g.exclude_from_analysis = 0
        AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL} AND b.issuer_bank = ?`, [bank])[0];
    const card = mCards.find(c => c.issuer_bank === bank);
    const exp = nat.t + casc.c;
    const eng = card ? card.totalAttempts : '???';
    const engA = card ? card.totalApproved : '???';
    const match = exp === eng && nat.a === engA;
    console.log('    ' + bank.substring(0, 30).padEnd(31) + ' sql=' + exp + ' eng=' + eng + ' app:sql=' + nat.a + ' eng=' + engA + ' ' + (match ? '✓' : '✗'));
  }

  // ── UPSELL INITIALS ──
  console.log('\n── UPSELL INITIALS ──');
  const natUps = querySql(`
    SELECT COUNT(*) as total, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as approved
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.derived_product_role = 'upsell_initial'
      AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND g.exclude_from_analysis = 0 AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
  `)[0];
  const cascUps = querySql(`
    SELECT COUNT(*) as c FROM orders o
    JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id = 1 AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
      AND o.derived_product_role = 'upsell_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
      AND o.is_test = 0 AND o.is_internal_test = 0 AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
  `)[0];
  const uCards = initData.upsell.cards;
  const uEngTotal = uCards.reduce((s, c) => s + c.totalAttempts, 0);
  const uEngApp = uCards.reduce((s, c) => s + c.totalApproved, 0);
  const uExpTotal = natUps.total + cascUps.c;
  console.log('  SQL: natural=' + natUps.total + ' + cascade=' + cascUps.c + ' = ' + uExpTotal + ' (app=' + natUps.approved + ')');
  console.log('  Engine: total=' + uEngTotal + ' app=' + uEngApp);
  console.log('  ' + (uExpTotal === uEngTotal && natUps.approved === uEngApp ? 'EXACT MATCH ✓' : 'DIFF ✗ total=' + (uEngTotal - uExpTotal) + ' app=' + (uEngApp - natUps.approved)));

  // ── REBILLS ──
  console.log('\n── REBILLS ──');
  const natReb = querySql(`
    SELECT COUNT(*) as total, COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as approved
    FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
    WHERE o.derived_product_role IN ('main_rebill','upsell_rebill')
      AND o.derived_attempt = 1 AND o.derived_cycle IN (1,2)
      AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.order_total NOT IN (6.96, 64.98) AND g.exclude_from_analysis = 0
      AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
  `)[0];
  const rebRow = querySql("SELECT result_json FROM analytics_cache WHERE output_type = 'flow-optix-v2'")[0];
  const rCards = JSON.parse(rebRow.result_json).cards;
  const rEngTotal = rCards.reduce((s, c) => s + c.totalAttempts, 0);
  const rEngApp = rCards.reduce((s, c) => s + c.totalApproved, 0);
  console.log('  SQL: total=' + natReb.total + ' app=' + natReb.approved);
  console.log('  Engine: total=' + rEngTotal + ' app=' + rEngApp);
  console.log('  ' + (natReb.total === rEngTotal && natReb.approved === rEngApp ? 'EXACT MATCH ✓' : 'DIFF ✗ total=' + (rEngTotal - natReb.total) + ' app=' + (rEngApp - natReb.approved)));

  // ── EXCLUSION CHECKS ──
  console.log('\n── EXCLUSION CHECKS ──');
  const bin144 = mCards.find(c => c.bins && c.bins.includes('144444'));
  console.log('  Test BIN 144444: ' + (bin144 ? 'PRESENT ✗' : 'Excluded ✓'));
  const hasPriceInit = mCards.some(c => c.priceOptimization);
  console.log('  Price opt on initials: ' + (hasPriceInit ? 'PRESENT ✗' : 'Excluded ✓'));
  const hasPriceReb = rCards.some(c => c.priceOptimization);
  console.log('  Price opt on rebills: ' + (hasPriceReb ? 'Present ✓' : 'Missing ✗'));

  // ── CASCADE SEPARATION ──
  console.log('\n── CASCADE SEPARATION ──');
  const cascOnCards = mCards.filter(c => c.cascadeTargets && c.cascadeTargets.length > 0).length;
  console.log('  Cards with cascade save data: ' + cascOnCards);
  console.log('  Cascade SEPARATE from natural: ' + (mCards[0] && mCards[0].cascadeTargets !== undefined ? 'YES ✓' : 'NO ✗'));

  // Verify cascade targets are NOT in totalAttempts
  const suttonCard = mCards.find(c => c.issuer_bank === 'SUTTON BANK');
  if (suttonCard) {
    const suttonCascTarget = querySql(`
      SELECT COUNT(*) as c FROM orders o
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
        AND o.is_test = 0 AND o.is_internal_test = 0 AND g.exclude_from_analysis = 0
        AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
        AND b.issuer_bank = 'SUTTON BANK'
    `)[0];
    console.log('  Sutton Bank: engine total=' + suttonCard.totalAttempts + ' cascade targets=' + suttonCascTarget.c + ' (should NOT be in total)');
    // If total includes cascade targets, it would be higher
    const natOnly = querySql(`
      SELECT COUNT(*) as t FROM orders o JOIN bin_lookup b ON o.cc_first_6 = b.bin
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = o.client_id
      WHERE o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_cascaded = 0 AND o.is_test = 0 AND o.is_internal_test = 0
        AND g.exclude_from_analysis = 0 AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
        AND b.issuer_bank = 'SUTTON BANK'`, [])[0];
    const cascCorr = querySql(`
      SELECT COUNT(*) as c FROM orders o
      JOIN gateways g ON o.original_gateway_id = g.gateway_id AND g.client_id = o.client_id
      JOIN bin_lookup b ON o.cc_first_6 = b.bin
      WHERE o.client_id = 1 AND o.is_cascaded = 1 AND o.original_gateway_id IS NOT NULL
        AND o.derived_product_role = 'main_initial' AND (o.derived_attempt = 1 OR o.derived_attempt IS NULL)
        AND o.is_test = 0 AND o.is_internal_test = 0 AND g.exclude_from_analysis = 0
        AND o.cc_first_6 NOT IN (${TEST_BINS}) AND ${PREPAID_EXCL}
        AND b.issuer_bank = 'SUTTON BANK'`, [])[0];
    const expectedNoTarget = natOnly.t + cascCorr.c;
    console.log('  Sutton Bank: nat=' + natOnly.t + ' + cascCorr=' + cascCorr.c + ' = ' + expectedNoTarget + ' eng=' + suttonCard.totalAttempts);
    console.log('  Double-counting check: ' + (expectedNoTarget === suttonCard.totalAttempts ? 'CLEAN — no double counting ✓' : 'DOUBLE COUNTED ✗'));
  }

  // ── DERIVED FIELD COVERAGE ──
  console.log('\n── DERIVED FIELD COVERAGE ──');
  const coverage = querySql(`
    SELECT COUNT(*) as total,
      COUNT(derived_product_role) as has_role,
      COUNT(processing_gateway_id) as has_pgw,
      COUNT(CASE WHEN derived_product_role IS NULL AND is_test = 0 AND is_internal_test = 0 THEN 1 END) as missing_role
    FROM orders
  `)[0];
  console.log('  Total orders: ' + coverage.total);
  console.log('  Has derived_product_role: ' + coverage.has_role);
  console.log('  Has processing_gateway_id: ' + coverage.has_pgw);
  console.log('  Missing role (non-test): ' + coverage.missing_role);

  // ── DATA FRESHNESS ──
  console.log('\n── DATA FRESHNESS ──');
  const fresh = querySql("SELECT MAX(acquisition_date) as latest FROM orders")[0];
  const cache = querySql("SELECT MIN(computed_at) as oldest, MAX(computed_at) as newest FROM analytics_cache")[0];
  console.log('  Latest order: ' + fresh.latest);
  console.log('  Cache computed: ' + cache.oldest + ' to ' + cache.newest);
  console.log('  All 14 engines cached: ' + (querySql("SELECT COUNT(*) as c FROM analytics_cache")[0].c === 14 ? 'YES ✓' : 'NO ✗'));

  console.log('\n================================================================');
  console.log('  AUDIT COMPLETE');
  console.log('================================================================');

  closeDb();
})();
