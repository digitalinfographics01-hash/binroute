/**
 * Match products_catalog to product_cogs via normalized name matching.
 * Stores matches in a product_cogs_match table for P&L joins.
 *
 * Strategy:
 *   1. Exact normalized match
 *   2. Containment match (shorter name contained in longer)
 *   3. High word overlap (80%+ of shorter name's words in longer)
 *
 * Each product gets at most one COGS match (best score wins).
 */
const { initDb, runSql, querySql } = require('../src/db/connection');

const CLIENT_ID = 6;

function norm(s) {
  return s
    .toLowerCase()
    // Strip common junk prefixes
    .replace(/^(copy of|set live[^-]*[-–]?)\s*/i, '')
    // Strip parenthetical edition/variant markers
    .replace(/\(copy\)/gi, '')
    // Normalize unicode dashes/quotes
    .replace(/[–—]/g, '-')
    .replace(/['']/g, "'")
    .replace(/[""]/g, '"')
    // Strip non-alphanumeric except spaces
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function words(s) {
  return norm(s).split(' ').filter(w => w.length > 2);
}

function wordOverlapScore(a, b) {
  const wa = new Set(words(a));
  const wb = new Set(words(b));
  if (wa.size === 0 || wb.size === 0) return 0;
  const shorter = wa.size <= wb.size ? wa : wb;
  const longer = wa.size <= wb.size ? wb : wa;
  const overlap = [...shorter].filter(w => longer.has(w)).length;
  return overlap / shorter.size;
}

(async () => {
  await initDb();

  // Create match table
  runSql(`CREATE TABLE IF NOT EXISTS product_cogs_match (
    client_id INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT,
    cogs_product_name TEXT,
    cogs REAL,
    match_type TEXT,
    match_score REAL,
    matched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (client_id, product_id)
  )`);
  runSql('DELETE FROM product_cogs_match WHERE client_id = ?', [CLIENT_ID]);

  const catalog = querySql(
    'SELECT product_id, product_name FROM products_catalog WHERE client_id = ? AND product_name IS NOT NULL',
    [CLIENT_ID]
  );
  const cogs = querySql('SELECT product_name, product_name_norm, cogs FROM product_cogs');

  // Build COGS lookup structures
  const cogsExact = new Map();
  cogs.forEach(c => cogsExact.set(norm(c.product_name), c));

  console.log(`Catalog: ${catalog.length} named products`);
  console.log(`COGS: ${cogs.length} products`);

  let exact = 0, contained = 0, overlap = 0, unmatched = 0;
  const matches = [];

  for (const cat of catalog) {
    const catNorm = norm(cat.product_name);

    // 1. Exact match
    if (cogsExact.has(catNorm)) {
      const c = cogsExact.get(catNorm);
      matches.push({ pid: cat.product_id, pname: cat.product_name, cname: c.product_name, cogs: c.cogs, type: 'exact', score: 1.0 });
      exact++;
      continue;
    }

    // 2+3. Containment and word overlap
    let bestMatch = null;
    let bestScore = 0;
    let bestType = null;

    for (const c of cogs) {
      const cogsNorm = norm(c.product_name);

      // Containment: one name fully inside the other
      if (catNorm.includes(cogsNorm) || cogsNorm.includes(catNorm)) {
        const shorter = catNorm.length <= cogsNorm.length ? catNorm : cogsNorm;
        const longer = catNorm.length <= cogsNorm.length ? cogsNorm : catNorm;
        const score = shorter.length / longer.length;
        if (score > bestScore && score >= 0.5) {
          bestScore = score;
          bestMatch = c;
          bestType = 'contained';
        }
      }

      // Word overlap
      const wScore = wordOverlapScore(cat.product_name, c.product_name);
      if (wScore > bestScore && wScore >= 0.8) {
        bestScore = wScore;
        bestMatch = c;
        bestType = 'word_overlap';
      }
    }

    if (bestMatch) {
      matches.push({ pid: cat.product_id, pname: cat.product_name, cname: bestMatch.product_name, cogs: bestMatch.cogs, type: bestType, score: bestScore });
      if (bestType === 'contained') contained++;
      else overlap++;
    } else {
      unmatched++;
    }
  }

  // Insert matches
  const insert = runSql;
  for (const m of matches) {
    insert(
      `INSERT INTO product_cogs_match (client_id, product_id, product_name, cogs_product_name, cogs, match_type, match_score)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [CLIENT_ID, m.pid, m.pname, m.cname, m.cogs, m.type, m.score]
    );
  }

  console.log(`\n=== Results ===`);
  console.log(`Exact:       ${exact}`);
  console.log(`Contained:   ${contained}`);
  console.log(`Word overlap: ${overlap}`);
  console.log(`Unmatched:   ${unmatched}`);
  console.log(`Total matched: ${matches.length} / ${catalog.length} (${(matches.length / catalog.length * 100).toFixed(1)}%)`);

  // Order coverage
  const orderCoverage = querySql(`
    SELECT COUNT(*) as total,
      SUM(CASE WHEN CAST(main_product_id AS TEXT) IN (SELECT product_id FROM product_cogs_match WHERE client_id = ?) THEN 1 ELSE 0 END) as matched
    FROM orders WHERE client_id = ? AND main_product_id IS NOT NULL AND order_status IN (2,6,8)
  `, [CLIENT_ID, CLIENT_ID]);
  const oc = orderCoverage[0];
  console.log(`\nApproved order coverage: ${oc.matched} / ${oc.total} (${(oc.matched / oc.total * 100).toFixed(1)}%)`);

  // Show some low-score matches for review
  const review = matches.filter(m => m.score < 0.9 && m.score >= 0.8).slice(0, 10);
  if (review.length > 0) {
    console.log(`\nBorderline matches (0.8-0.9 score) for review:`);
    review.forEach(m => {
      console.log(`  [${m.score.toFixed(2)}] ${m.type}`);
      console.log(`    CAT:  ${m.pname}`);
      console.log(`    COGS: ${m.cname} ($${m.cogs})`);
    });
  }
})().catch(e => { console.error('FAIL:', e); process.exit(1); });
