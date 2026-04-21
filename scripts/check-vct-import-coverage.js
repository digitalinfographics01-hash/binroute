// Check VCT (client_id=6) import coverage — which days are fully imported vs partial/missing.

const Database = require('better-sqlite3');
const db = new Database('/opt/binroute/data/binroute.db', { readonly: true });

console.log('=== Daily order counts for client 6 (last 130 days) ===');
const cutoff = new Date(Date.now() - 130 * 86400000).toISOString().slice(0, 10);
const days = db.prepare(`
  SELECT
    substr(date_created, 1, 10) AS day,
    COUNT(*) AS n
  FROM orders
  WHERE client_id = 6 AND date_created >= ?
  GROUP BY day
  ORDER BY day DESC
`).all(cutoff);

console.log(`Days with data: ${days.length}`);
console.log(`\nPer-day counts (newest → oldest):`);
days.forEach(d => console.log(`  ${d.day}  ${String(d.n).padStart(7)}`));

// Gap/low-day detection
const vals = days.map(d => d.n);
const median = vals.slice().sort((a,b) => a - b)[Math.floor(vals.length / 2)];
console.log(`\nMedian orders/day: ${median}`);

const lowDays = days.filter(d => d.n < median * 0.5);
console.log(`\n=== Suspect days (< 50% of median, likely incomplete) ===`);
lowDays.forEach(d => console.log(`  ${d.day}  ${d.n}  (${Math.round(d.n/median*100)}% of median)`));

// Count rows without date_created
const stats = db.prepare(`
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN date_created IS NULL OR date_created = '' THEN 1 ELSE 0 END) AS no_date,
    MIN(CASE WHEN date_created IS NOT NULL AND date_created != '' THEN date_created END) AS min_d,
    MAX(CASE WHEN date_created IS NOT NULL AND date_created != '' THEN date_created END) AS max_d
  FROM orders
  WHERE client_id = 6
`).get();
console.log(`\n=== Overall stats ===`);
console.log(stats);
