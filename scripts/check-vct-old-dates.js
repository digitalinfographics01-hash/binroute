// What date columns do the 867K pre-reimport VCT rows actually have?

const Database = require('better-sqlite3');
const db = new Database('/opt/binroute/data/binroute.db', { readonly: true });

console.log('=== Date-ish columns on orders ===');
const cols = db.prepare(`PRAGMA table_info(orders)`).all();
const dateCols = cols.filter(c => /date|time|stamp|created|updated/i.test(c.name));
dateCols.forEach(c => console.log(`  ${c.name}  (${c.type})`));

console.log('\n=== For rows WITHOUT date_created (client 6) — coverage of other date fields ===');
const dateColNames = dateCols.map(c => c.name).filter(n => n !== 'date_created');
for (const col of dateColNames) {
  const r = db.prepare(`
    SELECT COUNT(*) AS populated
    FROM orders
    WHERE client_id = 6
      AND (date_created IS NULL OR date_created = '')
      AND "${col}" IS NOT NULL AND "${col}" != ''
  `).get();
  console.log(`  ${col.padEnd(30)} populated: ${r.populated}`);
}

console.log('\n=== Sample of rows WITHOUT date_created ===');
db.prepare(`
  SELECT order_id, created_at, updated_at,
         CASE WHEN time_stamp IS NOT NULL THEN time_stamp ELSE '(no time_stamp)' END AS time_stamp
  FROM orders
  WHERE client_id = 6
    AND (date_created IS NULL OR date_created = '')
  LIMIT 5
`).all().forEach(r => console.log(JSON.stringify(r)));

console.log('\n=== created_at range for pre-reimport rows ===');
const r = db.prepare(`
  SELECT MIN(created_at) AS min_c, MAX(created_at) AS max_c, COUNT(*) AS n
  FROM orders
  WHERE client_id = 6
    AND (date_created IS NULL OR date_created = '')
    AND created_at IS NOT NULL AND created_at != ''
`).get();
console.log(r);
