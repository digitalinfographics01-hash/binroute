// Quick CB signal check for VCT (client_id=6). Limited queries, no slow sorts.

const Database = require('better-sqlite3');
const db = new Database('/opt/binroute/data/binroute.db', { readonly: true });

// Sample of CB orders — what does their system_notes look like?
console.log('=== 8 CB orders with system_notes snippets (client 6) ===');
const cbSamples = db.prepare(`
  SELECT order_id, date_created, chargeback_date, return_reason,
         substr(system_notes, 1, 2500) AS notes
  FROM orders
  WHERE client_id=6 AND is_chargeback=1
  LIMIT 8
`).all();
cbSamples.forEach(r => {
  console.log(`\n--- [order ${r.order_id}]  created=${r.date_created}  cb_date=${r.chargeback_date}  return_reason=${r.return_reason || 'null'} ---`);
  const notes = (r.notes || '').replace(/\\n/g, '\n').replace(/\r/g, '').trim();
  console.log(notes.substring(0, 1500));
});

// Keyword search — ONLY on CB rows (4,274 rows, small set — fast LIKE)
console.log('\n\n=== Keyword hits within CB orders (client 6 = 4,274 CB rows) ===');
const keywords = ['RDR', 'CDRN', 'Ethoca', 'ethoca', 'Verifi', 'verifi', 'chargeback', 'Chargeback', 'dispute', 'Dispute', 'pre-arb', 'representment', 'reason code', 'Alert'];
for (const kw of keywords) {
  const r = db.prepare(`
    SELECT COUNT(*) AS hits
    FROM orders
    WHERE client_id=6 AND is_chargeback=1 AND system_notes LIKE '%' || ? || '%'
  `).get(kw);
  console.log(`  "${kw}"  hits: ${r.hits}`);
}

// Return reason distribution on CBs
console.log('\n=== return_reason distribution on CBs (client 6) ===');
db.prepare(`
  SELECT COALESCE(return_reason, '(null)') AS reason, COUNT(*) AS cnt
  FROM orders
  WHERE client_id=6 AND is_chargeback=1
  GROUP BY reason
  ORDER BY cnt DESC
  LIMIT 15
`).all().forEach(r => console.log(`  ${r.reason.padEnd(50)} ${r.cnt}`));
