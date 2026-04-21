// Import daily ad spend from Google Sheets 'Data' tab.
// ONLY pulls date + store + ad_cost. Everything else comes from Sticky.

const { google } = require('googleapis');
const Database = require('better-sqlite3');

const DB_PATH = '/opt/binroute/data/binroute.db';
const CREDS = '/opt/binroute/secrets/sheets-credentials.json';
const SHEET_ID = '1vCdMmYz4V0AzYJw7Kh9Dyb-J-xvYodA6DNRvPqpHN8U';
const TAB = 'Data';

function parseMoney(v) {
  if (v == null || v === '') return null;
  const s = String(v).replace(/[$,€£\s]/g, '');
  if (!s || s === '-') return null;
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function parseDate(v) {
  if (!v) return null;
  const s = String(v).trim();
  // Accept MM-DD-YYYY or M/D/YYYY -> YYYY-MM-DD
  const m = s.match(/^(\d{1,2})[-\/](\d{1,2})[-\/](\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ad_spend_daily_store (
      date        TEXT NOT NULL,
      store       TEXT NOT NULL,
      ad_cost     REAL NOT NULL,
      imported_at TEXT NOT NULL,
      source_row  INTEGER,
      PRIMARY KEY (date, store)
    );
    CREATE INDEX IF NOT EXISTS idx_ad_spend_date  ON ad_spend_daily_store(date);
    CREATE INDEX IF NOT EXISTS idx_ad_spend_store ON ad_spend_daily_store(store);

    CREATE TABLE IF NOT EXISTS ad_spend_imports (
      tab         TEXT NOT NULL,
      pulled_at   TEXT NOT NULL,
      rows_read   INTEGER,
      rows_upsert INTEGER,
      rows_skip   INTEGER,
      min_date    TEXT,
      max_date    TEXT,
      status      TEXT,
      error       TEXT
    );
  `);
}

(async () => {
  const started = new Date().toISOString();
  const db = new Database(DB_PATH);
  ensureSchema(db);

  const auth = new google.auth.GoogleAuth({
    keyFile: CREDS,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
  });
  const sheets = google.sheets({ version: 'v4', auth });

  console.log('Pulling tab:', TAB);
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${TAB}!A2:E10000`  // A=date, B=store, C=revenue, D=ROAS, E=AD-Cost
  });
  const rows = res.data.values || [];
  console.log('Rows fetched:', rows.length);

  const upsert = db.prepare(`
    INSERT INTO ad_spend_daily_store (date, store, ad_cost, imported_at, source_row)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(date, store) DO UPDATE SET
      ad_cost     = excluded.ad_cost,
      imported_at = excluded.imported_at,
      source_row  = excluded.source_row
  `);

  let upserted = 0, skipped = 0, minDate = null, maxDate = null;
  const skipSamples = [];

  const tx = db.transaction((batch) => {
    for (let i = 0; i < batch.length; i++) {
      const r = batch[i];
      const date = parseDate(r[0]);
      const store = r[1] ? String(r[1]).trim() : null;
      const adCost = parseMoney(r[4]);
      if (!date || !store || adCost == null) {
        skipped++;
        if (skipSamples.length < 5) skipSamples.push({ row: i + 2, raw: r.slice(0, 5) });
        continue;
      }
      upsert.run(date, store, adCost, started, i + 2);
      upserted++;
      if (!minDate || date < minDate) minDate = date;
      if (!maxDate || date > maxDate) maxDate = date;
    }
  });
  tx(rows);

  db.prepare(`
    INSERT INTO ad_spend_imports (tab, pulled_at, rows_read, rows_upsert, rows_skip, min_date, max_date, status, error)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(TAB, started, rows.length, upserted, skipped, minDate, maxDate, 'ok', null);

  console.log(`Upserted: ${upserted} | Skipped: ${skipped}`);
  console.log(`Date range: ${minDate} -> ${maxDate}`);
  if (skipSamples.length) console.log('Skip samples:', JSON.stringify(skipSamples));

  console.log('\n=== By store ===');
  const stores = db.prepare(`
    SELECT store, COUNT(*) AS days, ROUND(SUM(ad_cost), 2) AS total_ad_cost,
           MIN(date) AS min_date, MAX(date) AS max_date
    FROM ad_spend_daily_store
    GROUP BY store
    ORDER BY total_ad_cost DESC
  `).all();
  stores.forEach(s => {
    console.log(`  ${s.store.padEnd(20)} ${String(s.days).padStart(4)} days | $${(s.total_ad_cost || 0).toLocaleString()} | ${s.min_date} -> ${s.max_date}`);
  });

  const grand = db.prepare(`SELECT ROUND(SUM(ad_cost), 2) AS total, COUNT(*) AS rows FROM ad_spend_daily_store`).get();
  console.log(`\nTOTAL: $${(grand.total || 0).toLocaleString()} across ${grand.rows} rows`);
})().catch(e => { console.error('FAIL:', e.message); process.exit(1); });
