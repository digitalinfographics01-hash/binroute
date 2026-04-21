// Import per-product COGS from the shared .xlsx "COGS sheet.xlsx", tab "COGS".
// Product-name keyed; case/whitespace normalized for matching.
// Also emits a diagnostic comparing COGS names vs actual product names in orders for VCT (client 6).

const { google } = require('googleapis');
const Database = require('better-sqlite3');
const XLSX = require('xlsx');

const DB_PATH = '/opt/binroute/data/binroute.db';
const CREDS = '/opt/binroute/secrets/sheets-credentials.json';
const COGS_FILE_ID = '1mkjZ4XVRf8AIarJ_xjbOhcPXYWkCmJOt';
// Priority order: canonical tab first. If a product only exists in a fallback tab,
// we still take it (maximizes coverage without overriding canonical values).
const COGS_TABS = [
  { name: 'Product URL + COGs', nameCol: 0, cogsCol: 1 },  // canonical per client
  { name: 'Sheet13',            nameCol: 0, cogsCol: 3, cogsColFallback: 2 }, // col D = final, col C = new
  { name: 'COGS',               nameCol: 0, cogsCol: 1 }
];
const CLIENT_ID = 6; // VCT

function normName(s) {
  if (s == null) return null;
  return String(s).trim().toLowerCase().replace(/\s+/g, ' ');
}

function parseCogs(v) {
  if (v == null || v === '' || v === '-') return null;
  if (typeof v === 'number') return isFinite(v) && v > 0 ? v : null;
  const s = String(v).replace(/[$,€£\s]/g, '');
  const n = parseFloat(s);
  return isNaN(n) || n <= 0 ? null : n;
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS product_cogs (
      product_name      TEXT NOT NULL,
      product_name_norm TEXT NOT NULL,
      cogs              REAL NOT NULL,
      source_tab        TEXT,
      source_row        INTEGER,
      imported_at       TEXT NOT NULL,
      PRIMARY KEY (product_name_norm)
    );
    CREATE INDEX IF NOT EXISTS idx_pcogs_name ON product_cogs(product_name);

    CREATE TABLE IF NOT EXISTS cogs_imports (
      file_id     TEXT NOT NULL,
      tab         TEXT NOT NULL,
      pulled_at   TEXT NOT NULL,
      rows_read   INTEGER,
      rows_upsert INTEGER,
      rows_skip   INTEGER,
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
    scopes: ['https://www.googleapis.com/auth/drive.readonly']
  });
  const drive = google.drive({ version: 'v3', auth });

  console.log(`Downloading COGS file ${COGS_FILE_ID}...`);
  const res = await drive.files.get(
    { fileId: COGS_FILE_ID, alt: 'media' },
    { responseType: 'arraybuffer' }
  );
  const buf = Buffer.from(res.data);
  console.log(`Downloaded ${buf.length} bytes`);

  const wb = XLSX.read(buf, { type: 'buffer' });

  const upsert = db.prepare(`
    INSERT INTO product_cogs (product_name, product_name_norm, cogs, source_tab, source_row, imported_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(product_name_norm) DO UPDATE SET
      product_name = excluded.product_name,
      cogs         = excluded.cogs,
      source_tab   = excluded.source_tab,
      source_row   = excluded.source_row,
      imported_at  = excluded.imported_at
  `);

  // Process tabs in priority order — canonical first. If a product is already in DB from
  // an earlier (higher-priority) tab, we DO NOT overwrite it with a lower-priority value.
  const seen = new Set();
  let grandUpserted = 0;

  const txForTab = db.transaction((batch, tabConf, startedAt) => {
    let up = 0, skip = 0, dup = 0;
    for (let i = 1; i < batch.length; i++) {
      const r = batch[i];
      const name = r[tabConf.nameCol] ? String(r[tabConf.nameCol]).trim() : null;
      let cogs = parseCogs(r[tabConf.cogsCol]);
      if (cogs == null && tabConf.cogsColFallback != null) cogs = parseCogs(r[tabConf.cogsColFallback]);
      if (!name || cogs == null) { skip++; continue; }
      const norm = normName(name);
      if (seen.has(norm)) { dup++; continue; }
      seen.add(norm);
      upsert.run(name, norm, cogs, tabConf.name, i + 1, startedAt);
      up++;
    }
    return { up, skip, dup };
  });

  for (const tabConf of COGS_TABS) {
    if (!wb.Sheets[tabConf.name]) {
      console.log(`Tab "${tabConf.name}" not found, skipping`);
      continue;
    }
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[tabConf.name], { header: 1, defval: '' });
    const { up, skip, dup } = txForTab(rows, tabConf, started);
    grandUpserted += up;
    console.log(`Tab "${tabConf.name}": ${up} upserted, ${skip} skipped(empty/bad), ${dup} already-seen`);
    db.prepare(`
      INSERT INTO cogs_imports (file_id, tab, pulled_at, rows_read, rows_upsert, rows_skip, status, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(COGS_FILE_ID, tabConf.name, started, rows.length - 1, up, skip, 'ok', null);
  }

  console.log(`\nGRAND TOTAL upserted: ${grandUpserted}`);

  // COGS stats
  const st = db.prepare(`SELECT COUNT(*) AS n, MIN(cogs) AS min_c, MAX(cogs) AS max_c, AVG(cogs) AS avg_c FROM product_cogs`).get();
  console.log(`COGS entries: ${st.n}  min=$${(st.min_c || 0).toFixed(2)}  max=$${(st.max_c || 0).toFixed(2)}  avg=$${(st.avg_c || 0).toFixed(2)}`);

  console.log(`\nBy source tab:`);
  db.prepare(`SELECT source_tab, COUNT(*) AS n FROM product_cogs GROUP BY source_tab ORDER BY n DESC`).all()
    .forEach(r => console.log(`  ${String(r.source_tab).padEnd(25)} ${r.n}`));

})().catch(e => { console.error('FAIL:', e.message); console.error(e.stack); process.exit(1); });
