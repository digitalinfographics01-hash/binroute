// Count how many products have COGS data across the 4 relevant tabs in COGS sheet.xlsx.
// Tabs: "COGS" (A=name,B=cogs), "Product URL + COGs" (A=name,B=cogs), "Sheet13" (A=name,B=old,C=new,D=final), "Selling Price" (A=name,B=price).
// Goal: find the tab (or union) that gives maximum product-cogs coverage.

const { google } = require('googleapis');
const XLSX = require('xlsx');

const CREDS = '/opt/binroute/secrets/sheets-credentials.json';
const COGS_FILE_ID = '1mkjZ4XVRf8AIarJ_xjbOhcPXYWkCmJOt';

function normName(s) { return s == null ? null : String(s).trim().toLowerCase().replace(/\s+/g,' '); }
function parseCogs(v) {
  if (v == null || v === '' || v === '-') return null;
  if (typeof v === 'number') return (isFinite(v) && v > 0) ? v : null;
  const n = parseFloat(String(v).replace(/[$,€£\s]/g,''));
  return (isNaN(n) || n <= 0) ? null : n;
}

(async () => {
  const auth = new google.auth.GoogleAuth({keyFile: CREDS, scopes: ['https://www.googleapis.com/auth/drive.readonly']});
  const drive = google.drive({version: 'v3', auth});
  console.log('Downloading...');
  const res = await drive.files.get({fileId: COGS_FILE_ID, alt: 'media'}, {responseType: 'arraybuffer'});
  const wb = XLSX.read(Buffer.from(res.data), {type: 'buffer'});

  function extract(tab, nameCol, cogsCol, cogsCol2) {
    if (!wb.Sheets[tab]) return { tab, rows: [], error: 'missing' };
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[tab], {header: 1, defval: ''});
    const out = new Map();
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      const name = r[nameCol] ? String(r[nameCol]).trim() : null;
      if (!name) continue;
      let cogs = parseCogs(r[cogsCol]);
      if (cogs == null && cogsCol2 != null) cogs = parseCogs(r[cogsCol2]);
      if (cogs == null) continue;
      const norm = normName(name);
      if (!out.has(norm)) out.set(norm, { name, cogs });
    }
    return { tab, products: out };
  }

  const ext1 = extract('COGS', 0, 1);
  const ext2 = extract('Product URL + COGs', 0, 1);
  const ext3 = extract('Sheet13', 0, 3, 2); // col D = final COGS, fallback to col C (new)
  const ext4 = extract('Selling Price', 0, 1);  // this is selling price, not cogs — for cross-ref

  console.log(`\nTab coverage (products with non-null COGS):`);
  console.log(`  "COGS"              : ${ext1.products.size}`);
  console.log(`  "Product URL + COGs": ${ext2.products.size}`);
  console.log(`  "Sheet13"           : ${ext3.products.size}`);
  console.log(`  "Selling Price"     : ${ext4.products.size} (prices, not COGS)`);

  // Union of COGS across all 3 COGS tabs
  const union = new Map();
  // Priority: COGS tab first (author says canonical), then Sheet13 (has "final" column), then Product URL + COGs
  for (const m of [ext1.products, ext3.products, ext2.products]) {
    for (const [k, v] of m) if (!union.has(k)) union.set(k, v);
  }
  console.log(`\nUNION of all 3 COGS tabs: ${union.size} unique products`);

  // Compare COGS values where a product exists in multiple tabs
  let disagreements = 0;
  const disagreeSamples = [];
  for (const [k, v1] of ext1.products) {
    const v2 = ext2.products.get(k);
    const v3 = ext3.products.get(k);
    const values = [v1?.cogs, v2?.cogs, v3?.cogs].filter(x => x != null);
    const uniq = [...new Set(values)];
    if (uniq.length > 1) {
      disagreements++;
      if (disagreeSamples.length < 8) {
        disagreeSamples.push({name: v1.name, cogs_tab: v1?.cogs, url_tab: v2?.cogs, sheet13: v3?.cogs});
      }
    }
  }
  console.log(`\nProducts where COGS tabs disagree: ${disagreements}`);
  if (disagreeSamples.length) {
    console.log('Sample disagreements:');
    disagreeSamples.forEach(s => console.log(`  "${s.name.substring(0,60)}"  COGS=${s.cogs_tab}  URL+COGs=${s.url_tab}  Sheet13=${s.sheet13}`));
  }

  // How many in Product URL + COGs are NOT in canonical COGS tab?
  let onlyInUrl = 0;
  for (const k of ext2.products.keys()) if (!ext1.products.has(k)) onlyInUrl++;
  console.log(`\nProducts only in "Product URL + COGs" (not in canonical COGS tab): ${onlyInUrl}`);

  let onlyInS13 = 0;
  for (const k of ext3.products.keys()) if (!ext1.products.has(k)) onlyInS13++;
  console.log(`Products only in "Sheet13" (not in canonical COGS tab): ${onlyInS13}`);
})().catch(e => {console.error('FAIL:', e.message); process.exit(1);});
