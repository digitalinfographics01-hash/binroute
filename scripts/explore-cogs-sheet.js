// Download the COGS .xlsx via Drive API, list tabs, show first 20 rows of each.

const { google } = require('googleapis');
const XLSX = require('xlsx');

const CREDS = '/opt/binroute/secrets/sheets-credentials.json';
const COGS_FILE_ID = '1mkjZ4XVRf8AIarJ_xjbOhcPXYWkCmJOt';

(async () => {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDS,
    scopes: ['https://www.googleapis.com/auth/drive.readonly']
  });
  const drive = google.drive({ version: 'v3', auth });

  console.log('Downloading COGS xlsx...');
  const res = await drive.files.get(
    { fileId: COGS_FILE_ID, alt: 'media' },
    { responseType: 'arraybuffer' }
  );
  const buf = Buffer.from(res.data);
  console.log(`Downloaded ${buf.length} bytes`);

  const wb = XLSX.read(buf, { type: 'buffer' });
  console.log(`\nTabs (${wb.SheetNames.length}):`);
  wb.SheetNames.forEach(n => {
    const ws = wb.Sheets[n];
    const range = XLSX.utils.decode_range(ws['!ref'] || 'A1:A1');
    const rows = range.e.r + 1;
    const cols = range.e.c + 1;
    console.log(`  - "${n}" (rows=${rows}, cols=${cols})`);
  });

  for (const name of wb.SheetNames) {
    console.log(`\n=== Tab: "${name}" (first 20 rows) ===`);
    const rows = XLSX.utils.sheet_to_json(wb.Sheets[name], { header: 1, defval: '' });
    rows.slice(0, 20).forEach((r, i) => console.log(`R${i + 1}:`, JSON.stringify(r)));
  }
})().catch(e => { console.error('FAIL:', e.message, e.stack); process.exit(1); });
