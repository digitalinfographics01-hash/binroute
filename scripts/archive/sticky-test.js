const https = require('https');
const querystring = require('querystring');

const BASE_URL = 'kytsanmanagementllc.sticky.io';
const USERNAME = 'anthropictest';
const PASSWORD = '6sYhs5sveUArXy';
const AUTH = Buffer.from(`${USERNAME}:${PASSWORD}`).toString('base64');

function postJSON(path, body) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(body);
    const options = {
      hostname: BASE_URL,
      path,
      method: 'POST',
      headers: {
        'Authorization': `Basic ${AUTH}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

function postForm(path, body) {
  return new Promise((resolve, reject) => {
    const postData = querystring.stringify(body);
    const options = {
      hostname: BASE_URL,
      path,
      method: 'POST',
      headers: {
        'Authorization': `Basic ${AUTH}`,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

const jf = (body) => postJSON('/api/v1/order_find', body);

function show(label, r) {
  const code = r.body?.response_code;
  const hit  = code === '100';
  const msg  = r.body?.error_message || r.body?.response_message || r.body?.decline_reason || r.body?.message || '';
  console.log(`  ${hit ? '✓ SUCCESS' : `[${code}]`} ${label}${msg ? ' → ' + msg : ''}`);
  if (hit) {
    console.log('\n\nFULL RESPONSE (first 3 orders):');
    const orders = r.body.data || r.body.orders || r.body;
    if (Array.isArray(orders)) {
      orders.slice(0, 3).forEach((o, i) => console.log(`\n--- Order ${i+1} ---\n`, JSON.stringify(o, null, 2)));
    } else {
      console.log(JSON.stringify(r.body, null, 2));
    }
    process.exit(0);
  }
  // Print full body for any novel code
  if (!['400','700','666','1004','330'].includes(code)) {
    console.log('    Full body:', JSON.stringify(r.body));
  }
  return r;
}

const SD = '01/01/2020';
const ED = '12/31/2026';

async function main() {
  console.log('='.repeat(70));
  console.log('order_find — broader search strategy');
  console.log('='.repeat(70));

  // ── 1. Active gateway IDs (172, 187-191) + wide date range ────────────────
  console.log('\n[1] Active gateway IDs + start_date/end_date (wide range)');
  for (const gid of [172, 187, 188, 189, 190, 191]) {
    show(`gateway_id=${gid}`,
      await jf({ gateway_id: gid, start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  }

  // ── 2. Active gateways without date filter ─────────────────────────────────
  console.log('\n[2] Active gateway IDs — no date filter');
  for (const gid of [172, 187, 188, 189, 190, 191]) {
    show(`gateway_id=${gid} no dates`,
      await jf({ gateway_id: gid, resultsPerPage: 10, page: 1 }));
  }

  // ── 3. Known order/customer details as filters ─────────────────────────────
  console.log('\n[3] Lookup by known customer/contact/email from order_view results');
  // From order 50001: customer_id=6376, email=barbarasparrish@gmail.com, contact_id=26322
  show('email exact',      await jf({ email_address: 'barbarasparrish@gmail.com', start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('customer_id=6376', await jf({ customer_id: 6376, start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('contact_id=26322', await jf({ contact_id: 26322, start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('email no dates',   await jf({ email_address: 'barbarasparrish@gmail.com', resultsPerPage: 10, page: 1 }));

  // ── 4. Known affiliate/afid ────────────────────────────────────────────────
  console.log('\n[4] Lookup by affiliate tracking (from orders: afid=JMB2S)');
  show('afid=JMB2S + dates',    await jf({ afid: 'JMB2S', start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('affiliate=JMB2S + dates', await jf({ affiliate: 'JMB2S', start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('afid no dates',          await jf({ afid: 'JMB2S', resultsPerPage: 10, page: 1 }));

  // ── 5. cc_first_6 (BIN) as filter ─────────────────────────────────────────
  console.log('\n[5] BIN lookup (cc_first_6 from known orders)');
  show('cc_first_6=511106',    await jf({ cc_first_6: '511106', start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('cc_first_6=546630',    await jf({ cc_first_6: '546630', start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));
  show('cc_type=master',       await jf({ cc_type: 'master',    start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 }));

  // ── 6. Try order_status as primary ────────────────────────────────────────
  console.log('\n[6] order_status only (no campaign, start_date/end_date)');
  for (const s of [7, 8, 1, 2, 3, 4, 5, 6]) {
    const r = await jf({ order_status: s, start_date: SD, end_date: ED, resultsPerPage: 10, page: 1 });
    show(`order_status=${s}`, r);
  }

  // ── 7. Newer date ranges — maybe only recent orders are indexed ────────────
  console.log('\n[7] Narrow recent date ranges');
  for (const [sd, ed] of [
    ['01/01/2024','03/31/2024'],
    ['04/01/2024','06/30/2024'],
    ['07/01/2024','12/31/2024'],
    ['01/01/2025','06/30/2025'],
    ['07/01/2025','12/31/2025'],
    ['01/01/2026','03/24/2026'],
  ]) {
    const r = await jf({ start_date: sd, end_date: ed, resultsPerPage: 10, page: 1 });
    show(`${sd}–${ed}`, r);
  }

  // ── 8. No filter at all variations ────────────────────────────────────────
  console.log('\n[8] Only pagination, no other fields (start_date/end_date removed)');
  show('resultsPerPage+page only',  await jf({ resultsPerPage: 10, page: 1 }));
  show('page only',                  await jf({ page: 1 }));
  show('no fields',                  await jf({}));

  // ── 9. Try order_find with form-encoded using start_date/end_date ──────────
  console.log('\n[9] Form-encoded start_date/end_date + gateway_id 172');
  show('form gateway=172',
    await postForm('/api/v1/order_find', { gateway_id: '172', start_date: SD, end_date: ED, resultsPerPage: '10', page: '1' }));
  show('form email lookup',
    await postForm('/api/v1/order_find', { email_address: 'barbarasparrish@gmail.com', start_date: SD, end_date: ED, resultsPerPage: '10', page: '1' }));
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
