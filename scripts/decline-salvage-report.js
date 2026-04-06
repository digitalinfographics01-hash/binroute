const Database = require('better-sqlite3');
const XLSX = require('xlsx');
const path = require('path');

const db = new Database(path.join(__dirname, '..', 'data', 'binroute.db'));

const banks = [
  { name: 'CAPITAL ONE, NATIONAL ASSOCIATION', brand: 'MASTERCARD', type: 'CREDIT', prepaid: 0 },
  { name: 'STRIDE BANK, NATIONAL ASSOCIATION', brand: 'VISA', type: 'DEBIT', prepaid: 0 },
  { name: 'SUTTON BANK', brand: 'VISA', type: 'DEBIT', prepaid: 1 },
  { name: 'THE BANCORP BANK, NATIONAL ASSOCIATION', brand: 'VISA', type: 'DEBIT', prepaid: 0 },
  { name: 'WELLS FARGO BANK, NATIONAL ASSOCIATION', brand: 'VISA', type: 'DEBIT', prepaid: 0 },
  { name: 'JPMORGAN CHASE BANK N.A. - DEBIT', brand: 'VISA', type: 'DEBIT', prepaid: 0 },
  { name: 'JPMORGAN CHASE BANK N.A.', brand: 'VISA', type: 'CREDIT', prepaid: 0 },
];

// Pick up card - SF is soft, all others are hard
const HARD_PICKUP = ['Pick up card - S', 'Pick up card - L', 'Pick up card - F', 'Pick up card - NF', 'Pick up card, Contact card issuer'];
const SOFT_PICKUP = ['Pick up card - SF'];

function classifyPickup(reason) {
  if (!reason) return null;
  if (SOFT_PICKUP.some(p => reason === p)) return 'SOFT (retryable)';
  if (HARD_PICKUP.some(p => reason === p)) return 'HARD (block)';
  return null;
}

function getTypeFilter(type) {
  return type ? "AND b.card_type = '" + type + "'" : '';
}

const wb = XLSX.utils.book_new();

// Sheet 1: Summary
const summaryData = [
  ['Bank', 'Brand', 'Type', 'Prepaid', 'C1 Att1 Rate', 'C1 Att2 Rate', 'C1 Att3 Rate', 'C1 Att4 Rate', 'Recommended Max Attempts', 'Verdict', 'Top Block Reasons']
];

// Sheet 2: Full decline breakdown
const declineData = [
  ['Bank', 'Brand', 'Type', 'Decline Reason', 'Category', 'Pickup Classification', 'Count', '% of Declines', 'Recommendation']
];

// Sheet 3: Salvage cycle
const salvageData = [
  ['Bank', 'Brand', 'Type', 'Attempt', 'Attempts', 'Approvals', 'Rate %', 'Cumulative Approvals', 'Marginal Value']
];

for (const bank of banks) {
  const label = bank.name + ' / ' + bank.brand + ' / ' + (bank.type || 'ALL') + (bank.prepaid ? ' / PREPAID' : '');
  const tf = getTypeFilter(bank.type);

  // Decline reasons
  const declines = db.prepare(`
    SELECT o.decline_reason, o.decline_category, COUNT(*) as c
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id != 3 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1
      AND o.order_status = 7
      AND b.issuer_bank = ? AND b.is_prepaid = ? AND b.card_brand = '${bank.brand}' ${tf}
    GROUP BY o.decline_reason, o.decline_category
    ORDER BY c DESC
  `).all(bank.name, bank.prepaid);

  const totalDec = declines.reduce((s, d) => s + d.c, 0);

  for (const d of declines) {
    const pct = totalDec > 0 ? Math.round(1000 * d.c / totalDec) / 10 : 0;
    const pickup = classifyPickup(d.decline_reason);
    let rec = '';
    if (d.decline_reason === 'Account Closed') rec = 'BLOCK - permanent';
    else if (HARD_PICKUP.includes(d.decline_reason)) rec = 'BLOCK - hard pickup';
    else if (d.decline_reason === 'Pick up card - SF') rec = 'RETRY - soft pickup';
    else if (d.decline_category === 'issuer' && d.decline_reason && d.decline_reason.includes('Insufficient')) rec = 'RETRY - may recover';
    else if (d.decline_category === 'issuer') rec = 'RETRY 1x - low recovery';
    else if (d.decline_category === 'processor') rec = 'BLOCK or FIX - processor issue';
    else if (d.decline_category === 'soft') rec = 'RETRY - temporary';
    else rec = 'REVIEW';

    declineData.push([
      bank.name, bank.brand, bank.type || 'ALL',
      d.decline_reason, d.decline_category || 'unknown',
      pickup || '', d.c, pct, rec
    ]);
  }

  // Salvage cycle
  const retries = db.prepare(`
    SELECT o.derived_attempt, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) as app,
      ROUND(100.0 * COUNT(CASE WHEN o.order_status IN (2,6,8) AND o.is_cascaded = 0 THEN 1 END) / NULLIF(COUNT(*), 0), 1) as rate
    FROM orders o
    JOIN bin_lookup b ON o.cc_first_6 = b.bin
    WHERE o.client_id != 3 AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.derived_product_role = 'main_rebill' AND o.derived_cycle = 1
      AND b.issuer_bank = ? AND b.is_prepaid = ? AND b.card_brand = '${bank.brand}' ${tf}
    GROUP BY o.derived_attempt
    ORDER BY o.derived_attempt
  `).all(bank.name, bank.prepaid);

  let cumApp = 0;
  let maxAttempts = 0;
  const blockReasons = [];

  for (const r of retries) {
    const prevCum = cumApp;
    cumApp += r.app;
    const marginal = r.app;
    salvageData.push([
      bank.name, bank.brand, bank.type || 'ALL',
      r.derived_attempt, r.att, r.app, r.rate, cumApp,
      marginal > 0 ? '+' + marginal : '0'
    ]);
    if (r.rate >= 1 && r.derived_attempt <= 6) maxAttempts = r.derived_attempt;
  }

  // If attempt 1 is already 0%, max attempts = 0 (BLOCK)
  const att1 = retries.find(r => r.derived_attempt === 1);
  if (!att1 || att1.rate < 1) maxAttempts = 0;

  // Determine verdict
  let verdict = '';
  if (maxAttempts === 0) verdict = 'BLOCK';
  else if (att1 && att1.rate < 5) verdict = 'VERY AGGRESSIVE PRICE DROP';
  else if (att1 && att1.rate < 10) verdict = 'AGGRESSIVE PRICE DROP';
  else if (att1 && att1.rate < 15) verdict = 'MODERATE PRICE DROP';
  else verdict = 'STANDARD';

  // Top block reasons
  const hardDeclines = declines.filter(d =>
    d.decline_reason === 'Account Closed' ||
    HARD_PICKUP.includes(d.decline_reason) ||
    (d.decline_reason && d.decline_reason.includes('Blocked, first used'))
  );
  const blockPct = totalDec > 0 ? Math.round(100 * hardDeclines.reduce((s, d) => s + d.c, 0) / totalDec) : 0;

  summaryData.push([
    bank.name, bank.brand, bank.type || 'ALL', bank.prepaid ? 'YES' : 'NO',
    retries[0] ? retries[0].rate : 0,
    retries[1] ? retries[1].rate : 0,
    retries[2] ? retries[2].rate : 0,
    retries[3] ? retries[3].rate : 0,
    maxAttempts, verdict,
    hardDeclines.map(d => d.decline_reason + ' (' + d.c + ')').join(', ')
  ]);
}

// Write sheets
const ws1 = XLSX.utils.aoa_to_sheet(summaryData);
const ws2 = XLSX.utils.aoa_to_sheet(declineData);
const ws3 = XLSX.utils.aoa_to_sheet(salvageData);

// Set column widths
ws1['!cols'] = [{ wch: 40 }, { wch: 12 }, { wch: 8 }, { wch: 8 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 12 }, { wch: 22 }, { wch: 28 }, { wch: 60 }];
ws2['!cols'] = [{ wch: 40 }, { wch: 12 }, { wch: 8 }, { wch: 60 }, { wch: 14 }, { wch: 18 }, { wch: 8 }, { wch: 12 }, { wch: 28 }];
ws3['!cols'] = [{ wch: 40 }, { wch: 12 }, { wch: 8 }, { wch: 8 }, { wch: 10 }, { wch: 10 }, { wch: 8 }, { wch: 18 }, { wch: 14 }];

XLSX.utils.book_append_sheet(wb, ws1, 'Summary');
XLSX.utils.book_append_sheet(wb, ws2, 'Decline Breakdown');
XLSX.utils.book_append_sheet(wb, ws3, 'Salvage Cycle');

const outPath = path.join(__dirname, '..', 'data', 'decline_salvage_report.xlsx');
XLSX.writeFile(wb, outPath);
console.log('Saved to', outPath);
console.log('Summary:', summaryData.length - 1, 'banks');
console.log('Decline reasons:', declineData.length - 1, 'rows');
console.log('Salvage cycles:', salvageData.length - 1, 'rows');

db.close();
