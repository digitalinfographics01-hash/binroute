/**
 * Build the decline_reason_classes lookup table.
 *
 * 1. Create the table if it doesn't exist
 * 2. Pull all distinct decline_reasons from orders (all clients)
 * 3. Apply the categorization rules from our locked-in framework
 * 4. INSERT OR IGNORE — idempotent, safe to re-run
 * 5. Report what was added vs already present
 *
 * Final classes (6):
 *   system_decline    — fake declines (CRM routing + customer cancellations); never count
 *   customer_input    — Bucket 1: typos / hard customer state; block from retry
 *   customer_funds    — insufficient funds, daily/withdrawal limits; timing-recoverable
 *   issuer_soft       — Do Not Honor, 51, generic issuer declines; processor-recoverable
 *   issuer_hard       — Pick up L/S/NF, MCC blocks, 57/63 not allowed; real but hard
 *   gateway_error     — General error, network/system errors, auth failures
 */
const { initDb, querySql, runSql, transaction, saveDb } = require('../src/db/connection');

const RULES = [
  // SYSTEM DECLINES — never count, never train, never retry
  { match: r => r === 'Prepaid Credit Cards Are Not Accepted', class: 'system_decline' },
  { match: r => /Customer requested stop/i.test(r), class: 'system_decline' },
  { match: r => /customer has stopped the payment/i.test(r), class: 'system_decline' },

  // CUSTOMER INPUT (Bucket 1) — real declines, but unrecoverable; block from retry
  { match: r => /CVV2? Mismatch/i.test(r), class: 'customer_input' },
  { match: r => /Invalid CVV/i.test(r), class: 'customer_input' },
  { match: r => /Incorrect CVV/i.test(r), class: 'customer_input' },
  { match: r => /Declined for CVV/i.test(r), class: 'customer_input' },
  { match: r => /Invalid (Credit )?card number/i.test(r) || /^14\b.*INVALID/i.test(r), class: 'customer_input' },
  { match: r => /Invalid CC/i.test(r), class: 'customer_input' },
  { match: r => /^C2\b.*CVV/i.test(r), class: 'customer_input' },
  { match: r => /Expired card/i.test(r), class: 'customer_input' },
  { match: r => /^54\b.*EXPIRED/i.test(r), class: 'customer_input' },
  { match: r => /Account Closed/i.test(r) || /Closed Account/i.test(r), class: 'customer_input' },
  { match: r => /No such account/i.test(r), class: 'customer_input' },
  { match: r => /Account not recognized/i.test(r), class: 'customer_input' },
  { match: r => /No such issuer/i.test(r), class: 'customer_input' },
  { match: r => /not associated to a valid card issuing/i.test(r), class: 'customer_input' },
  { match: r => /No credit account/i.test(r), class: 'customer_input' },
  { match: r => /Invalid Billing Address/i.test(r), class: 'customer_input' },
  { match: r => /Invalid Zip/i.test(r), class: 'customer_input' },
  { match: r => /tries exceeded/i.test(r), class: 'customer_input' },
  { match: r => /Incorrect PIN/i.test(r), class: 'customer_input' },
  // Long-form variants from other processors
  { match: r => /No card number on file/i.test(r), class: 'customer_input' },
  { match: r => /Not a valid card number/i.test(r), class: 'customer_input' },
  { match: r => /card number is not valid/i.test(r), class: 'customer_input' },
  { match: r => /card number is invalid/i.test(r), class: 'customer_input' },
  { match: r => /Invalid card security code/i.test(r), class: 'customer_input' },
  { match: r => /CVV number is invalid/i.test(r), class: 'customer_input' },
  { match: r => /CID Verification/i.test(r), class: 'customer_input' },
  { match: r => /Card Type Verification/i.test(r), class: 'customer_input' },
  { match: r => /Verification Data Failed/i.test(r), class: 'customer_input' },
  { match: r => /Invalid expiration date/i.test(r), class: 'customer_input' },
  { match: r => /Expiration (year|month) .* is expired/i.test(r), class: 'customer_input' },
  { match: r => /card has expired/i.test(r), class: 'customer_input' },
  { match: r => /card account has been closed/i.test(r), class: 'customer_input' },
  { match: r => /No checking account/i.test(r), class: 'customer_input' },
  { match: r => /Incorrect payment information/i.test(r), class: 'customer_input' },

  // CUSTOMER FUNDS — timing-recoverable
  { match: r => /Insufficient funds/i.test(r), class: 'customer_funds' },
  { match: r => /insufficient funds for the payment/i.test(r), class: 'customer_funds' },
  { match: r => /Activity limit/i.test(r), class: 'customer_funds' },
  { match: r => /Daily threshold/i.test(r), class: 'customer_funds' },
  { match: r => /(Exceeds|over).*withdrawal limit/i.test(r), class: 'customer_funds' },
  { match: r => /Exceeds issuer withdrawal/i.test(r), class: 'customer_funds' },
  { match: r => /Hold.*call/i.test(r), class: 'customer_funds' },
  { match: r => /Over (limit|the limit)/i.test(r), class: 'customer_funds' },
  { match: r => /withdrawal frequency limit/i.test(r), class: 'customer_funds' },
  { match: r => /daily approval limit/i.test(r), class: 'customer_funds' },

  // ISSUER HARD — real declines, no processor change fixes
  { match: r => /Blocked.*first used/i.test(r), class: 'issuer_hard' },
  { match: r => /Pick up card.*[\s-](L|S|NF)$/i.test(r), class: 'issuer_hard' },
  { match: r => /Issuer Declined MCC/i.test(r), class: 'issuer_hard' },
  { match: r => /^57\b.*NOT ALLOWED/i.test(r), class: 'issuer_hard' },
  { match: r => /^63\b.*NOT ALLOWED/i.test(r), class: 'issuer_hard' },
  { match: r => /^No account/i.test(r), class: 'issuer_hard' },
  { match: r => /^1A\b.*ADD AUTH/i.test(r), class: 'issuer_hard' },
  { match: r => /Restricted card/i.test(r), class: 'issuer_hard' },
  { match: r => /Suspected fraud/i.test(r), class: 'issuer_hard' },
  { match: r => /REJECTED CONTACT CUST SERV/i.test(r), class: 'issuer_hard' },
  { match: r => /Credit card network does not allow/i.test(r), class: 'issuer_hard' },
  { match: r => /Transaction not permitted by issuer/i.test(r), class: 'issuer_hard' },
  { match: r => /Security violation/i.test(r), class: 'issuer_hard' },
  { match: r => /not allow this card to be charged/i.test(r), class: 'issuer_hard' },
  { match: r => /not allow this type of purchase/i.test(r), class: 'issuer_hard' },
  { match: r => /Call Voice Center/i.test(r), class: 'issuer_hard' },
  // Long-form variants
  { match: r => /Fraudulent card/i.test(r), class: 'issuer_hard' },
  { match: r => /^Lost card/i.test(r), class: 'issuer_hard' },
  { match: r => /reported as lost or stolen/i.test(r), class: 'issuer_hard' },
  { match: r => /Transaction not allowed\./i.test(r), class: 'issuer_hard' },
  { match: r => /transaction type is not permitted/i.test(r), class: 'issuer_hard' },
  { match: r => /internal rule that prevent/i.test(r), class: 'issuer_hard' },
  { match: r => /Risk Rules/i.test(r), class: 'issuer_hard' },
  { match: r => /restriction preventing approval/i.test(r), class: 'issuer_hard' },
  { match: r => /Violation, cannot complete/i.test(r), class: 'issuer_hard' },
  { match: r => /issuing bank indicated that the card number/i.test(r), class: 'issuer_hard' },

  // ISSUER SOFT — processor-dependent, salvage lookup territory
  { match: r => /Do Not Honor/i.test(r), class: 'issuer_soft' },
  { match: r => /^51\b.*DECLINED/i.test(r), class: 'issuer_soft' },
  { match: r => /^03\b.*DECLINED/i.test(r), class: 'issuer_soft' },
  { match: r => /^Issuer Declined$/i.test(r), class: 'issuer_soft' },
  { match: r => /Pick up card[\s-]+(SF|F)$/i.test(r), class: 'issuer_soft' },
  { match: r => /^Pick up card$/i.test(r), class: 'issuer_soft' },
  { match: r => /^Pick up card,/i.test(r), class: 'issuer_soft' },
  { match: r => /^Declined$/i.test(r) || /This transaction has been declined/i.test(r), class: 'issuer_soft' },
  { match: r => /Declined.*Contact.*issuer/i.test(r), class: 'issuer_soft' },
  { match: r => /Cardholder.*activate/i.test(r), class: 'issuer_soft' },
  { match: r => /Generic decline/i.test(r), class: 'issuer_soft' },
  // Long-form variants
  { match: r => /Transaction was declined by processor/i.test(r), class: 'issuer_soft' },
  { match: r => /Call issuer for further information/i.test(r), class: 'issuer_soft' },
  { match: r => /declined for an unknown reason/i.test(r), class: 'issuer_soft' },

  // GATEWAY ERROR — system, network, setup, auth
  { match: r => /General error/i.test(r), class: 'gateway_error' },
  { match: r => /Invalid merchant/i.test(r), class: 'gateway_error' },
  { match: r => /Authentication Failed/i.test(r), class: 'gateway_error' },
  { match: r => /Error Processing Transaction/i.test(r), class: 'gateway_error' },
  { match: r => /Unknown Response from Gateway/i.test(r), class: 'gateway_error' },
  { match: r => /Re-enter transaction/i.test(r), class: 'gateway_error' },
  { match: r => /Bad Bin or Host Disconnect/i.test(r), class: 'gateway_error' },
  { match: r => /Invalid amount/i.test(r) || /Amount Error/i.test(r), class: 'gateway_error' },
  { match: r => /Invalid transaction/i.test(r), class: 'gateway_error' },
  { match: r => /^12\b.*INVALID TRANSACTION/i.test(r), class: 'gateway_error' },
  { match: r => /Transaction not permitted by acquirer/i.test(r), class: 'gateway_error' },
  { match: r => /Invalid bankcard merchant/i.test(r), class: 'gateway_error' },
  { match: r => /^05\b.*SYSTEM_ERROR/i.test(r), class: 'gateway_error' },
  { match: r => /Terminal not programmed/i.test(r), class: 'gateway_error' },
  { match: r => /Issuer.*switch.*unavailable/i.test(r), class: 'gateway_error' },
  { match: r => /Issuer system malfunction/i.test(r), class: 'gateway_error' },
  { match: r => /communication|timeout|connection/i.test(r), class: 'gateway_error' },
  { match: r => /^91\b.*TIMEOUT/i.test(r), class: 'gateway_error' },
  { match: r => /Response Timeout/i.test(r), class: 'gateway_error' },
  { match: r => /^Error/i.test(r), class: 'gateway_error' },
  // Long-form variants from other processors
  { match: r => /Transaction was rejected by gateway/i.test(r), class: 'gateway_error' },
  { match: r => /Transaction error returned by processor/i.test(r), class: 'gateway_error' },
  { match: r => /No detail field found in error response/i.test(r), class: 'gateway_error' },
  { match: r => /<html>/i.test(r), class: 'gateway_error' },
  { match: r => /Application is not enabled/i.test(r), class: 'gateway_error' },
  { match: r => /not allowed to process sale transactions/i.test(r), class: 'gateway_error' },
  { match: r => /Invalid username or password/i.test(r), class: 'gateway_error' },
  { match: r => /^Unknown Error$/i.test(r), class: 'gateway_error' },
  { match: r => /Merchant account may be boarded/i.test(r), class: 'gateway_error' },
  { match: r => /I\/O error on POST/i.test(r), class: 'gateway_error' },
  { match: r => /merchant_identity.*required/i.test(r), class: 'gateway_error' },
  { match: r => /UNPROCESSABLE_ENTITY/i.test(r), class: 'gateway_error' },
  { match: r => /BAD_REQUEST/i.test(r), class: 'gateway_error' },
  { match: r => /INVALID_FIELD/i.test(r), class: 'gateway_error' },
  { match: r => /Token Error/i.test(r), class: 'gateway_error' },
  { match: r => /state field is required/i.test(r), class: 'gateway_error' },
  { match: r => /security code has never been passed/i.test(r), class: 'gateway_error' },
  { match: r => /Customer Vault/i.test(r), class: 'gateway_error' },
  { match: r => /Amount exceeds the maximum ticket/i.test(r), class: 'gateway_error' },
  { match: r => /maximum ticket allowed/i.test(r), class: 'gateway_error' },
];

function categorize(reason) {
  if (!reason) return null;
  for (const rule of RULES) {
    if (rule.match(reason)) return rule.class;
  }
  return null;
}

(async () => {
  await initDb();

  // 1. Create table if not exists
  console.log('Creating decline_reason_classes table if missing...');
  runSql(`
    CREATE TABLE IF NOT EXISTS decline_reason_classes (
      decline_reason TEXT PRIMARY KEY,
      decline_class  TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
  saveDb();

  // 2. How many existing rows?
  const existing = querySql('SELECT COUNT(*) n FROM decline_reason_classes')[0].n;
  console.log('  Existing rows: ' + existing);
  console.log();

  // 3. Pull all distinct decline_reasons from all clients
  const reasons = querySql(`
    SELECT decline_reason, COUNT(*) as n, COUNT(DISTINCT client_id) as clients
    FROM orders
    WHERE decline_reason IS NOT NULL AND decline_reason != ''
    GROUP BY decline_reason
    ORDER BY n DESC
  `);
  console.log('Distinct decline_reasons in orders: ' + reasons.length);

  // 4. Categorize and prepare insert rows
  const toInsert = [];
  let unknown = 0;
  const classCounts = {};

  for (const r of reasons) {
    const cls = categorize(r.decline_reason);
    if (!cls) { unknown++; continue; }
    classCounts[cls] = (classCounts[cls] || 0) + 1;
    toInsert.push({ reason: r.decline_reason, cls });
  }
  console.log('Categorized: ' + toInsert.length);
  console.log('Unknown:     ' + unknown);
  console.log();

  console.log('Class breakdown of distinct reasons:');
  Object.entries(classCounts).sort((a, b) => b[1] - a[1]).forEach(([c, n]) => {
    console.log('  ' + c.padEnd(20) + ' ' + String(n).padStart(4));
  });
  console.log();

  if (unknown > 0) {
    console.log('UNKNOWN reasons (will NOT be inserted):');
    for (const r of reasons) {
      if (!categorize(r.decline_reason)) {
        console.log('  ' + String(r.n).padStart(7) + '  ' + r.decline_reason.substring(0, 80));
      }
    }
    console.log();
  }

  // 5. Insert (idempotent — UPDATE on conflict so re-runs refresh the class)
  console.log('Inserting / updating ' + toInsert.length + ' rows...');
  let inserted = 0;
  let updated = 0;
  transaction(() => {
    for (const row of toInsert) {
      const before = querySql('SELECT decline_class FROM decline_reason_classes WHERE decline_reason = ?', [row.reason]);
      if (before.length === 0) {
        runSql(
          'INSERT INTO decline_reason_classes (decline_reason, decline_class) VALUES (?, ?)',
          [row.reason, row.cls]
        );
        inserted++;
      } else if (before[0].decline_class !== row.cls) {
        runSql(
          "UPDATE decline_reason_classes SET decline_class = ?, updated_at = datetime('now') WHERE decline_reason = ?",
          [row.cls, row.reason]
        );
        updated++;
      }
    }
  });
  saveDb();
  console.log('  Inserted: ' + inserted);
  console.log('  Updated:  ' + updated);
  console.log();

  // 6. Final verification
  const finalCount = querySql('SELECT COUNT(*) n FROM decline_reason_classes')[0].n;
  console.log('Final table size: ' + finalCount + ' rows');
  console.log();

  // 7. Sanity check: how many decline orders would now match a class?
  const orderCoverage = querySql(`
    SELECT COUNT(*) as total,
           SUM(CASE WHEN drc.decline_class IS NOT NULL THEN 1 ELSE 0 END) as classified
    FROM orders o
    LEFT JOIN decline_reason_classes drc ON drc.decline_reason = o.decline_reason
    WHERE o.client_id IN (1,2,3,4,5)
      AND o.order_status = 7
      AND o.decline_reason IS NOT NULL AND o.decline_reason != ''
  `)[0];
  console.log('Coverage on client 1-5 declined orders:');
  console.log('  Total:      ' + orderCoverage.total);
  console.log('  Classified: ' + orderCoverage.classified + ' (' + ((orderCoverage.classified / orderCoverage.total) * 100).toFixed(2) + '%)');
  console.log('  Unmatched:  ' + (orderCoverage.total - orderCoverage.classified));

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
