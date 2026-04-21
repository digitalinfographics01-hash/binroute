/**
 * Pull top decline reasons across all 5 subscription clients,
 * propose a category for each, and present the mapping for review.
 */
const { initDb, querySql } = require('../src/db/connection');

// Final categorization rules — confirmed by user 2026-04-10
const proposedCategories = [
  // SYSTEM ROUTING — fake declines (crm_routing_rule), already filtered
  { match: r => r === 'Prepaid Credit Cards Are Not Accepted', class: 'system_routing' },

  // CUSTOMER CANCELLED — explicit customer stop request (separate class)
  { match: r => /Customer requested stop/i.test(r), class: 'customer_cancelled' },

  // CUSTOMER INPUT (Bucket 1) — typos, hard customer state
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
  { match: r => /^P?IN tries exceeded$/i.test(r) || /tries exceeded/i.test(r), class: 'customer_input' },
  { match: r => /Incorrect PIN/i.test(r), class: 'customer_input' },

  // CUSTOMER FUNDS — timing-recoverable
  { match: r => /Insufficient funds/i.test(r), class: 'customer_funds' },
  { match: r => /insufficient funds for the payment/i.test(r), class: 'customer_funds' },
  { match: r => /Activity limit/i.test(r), class: 'customer_funds' },
  { match: r => /Daily threshold/i.test(r), class: 'customer_funds' },
  { match: r => /(Exceeds|over).*withdrawal limit/i.test(r), class: 'customer_funds' },
  { match: r => /Exceeds issuer withdrawal/i.test(r), class: 'customer_funds' },
  { match: r => /Hold.*call/i.test(r), class: 'customer_funds' },
  { match: r => /Over (limit|the limit)/i.test(r), class: 'customer_funds' },

  // ISSUER HARD — genuinely hard blocks
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

  // ISSUER SOFT — processor-dependent
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

  // GATEWAY ERROR — system/network/setup
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
];

function categorize(reason) {
  if (!reason) return 'unknown';
  for (const rule of proposedCategories) {
    if (rule.match(reason)) return rule.class;
  }
  return 'unknown';
}

(async () => {
  await initDb();

  // Pull top decline reasons across all 5 clients
  const reasons = querySql(`
    SELECT decline_reason, COUNT(*) as n
    FROM orders
    WHERE client_id IN (1,2,3,4,5)
      AND order_status = 7
      AND decline_reason IS NOT NULL
      AND decline_reason != ''
    GROUP BY decline_reason
    ORDER BY n DESC
    LIMIT 80
  `);

  console.log('='.repeat(90));
  console.log('TOP 80 DECLINE REASONS (clients 1-5) WITH PROPOSED CATEGORY');
  console.log('='.repeat(90));
  console.log();
  console.log('Count    | Class             | Decline Reason');
  console.log('-'.repeat(90));

  const classCounts = {};
  let totalCategorized = 0;
  let totalUnknown = 0;

  for (const r of reasons) {
    const cls = categorize(r.decline_reason);
    classCounts[cls] = (classCounts[cls] || 0) + r.n;
    if (cls === 'unknown') totalUnknown += r.n;
    else totalCategorized += r.n;
    console.log(
      String(r.n).padStart(8) + ' | ' +
      cls.padEnd(17) + ' | ' +
      r.decline_reason.substring(0, 60)
    );
  }

  console.log();
  console.log('='.repeat(90));
  console.log('CLASS DISTRIBUTION');
  console.log('='.repeat(90));
  Object.entries(classCounts).sort((a, b) => b[1] - a[1]).forEach(([cls, n]) => {
    const pct = ((n / (totalCategorized + totalUnknown)) * 100).toFixed(1);
    console.log('  ' + cls.padEnd(20) + ' ' + String(n).padStart(8) + '  (' + pct + '%)');
  });
  console.log();
  console.log('Total categorized: ' + totalCategorized);
  console.log('Total unknown:     ' + totalUnknown + ' (need explicit rules)');

  // Show all "unknown" reasons so we can add them
  if (totalUnknown > 0) {
    console.log();
    console.log('='.repeat(90));
    console.log('UNKNOWN reasons (need user input):');
    console.log('='.repeat(90));
    reasons.filter(r => categorize(r.decline_reason) === 'unknown').forEach(r => {
      console.log('  ' + String(r.n).padStart(8) + '  ' + r.decline_reason.substring(0, 80));
    });
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
