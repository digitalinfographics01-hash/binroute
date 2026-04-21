const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  console.log('=== Chase card type breakdown (all attempts) ===\n');
  const types = db.prepare(`
    SELECT card_type, is_prepaid, COUNT(*) as cnt,
      SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END) as approved,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND issuer_bank = 'JPMORGAN CHASE BANK N.A.'
    GROUP BY card_type, is_prepaid ORDER BY cnt DESC
  `).all();
  console.table(types);

  console.log('\n=== Chase rebills by card_type x initial_processor ===\n');
  const rebills = db.prepare(`
    SELECT card_type, is_prepaid, initial_processor, COUNT(*) as cnt,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'rebill'
      AND derived_product_role LIKE '%main%'
      AND issuer_bank = 'JPMORGAN CHASE BANK N.A.'
    GROUP BY card_type, is_prepaid, initial_processor
    HAVING cnt >= 30
    ORDER BY card_type, cnt DESC
  `).all();
  console.table(rebills);

  console.log('\n=== Chase initials by card_type ===\n');
  const initials = db.prepare(`
    SELECT card_type, is_prepaid, COUNT(*) as cnt,
      ROUND(100.0*SUM(CASE WHEN outcome = 'approved' THEN 1 ELSE 0 END)/COUNT(*),1) as appr_pct
    FROM transaction_attempts
    WHERE feature_version >= 3 AND model_target = 'initial'
      AND issuer_bank = 'JPMORGAN CHASE BANK N.A.'
    GROUP BY card_type, is_prepaid ORDER BY cnt DESC
  `).all();
  console.table(initials);
})();
