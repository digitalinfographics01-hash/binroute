const {initDb, getDb} = require('../src/db/connection');
(async () => {
  await initDb();
  const db = getDb();

  // Cascade chain coverage for VCT
  console.log('=== VCT Cascade Chain Coverage ===\n');

  const total = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE client_id = 6").get();
  const hasCascade = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE client_id = 6 AND cascade_chain IS NOT NULL AND cascade_chain != ''").get();
  const isCascaded = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE client_id = 6 AND is_cascaded = 1").get();
  const cascadedWithChain = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE client_id = 6 AND is_cascaded = 1 AND cascade_chain IS NOT NULL AND cascade_chain != ''").get();
  const cascadedNoChain = db.prepare("SELECT COUNT(*) as cnt FROM orders WHERE client_id = 6 AND is_cascaded = 1 AND (cascade_chain IS NULL OR cascade_chain = '')").get();

  console.log(`Total orders: ${total.cnt}`);
  console.log(`Has cascade_chain: ${hasCascade.cnt} (${(100*hasCascade.cnt/total.cnt).toFixed(1)}%)`);
  console.log(`is_cascaded=1: ${isCascaded.cnt} (${(100*isCascaded.cnt/total.cnt).toFixed(1)}%)`);
  console.log(`Cascaded WITH chain: ${cascadedWithChain.cnt}`);
  console.log(`Cascaded WITHOUT chain: ${cascadedNoChain.cnt}`);

  // Check what cascade_chain looks like
  console.log('\n=== Sample cascade chains ===');
  const samples = db.prepare("SELECT order_id, cascade_chain, is_cascaded, order_status, gateway_id FROM orders WHERE client_id = 6 AND cascade_chain IS NOT NULL AND cascade_chain != '' LIMIT 5").all();
  samples.forEach(s => {
    console.log(`Order ${s.order_id}: cascaded=${s.is_cascaded}, status=${s.order_status}, gw=${s.gateway_id}`);
    console.log(`  chain: ${s.cascade_chain}`);
  });

  // For non-cascaded orders, do we have gateway data?
  console.log('\n=== Gateway coverage ===');
  const gwCoverage = db.prepare(`
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN gateway_id IS NOT NULL THEN 1 ELSE 0 END) as has_gw,
      SUM(CASE WHEN processing_gateway_id IS NOT NULL THEN 1 ELSE 0 END) as has_proc_gw
    FROM orders WHERE client_id = 6
  `).get();
  console.log(gwCoverage);

  // Cascade chain by billing model
  console.log('\n=== Cascade stats by billing model ===');
  const byModel = db.prepare(`
    SELECT billing_model_name,
      COUNT(*) as total,
      SUM(CASE WHEN is_cascaded = 1 THEN 1 ELSE 0 END) as cascaded,
      SUM(CASE WHEN cascade_chain IS NOT NULL AND cascade_chain != '' THEN 1 ELSE 0 END) as has_chain,
      ROUND(100.0 * SUM(CASE WHEN is_cascaded = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) as cascade_pct
    FROM orders WHERE client_id = 6
    GROUP BY billing_model_name
  `).all();
  console.table(byModel);

  // Check retry_attempt distribution
  console.log('\n=== Retry attempt distribution ===');
  const retries = db.prepare(`
    SELECT retry_attempt, COUNT(*) as cnt,
      SUM(CASE WHEN order_status IN (2,6,8) THEN 1 ELSE 0 END) as approved
    FROM orders WHERE client_id = 6
    GROUP BY retry_attempt ORDER BY retry_attempt
  `).all();
  console.table(retries);

  // Compare to clients 1-5
  console.log('\n=== Cascade chain coverage across all clients ===');
  const allClients = db.prepare(`
    SELECT client_id,
      COUNT(*) as total,
      SUM(CASE WHEN is_cascaded = 1 THEN 1 ELSE 0 END) as cascaded,
      SUM(CASE WHEN cascade_chain IS NOT NULL AND cascade_chain != '' THEN 1 ELSE 0 END) as has_chain
    FROM orders GROUP BY client_id ORDER BY client_id
  `).all();
  console.table(allClients);
})();
