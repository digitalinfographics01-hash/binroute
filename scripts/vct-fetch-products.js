const {initDb, getDb} = require('../src/db/connection');
const StickyClient = require('../src/api/sticky-client');
(async () => {
  await initDb();
  const db = getDb();

  const client = db.prepare("SELECT * FROM clients WHERE id = 6").get();
  const sticky = new StickyClient({
    baseUrl: client.sticky_base_url,
    username: client.sticky_username,
    password: client.sticky_password
  });

  // Key product IDs to look up
  const productIds = [1, 1033, 13034, 7061, 697, 546, 749, 849, 28867, 7234, 28485, 13035, 568, 6695, 7062, 9411, 13459, 14878, 14879, 14940, 14992];

  console.log('=== VCT Product Names ===\n');
  for (const pid of productIds) {
    try {
      const result = await sticky._post('product_index', { product_id: pid });
      if (result && result.response_code === '100') {
        console.log(`Product ${pid}: ${result.product_name || '?'} — $${result.product_price || '?'} (category: ${result.product_category_name || 'none'})`);
      } else if (result && result.products) {
        result.products.forEach(p => console.log(`Product ${pid}: ${p.product_name || p.name || '?'} — $${p.price || '?'}`));
      } else {
        console.log(`Product ${pid}: ${JSON.stringify(result).substring(0, 150)}`);
      }
    } catch(e) {
      console.log(`Product ${pid}: FAILED — ${e.message}`);
    }
  }
})();
