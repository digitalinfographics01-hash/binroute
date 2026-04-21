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

  // Try v2 API - just first page to see product names
  try {
    const result = await sticky.productIndex();
    console.log(`Got ${result.products.length} products from v2 API\n`);

    // Look for our key product IDs
    const keyIds = new Set([1, 1033, 13034, 7061, 697, 546, 749, 849, 28867, 7234, 28485, 13035, 568, 6695, 7062, 9411, 13459, 14878, 14879, 14940, 14992].map(String));

    const found = result.products.filter(p => keyIds.has(String(p.product_id)));
    console.log(`Found ${found.length} of ${keyIds.size} key products:\n`);
    found.forEach(p => console.log(`  Product ${p.product_id}: ${p.product_name} — $${p.price} (SKU: ${p.sku})`));

    // Search for "reward" or "decline" in ALL product names
    console.log('\n=== Products with "reward" in name ===');
    const rewards = result.products.filter(p => p.product_name && p.product_name.toLowerCase().includes('reward'));
    rewards.forEach(p => console.log(`  Product ${p.product_id}: ${p.product_name} — $${p.price}`));
    console.log(`Total: ${rewards.length}`);

    console.log('\n=== Products with "decline" in name ===');
    const declines = result.products.filter(p => p.product_name && p.product_name.toLowerCase().includes('decline'));
    declines.forEach(p => console.log(`  Product ${p.product_id}: ${p.product_name} — $${p.price}`));
    console.log(`Total: ${declines.length}`);

    console.log('\n=== Products with "sub" or "recur" or "trial" in name ===');
    const subs = result.products.filter(p => p.product_name && (p.product_name.toLowerCase().includes('sub') || p.product_name.toLowerCase().includes('recur') || p.product_name.toLowerCase().includes('trial')));
    subs.slice(0, 20).forEach(p => console.log(`  Product ${p.product_id}: ${p.product_name} — $${p.price}`));
    console.log(`Total: ${subs.length}`);

    console.log('\n=== Products with "$0" or "free" or "test" in name ===');
    const free = result.products.filter(p => p.product_name && (p.product_name.toLowerCase().includes('free') || p.product_name.toLowerCase().includes('test') || p.price === '0.00' || p.price === '0'));
    free.slice(0, 20).forEach(p => console.log(`  Product ${p.product_id}: ${p.product_name} — $${p.price}`));
    console.log(`Total: ${free.length}`);

  } catch(e) {
    console.log('FAILED:', e.message);
  }
})();
