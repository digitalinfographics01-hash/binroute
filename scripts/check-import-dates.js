const { initDb, querySql } = require('../src/db/connection');
(async () => {
  await initDb();
  const q = (sql) => querySql(sql);

  console.log('Total client 6 orders:', q("SELECT COUNT(*) as n FROM orders WHERE client_id=6")[0].n);

  console.log('\nOrders by month:');
  q("SELECT substr(acquisition_date, 1, 7) as month, COUNT(*) as n FROM orders WHERE client_id=6 GROUP BY month ORDER BY month")
    .forEach(r => console.log('  ' + r.month + ': ' + r.n));

  console.log('\nOrders Apr 8-9 by date:');
  q("SELECT date(acquisition_date) as d, COUNT(*) as n FROM orders WHERE client_id=6 AND acquisition_date >= '2026-04-08' GROUP BY d ORDER BY d")
    .forEach(r => console.log('  ' + r.d + ': ' + r.n));

  // Check: are orders with NULL acquisition_date?
  const nullDates = q("SELECT COUNT(*) as n FROM orders WHERE client_id=6 AND (acquisition_date IS NULL OR acquisition_date = '')")[0].n;
  console.log('\nNULL/empty acquisition_date:', nullDates);

  // Sample some recent orders to see what acquisition_date looks like
  console.log('\nSample orders (newest 10):');
  q("SELECT order_id, acquisition_date, order_status, billing_cycle FROM orders WHERE client_id=6 ORDER BY id DESC LIMIT 10")
    .forEach(r => console.log('  ' + r.order_id + ' acq=' + r.acquisition_date + ' status=' + r.order_status + ' cycle=' + r.billing_cycle));

  process.exit(0);
})();
