const { initDb, querySql } = require('../src/db/connection');
(async () => {
  await initDb();

  // Latest order per client
  const latest = querySql(`
    SELECT client_id, MAX(acquisition_date) as latest_order, COUNT(*) as total_orders
    FROM orders GROUP BY client_id ORDER BY client_id
  `);
  console.log('\n=== Latest orders per client ===');
  console.table(latest);

  // Last 3 days of imports per client
  const recent = querySql(`
    SELECT client_id, DATE(acquisition_date) as day, COUNT(*) as orders
    FROM orders
    WHERE acquisition_date >= DATE('now', '-3 days')
    GROUP BY client_id, DATE(acquisition_date)
    ORDER BY client_id, day DESC
  `);
  console.log('\n=== Recent daily imports ===');
  console.table(recent);

  // Check if scheduler_log exists
  try {
    const sched = querySql(`
      SELECT * FROM scheduler_log ORDER BY started_at DESC LIMIT 5
    `);
    console.log('\n=== Last 5 scheduler runs ===');
    console.table(sched);
  } catch (e) {
    // Try import_log instead
    try {
      const logs = querySql(`
        SELECT * FROM import_log ORDER BY id DESC LIMIT 10
      `);
      console.log('\n=== Last 10 import logs ===');
      console.table(logs);
    } catch (e2) {
      console.log('\nNo scheduler_log or import_log table found');
    }
  }

  process.exit(0);
})();
