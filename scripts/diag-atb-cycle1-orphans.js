/**
 * ATB cycle 1 orphan diagnostic.
 *
 * Hypothesis: some ATB cycle 1 first-attempt orders don't have a matching
 * cycle 0 attempt 1 in our data, possibly because the original initial
 * happened BEFORE our import window started.
 *
 * For each ATB customer in a cycle 1 first attempt, check:
 *   - Do they have any cycle 0 orders in our data?
 *   - If yes, what was the outcome?
 *   - If no, when did the cycle 1 first attempt happen (early or recent)?
 */
const { initDb, querySql } = require('../src/db/connection');

(async () => {
  await initDb();

  const CLIENT_ID = 5;

  // Earliest order date for ATB (our data window start)
  const earliest = querySql(`
    SELECT MIN(acquisition_date) as first_date FROM orders WHERE client_id = ?
  `, [CLIENT_ID])[0].first_date;
  console.log('ATB earliest order date in our DB: ' + earliest);
  console.log();

  // Get all ATB customers who have a cycle 1 first attempt
  const cycle1Customers = querySql(`
    SELECT DISTINCT customer_id, MIN(acquisition_date) as first_cycle1_date
    FROM orders
    WHERE client_id = ?
      AND COALESCE(is_internal_test, 0) = 0
      AND derived_cycle = 1
      AND derived_attempt = 1
      AND customer_id IS NOT NULL
      AND customer_id != 0
    GROUP BY customer_id
  `, [CLIENT_ID]);
  console.log('Total unique customers with cycle 1 first attempt: ' + cycle1Customers.length);

  // For each, check if they have ANY cycle 0 order (any attempt)
  let hasCycle0Approved = 0;
  let hasCycle0Attempt1Approved = 0;
  let hasCycle0AttemptGTE2Approved = 0;
  let hasCycle0AllDeclined = 0;
  let noCycle0AtAll = 0;
  const noCycle0Samples = [];
  const earlyOrphans = [];
  const lateOrphans = [];

  for (const c of cycle1Customers) {
    const cycle0 = querySql(`
      SELECT
        derived_attempt,
        order_status,
        acquisition_date
      FROM orders
      WHERE client_id = ?
        AND customer_id = ?
        AND derived_cycle = 0
        AND COALESCE(is_internal_test, 0) = 0
      ORDER BY acquisition_date ASC
    `, [CLIENT_ID, c.customer_id]);

    if (cycle0.length === 0) {
      noCycle0AtAll++;
      if (noCycle0Samples.length < 10) {
        noCycle0Samples.push({ customer_id: c.customer_id, first_cycle1: c.first_cycle1_date });
      }
      // Check if cycle 1 fired early in our window (< 60 days from earliest)
      const earlyDate = new Date(earliest);
      earlyDate.setDate(earlyDate.getDate() + 60);
      if (new Date(c.first_cycle1_date) < earlyDate) {
        earlyOrphans.push(c);
      } else {
        lateOrphans.push(c);
      }
    } else {
      const approvedAttempts = cycle0.filter(o => [2,6,8,'2','6','8'].includes(o.order_status));
      if (approvedAttempts.length > 0) {
        hasCycle0Approved++;
        if (approvedAttempts.some(o => o.derived_attempt === 1)) {
          hasCycle0Attempt1Approved++;
        } else {
          hasCycle0AttemptGTE2Approved++;
        }
      } else {
        hasCycle0AllDeclined++;
      }
    }
  }

  console.log();
  console.log('Categorization:');
  console.log('  Has cycle 0 approved at attempt 1 (expected):       ' + hasCycle0Attempt1Approved);
  console.log('  Has cycle 0 approved at attempt ≥2 (reprocessing):  ' + hasCycle0AttemptGTE2Approved);
  console.log('  Has cycle 0 records but ALL declined (anomaly):     ' + hasCycle0AllDeclined);
  console.log('  Has NO cycle 0 records at all (orphan):             ' + noCycle0AtAll);
  console.log();
  console.log('Total accounted: ' + (hasCycle0Attempt1Approved + hasCycle0AttemptGTE2Approved + hasCycle0AllDeclined + noCycle0AtAll));
  console.log();

  if (noCycle0AtAll > 0) {
    console.log('Of the ' + noCycle0AtAll + ' orphans:');
    console.log('  Cycle 1 fired in first 60 days of data (likely missing initials): ' + earlyOrphans.length);
    console.log('  Cycle 1 fired later (real anomalies): ' + lateOrphans.length);
    console.log();
    console.log('Sample orphan customers (no cycle 0 records):');
    noCycle0Samples.forEach(s => console.log('  customer ' + s.customer_id + ' first cycle1: ' + s.first_cycle1));
  }

  process.exit(0);
})().catch(e => { console.error('FATAL:', e.message); console.error(e.stack); process.exit(1); });
