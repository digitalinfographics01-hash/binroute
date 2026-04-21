const {initDb, querySql} = require('../src/db/connection');
(async () => {
  await initDb();

  var sql = `
    SELECT
      CASE
        WHEN hours < 1 THEN '0_lt1h'
        WHEN hours < 6 THEN '1_1to6h'
        WHEN hours < 24 THEN '2_6to24h'
        WHEN hours < 168 THEN '3_1to7d'
        WHEN hours < 720 THEN '4_7to30d'
        ELSE '5_gt30d'
      END as gap,
      same_product,
      COUNT(*) as cnt
    FROM (
      SELECT
        (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 as hours,
        CASE WHEN a.main_product_id = b.main_product_id THEN 'same' ELSE 'diff' END as same_product
      FROM orders a
      JOIN orders b ON a.client_id=b.client_id AND a.customer_id=b.customer_id
        AND a.campaign_id=b.campaign_id AND a.billing_cycle=0 AND b.billing_cycle=0
        AND a.order_total > 0 AND b.order_total > 0 AND a.order_id < b.order_id
        AND (julianday(b.acquisition_date) - julianday(a.acquisition_date)) * 24 > 0.5
      WHERE a.client_id=6 AND a.is_test=0 AND COALESCE(a.is_internal_test,0)=0
        AND a.customer_id IS NOT NULL
        AND a.campaign_id NOT IN (29,73,41,64,77,13,86,88,100,101,102,98,79,82,84,37,49,7,12,3,11,60,4)
    )
    GROUP BY gap, same_product
    ORDER BY gap, same_product
  `;
  var r = querySql(sql);
  console.table(r);

  // Totals
  var totalSame = 0, totalDiff = 0;
  r.forEach(function(row) {
    if (row.same_product === 'same') totalSame += row.cnt;
    else totalDiff += row.cnt;
  });
  console.log("Total same product:", totalSame);
  console.log("Total diff product:", totalDiff);
})();
