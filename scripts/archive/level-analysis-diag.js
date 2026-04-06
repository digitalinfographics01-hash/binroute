/**
 * Section 2 Diagnostic — Level analysis for top 5 BIN groups.
 * Shows variance at each level to determine optimal grouping.
 */
const { initializeDatabase } = require("../src/db/schema");
const { querySql } = require("../src/db/connection");

const VARIANCE_THRESHOLDS = {
  1: 5,   // L1→L2: 5pp between brands
  2: 5,   // L2→L3: 5pp between card types
  3: 5,   // L3→L4: 5pp between card levels
  4: 10,  // L4→L5: 10pp between individual BINs (outlier)
};

const CONFIDENCE = {
  1: { HIGH: 200, MEDIUM: 100, LOW: 50 },
  2: { HIGH: 150, MEDIUM: 75, LOW: 30 },
  3: { HIGH: 100, MEDIUM: 50, LOW: 30 },
  4: { HIGH: 75, MEDIUM: 40, LOW: 30 },
  5: { HIGH: 50, MEDIUM: 30, LOW: 0 },
};

function getConfidence(level, attempts) {
  const t = CONFIDENCE[level] || CONFIDENCE[2];
  if (attempts >= t.HIGH) return "HIGH";
  if (attempts >= t.MEDIUM) return "MEDIUM";
  if (attempts >= t.LOW) return "LOW";
  return "INSUFFICIENT";
}

async function run() {
  await initializeDatabase();

  // Get top 5 L1 groups (bank) by rebill volume
  console.log("=== LEVEL ANALYSIS DIAGNOSTIC — Top 5 BIN groups ===");
  console.log("TX: tp_rebill + tp_rebill_salvage | Active gateways only");
  console.log();

  const l1Groups = querySql(`
    SELECT b.issuer_bank as bank, COUNT(*) as att,
      COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END) as app,
      ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
        NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate,
      COUNT(DISTINCT o.cc_first_6) as bins
    FROM orders o
    LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
    JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
    WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage')
    AND o.is_test = 0 AND o.is_internal_test = 0
    AND o.order_status IN (2,6,7,8)
    AND COALESCE(g.exclude_from_analysis,0) != 1
    AND b.issuer_bank IS NOT NULL
    GROUP BY b.issuer_bank
    HAVING COUNT(*) >= 50
    ORDER BY att DESC LIMIT 5
  `);

  for (const l1 of l1Groups) {
    console.log("═══════════════════════════════════════════════════");
    console.log("L1: " + l1.bank);
    console.log("    " + l1.att + " att | " + l1.app + " app | " + l1.rate + "% | " + l1.bins + " BINs | conf=" + getConfidence(1, l1.att));
    console.log();

    // L2: Split by brand
    const l2Groups = querySql(`
      SELECT b.card_brand as brand, COUNT(*) as att,
        ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
          NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate,
        COUNT(DISTINCT o.cc_first_6) as bins
      FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
      JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
      WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
      AND o.order_status IN (2,6,7,8) AND COALESCE(g.exclude_from_analysis,0) != 1
      AND b.issuer_bank = '${l1.bank.replace(/'/g, "''")}'
      GROUP BY b.card_brand HAVING COUNT(*) >= 30
      ORDER BY att DESC
    `);

    const l2Rates = l2Groups.map(g => g.rate || 0);
    const l2Variance = l2Rates.length >= 2 ? Math.round((Math.max(...l2Rates) - Math.min(...l2Rates)) * 10) / 10 : 0;
    const l2Promote = l2Variance >= VARIANCE_THRESHOLDS[1] && l2Groups.length >= 2;

    console.log("  L2 — By Brand (" + l2Groups.length + " sub-groups, variance=" + l2Variance + "pp" + (l2Promote ? " → PROMOTE" : "") + ")");
    for (const g of l2Groups) {
      console.log("    " + (g.brand || "?").padEnd(15) + g.att + " att | " + g.rate + "% | " + g.bins + " BINs | " + getConfidence(2, g.att));
    }

    // For each L2, check L3 variance
    for (const l2 of l2Groups) {
      if (l2.att < 50) continue;

      const l3Groups = querySql(`
        SELECT b.card_type as ctype, b.is_prepaid as prep, COUNT(*) as att,
          ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
            NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate,
          COUNT(DISTINCT o.cc_first_6) as bins
        FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
        JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
        WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
        AND o.order_status IN (2,6,7,8) AND COALESCE(g.exclude_from_analysis,0) != 1
        AND b.issuer_bank = '${l1.bank.replace(/'/g, "''")}'
        AND b.card_brand = '${(l2.brand || "").replace(/'/g, "''")}'
        GROUP BY b.card_type, b.is_prepaid HAVING COUNT(*) >= 30
        ORDER BY att DESC
      `);

      const l3Rates = l3Groups.map(g => g.rate || 0);
      const l3Variance = l3Rates.length >= 2 ? Math.round((Math.max(...l3Rates) - Math.min(...l3Rates)) * 10) / 10 : 0;
      const l3Promote = l3Variance >= VARIANCE_THRESHOLDS[2] && l3Groups.length >= 2;

      if (l3Groups.length > 1) {
        console.log();
        console.log("  L3 — " + l1.bank + " · " + l2.brand + " by Type+Prepaid (variance=" + l3Variance + "pp" + (l3Promote ? " → PROMOTE" : "") + ")");
        for (const g of l3Groups) {
          const label = (g.ctype || "?") + (g.prep ? " PREPAID" : "");
          console.log("    " + label.padEnd(20) + g.att + " att | " + g.rate + "% | " + g.bins + " BINs | " + getConfidence(3, g.att));
        }
      }

      // For each L3, check L4 variance
      for (const l3 of l3Groups) {
        if (l3.att < 50) continue;

        const l4Groups = querySql(`
          SELECT b.card_level as clevel, COUNT(*) as att,
            ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
              NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate,
            COUNT(DISTINCT o.cc_first_6) as bins
          FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
          JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
          WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
          AND o.order_status IN (2,6,7,8) AND COALESCE(g.exclude_from_analysis,0) != 1
          AND b.issuer_bank = '${l1.bank.replace(/'/g, "''")}'
          AND b.card_brand = '${(l2.brand || "").replace(/'/g, "''")}'
          AND b.card_type = '${(l3.ctype || "").replace(/'/g, "''")}'
          AND b.is_prepaid = ${l3.prep || 0}
          GROUP BY b.card_level HAVING COUNT(*) >= 30
          ORDER BY att DESC
        `);

        const l4Rates = l4Groups.map(g => g.rate || 0);
        const l4Variance = l4Rates.length >= 2 ? Math.round((Math.max(...l4Rates) - Math.min(...l4Rates)) * 10) / 10 : 0;
        const l4Promote = l4Variance >= VARIANCE_THRESHOLDS[3] && l4Groups.length >= 2;

        if (l4Groups.length > 1) {
          const typeLabel = (l3.ctype || "?") + (l3.prep ? " PREPAID" : "");
          console.log();
          console.log("  L4 — " + l2.brand + " " + typeLabel + " by Card Level (variance=" + l4Variance + "pp" + (l4Promote ? " → PROMOTE" : "") + ")");
          for (const g of l4Groups) {
            console.log("    " + (g.clevel || "?").padEnd(25) + g.att + " att | " + g.rate + "% | " + g.bins + " BINs | " + getConfidence(4, g.att));
          }
        }

        // L5: Check for BIN outliers within each L4
        for (const l4 of l4Groups) {
          if (l4.att < 50) continue;

          const l5Bins = querySql(`
            SELECT o.cc_first_6 as bin, COUNT(*) as att,
              ROUND(COUNT(CASE WHEN o.order_status IN (2,6,8) THEN 1 END)*100.0/
                NULLIF(COUNT(CASE WHEN o.order_status IN (2,6,7,8) THEN 1 END),0),1) as rate
            FROM orders o LEFT JOIN bin_lookup b ON o.cc_first_6 = b.bin
            JOIN gateways g ON o.gateway_id = g.gateway_id AND g.client_id = 1
            WHERE o.tx_type IN ('tp_rebill','tp_rebill_salvage') AND o.is_test = 0 AND o.is_internal_test = 0
            AND o.order_status IN (2,6,7,8) AND COALESCE(g.exclude_from_analysis,0) != 1
            AND b.issuer_bank = '${l1.bank.replace(/'/g, "''")}'
            AND b.card_brand = '${(l2.brand || "").replace(/'/g, "''")}'
            AND b.card_type = '${(l3.ctype || "").replace(/'/g, "''")}'
            AND b.is_prepaid = ${l3.prep || 0}
            AND b.card_level = '${(l4.clevel || "").replace(/'/g, "''")}'
            GROUP BY o.cc_first_6 HAVING COUNT(*) >= 30
            ORDER BY att DESC
          `);

          const groupRate = l4.rate || 0;
          const outliers = l5Bins.filter(b => Math.abs((b.rate || 0) - groupRate) >= VARIANCE_THRESHOLDS[4]);

          if (outliers.length > 0) {
            console.log();
            console.log("  L5 OUTLIERS — " + (l4.clevel || "?") + " (group avg=" + groupRate + "%)");
            for (const o of outliers) {
              const diff = ((o.rate || 0) - groupRate).toFixed(1);
              const dir = diff > 0 ? "+" : "";
              console.log("    BIN " + o.bin + " | " + o.att + " att | " + o.rate + "% | " + dir + diff + "pp vs group | " + getConfidence(5, o.att));
            }
          }
        }
      }
    }
    console.log();
  }

  process.exit(0);
}
run();
