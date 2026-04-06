const { querySql, runSql, saveDb, transaction } = require('../db/connection');

/**
 * BIN Tiering Engine.
 * Tier 1: BINs covering top 80% of transaction volume → optimize first
 * Tier 2: next 15% → monitor
 * Tier 3: long tail → accumulate data, do not touch
 */

/**
 * Calculate BIN tiers for a client.
 * Groups by BIN (cc_first_6), counts total transactions, assigns tiers.
 */
function calculateBinTiers(clientId, windowDays = 90) {
  console.log(`[BinTiering] Calculating tiers for client ${clientId} (${windowDays}-day window)...`);

  // Get transaction volume per BIN within the analysis window
  const bins = querySql(`
    SELECT
      cc_first_6 as bin,
      COUNT(*) as volume
    FROM orders
    WHERE client_id = ?
      AND cc_first_6 IS NOT NULL AND cc_first_6 != ''
      AND acquisition_date >= date('now', '-' || ? || ' days')
    GROUP BY cc_first_6
    ORDER BY volume DESC
  `, [clientId, windowDays]);

  if (bins.length === 0) {
    console.log('[BinTiering] No BIN data found.');
    return { tiers: {}, bins: [] };
  }

  const totalVolume = bins.reduce((sum, b) => sum + b.volume, 0);
  let cumulative = 0;

  const tieredBins = bins.map(b => {
    cumulative += b.volume;
    const cumulativePct = cumulative / totalVolume;

    let tier;
    if (cumulativePct <= 0.80) {
      tier = 1;
    } else if (cumulativePct <= 0.95) {
      tier = 2;
    } else {
      tier = 3;
    }

    return {
      bin: b.bin,
      volume: b.volume,
      volumePct: (b.volume / totalVolume * 100).toFixed(2),
      cumulativePct: (cumulativePct * 100).toFixed(2),
      tier,
    };
  });

  const tierCounts = { 1: 0, 2: 0, 3: 0 };
  tieredBins.forEach(b => tierCounts[b.tier]++);

  console.log(`[BinTiering] Tier 1: ${tierCounts[1]} BINs, Tier 2: ${tierCounts[2]}, Tier 3: ${tierCounts[3]} (total volume: ${totalVolume})`);

  return {
    totalVolume,
    tierCounts,
    bins: tieredBins,
  };
}

module.exports = { calculateBinTiers };
