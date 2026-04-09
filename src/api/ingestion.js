const fs = require('fs');
const path = require('path');
const { initDb, runSql, querySql, queryOneSql, saveDb, closeDb, transaction, checkpointWal } = require('../db/connection');
const { initializeDatabase } = require('../db/schema');
const StickyClient = require('./sticky-client');

// Per-client paths to avoid conflicts during parallel imports
function getCheckpointPath(clientId) {
  return path.join(__dirname, '..', '..', `checkpoint_client${clientId}.json`);
}
function getLogPath(clientId) {
  return path.join(__dirname, '..', '..', `import_log_client${clientId}.txt`);
}
// Legacy paths (fallback for single-client compat)
const CHECKPOINT_PATH = path.join(__dirname, '..', '..', 'checkpoint.json');
const LOG_PATH = path.join(__dirname, '..', '..', 'import_log.txt');

function logImport(msg, logPath) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(logPath || LOG_PATH, line + '\n');
}

/**
 * Data ingestion engine.
 * Uses order_find with campaign_id="all" and return_type="order_view"
 * to pull all orders in a single paginated call — no per-campaign iteration.
 */
class DataIngestion {
  constructor(clientId) {
    this.clientId = clientId;
    this.client = null;
    this.stats = { orders: 0, gateways: 0, errors: 0 };
  }

  init() {
    const row = queryOneSql('SELECT * FROM clients WHERE id = ?', [this.clientId]);
    if (!row) throw new Error(`Client ${this.clientId} not found`);
    this.client = new StickyClient({
      baseUrl: row.sticky_base_url,
      username: row.sticky_username,
      password: row.sticky_password,
    });
    return this;
  }

  // ──────────────────────────────────────────────
  // GATEWAY SYNC
  // ──────────────────────────────────────────────

  async syncGateways(startId = 1) {
    console.log(`[Ingestion] Scanning gateways from ID ${startId} (dynamic, no upper limit)...`);

    const { gateways: rawGateways, highestId } = await this.client.scanGateways(startId);
    console.log(`[Ingestion] Found ${rawGateways.length} gateways, highest ID: ${highestId}`);

    transaction(() => {
      for (const gw of rawGateways) {
        const existing = queryOneSql(
          'SELECT lifecycle_state, gateway_active, gateway_descriptor FROM gateways WHERE client_id = ? AND gateway_id = ?',
          [this.clientId, gw.gateway_id]
        );

        if (existing) {
          runSql(`
            UPDATE gateways SET
              gateway_alias = ?, gateway_provider = ?, gateway_descriptor = ?,
              gateway_active = ?, gateway_type = ?, gateway_currency = ?,
              mid_group = ?, global_monthly_cap = ?, monthly_sales = ?,
              last_checked = datetime('now'), updated_at = datetime('now')
            WHERE client_id = ? AND gateway_id = ?
          `, [
            gw.gateway_alias || null, gw.gateway_provider || null,
            gw.gateway_descriptor || null,
            parseInt(gw.gateway_active, 10) || 0,
            gw.gateway_type || null, gw.gateway_currency || null,
            gw.mid_group || null,
            parseFloat(gw.global_monthly_cap) || null,
            parseFloat(gw.monthly_sales) || null,
            this.clientId, gw.gateway_id,
          ]);
        } else {
          runSql(`
            INSERT INTO gateways (
              client_id, gateway_id, gateway_alias, gateway_provider,
              gateway_descriptor, gateway_active, gateway_created,
              gateway_type, gateway_currency, mid_group,
              global_monthly_cap, monthly_sales, last_checked
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
          `, [
            this.clientId, gw.gateway_id,
            gw.gateway_alias || null, gw.gateway_provider || null,
            gw.gateway_descriptor || null,
            parseInt(gw.gateway_active, 10) || 0,
            gw.gateway_created || null,
            gw.gateway_type || null, gw.gateway_currency || null,
            gw.mid_group || null,
            parseFloat(gw.global_monthly_cap) || null,
            parseFloat(gw.monthly_sales) || null,
          ]);
        }

        // Detect closures: gateway_active=0 OR gateway_alias contains "closed"
        const isNowClosed = parseInt(gw.gateway_active, 10) === 0 ||
          (gw.gateway_alias && gw.gateway_alias.toLowerCase().includes('closed'));

        if (isNowClosed && existing && existing.lifecycle_state !== 'closed') {
          runSql('UPDATE gateways SET lifecycle_state = ? WHERE client_id = ? AND gateway_id = ?',
            ['closed', this.clientId, gw.gateway_id]);
          this._createAlert('P0', 'mid_closure', `MID Closed: Gateway ${gw.gateway_id}`,
            `Gateway ${gw.gateway_id} (${gw.gateway_descriptor || gw.gateway_alias}) is now closed.`,
            gw.gateway_id);
        } else if (!isNowClosed && !existing) {
          runSql('UPDATE gateways SET lifecycle_state = ? WHERE client_id = ? AND gateway_id = ?',
            ['ramp-up', this.clientId, gw.gateway_id]);
        } else if (!isNowClosed && existing && existing.lifecycle_state === 'ramp-up') {
          runSql('UPDATE gateways SET lifecycle_state = ? WHERE client_id = ? AND gateway_id = ?',
            ['active', this.clientId, gw.gateway_id]);
        }

        this.stats.gateways++;
      }
    });

    this._updateSyncState('gateway_scan');
    console.log(`[Ingestion] Synced ${this.stats.gateways} gateways.`);
    return rawGateways.length;
  }

  // ──────────────────────────────────────────────
  // CAMPAIGN SYNC
  // ──────────────────────────────────────────────

  async syncCampaigns() {
    console.log('[Ingestion] Syncing campaigns...');

    // Get all unique campaign_ids from orders
    const orderCamps = querySql(
      'SELECT DISTINCT campaign_id FROM orders WHERE client_id = ? AND campaign_id IS NOT NULL',
      [this.clientId]
    );
    const campIds = orderCamps.map(c => c.campaign_id).sort((a, b) => a - b);
    console.log(`[Ingestion] Found ${campIds.length} unique campaign IDs in orders`);

    let synced = 0, failed = 0;

    for (const campId of campIds) {
      try {
        const resp = await this.client.getCampaign(campId);
        if (!resp || resp.status !== 'SUCCESS' || !resp.data) {
          console.log(`[Ingestion] Campaign ${campId}: not found or error`);
          failed++;
          continue;
        }

        const c = resp.data;
        const existing = queryOneSql(
          'SELECT id FROM campaigns WHERE client_id = ? AND campaign_id = ?',
          [this.clientId, campId]
        );

        const name = c.name || null;
        const isActive = c.is_active ? 1 : 0;
        const gwId = c.gateway?.id || null;
        const isPaymentRouted = c.gateway?.account_id ? 1 : 0;

        if (existing) {
          runSql(
            "UPDATE campaigns SET campaign_name = ?, is_payment_routed = ?, gateway_ids = ?, updated_at = datetime('now') WHERE client_id = ? AND campaign_id = ?",
            [name, isPaymentRouted, gwId ? String(gwId) : null, this.clientId, campId]
          );
        } else {
          runSql(
            'INSERT INTO campaigns (client_id, campaign_id, campaign_name, is_payment_routed, gateway_ids) VALUES (?, ?, ?, ?, ?)',
            [this.clientId, campId, name, isPaymentRouted, gwId ? String(gwId) : null]
          );
        }
        synced++;
        if (synced % 10 === 0) console.log(`[Ingestion] Synced ${synced}/${campIds.length} campaigns...`);
      } catch (err) {
        console.log(`[Ingestion] Campaign ${campId} error: ${err.message}`);
        failed++;
      }
    }

    saveDb();
    console.log(`[Ingestion] Campaign sync complete: ${synced} synced, ${failed} failed`);
    return synced;
  }

  // ──────────────────────────────────────────────
  // FULL ORDER PULL — PAGINATED
  // campaign_id="all" + return_type="order_view"
  // Every page of order_find returns full order details inline.
  // No separate order_view calls needed.
  // ──────────────────────────────────────────────

  async pullTransactions(startDate, endDate, options = {}) {
    // Per-client paths to avoid conflicts during parallel imports
    const logPath = getLogPath(this.clientId);
    const cpPath = getCheckpointPath(this.clientId);
    const log = (msg) => logImport(msg, logPath);

    // Configurable options with safe defaults
    const DAY_CONCURRENCY = options.dayConcurrency || 3;
    const CHUNK_TARGET = options.chunkTarget || 400;
    const MAX_DAY_RETRIES = 3;

    log(`=== IMPORT START [client ${this.clientId}]: ${startDate} to ${endDate} ===`);
    log(`Config: dayConcurrency=${DAY_CONCURRENCY}, chunkTarget=${CHUNK_TARGET}`);

    const days = this._generateDayList(startDate, endDate);
    log(`Date range: ${days.length} days, day concurrency: ${DAY_CONCURRENCY}`);

    // Load checkpoint — skip verified days, retry partial/in_progress days
    const checkpoint = this._loadCheckpoint(cpPath, log);

    const pendingDays = days.filter(d => {
      const entry = checkpoint.days[d];
      // in_progress = crashed mid-day, re-fetch (INSERT OR IGNORE handles dupes)
      return !entry || entry.status !== 'verified';
    });
    const verifiedCount = days.length - pendingDays.length;
    log(`Pending: ${pendingDays.length} days (${verifiedCount} verified)`);

    // Progress tracking
    this.progress = {
      status: 'running', startedAt: new Date().toISOString(),
      totalDays: days.length, completedDays: verifiedCount,
      verifiedDays: verifiedCount, partialDays: 0, failedDays: 0,
      totalOrdersFetched: 0, totalOrdersSaved: 0, totalApiCalls: 0,
      totalChunksCompleted: 0, totalChunksFailed: 0,
      currentDay: null, ordersPerSecond: 0,
    };
    const progressStart = Date.now();

    // onChunk callback — writes each chunk to DB immediately (async-safe)
    const onChunk = async (orders) => {
      if (orders.length === 0) return 0;
      const t = Date.now();
      const saved = this._saveOrderBatchToDB(orders);
      const saveMs = Date.now() - t;
      this.progress.totalOrdersSaved += saved;
      // Warn on slow DB writes (potential lock contention)
      if (saveMs > 2000) {
        log(`  WARNING: DB write took ${saveMs}ms for ${orders.length} orders (possible lock contention)`);
      }
      return saved;
    };

    // Worker pool — process DAY_CONCURRENCY days at a time
    const queue = [...pendingDays];
    const workers = Array(Math.min(DAY_CONCURRENCY, queue.length)).fill(null).map(async () => {
      while (queue.length > 0) {
        const day = queue.shift();
        this.progress.currentDay = day;

        const retries = (checkpoint.days[day]?.retries || 0);
        if (retries >= MAX_DAY_RETRIES) {
          log(`  ${day}: skipped (${retries} retries exhausted)`);
          this.progress.failedDays++;
          continue;
        }

        try {
          const result = await this._fetchAndVerifyDay(day, log, {
            chunkTarget: CHUNK_TARGET,
            onChunk,
            checkpoint,
            cpPath,
          });

          this.progress.totalApiCalls += result.apiCalls;
          this.progress.totalChunksCompleted += result.chunksSaved || 0;
          this.progress.totalChunksFailed += result.chunksFailed || 0;

          // Verification: compare fetched orders vs API probe total.
          // We use totalFetched (orders received from API) not DB date count,
          // because acquisition_date may differ from the API's create date
          // (e.g., rebills have acquisition_date = original subscription date).
          const fetched = result.totalFetched || 0;
          const coverage = result.apiTotal > 0
            ? Math.min(1, fetched / result.apiTotal)
            : 1;

          if (coverage >= 0.98) {
            checkpoint.days[day] = {
              status: 'verified', api_total: result.apiTotal,
              fetched, saved: result.totalSaved || 0,
              retries, completed_at: new Date().toISOString()
            };
            this.progress.verifiedDays++;
            log(`  ${day}: verified ${fetched}/${result.apiTotal} fetched (${(coverage * 100).toFixed(1)}%), ${result.totalSaved || 0} saved`);
          } else {
            checkpoint.days[day] = {
              status: 'partial', api_total: result.apiTotal,
              fetched, saved: result.totalSaved || 0,
              retries: retries + 1, completed_at: new Date().toISOString()
            };
            this.progress.partialDays++;
            log(`  ${day}: PARTIAL ${fetched}/${result.apiTotal} fetched (${(coverage * 100).toFixed(1)}%) — will retry`);
          }
        } catch (err) {
          checkpoint.days[day] = {
            status: 'failed', retries: retries + 1,
            error: err.message, completed_at: new Date().toISOString()
          };
          this.progress.failedDays++;
          log(`  ${day}: FAILED - ${err.message}`);
        }

        this.progress.completedDays++;

        // Update throughput
        const elapsedSec = (Date.now() - progressStart) / 1000;
        this.progress.ordersPerSecond = elapsedSec > 0
          ? Math.round(this.progress.totalOrdersSaved / elapsedSec)
          : 0;

        // Save checkpoint after each day
        this._saveCheckpoint(cpPath, checkpoint);
      }
    });

    await Promise.all(workers);

    // Retry partial days (second pass) — same per-chunk pattern
    const partialDays = days.filter(d => checkpoint.days[d]?.status === 'partial');
    if (partialDays.length > 0) {
      log(`Retrying ${partialDays.length} partial days...`);
      for (const day of partialDays) {
        try {
          const result = await this._fetchAndVerifyDay(day, log, {
            chunkTarget: CHUNK_TARGET,
            onChunk,
            checkpoint,
            cpPath,
          });
          const fetched = result.totalFetched || 0;
          const coverage = result.apiTotal > 0 ? Math.min(1, fetched / result.apiTotal) : 1;
          checkpoint.days[day].fetched = fetched;
          checkpoint.days[day].saved = result.totalSaved || 0;
          checkpoint.days[day].retries++;
          if (coverage >= 0.98) {
            checkpoint.days[day].status = 'verified';
            log(`  ${day}: retry verified ${fetched}/${result.apiTotal} fetched, ${result.totalSaved || 0} saved`);
          } else {
            log(`  ${day}: retry still partial ${fetched}/${result.apiTotal} (${(coverage * 100).toFixed(1)}%)`);
          }
        } catch (err) {
          checkpoint.days[day].retries++;
          log(`  ${day}: retry FAILED - ${err.message}`);
        }
        this._saveCheckpoint(cpPath, checkpoint);
      }
    }

    // Final summary
    const finalCount = this._getDBCount();
    const verified = days.filter(d => checkpoint.days[d]?.status === 'verified').length;
    const partial = days.filter(d => checkpoint.days[d]?.status === 'partial').length;
    const failed = days.filter(d => checkpoint.days[d]?.status === 'failed').length;
    const elapsedTotal = ((Date.now() - progressStart) / 1000).toFixed(1);

    log(`=== IMPORT COMPLETE ===`);
    log(`Days: ${verified} verified, ${partial} partial, ${failed} failed | DB: ${finalCount} orders | Time: ${elapsedTotal}s`);
    log(`Throughput: ${this.progress.ordersPerSecond} orders/sec, ${this.progress.totalApiCalls} API calls`);

    this.progress.status = partial > 0 || failed > 0 ? 'completed_with_gaps' : 'completed';

    // Keep checkpoint for audit (rename, don't delete)
    if (fs.existsSync(cpPath)) {
      const auditPath = cpPath.replace('.json', `_done_${Date.now()}.json`);
      try { fs.renameSync(cpPath, auditPath); } catch { /* ignore */ }
    }

    this._updateSyncState('full_pull');
  }

  /**
   * Fetch all orders for a single day using queue-based time chunking.
   *
   * When options.onChunk is provided (async callback):
   *   - Each chunk's orders are passed to onChunk immediately (no accumulation)
   *   - Returns { apiTotal, apiCalls, chunksSaved, chunksFailed }
   * When options.onChunk is absent (backward compat for pullUpdated):
   *   - Accumulates all orders in memory and returns them
   *   - Returns { orders: [...], apiTotal, apiCalls }
   */
  async _fetchAndVerifyDay(day, log, options = {}) {
    const { chunkTarget = 400, onChunk = null, checkpoint = null, cpPath = null } = options;
    const streaming = typeof onChunk === 'function';

    // Step 1: Probe to get total_orders
    const probe = await this._orderFindTimeRange(day, '00:00:00', '23:59:59');
    if (!probe.data || probe.data.response_code !== '100') {
      if (probe.data?.response_code === '200') {
        return streaming ? { apiTotal: 0, apiCalls: 1, chunksSaved: 0, chunksFailed: 0, totalFetched: 0, totalSaved: 0 } : { orders: [], apiTotal: 0, apiCalls: 1 };
      }
      throw new Error(`Probe failed: code=${probe.data?.response_code}`);
    }

    const apiTotal = parseInt(probe.data.total_orders || 0, 10);
    let apiCalls = 1;

    // Step 2: If ≤450 orders, the probe response has them all
    if (apiTotal <= 450) {
      const orders = this.client.parseOrdersFromResponse(probe.data);
      if (streaming) {
        const saved = await onChunk(orders);
        return { apiTotal, apiCalls, chunksSaved: 1, chunksFailed: 0, totalFetched: orders.length, totalSaved: saved || orders.length };
      }
      return { orders, apiTotal, apiCalls };
    }

    // Step 3: Pre-compute time chunks
    const numChunks = Math.ceil(apiTotal / chunkTarget);
    const minutesPerChunk = Math.floor(1440 / numChunks);
    const chunks = [];
    for (let i = 0; i < numChunks; i++) {
      const startMin = i * minutesPerChunk;
      const endMin = (i === numChunks - 1) ? 1439 : ((i + 1) * minutesPerChunk - 1);
      chunks.push({
        startTime: this._minutesToTime(startMin),
        endTime: this._minutesToTime(endMin).replace(/:00$/, ':59'),
      });
    }

    log(`  ${day}: ${apiTotal} orders → ${numChunks} chunks (~${Math.round(apiTotal / numChunks)} each)`);

    // Step 4: Process chunks sequentially (rate limiter handles pacing)
    const allOrders = streaming ? null : [];
    const retryQueue = [];
    let chunksSaved = 0;
    let chunksFailed = 0;
    let totalSaved = 0;
    let totalFetched = 0;

    for (let ci = 0; ci < chunks.length; ci++) {
      const chunk = chunks[ci];
      const fetchStart = Date.now();
      const result = await this._orderFindTimeRange(day, chunk.startTime, chunk.endTime);
      const fetchMs = Date.now() - fetchStart;
      apiCalls++;

      if (!result.data || result.data.response_code !== '100') {
        if (result.data?.response_code === '200') { chunksSaved++; continue; }
        retryQueue.push(chunk);
        chunksFailed++;
        continue;
      }

      const chunkTotal = parseInt(result.data.total_orders || 0, 10);
      const orders = this.client.parseOrdersFromResponse(result.data);

      if (streaming) {
        // Write to DB immediately via callback (await for async safety)
        const saveStart = Date.now();
        const saved = await onChunk(orders);
        const saveMs = Date.now() - saveStart;
        totalSaved += (saved || orders.length);
        totalFetched += orders.length;
        chunksSaved++;
        log(`    ${day}: chunk ${ci + 1}/${chunks.length}, ${orders.length} fetched, fetch ${fetchMs}ms, save ${saveMs}ms, total ${totalSaved}`);

        // Checkpoint every 5 chunks (commit-then-checkpoint)
        if (checkpoint && cpPath && chunksSaved % 5 === 0) {
          checkpoint.days[day] = {
            status: 'in_progress', api_total: apiTotal,
            chunks_total: chunks.length, chunks_done: chunksSaved,
            updated_at: new Date().toISOString()
          };
          this._saveCheckpoint(cpPath, checkpoint);
        }

        // WAL checkpoint every 10 chunks to keep WAL file manageable
        if (chunksSaved % 10 === 0) {
          try { checkpointWal(); } catch { /* ignore WAL checkpoint errors */ }
        }
      } else {
        allOrders.push(...orders);
      }

      // If chunk hit the 500 cap, subdivide and re-queue
      if (chunkTotal > 500 && orders.length >= 500) {
        const startMin = this._timeToMinutes(chunk.startTime);
        const endMin = this._timeToMinutes(chunk.endTime);
        const midMin = Math.floor((startMin + endMin) / 2);
        if (midMin > startMin && midMin < endMin) {
          retryQueue.push(
            { startTime: chunk.startTime, endTime: this._minutesToTime(midMin).replace(/:00$/, ':59') },
            { startTime: this._minutesToTime(midMin + 1), endTime: chunk.endTime }
          );
        }
      }
    }

    // Step 5: Process retry queue (subdivided chunks)
    for (const chunk of retryQueue) {
      const fetchStart = Date.now();
      const result = await this._orderFindTimeRange(day, chunk.startTime, chunk.endTime);
      const fetchMs = Date.now() - fetchStart;
      apiCalls++;
      if (result.data && result.data.response_code === '100') {
        const orders = this.client.parseOrdersFromResponse(result.data);

        if (streaming) {
          const saveStart = Date.now();
          const saved = await onChunk(orders);
          const saveMs = Date.now() - saveStart;
          totalSaved += (saved || orders.length);
          totalFetched += orders.length;
          chunksSaved++;
          log(`    ${day}: retry chunk, ${orders.length} fetched, fetch ${fetchMs}ms, save ${saveMs}ms, total ${totalSaved}`);

          if (chunksSaved % 10 === 0) {
            try { checkpointWal(); } catch { /* ignore */ }
          }
        } else {
          allOrders.push(...orders);
        }

        // If STILL hitting cap, split again
        const chunkTotal = parseInt(result.data.total_orders || 0, 10);
        if (chunkTotal > 500 && orders.length >= 500) {
          const startMin = this._timeToMinutes(chunk.startTime);
          const endMin = this._timeToMinutes(chunk.endTime);
          const midMin = Math.floor((startMin + endMin) / 2);
          if (midMin > startMin && midMin < endMin) {
            for (const subChunk of [
              { startTime: chunk.startTime, endTime: this._minutesToTime(midMin).replace(/:00$/, ':59') },
              { startTime: this._minutesToTime(midMin + 1), endTime: chunk.endTime }
            ]) {
              const subResult = await this._orderFindTimeRange(day, subChunk.startTime, subChunk.endTime);
              apiCalls++;
              if (subResult.data && subResult.data.response_code === '100') {
                const subOrders = this.client.parseOrdersFromResponse(subResult.data);
                if (streaming) {
                  const saved = await onChunk(subOrders);
                  totalSaved += (saved || subOrders.length);
                  totalFetched += subOrders.length;
                  chunksSaved++;
                } else {
                  allOrders.push(...subOrders);
                }
              }
            }
          }
        }
      }
    }

    if (streaming) {
      return { apiTotal, apiCalls, chunksSaved, chunksFailed, totalFetched, totalSaved };
    }
    return { orders: allOrders, apiTotal, apiCalls };
  }

  /**
   * Make a single order_find API call for a day + time range.
   */
  async _orderFindTimeRange(day, startTime, endTime) {
    const data = await this.client._post('order_find', {
      campaign_id: 'all',
      start_date: day, end_date: day,
      start_time: startTime, end_time: endTime,
      date_type: 'create', criteria: 'all', search_type: 'all',
      return_type: 'order_view', results_per_page: 5000, page: 1
    });
    return { data };
  }

  /**
   * Generate a list of single-day date strings (MM/DD/YYYY) between start and end inclusive.
   */
  _generateDayList(startDate, endDate) {
    const [sm, sd, sy] = startDate.split('/').map(Number);
    const [em, ed, ey] = endDate.split('/').map(Number);
    const start = new Date(sy, sm - 1, sd);
    const end = new Date(ey, em - 1, ed);
    const days = [];
    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const mm = String(d.getMonth() + 1).padStart(2, '0');
      const dd = String(d.getDate()).padStart(2, '0');
      days.push(`${mm}/${dd}/${d.getFullYear()}`);
    }
    return days;
  }

  _timeToMinutes(time) {
    const [h, m] = time.split(':').map(Number);
    return h * 60 + m;
  }

  _minutesToTime(minutes) {
    const h = String(Math.floor(minutes / 60)).padStart(2, '0');
    const m = String(minutes % 60).padStart(2, '0');
    return `${h}:${m}:00`;
  }

  /**
   * Load checkpoint from file. Returns { days: { ... } } with per-day status.
   * Migrates old format (flat completed_days array) to new format.
   */
  _loadCheckpoint(cpPath, log) {
    if (!fs.existsSync(cpPath)) return { days: {} };
    try {
      const raw = JSON.parse(fs.readFileSync(cpPath, 'utf8'));
      // New format: { days: { "10/04/2025": { status, api_total, db_count } } }
      if (raw.days && typeof raw.days === 'object') {
        const verified = Object.values(raw.days).filter(d => d.status === 'verified').length;
        log(`Checkpoint loaded: ${verified} verified days`);
        return raw;
      }
      // Old format: { completed_days: ["10/04/2025", ...] }
      if (raw.completed_days) {
        const days = {};
        for (const d of raw.completed_days) {
          days[d] = { status: 'verified', api_total: null, db_count: null, retries: 0 };
        }
        log(`Checkpoint migrated: ${raw.completed_days.length} days from old format`);
        return { days };
      }
      return { days: {} };
    } catch { return { days: {} }; }
  }

  /**
   * Save checkpoint atomically (write to temp, then rename).
   */
  _saveCheckpoint(cpPath, checkpoint) {
    const tmpPath = cpPath + '.tmp';
    fs.writeFileSync(tmpPath, JSON.stringify(checkpoint, null, 2));
    fs.renameSync(tmpPath, cpPath);
  }

  /**
   * Save a batch of raw orders to DB inside a transaction.
   * Uses INSERT OR IGNORE for duplicate protection.
   * Returns number of orders saved.
   */
  _saveOrderBatchToDB(rawOrders) {
    let saved = 0;
    try {
      transaction(() => {
        for (const raw of rawOrders) {
          if (!raw.order_id) continue;
          try {
            const order = this.client.normalizeOrder(raw);
            this._insertOrderSafe(order);
            saved++;
            this.stats.orders++;
          } catch (err) {
            // Log but don't stop the batch
            this.stats.errors++;
          }
        }
      });
    } catch (err) {
      // Transaction failed — retry once without transaction wrapper
      logImport(`Transaction failed: ${err.message}. Retrying batch individually...`);
      for (const raw of rawOrders) {
        if (!raw.order_id) continue;
        try {
          const order = this.client.normalizeOrder(raw);
          this._insertOrderSafe(order);
          saveDb();
          saved++;
          this.stats.orders++;
        } catch { this.stats.errors++; }
      }
    }
    return saved;
  }

  /**
   * INSERT OR IGNORE — never creates duplicates.
   */
  _insertOrderSafe(order) {
    runSql(`
      INSERT OR IGNORE INTO orders (
        client_id, order_id, customer_id, contact_id, is_anonymous_decline,
        campaign_id, gateway_id, gateway_descriptor,
        cc_first_6, cc_type, order_status, order_total,
        decline_reason, decline_reason_details,
        acquisition_date, billing_cycle, is_cascaded, retry_attempt,
        is_recurring, tx_type, product_ids, ancestor_id,
        billing_country, billing_state, ip_address,
        prepaid, prepaid_match,
        email_address, preserve_gateway,
        is_chargeback, chargeback_date, is_refund, refund_amount, refund_date,
        is_void, void_amount, void_date, amount_refunded_to_date,
        click_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, device_category,
        created_by, billing_model_id, billing_model_name, offer_id, subscription_id,
        coupon_id, coupon_discount_amount, decline_salvage_discount_percent, rebill_discount_percent,
        stop_after_next_rebill, on_hold, hold_date, order_confirmed,
        parent_id, child_id, is_in_trial, order_subtotal, shipping_total, tax_total,
        c1, c2, c3, affid
      ) VALUES (${Array(68).fill('?').join(',')})
    `, [
      this.clientId, order.order_id, order.customer_id, order.contact_id, order.is_anonymous_decline,
      order.campaign_id, order.gateway_id, order.gateway_descriptor,
      order.cc_first_6, order.cc_type, order.order_status, order.order_total,
      order.decline_reason, order.decline_reason_details,
      order.acquisition_date, order.billing_cycle, order.is_cascaded, order.retry_attempt,
      order.is_recurring, order.tx_type, order.product_ids, order.ancestor_id,
      order.billing_country, order.billing_state, order.ip_address,
      order.prepaid || '0', order.prepaid_match || 'No',
      order.email_address, order.preserve_gateway,
      order.is_chargeback, order.chargeback_date, order.is_refund, order.refund_amount, order.refund_date,
      order.is_void, order.void_amount, order.void_date, order.amount_refunded_to_date,
      order.click_id, order.utm_source, order.utm_medium, order.utm_campaign, order.utm_content, order.utm_term, order.device_category,
      order.created_by, order.billing_model_id, order.billing_model_name, order.offer_id, order.subscription_id,
      order.coupon_id, order.coupon_discount_amount, order.decline_salvage_discount_percent, order.rebill_discount_percent,
      order.stop_after_next_rebill, order.on_hold, order.hold_date, order.order_confirmed,
      order.parent_id, order.child_id, order.is_in_trial, order.order_subtotal, order.shipping_total, order.tax_total,
      order.c1, order.c2, order.c3, order.affid,
    ]);
  }

  _getDBCount() {
    const row = queryOneSql('SELECT COUNT(*) as cnt FROM orders WHERE client_id = ?', [this.clientId]);
    return row?.cnt || 0;
  }

  /**
   * Count distinct orders for a specific day in DB.
   * Uses range query on acquisition_date — fully uses idx_orders_client_date index.
   * @param {string} day - MM/DD/YYYY format
   */
  _getDBCountForDay(day) {
    const [mm, dd, yyyy] = day.split('/');
    const dayStart = `${yyyy}-${mm}-${dd}`;
    const nextDay = new Date(`${yyyy}-${mm}-${dd}T00:00:00`);
    nextDay.setDate(nextDay.getDate() + 1);
    const dayEnd = nextDay.toISOString().split('T')[0];
    const row = queryOneSql(
      'SELECT COUNT(DISTINCT order_id) as cnt FROM orders WHERE client_id = ? AND acquisition_date >= ? AND acquisition_date < ?',
      [this.clientId, dayStart, dayEnd]
    );
    return row?.cnt || 0;
  }

  // Legacy checkpoint methods — kept for backward compat with old checkpoint files
  _writeCheckpoint() { /* no-op — replaced by _saveCheckpoint */ }
  _writeCheckpointTo() { /* no-op — replaced by _saveCheckpoint */ }

  // ──────────────────────────────────────────────
  // INCREMENTAL SYNC — order_find + return_type=order_view
  // Uses the same paginated order_find as pullTransactions but with
  // INSERT OR REPLACE (upsert) so existing orders get updated and
  // new orders get added. Use a narrow date range (e.g., last 7 days)
  // to keep it fast. order_find_updated does NOT return full order
  // details with return_type=order_view — only last_modified timestamps.
  // ──────────────────────────────────────────────

  async pullUpdated(startDate, endDate) {
    console.log(`[Ingestion] Incremental sync: ${startDate} to ${endDate}`);

    // Use same queue-based chunking as pullTransactions
    const days = this._generateDayList(startDate, endDate);
    console.log(`[Ingestion] Date range: ${days.length} days`);

    let totalImported = 0;

    for (const day of days) {
      try {
        const result = await this._fetchAndVerifyDay(day, (msg) => console.log(`[Ingestion] ${msg}`));
        for (const raw of result.orders) {
          if (!raw.order_id) continue;
          try {
            const order = this.client.normalizeOrder(raw);
            this._upsertOrder(order);
            this.stats.orders++;
            totalImported++;
          } catch { this.stats.errors++; }
        }
        saveDb();
      } catch (err) {
        console.log(`[Ingestion] ${day}: FAILED - ${err.message}`);
      }
    }

    saveDb();
    this._updateSyncState('incremental_pull');
    console.log(`[Ingestion] Incremental complete. Processed: ${totalImported}`);
  }

  // ──────────────────────────────────────────────
  // MID STATUS CHECK
  // ──────────────────────────────────────────────

  async checkMidStatus() {
    console.log(`[Ingestion] Checking MID status...`);
    const gateways = querySql(
      'SELECT gateway_id, lifecycle_state, gateway_active, gateway_descriptor, last_checked FROM gateways WHERE client_id = ? AND lifecycle_state != ?',
      [this.clientId, 'closed']
    );

    let changes = 0;
    for (const gw of gateways) {
      try {
        const data = await this.client.gatewayView(gw.gateway_id);
        if (!data || data.response_code !== '100') continue;

        const isNowClosed = parseInt(data.gateway_active, 10) === 0 ||
          (data.gateway_descriptor && data.gateway_descriptor.toLowerCase().includes('closed'));

        if (isNowClosed && gw.lifecycle_state !== 'closed') {
          runSql("UPDATE gateways SET lifecycle_state = 'closed', gateway_active = 0, last_checked = datetime('now') WHERE client_id = ? AND gateway_id = ?",
            [this.clientId, gw.gateway_id]);
          this._createAlert('P0', 'mid_closure', `MID Closed: Gateway ${gw.gateway_id}`,
            `Gateway ${gw.gateway_id} (${data.gateway_descriptor || ''}) just went inactive.`,
            gw.gateway_id);
          changes++;
        } else {
          runSql("UPDATE gateways SET last_checked = datetime('now') WHERE client_id = ? AND gateway_id = ?",
            [this.clientId, gw.gateway_id]);
        }
      } catch (err) {
        console.error(`[Ingestion] Error checking gateway ${gw.gateway_id}: ${err.message}`);
      }
    }

    saveDb();
    this._updateSyncState('mid_check');
    console.log(`[Ingestion] MID check complete. Changes: ${changes}`);
    return changes;
  }

  // ──────────────────────────────────────────────
  // DB OPERATIONS
  // ──────────────────────────────────────────────

  /** Used by incremental updates — INSERT OR REPLACE for updated orders */
  _upsertOrder(order) {
    runSql(`
      INSERT OR REPLACE INTO orders (
        client_id, order_id, customer_id, contact_id, is_anonymous_decline,
        campaign_id, gateway_id, gateway_descriptor,
        cc_first_6, cc_type, order_status, order_total,
        decline_reason, decline_reason_details,
        acquisition_date, billing_cycle, is_cascaded, retry_attempt,
        is_recurring, tx_type, product_ids, ancestor_id,
        billing_country, billing_state, ip_address,
        prepaid, prepaid_match,
        email_address, preserve_gateway,
        is_chargeback, chargeback_date, is_refund, refund_amount, refund_date,
        is_void, void_amount, void_date, amount_refunded_to_date,
        click_id, utm_source, utm_medium, utm_campaign, utm_content, utm_term, device_category,
        created_by, billing_model_id, billing_model_name, offer_id, subscription_id,
        coupon_id, coupon_discount_amount, decline_salvage_discount_percent, rebill_discount_percent,
        stop_after_next_rebill, on_hold, hold_date, order_confirmed,
        parent_id, child_id, is_in_trial, order_subtotal, shipping_total, tax_total,
        c1, c2, c3, affid
      ) VALUES (${Array(68).fill('?').join(',')})
    `, [
      this.clientId, order.order_id, order.customer_id, order.contact_id, order.is_anonymous_decline,
      order.campaign_id, order.gateway_id, order.gateway_descriptor,
      order.cc_first_6, order.cc_type, order.order_status, order.order_total,
      order.decline_reason, order.decline_reason_details,
      order.acquisition_date, order.billing_cycle, order.is_cascaded, order.retry_attempt,
      order.is_recurring, order.tx_type, order.product_ids, order.ancestor_id,
      order.billing_country, order.billing_state, order.ip_address,
      order.prepaid || '0', order.prepaid_match || 'No',
      order.email_address, order.preserve_gateway,
      order.is_chargeback, order.chargeback_date, order.is_refund, order.refund_amount, order.refund_date,
      order.is_void, order.void_amount, order.void_date, order.amount_refunded_to_date,
      order.click_id, order.utm_source, order.utm_medium, order.utm_campaign, order.utm_content, order.utm_term, order.device_category,
      order.created_by, order.billing_model_id, order.billing_model_name, order.offer_id, order.subscription_id,
      order.coupon_id, order.coupon_discount_amount, order.decline_salvage_discount_percent, order.rebill_discount_percent,
      order.stop_after_next_rebill, order.on_hold, order.hold_date, order.order_confirmed,
      order.parent_id, order.child_id, order.is_in_trial, order.order_subtotal, order.shipping_total, order.tax_total,
      order.c1, order.c2, order.c3, order.affid,
    ]);
  }

  _createAlert(priority, type, title, description, gatewayId = null) {
    const existing = queryOneSql(
      'SELECT id FROM alerts WHERE client_id = ? AND alert_type = ? AND gateway_id = ? AND is_resolved = 0',
      [this.clientId, type, gatewayId]
    );
    if (!existing) {
      runSql(
        'INSERT INTO alerts (client_id, priority, alert_type, title, description, gateway_id) VALUES (?, ?, ?, ?, ?, ?)',
        [this.clientId, priority, type, title, description, gatewayId]
      );
    }
  }

  _updateSyncState(syncType) {
    const existing = queryOneSql(
      'SELECT id FROM sync_state WHERE client_id = ? AND sync_type = ?',
      [this.clientId, syncType]
    );
    const total = this.stats.orders + this.stats.gateways;
    if (existing) {
      runSql(
        "UPDATE sync_state SET last_sync_at = datetime('now'), records_synced = ?, status = 'complete', error_message = NULL WHERE client_id = ? AND sync_type = ?",
        [total, this.clientId, syncType]
      );
    } else {
      runSql(
        "INSERT INTO sync_state (client_id, sync_type, last_sync_at, records_synced, status) VALUES (?, ?, datetime('now'), ?, 'complete')",
        [this.clientId, syncType, total]
      );
    }
    saveDb();
  }

  getStats() { return { ...this.stats }; }
}

// ──────────────────────────────────────────────
// CLI
// ──────────────────────────────────────────────

function daysAgo(n) {
  const d = new Date(); d.setDate(d.getDate() - n); return d;
}
function formatDate(d) {
  return `${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}/${d.getFullYear()}`;
}

async function runIngestion() {
  await initializeDatabase();
  const args = process.argv.slice(2);
  const command = args[0] || 'help';
  const clientId = parseInt(args[1], 10) || 1;
  const ingestion = new DataIngestion(clientId);

  switch (command) {
    case 'test': {
      // Quick test: 3-day pull to verify API works
      ingestion.init();
      const start = formatDate(daysAgo(3));
      const end = formatDate(new Date());
      console.log(`[Test] order_find campaign_id="all", ${start} to ${end}`);
      const data = await ingestion.client.orderFindAll(start, end, 1, 5);
      console.log(`[Test] response_code: ${data?.response_code}`);
      console.log(`[Test] total_orders: ${data?.total_orders || data?.totalResults || 'N/A'}`);
      console.log(`[Test] top-level keys: ${Object.keys(data || {}).join(', ')}`);
      const orders = ingestion.client.parseOrdersFromResponse(data);
      console.log(`[Test] orders parsed: ${orders.length}`);
      if (orders.length > 0) {
        console.log(`[Test] First order fields: ${Object.keys(orders[0]).join(', ')}`);
        console.log(`[Test] Sample order:`, JSON.stringify(ingestion.client.normalizeOrder(orders[0]), null, 2));
      }
      break;
    }
    case 'pull': {
      ingestion.init();
      const startDate = args[2] || formatDate(daysAgo(90));
      const endDate = args[3] || formatDate(new Date());
      await ingestion.pullTransactions(startDate, endDate);
      break;
    }
    case 'updated': {
      ingestion.init();
      const startDate = args[2] || formatDate(daysAgo(1));
      const endDate = args[3] || formatDate(new Date());
      await ingestion.pullUpdated(startDate, endDate);
      break;
    }
    case 'gateways': {
      ingestion.init();
      await ingestion.syncGateways(parseInt(args[2], 10) || 1);
      break;
    }
    case 'mid-check': {
      ingestion.init();
      await ingestion.checkMidStatus();
      break;
    }
    case 'full': {
      ingestion.init();
      console.log('[Ingestion] Running full sync...');
      await ingestion.syncGateways();
      await ingestion.pullTransactions(formatDate(daysAgo(90)), formatDate(new Date()));
      console.log('[Ingestion] Full sync complete.', ingestion.getStats());
      break;
    }
    default:
      console.log('Usage: node ingestion.js <command> [clientId] [args...]');
      console.log('  test [clientId]                    — 3-day test pull');
      console.log('  pull [clientId] [start] [end]      — historical import');
      console.log('  updated [clientId] [start] [end]   — daily incremental');
      console.log('  gateways [clientId]                — sync gateways');
      console.log('  mid-check [clientId]               — hourly MID check');
      console.log('  full [clientId]                    — gateways + 90-day pull');
  }

  closeDb();
}

if (require.main === module) {
  runIngestion().catch(err => {
    console.error('[Ingestion] Fatal:', err.message);
    closeDb();
    process.exit(1);
  });
}

module.exports = DataIngestion;
