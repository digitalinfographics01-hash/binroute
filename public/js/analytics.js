/**
 * BinRoute — Analytics Screen
 */
const Analytics = {
  activeTab: 'overview',
  sortCol: null,
  sortDir: 'asc',
  txTypeFilter: 'all',
  crmCategory: 'all',
  txTypeSubTab: null,

  // ── Helpers ──

  rateCellHtml(rate) {
    if (rate == null) return '<td style="color:var(--text-muted)">—</td>';
    const color = rate >= 80 ? 'var(--success)' : rate >= 60 ? 'var(--warning)' : 'var(--danger)';
    return `<td style="color:${color};font-weight:600">${rate.toFixed(1)}%</td>`;
  },

  confidencePill(level) {
    if (!level) return '<span class="pill" style="background:var(--bg);color:var(--text-muted)">—</span>';
    const map = {
      HIGH: { bg: 'var(--success-light)', color: 'var(--success)' },
      MEDIUM: { bg: 'var(--warning-light)', color: 'var(--warning)' },
      LOW: { bg: 'var(--danger-light)', color: 'var(--danger)' },
    };
    const key = (level && typeof level === 'string') ? level.toUpperCase() : String(level || 'LOW').toUpperCase();
    const s = map[key] || map.LOW;
    return `<span class="pill" style="background:${s.bg};color:${s.color};font-weight:600">${level}</span>`;
  },

  priorityPill(priority) {
    if (!priority) return '';
    const map = {
      P1: { bg: 'var(--danger-light)', color: 'var(--danger)' },
      P2: { bg: '#fff7ed', color: '#ea580c' },
      P3: { bg: 'var(--warning-light)', color: 'var(--warning)' },
      P4: { bg: 'var(--bg)', color: 'var(--text-muted)' },
    };
    const s = map[priority] || map.P4;
    return `<span class="pill" style="background:${s.bg};color:${s.color};font-weight:600">${priority}</span>`;
  },

  moneyFmt(amount) {
    if (amount == null) return '—';
    return '$' + Number(amount).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  },

  csvExport(rows, filename) {
    if (!rows || rows.length === 0) return;
    const headers = Object.keys(rows[0]);
    const lines = [headers.join(',')];
    for (const row of rows) {
      lines.push(headers.map(h => {
        let val = row[h] == null ? '' : String(row[h]);
        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
          val = '"' + val.replace(/"/g, '""') + '"';
        }
        return val;
      }).join(','));
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  },

  formatEntityLabel(r) {
    if (r.entity_type === 'cluster' && r.entity_id && r.entity_id.includes('Unknown')) {
      const binCount = r.bins ? r.bins.length : '?';
      return `Unenriched BIN Cluster (${binCount} BINs)`;
    }
    if (r.entity_label && r.entity_label.includes('Unknown|Unknown')) {
      const binCount = r.bins ? r.bins.length : '?';
      return `Unenriched BIN Cluster (${binCount} BINs)`;
    }
    return r.entity_label || r.entity_id || '—';
  },

  sortRows(rows, col, dir) {
    return [...rows].sort((a, b) => {
      let va = a[col], vb = b[col];
      if (va == null) va = '';
      if (vb == null) vb = '';
      if (typeof va === 'number' && typeof vb === 'number') {
        return dir === 'asc' ? va - vb : vb - va;
      }
      return dir === 'asc'
        ? String(va).localeCompare(String(vb))
        : String(vb).localeCompare(String(va));
    });
  },

  // ── Main render ──

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading analytics...</div>';
    this.clientId = clientId;

    // Check onboarding status
    let onboarding = null;
    try {
      const obRes = await fetch(`/api/config/clients/${clientId}/onboarding-status`);
      onboarding = await obRes.json();
    } catch (e) { /* ignore */ }

    const tabs = [
      { id: 'overview', label: 'Overview' },
      { id: 'bin-profiles', label: 'BIN Profiles' },
      { id: 'bin-clusters', label: 'BIN Clusters' },
      { id: 'gateway-profiles', label: 'Gateway Profiles' },
      // { id: 'decline-matrix', label: 'Decline Matrix' }, // hidden — re-enable when needed
      { id: 'txtype-analysis', label: 'TX Type Analysis' },
      { id: 'routing', label: 'Routing' },
      { id: 'lift-opportunities', label: 'Lift Opportunities' },
      { id: 'confidence', label: 'Confidence' },
      { id: 'trends', label: 'Trends' },
      { id: 'price-points', label: 'Rebill Pricing' },
      { id: 'crm-rules', label: 'CRM Rules' },
    ];

    let html = '';

    // Onboarding warning banner
    if (onboarding && !onboarding.analyticsReady) {
      const incomplete = onboarding.steps.filter(s => !s.done).map(s => s.label);
      html += `
        <div class="banner banner-config" style="margin-bottom:16px">
          <span class="banner-icon">&#9888;</span>
          <div>
            <strong>Onboarding incomplete (${onboarding.completedCount}/${onboarding.totalSteps})</strong> — Analytics results may be inaccurate.
            Missing: ${incomplete.join(', ')}.
            <a href="#" onclick="App.navigate('dashboard');return false" style="color:inherit;text-decoration:underline;margin-left:8px">View Checklist</a>
          </div>
        </div>`;
    }

    html += `
      <div class="main-header">
        <h2>Analytics</h2>
      </div>
      <div class="tabs" style="margin-bottom:20px">
        ${tabs.map(t => `<div class="tab ${this.activeTab === t.id ? 'active' : ''}" onclick="Analytics.switchTab('${t.id}')">${t.label}</div>`).join('')}
      </div>
      <div id="analyticsContent"></div>`;

    main.innerHTML = html;

    // Fetch server cache timestamps (non-blocking)
    fetch(`/api/analytics/${this.clientId}/cache-info`)
      .then(r => r.json())
      .then(info => { this._serverCacheInfo = info; })
      .catch(() => {});

    this.renderActiveTab();
  },

  switchTab(tab) {
    this.activeTab = tab;
    this.sortCol = null;
    this.sortDir = 'asc';
    // Update tab active states
    document.querySelectorAll('.tabs .tab').forEach(el => {
      el.classList.toggle('active', el.textContent.trim() === this.getTabLabel(tab));
    });
    this.renderActiveTab();
  },

  getTabLabel(id) {
    const map = {
      'overview': 'Overview', 'bin-profiles': 'BIN Profiles', 'bin-clusters': 'BIN Clusters',
      'gateway-profiles': 'Gateway Profiles', 'decline-matrix': 'Decline Matrix',
      'txtype-analysis': 'TX Type Analysis', 'routing': 'Routing',
      'lift-opportunities': 'Lift Opportunities', 'confidence': 'Confidence',
      'trends': 'Trends', 'price-points': 'Rebill Pricing', 'crm-rules': 'CRM Rules',
    };
    return map[id] || id;
  },

  // Cache for loaded results per tab
  _cache: {},
  _cacheTime: {},

  renderActiveTab() {
    if (this.activeTab === 'overview') {
      this.renderOverview();
      return;
    }
    const el = document.getElementById('analyticsContent');
    const tab = this.activeTab;
    const cached = this._cache[tab];
    const cacheAge = this._cacheTime[tab] ? Math.round((Date.now() - this._cacheTime[tab]) / 60000) : null;

    if (cached) {
      this._renderTabData(tab, cached, cacheAge);
    } else {
      // Auto-fetch from server (serves from DB cache if precomputed)
      this.runTab(tab);
    }
  },

  async runTab(tab) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Running analysis...</div>';

    const endpointMap = {
      'bin-profiles': 'bin-profiles',
      'bin-clusters': 'bin-clusters',
      'gateway-profiles': 'gateway-profiles',
      'decline-matrix': 'decline-matrix',
      'txtype-analysis': 'txtype-analysis',
      'routing': 'routing-recommendations',
      'lift-opportunities': 'lift-opportunities',
      'confidence': 'confidence-layer',
      'trends': 'trend-detection',
      'price-points': 'price-points',
      'crm-rules': 'crm-rules',
    };

    const endpoint = endpointMap[tab];
    if (!endpoint) return;

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 30000);
      let fetchUrl = `/api/analytics/${this.clientId}/${endpoint}`;
      if (tab === 'price-points') {
        fetchUrl += `?level=${this.pricePointLevel || 2}`;
      }
      const res = await fetch(fetchUrl, { signal: controller.signal });
      clearTimeout(timeout);
      const data = await res.json();
      if (data && data.status === 'not_computed') {
        el.innerHTML = `<div class="empty-state" style="padding:60px 20px;text-align:center">
          <h3 style="margin-bottom:8px">Analytics not yet computed.</h3>
          <p style="color:var(--text-muted);margin-bottom:16px">Click Refresh to generate.</p>
          <button class="btn btn-primary" onclick="Analytics.triggerRecompute()">Refresh</button></div>`;
        return;
      }
      this._cache[tab] = data;
      this._cacheTime[tab] = Date.now();
      this._renderTabData(tab, data, 0);
    } catch (err) {
      if (err.name === 'AbortError') {
        el.innerHTML = `<div class="empty-state"><h3>Analysis timed out</h3><p>This analysis took too long (>30s). Try filtering to a smaller date range.</p>
          <button class="btn btn-primary" onclick="Analytics.runTab('${tab}')" style="margin-top:12px">Retry</button></div>`;
      } else {
        el.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p>
          <button class="btn btn-primary" onclick="Analytics.runTab('${tab}')" style="margin-top:12px">Retry</button></div>`;
      }
    }
  },

  async triggerRecompute() {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Recomputing analytics in background...</div>';
    try {
      await fetch(`/api/analytics/${this.clientId}/recompute`, { method: 'POST' });
      el.innerHTML = `<div class="empty-state" style="padding:60px 20px;text-align:center">
        <h3 style="margin-bottom:8px">Recomputation started.</h3>
        <p style="color:var(--text-muted);margin-bottom:16px">This runs in the background. Reload the tab in a minute or two.</p>
        <button class="btn btn-primary" onclick="Analytics._cache = {}; Analytics._cacheTime = {}; Analytics.renderActiveTab()">Reload Tab</button></div>`;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p></div>`;
    }
  },

  async _renderTabData(tab, data, cacheAge) {
    switch (tab) {
      case 'bin-profiles': this.renderBinProfiles(data); break;
      case 'bin-clusters': this.renderBinClusters(data); break;
      case 'gateway-profiles': this.renderGatewayProfiles(data); break;
      case 'decline-matrix': this.renderDeclineMatrix(data); break;
      case 'txtype-analysis': this.renderTxTypeAnalysis(data); break;
      case 'routing': this.renderRouting(data); break;
      case 'lift-opportunities': this.renderLiftOpportunities(data); break;
      case 'confidence': this.renderConfidence(data); break;
      case 'trends': this.renderTrends(data); break;
      case 'price-points': this.renderPricePoints(data); break;
      case 'crm-rules': await this.renderCrmRules(data); break;
    }
    // Add refresh button + last updated to top
    const el = document.getElementById('analyticsContent');
    let tsLabel = cacheAge != null ? (cacheAge === 0 ? 'Just calculated' : cacheAge + 'm ago') : '';
    // Try to show server cache timestamp
    if (this._serverCacheInfo && this._serverCacheInfo[this._tabToEndpoint(tab)]) {
      const ts = this._serverCacheInfo[this._tabToEndpoint(tab)];
      tsLabel = 'Last updated: ' + timeAgo(ts);
    }
    const header = `<div style="display:flex;justify-content:flex-end;align-items:center;gap:12px;margin-bottom:12px">
      <span style="font-size:12px;color:var(--text-muted)">${tsLabel}</span>
      <button class="btn btn-sm btn-secondary" onclick="Analytics.runTab('${tab}')">Refresh</button>
    </div>`;
    el.innerHTML = header + el.innerHTML;
  },

  // ── Overview ──

  async renderOverview() {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading overview...</div>';

    try {
      // Only fetch data-quality on overview load (fast query)
      // Other analytics are on-demand via their tabs
      const dqRes = await fetch(`/api/analytics/${this.clientId}/data-quality`);
      const dq = await dqRes.json();

      // Use cached results if available, otherwise show empty
      const recsRaw = this._cache['routing'] || [];
      const recs = Array.isArray(recsRaw) ? recsRaw : (recsRaw.recommendations || []);
      const trends = this._cache['trends'] || { alerts: [] };
      const lift = this._cache['lift-opportunities'] || { summary: {} };

      let html = '';

      // Run Full Analysis button
      const lastRun = this._lastFullRun ? timeAgo(this._lastFullRun.toISOString().replace('Z','')) : 'Never';
      html += `<div style="display:flex;justify-content:flex-end;align-items:center;gap:12px;margin-bottom:16px">
        <span style="font-size:12px;color:var(--text-muted)">Last run: ${lastRun}</span>
        <button class="btn btn-primary" id="runAnalysisBtn" onclick="Analytics.runFullAnalysis()">&#9881; Run Full Analysis</button>
      </div>`;

      // Data quality bar
      if (dq && dq.indicators && dq.indicators.length > 0) {
        html += `<div class="card" style="padding:12px 16px;margin-bottom:16px;display:flex;gap:24px;align-items:center;flex-wrap:wrap">
          <span style="font-size:12px;font-weight:600;color:var(--text-secondary)">DATA QUALITY</span>
          ${dq.indicators.map(i => {
            const color = i.value >= 90 ? 'var(--success)' : i.value >= 70 ? 'var(--warning)' : 'var(--danger)';
            return `<span style="font-size:12px;color:var(--text-secondary)">${i.label}: <strong style="color:${color}">${i.value}%</strong> <span style="opacity:0.6">(${i.count}/${i.of})</span></span>`;
          }).join('')}
        </div>`;
      }

      // Summary cards
      const totalBins = recs.length > 0 ? new Set(recs.map(r => r.entity_id || r.bin)).size : 0;
      const totalGateways = recs.length > 0 ? new Set(recs.flatMap(r => [r.current_gateway?.gateway_id, r.recommended_gateway?.gateway_id]).filter(Boolean)).size : 0;
      const totalOpportunity = lift.summary?.total_monthly_revenue || 0;

      html += `<div class="kpi-row">
        <div class="kpi-card">
          <div class="kpi-label">Total BINs Analyzed</div>
          <div class="kpi-value">${formatNum(totalBins)}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Total Gateways</div>
          <div class="kpi-value">${formatNum(totalGateways)}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Total Opportunity</div>
          <div class="kpi-value success">${this.moneyFmt(totalOpportunity)}/mo</div>
        </div>
      </div>`;

      // Top 5 routing recommendations
      const topRecs = recs.slice(0, 5);
      html += `<div class="card" style="margin-bottom:20px">
        <h3 style="padding:16px 16px 0;font-size:15px;font-weight:600">Top Routing Recommendations</h3>
        <div class="table-wrap"><table>
          <thead><tr>
            <th>BIN</th><th>TX Group</th><th>Current GW</th><th>Recommended GW</th><th>Lift</th><th>Revenue</th><th>Confidence</th>
          </tr></thead>
          <tbody>`;

      if (topRecs.length === 0) {
        html += '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px">No recommendations yet. Click "Run Full Analysis" above.</td></tr>';
      } else {
        for (const r of topRecs) {
          const label = this.formatEntityLabel(r);
          const isCluster = r.entity_type === 'cluster';
          html += `<tr>
            <td><strong>${label}</strong>${isCluster ? `<div style="font-size:11px;color:var(--text-muted)">no issuer data available</div>` : ''}</td>
            <td>${r.tx_group || '—'}</td>
            <td>${r.current_gateway?.gateway_name || '—'}</td>
            <td>${r.recommended_gateway?.gateway_name || '—'}</td>
            <td style="color:var(--success);font-weight:600">+${(r.lift_pp || 0).toFixed(1)}pp</td>
            <td style="font-weight:600">${this.moneyFmt(r.monthly_revenue_impact)}/mo</td>
            <td>${this.confidencePill(r.confidence?.level)}</td>
          </tr>`;
        }
      }
      html += '</tbody></table></div>';
      // Unenriched BIN note
      const unenrichedRecs = recs.filter(r => r.entity_type === 'cluster' && r.entity_id && r.entity_id.includes('Unknown'));
      if (unenrichedRecs.length > 0) {
        const uBinCount = unenrichedRecs[0].bins ? unenrichedRecs[0].bins.length : '?';
        html += `<div style="padding:0 16px 12px;font-size:12px;color:var(--text-muted)">${uBinCount} BINs without issuer enrichment grouped together. Consider running additional BIN lookups to improve routing precision.</div>`;
      }
      html += '</div>';

      // Top 5 trend alerts
      const alerts = (trends.alerts || trends || []).slice(0, 5);
      html += `<div class="card">
        <h3 style="padding:16px 16px 0;font-size:15px;font-weight:600">Recent Trend Alerts</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Priority</th><th>Type</th><th>Entity</th><th>Message</th><th>Detected</th></tr></thead>
          <tbody>`;

      if (alerts.length === 0) {
        html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:24px">No trend alerts.</td></tr>';
      } else {
        for (const a of alerts) {
          html += `<tr>
            <td>${this.priorityPill(a.priority)}</td>
            <td>${a.type || a.alert_type || '—'}</td>
            <td><strong>${a.entity || a.bin || a.gateway || '—'}</strong></td>
            <td>${a.message || a.description || '—'}</td>
            <td style="color:var(--text-secondary)">${a.detected_at ? timeAgo(a.detected_at) : '—'}</td>
          </tr>`;
        }
      }
      html += '</tbody></table></div></div>';

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load overview</h3><p>${err.message}</p></div>`;
    }
  },

  async runFullAnalysis() {
    const btn = document.getElementById('runAnalysisBtn');
    if (btn) { btn.disabled = true; }

    const el = document.getElementById('analyticsContent');

    const steps = [
      { tab: 'bin-profiles', endpoint: 'bin-profiles', label: 'BIN Profiles' },
      { tab: 'bin-clusters', endpoint: 'bin-clusters', label: 'BIN Clusters' },
      { tab: 'gateway-profiles', endpoint: 'gateway-profiles', label: 'Gateway Profiles' },
      { tab: 'decline-matrix', endpoint: 'decline-matrix', label: 'Decline Matrix' },
      { tab: 'txtype-analysis', endpoint: 'txtype-analysis', label: 'TX Type Analysis' },
      { tab: 'routing', endpoint: 'routing-recommendations', label: 'Routing Recommendations' },
      { tab: 'lift-opportunities', endpoint: 'lift-opportunities', label: 'Lift Opportunities' },
      { tab: 'confidence', endpoint: 'confidence-layer', label: 'Confidence Layer' },
      { tab: 'trends', endpoint: 'trend-detection', label: 'Trend Detection' },
      { tab: 'price-points', endpoint: 'price-points', label: 'Rebill Pricing' },
      { tab: 'crm-rules', endpoint: 'crm-rules', label: 'CRM Rules' },
    ];

    let completed = 0;
    let failed = 0;
    const errors = [];

    for (const step of steps) {
      // Update progress
      el.innerHTML = `
        <div style="padding:40px;text-align:center">
          <div class="spinner" style="margin:0 auto 16px"></div>
          <div style="font-size:16px;font-weight:600;margin-bottom:8px">Running analysis... (${completed}/${steps.length} complete)</div>
          <div style="color:var(--text-secondary);font-size:13px">Now running: ${step.label}</div>
          <div class="progress-bar" style="margin-top:16px;max-width:400px;margin-left:auto;margin-right:auto">
            <div class="progress-bar-fill" style="width:${Math.round((completed / steps.length) * 100)}%"></div>
          </div>
          ${failed > 0 ? `<div style="color:var(--warning);font-size:12px;margin-top:8px">${failed} failed</div>` : ''}
        </div>`;

      try {
        const controller = new AbortController();
        const timeoutMs = ['decline-matrix', 'txtype-analysis', 'price-points', 'salvage-sequence'].includes(step.endpoint) ? 120000 : 45000;
        const timeout = setTimeout(() => controller.abort(), timeoutMs);
        const res = await fetch(`/api/analytics/${this.clientId}/${step.endpoint}`, { signal: controller.signal });
        clearTimeout(timeout);
        const data = await res.json();
        this._cache[step.tab] = data;
        this._cacheTime[step.tab] = Date.now();
        completed++;
      } catch (err) {
        failed++;
        errors.push({ label: step.label, error: err.name === 'AbortError' ? 'Timeout (>30s)' : err.message });
        completed++;
      }
    }

    // Store last run timestamp
    this._lastFullRun = new Date();

    // Build summary
    const recs = this._cache['routing'] || [];
    const lift = this._cache['lift-opportunities'] || { summary: {} };
    const trends = this._cache['trends'] || { alerts: [] };
    const crm = this._cache['crm-rules'] || { rules: [] };
    const recsCount = Array.isArray(recs) ? recs.length : (recs.length || 0);
    const liftMonthly = lift.summary?.total_monthly_revenue || 0;
    const alertCount = trends.alerts?.length || 0;
    const rulesCount = crm.rules?.length || 0;

    let summaryHtml = `
      <div style="padding:40px;text-align:center">
        <div style="font-size:24px;margin-bottom:8px">&#10003;</div>
        <div style="font-size:16px;font-weight:600;margin-bottom:16px">Analysis Complete</div>
        <div class="kpi-row" style="max-width:600px;margin:0 auto 20px">
          <div class="kpi-card"><div class="kpi-label">Routing Recs</div><div class="kpi-value accent">${recsCount}</div></div>
          <div class="kpi-card"><div class="kpi-label">Monthly Opportunity</div><div class="kpi-value success">${this.moneyFmt(liftMonthly)}</div></div>
          <div class="kpi-card"><div class="kpi-label">Trend Alerts</div><div class="kpi-value ${alertCount > 0 ? 'warning' : ''}">${alertCount}</div></div>
          <div class="kpi-card"><div class="kpi-label">CRM Rules</div><div class="kpi-value">${rulesCount}</div></div>
        </div>
        ${failed > 0 ? `<div style="color:var(--warning);font-size:13px;margin-bottom:12px">${failed} output(s) failed: ${errors.map(e => e.label).join(', ')}</div>` : ''}
        <div style="font-size:12px;color:var(--text-muted);margin-bottom:16px">Last run: just now</div>
        <button class="btn btn-secondary" onclick="Analytics.renderOverview()">View Overview</button>
      </div>`;

    el.innerHTML = summaryHtml;
  },

  // ── BIN Profiles ──

  async renderBinProfiles(preloaded) {
    const el = document.getElementById('analyticsContent');
    if (!preloaded) { el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading...</div>'; }
    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/bin-profiles`).then(r => r.json());
      const profiles = data.profiles || data || [];

      if (!this.sortCol) { this.sortCol = 'attempts'; this.sortDir = 'desc'; }
      const sorted = this.sortRows(profiles, this.sortCol, this.sortDir);

      this._binProfiles = profiles;

      let html = `<div class="card"><div class="table-wrap"><table>
        <thead><tr>
          ${['bin','issuer','brand','type','level','attempts','clean_rate','cascade_rate','best_gateway','confidence'].map(col => {
            const label = { bin:'BIN', issuer:'Issuer', brand:'Brand', type:'Type', level:'Level', attempts:'Attempts', clean_rate:'Clean Rate', cascade_rate:'Cascade Rate', best_gateway:'Best GW', confidence:'Confidence' }[col];
            const arrow = this.sortCol === col ? (this.sortDir === 'asc' ? ' &#9650;' : ' &#9660;') : '';
            return `<th style="cursor:pointer" onclick="Analytics.sortBinProfiles('${col}')">${label}${arrow}</th>`;
          }).join('')}
          <th></th>
        </tr></thead>
        <tbody>`;

      if (sorted.length === 0) {
        html += '<tr><td colspan="11" style="text-align:center;color:var(--text-muted);padding:24px">No BIN profiles found. Run analysis first.</td></tr>';
      } else {
        for (let i = 0; i < sorted.length; i++) {
          const p = sorted[i];
          html += `<tr>
            <td><strong>${p.bin || '—'}</strong></td>
            <td>${p.issuer || '—'}</td>
            <td>${p.brand || p.card_brand || '—'}</td>
            <td>${p.type || p.card_type || '—'}</td>
            <td>${p.level || p.card_level || '—'}</td>
            <td>${formatNum(p.attempts)}</td>
            ${this.rateCellHtml(p.clean_rate)}
            ${this.rateCellHtml(p.cascade_rate)}
            <td>${(typeof p.best_gateway === 'object' ? p.best_gateway?.gateway_name : p.best_gateway) || p.best_gw || '—'}</td>
            <td>${this.confidencePill(typeof p.confidence === 'object' ? p.confidence?.level : (p.confidence || p.confidence_level))}</td>
            <td><button class="btn btn-sm btn-secondary" onclick="Analytics.toggleBinDetail(${i})">&#9660;</button></td>
          </tr>
          <tr id="binDetail_${i}" style="display:none">
            <td colspan="11" style="background:var(--bg);padding:12px 16px">
              ${this.buildBinDetailHtml(p)}
            </td>
          </tr>`;
        }
      }

      html += '</tbody></table></div></div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load BIN profiles</h3><p>${err.message}</p></div>`;
    }
  },

  buildBinDetailHtml(profile) {
    let html = '';

    // Per-tx_type breakdown
    const txBreakdown = profile.tx_type_breakdown || profile.tx_types || [];
    if (txBreakdown.length > 0) {
      html += `<div style="margin-bottom:12px"><strong style="font-size:12px;color:var(--text-secondary)">TX TYPE BREAKDOWN</strong>
        <table style="margin-top:6px;font-size:13px"><thead><tr><th>TX Type</th><th>Attempts</th><th>Clean Rate</th><th>Cascade Rate</th></tr></thead><tbody>`;
      for (const t of txBreakdown) {
        html += `<tr><td>${t.tx_type || t.transaction_type || '—'}</td><td>${formatNum(t.attempts)}</td>${this.rateCellHtml(t.clean_rate)}${this.rateCellHtml(t.cascade_rate)}</tr>`;
      }
      html += '</tbody></table></div>';
    }

    // Decline reasons
    const declines = profile.decline_reasons || profile.top_declines || [];
    if (declines.length > 0) {
      html += `<div style="margin-bottom:12px"><strong style="font-size:12px;color:var(--text-secondary)">TOP DECLINE REASONS</strong><ul style="margin-top:4px;padding-left:20px;font-size:13px">`;
      for (const d of declines) {
        html += `<li>${d.reason || d.decline_reason || '—'}: ${formatNum(d.count)} (${(d.pct || d.percentage || 0).toFixed(1)}%)</li>`;
      }
      html += '</ul></div>';
    }

    // Trend
    if (profile.trend) {
      const tColor = profile.trend === 'improving' ? 'var(--success)' : profile.trend === 'declining' ? 'var(--danger)' : 'var(--text-secondary)';
      html += `<div><strong style="font-size:12px;color:var(--text-secondary)">TREND:</strong> <span style="color:${tColor};font-weight:600">${profile.trend}</span></div>`;
    }

    if (!html) html = '<span style="color:var(--text-muted)">No detailed data available.</span>';
    return html;
  },

  sortBinProfiles(col) {
    if (this.sortCol === col) {
      this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this.sortCol = col;
      this.sortDir = ['attempts', 'clean_rate', 'cascade_rate'].includes(col) ? 'desc' : 'asc';
    }
    this.renderBinProfiles();
  },

  toggleBinDetail(idx) {
    const row = document.getElementById(`binDetail_${idx}`);
    if (row) row.style.display = row.style.display === 'none' ? '' : 'none';
  },

  // ── BIN Clusters ──

  async renderBinClusters(preloaded) {
    const el = document.getElementById('analyticsContent');
    if (!preloaded) { el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading...</div>'; }
    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/bin-clusters`).then(r => r.json());
      const clusters = data.clusters || data || [];

      if (clusters.length === 0) {
        el.innerHTML = '<div class="empty-state"><h3>No BIN Clusters</h3><p>Run analysis to generate cluster groupings.</p></div>';
        return;
      }

      let html = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px">';

      for (const c of clusters) {
        html += `<div class="card" style="padding:20px">
          <h3 style="font-size:15px;font-weight:600;margin-bottom:12px">${c.name || c.cluster_name || 'Cluster'}</h3>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px">
            <div><span style="color:var(--text-secondary)">BINs:</span> <strong>${formatNum(c.bin_count)}</strong></div>
            <div><span style="color:var(--text-secondary)">Volume:</span> <strong>${formatNum(c.total_volume || c.volume || c.total_attempts)}</strong></div>
            <div><span style="color:var(--text-secondary)">Avg Approval Rate:</span> <strong style="color:${(c.avg_approval_rate || c.avg_clean_rate || 0) >= 70 ? 'var(--success)' : 'var(--warning)'}">${(c.avg_approval_rate || c.avg_clean_rate || 0).toFixed(1)}%</strong></div>
            <div><span style="color:var(--text-secondary)">Cascade Rate:</span> <strong>${(c.cascade_rate || c.avg_cascade_rate || 0).toFixed(1)}%</strong></div>
          </div>
          <div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--border);font-size:13px">
            <span style="color:var(--text-secondary)">Recommended GW:</span>
            <strong>${(typeof c.recommended_gateway === 'object' ? c.recommended_gateway?.gateway_name || c.recommended_gateway?.alias : c.recommended_gateway) || (typeof c.best_gateway === 'object' ? c.best_gateway?.gateway_name || c.best_gateway?.alias : c.best_gateway) || 'Not enough data'}</strong>
          </div>
          ${c.bins && c.bins.length > 0 ? `
            <div style="margin-top:8px;font-size:12px;color:var(--text-muted)">
              BINs: ${c.bins.slice(0, 10).join(', ')}${c.bins.length > 10 ? ` +${c.bins.length - 10} more` : ''}
            </div>` : ''}
        </div>`;
      }

      html += '</div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load BIN clusters</h3><p>${err.message}</p></div>`;
    }
  },

  // ── Gateway Profiles ──

  async renderGatewayProfiles(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading gateway profiles...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/gateway-profiles`).then(r => r.json());
      const profiles = data.profiles || data || [];

      let html = `<div class="card"><div class="table-wrap"><table>
        <thead><tr>
          <th>Gateway</th><th>Bank</th><th>Processor</th><th>MCC</th><th>Attempts</th>
          <th>Clean Rate</th><th>Cascade Rate</th><th>Revenue</th><th>Trend</th>
        </tr></thead>
        <tbody>`;

      if (profiles.length === 0) {
        html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No gateway profiles found.</td></tr>';
      } else {
        for (const g of profiles) {
          const trendColor = g.trend === 'improving' ? 'var(--success)' : g.trend === 'declining' ? 'var(--danger)' : 'var(--text-secondary)';
          html += `<tr>
            <td><strong>${g.gateway || g.gateway_name || g.alias || '—'}</strong></td>
            <td>${g.bank || g.bank_name || '—'}</td>
            <td>${g.processor || g.processor_name || '—'}</td>
            <td>${g.mcc || '—'}</td>
            <td>${formatNum(g.attempts)}</td>
            ${this.rateCellHtml(g.clean_rate)}
            ${this.rateCellHtml(g.cascade_rate)}
            <td>${this.moneyFmt(g.revenue)}</td>
            <td style="color:${trendColor};font-weight:500">${g.trend || '—'}</td>
          </tr>`;

          // Per-tx_type breakdown inline
          const txb = g.tx_type_breakdown || g.tx_types || [];
          if (txb.length > 0) {
            html += `<tr><td colspan="9" style="padding:4px 16px 12px;background:var(--bg)">
              <div style="display:flex;gap:16px;flex-wrap:wrap;font-size:12px">`;
            for (const t of txb) {
              const rColor = (t.clean_rate || 0) >= 70 ? 'var(--success)' : 'var(--warning)';
              html += `<span style="color:var(--text-secondary)">${t.tx_type || t.transaction_type}: <strong style="color:${rColor}">${(t.clean_rate || 0).toFixed(1)}%</strong> (${formatNum(t.attempts)})</span>`;
            }
            html += '</div></td></tr>';
          }
        }
      }

      html += '</tbody></table></div></div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load gateway profiles</h3><p>${err.message}</p></div>`;
    }
  },

  // ── Decline Matrix ──

  async renderDeclineMatrix(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading decline matrix...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/decline-matrix`).then(r => r.json());
      const matrix = data.matrix || data.declines || data || [];

      // Recoverable split summary
      const recoverable = matrix.filter(d => d.recoverable !== false);
      const nonRecoverable = matrix.filter(d => d.recoverable === false);

      let html = `<div class="kpi-row" style="margin-bottom:16px">
        <div class="kpi-card">
          <div class="kpi-label">Total Decline Reasons</div>
          <div class="kpi-value">${matrix.length}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Recoverable</div>
          <div class="kpi-value success">${recoverable.length}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Non-Recoverable</div>
          <div class="kpi-value danger">${nonRecoverable.length}</div>
        </div>
      </div>`;

      html += `<div class="card"><div class="table-wrap"><table>
        <thead><tr>
          <th>Decline Reason</th><th>Category</th><th>Count</th><th>Recovery Rate</th>
          <th>Best Recovery GW</th><th>Recoverable</th><th>Action</th>
        </tr></thead>
        <tbody>`;

      if (matrix.length === 0) {
        html += '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:24px">No decline data found.</td></tr>';
      } else {
        const categoryColors = {
          'hard': 'var(--danger)', 'soft': 'var(--warning)', 'fraud': 'var(--danger)',
          'technical': 'var(--info)', 'issuer': 'var(--warning)', 'network': 'var(--accent)',
        };

        for (const d of matrix) {
          const cat = d.decline_category || d.category || '—';
          const catColor = categoryColors[cat.toLowerCase()] || 'var(--text-secondary)';
          const recoveryRate = d.recovery ? d.recovery.recovery_rate : (d.recovery_rate || 0);
          const bestGw = d.best_recovery_gateway ? (d.best_recovery_gateway.gateway_name || d.best_recovery_gateway) : '—';
          const bankChg = d.requires_bank_change ? ' <span class="pill" style="background:var(--warning-light);color:#92400e;font-size:10px">Bank Change</span>' : '';
          html += `<tr>
            <td><strong>${d.decline_reason || d.reason || '—'}</strong>${bankChg}</td>
            <td><span class="pill" style="background:${catColor}20;color:${catColor}">${cat}</span></td>
            <td>${formatNum(d.total || d.count)}</td>
            ${this.rateCellHtml(recoveryRate)}
            <td>${bestGw}</td>
            <td>${d.recoverable === false
              ? '<span style="color:var(--danger);font-weight:600">No</span>'
              : '<span style="color:var(--success);font-weight:600">Yes</span>'}</td>
            <td style="font-size:12px;color:var(--text-secondary)">${d.action || d.recommended_action || '—'}</td>
          </tr>`;
        }
      }

      html += '</tbody></table></div></div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load decline matrix</h3><p>${err.message}</p></div>`;
    }
  },

  // ── TX Type Analysis ──

  async renderTxTypeAnalysis(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading TX type analysis...</div>';

    try {
      const rawData = preloaded || await fetch(`/api/analytics/${this.clientId}/txtype-analysis`).then(r => r.json());
      // API returns object keyed by tx_type — convert to array
      const types = Array.isArray(rawData) ? rawData :
        (rawData.tx_types ? rawData.tx_types :
          Object.values(rawData).filter(v => v && typeof v === 'object' && v.tx_type));

      if (types.length === 0) {
        el.innerHTML = '<div class="empty-state"><h3>No TX Type Data</h3><p>Run analysis to generate TX type breakdowns.</p></div>';
        return;
      }

      // Set default sub tab
      if (!this.txTypeSubTab || !types.find(t => t?.tx_type === this.txTypeSubTab)) {
        this.txTypeSubTab = types[0]?.tx_type;
      }

      // Sub-tabs for each tx_type
      let html = `<div class="tabs" style="margin-bottom:16px;flex-wrap:wrap">
        ${types.map(t => {
          const name = t?.tx_type || 'unknown';
          return `<div class="tab ${this.txTypeSubTab === name ? 'active' : ''}" onclick="Analytics.switchTxTypeTab('${name}')">${this.txGroupLabel(name)}</div>`;
        }).join('')}
      </div>`;

      const active = types.find(t => t?.tx_type === this.txTypeSubTab);
      if (active) {
        // KPI cards
        html += `<div class="kpi-row" style="margin-bottom:16px">
          <div class="kpi-card">
            <div class="kpi-label">Total Attempts</div>
            <div class="kpi-value">${formatNum(active.overview?.volume || active.attempts || 0)}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Clean Rate</div>
            <div class="kpi-value ${(active.overview?.clean_rate || active.clean_rate || 0) >= 70 ? 'success' : 'warning'}">${(active.overview?.clean_rate || active.clean_rate || 0).toFixed(1)}%</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Cascade Rate</div>
            <div class="kpi-value">${(active.overview?.cascade_rate || active.cascade_rate || 0).toFixed(1)}%</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Revenue</div>
            <div class="kpi-value">${this.moneyFmt(active.overview?.revenue || active.avg_ticket || 0)}</div>
          </div>
        </div>`;

        // Gateway ranking table
        const gwRanking = active.gateway_ranking || active.gateways || [];
        html += `<div class="card" style="margin-bottom:16px">
          <h3 style="padding:16px 16px 0;font-size:14px;font-weight:600">Gateway Ranking</h3>
          <div class="table-wrap"><table>
            <thead><tr><th>Rank</th><th>Gateway</th><th>Attempts</th><th>Clean Rate</th><th>Cascade Rate</th></tr></thead>
            <tbody>`;

        if (gwRanking.length === 0) {
          html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:16px">No gateway data.</td></tr>';
        } else {
          gwRanking.forEach((gw, idx) => {
            html += `<tr>
              <td>${idx + 1}</td>
              <td><strong>${gw.gateway_name || gw.gateway || '—'}</strong></td>
              <td>${formatNum(gw.total || gw.attempts || 0)}</td>
              ${this.rateCellHtml(gw.clean_rate ?? gw.approval_rate)}
              ${this.rateCellHtml(gw.cascade_rate)}
            </tr>`;
          });
        }
        html += '</tbody></table></div></div>';

        // Top BINs table
        const topBins = active.top_bins || active.bins || [];
        html += `<div class="card" style="margin-bottom:16px">
          <h3 style="padding:16px 16px 0;font-size:14px;font-weight:600">Top BINs</h3>
          <div class="table-wrap"><table>
            <thead><tr><th>BIN</th><th>Issuer</th><th>Attempts</th><th>Clean Rate</th><th>Best GW</th></tr></thead>
            <tbody>`;

        if (topBins.length === 0) {
          html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:16px">No BIN data.</td></tr>';
        } else {
          for (const b of topBins) {
            html += `<tr>
              <td><strong>${b.bin || '—'}</strong></td>
              <td>${b.issuer_bank || b.issuer || '—'}</td>
              <td>${formatNum(b.total || b.attempts || 0)}</td>
              ${this.rateCellHtml(b.clean_rate ?? b.approval_rate)}
              <td>${(typeof b.best_gateway === 'object' ? b.best_gateway?.gateway_name : b.best_gateway) || b.best_gw || '—'}</td>
            </tr>`;
          }
        }
        html += '</tbody></table></div></div>';

        // Card type/brand charts (as summary tables)
        const cardTypes = active.card_type_breakdown || active.card_types || [];
        const cardBrands = active.card_brand_breakdown || active.card_brands || [];

        if (cardTypes.length > 0 || cardBrands.length > 0) {
          html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">';

          if (cardTypes.length > 0) {
            html += `<div class="card" style="padding:16px">
              <h3 style="font-size:14px;font-weight:600;margin-bottom:12px">By Card Type</h3>`;
            for (const ct of cardTypes) {
              const pct = active.attempts ? ((ct.attempts / active.attempts) * 100) : 0;
              html += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;font-size:13px">
                <span>${ct.card_type || ct.type || '—'}</span>
                <span>
                  <span style="color:var(--text-muted)">${formatNum(ct.attempts)}</span>
                  ${rateBarHtml(ct.clean_rate || 0, 80)}
                </span>
              </div>`;
            }
            html += '</div>';
          }

          if (cardBrands.length > 0) {
            html += `<div class="card" style="padding:16px">
              <h3 style="font-size:14px;font-weight:600;margin-bottom:12px">By Card Brand</h3>`;
            for (const cb of cardBrands) {
              html += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;font-size:13px">
                <span>${cb.brand || cb.card_brand || '—'}</span>
                <span>
                  <span style="color:var(--text-muted)">${formatNum(cb.attempts)}</span>
                  ${rateBarHtml(cb.clean_rate || 0, 80)}
                </span>
              </div>`;
            }
            html += '</div>';
          }

          html += '</div>';
        }
      }

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load TX type analysis</h3><p>${err.message}</p></div>`;
    }
  },

  switchTxTypeTab(name) {
    this.txTypeSubTab = name;
    this.renderTxTypeAnalysis();
  },

  // ── Routing Recommendations ──

  async renderRouting(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading routing recommendations...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/routing-recommendations`).then(r => r.json());
      let allRecs = data.recommendations || data || [];

      // Filter by tx_type group
      let recs = allRecs;
      if (this.txTypeFilter !== 'all') {
        const filterMap = { initials: 'INITIALS', rebills: 'REBILLS', upsells: 'UPSELLS' };
        const group = filterMap[this.txTypeFilter];
        recs = allRecs.filter(r => r.tx_group === group);
      }

      recs = this.sortRows(recs, 'monthly_revenue_impact', 'desc');
      const totalOpp = recs.reduce((s, r) => s + (r.monthly_revenue_impact || 0), 0);

      // Count per group
      const groupCounts = {};
      for (const r of allRecs) { groupCounts[r.tx_group] = (groupCounts[r.tx_group] || 0) + 1; }

      let html = '';

      // Summary bar
      html += `<div class="card" style="padding:16px 20px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
        <div style="display:flex;gap:24px;align-items:center">
          <div><span style="font-size:24px;font-weight:700;color:var(--text)">${recs.length}</span> <span style="color:var(--text-secondary);font-size:13px">Recommendations</span></div>
          <div><span style="font-size:24px;font-weight:700;color:var(--success)">${this.moneyFmt(totalOpp)}</span> <span style="color:var(--text-secondary);font-size:13px">/mo opportunity</span></div>
        </div>
        <div style="font-size:13px;color:var(--text-muted)">${this.moneyFmt(totalOpp * 12)}/yr projected</div>
      </div>`;

      // Filter tabs with counts
      html += `<div class="tabs" style="margin-bottom:16px">
        ${['all','initials','rebills','upsells'].map(f => {
          const label = { all: 'All', initials: 'Initials', rebills: 'Rebills', upsells: 'Upsells' }[f];
          const groupKey = { initials: 'INITIALS', rebills: 'REBILLS', upsells: 'UPSELLS' }[f];
          const count = f === 'all' ? allRecs.length : (groupCounts[groupKey] || 0);
          return `<div class="tab ${this.txTypeFilter === f ? 'active' : ''}" onclick="Analytics.setRoutingFilter('${f}')">${label} (${count})</div>`;
        }).join('')}
      </div>`;

      if (recs.length === 0) {
        html += '<div class="empty-state"><h3>No Recommendations</h3><p>No routing recommendations for this filter.</p></div>';
      } else {
        html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px">';
        for (const r of recs) {
          const label = this.formatEntityLabel(r);
          const isCluster = r.entity_type === 'cluster';
          const conf = typeof r.confidence === 'object' ? (r.confidence?.level || 'low') : (r.confidence || 'low');
          const borderColor = this.confidenceBorder(conf);
          const monthlyRev = r.monthly_revenue_impact || 0;
          const revColor = this.revenueColor(monthlyRev);
          const curGw = typeof r.current_gateway === 'object' ? r.current_gateway : {};
          const recGw = typeof r.recommended_gateway === 'object' ? r.recommended_gateway : {};

          html += `<div class="card" style="padding:0;border-left:4px solid ${borderColor};overflow:hidden">
            <div style="padding:16px 16px 12px;display:flex;justify-content:space-between;align-items:flex-start">
              <div>
                <div style="font-size:14px;font-weight:600">${label}</div>
                ${isCluster && r.bins ? `<div style="font-size:11px;color:var(--text-muted)">${r.bins.length} BINs, no issuer data</div>` : ''}
                <span class="pill pill-active" style="font-size:11px;margin-top:4px;display:inline-block">${this.txGroupLabel(r.tx_group)}</span>
              </div>
              <div style="text-align:right">
                ${this.confidencePill(conf)}
                <div style="font-size:18px;font-weight:700;color:${revColor};margin-top:4px">${this.moneyFmt(monthlyRev)}/mo</div>
              </div>
            </div>
            <div style="padding:0 16px 12px;font-size:13px;border-bottom:1px solid var(--border)">
              <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase">Routing Change</div>
              <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 8px">
                <span style="color:var(--text-muted)">FROM:</span> <span>${curGw.gateway_name || '—'} <span style="color:var(--danger)">${(curGw.rate || 0).toFixed(1)}%</span></span>
                <span style="color:var(--text-muted)">TO:</span> <strong>${recGw.gateway_name || '—'} <span style="color:var(--success)">${(recGw.rate || 0).toFixed(1)}%</span></strong>
              </div>
            </div>
            <div style="padding:12px 16px;font-size:13px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px">
              <div><span style="color:var(--text-muted)">Lift:</span> <strong style="color:var(--success)">+${(r.lift_pp || 0).toFixed(1)}pp</strong></div>
              <div><span style="color:var(--text-muted)">Monthly:</span> <strong>${formatNum(r.monthly_attempts || 0)}</strong></div>
              <div><span style="color:var(--text-muted)">Annual:</span> <strong>${this.moneyFmt(r.annual_revenue_impact)}</strong></div>
            </div>
            ${r.cascade ? `<div style="padding:8px 16px;font-size:11px;color:var(--text-muted);background:var(--bg);border-top:1px solid var(--border)">
              Cascade: ${r.cascade.chain?.map(c => c.gateway_name?.split('_')[0] || 'GW').join(' → ') || '—'}
            </div>` : ''}
          </div>`;
        }
        html += '</div>';
      }

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load routing recommendations</h3><p>${err.message}</p></div>`;
    }
  },

  setRoutingFilter(filter) {
    this.txTypeFilter = filter;
    this.renderRouting();
  },

  // ── Lift Opportunities ──

  async renderLiftOpportunities(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading lift opportunities...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/lift-opportunities`).then(r => r.json());
      const summary = data.summary || {};
      const opportunities = data.opportunities || data || [];

      const totalMonthly = summary.total_monthly_revenue || opportunities.reduce((s, o) => s + (o.monthly_revenue_impact || 0), 0);
      const totalAnnual = summary.total_annual_revenue || totalMonthly * 12;
      const top10Pct = summary.top_10_bins_pct || 100;

      let html = '';

      // Summary bar (same pattern as CRM rules)
      html += `<div class="card" style="padding:16px 20px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
        <div style="display:flex;gap:24px;align-items:center">
          <div><span style="font-size:24px;font-weight:700;color:var(--success)">${this.moneyFmt(totalMonthly)}</span> <span style="color:var(--text-secondary);font-size:13px">/mo opportunity</span></div>
          <div><span style="font-size:24px;font-weight:700;color:var(--text)">${this.moneyFmt(totalAnnual)}</span> <span style="color:var(--text-secondary);font-size:13px">/yr projected</span></div>
        </div>
        <div style="font-size:13px;color:var(--text-muted)">${opportunities.length} opportunities &middot; Top 10 = ${top10Pct}% of total</div>
      </div>`;

      // Breakdown by tx_type group
      const byGroup = summary.by_tx_group ? Object.values(summary.by_tx_group) : [];
      if (byGroup.length > 0) {
        html += `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-bottom:16px">`;
        for (const g of byGroup) {
          html += `<div class="card" style="padding:14px">
            <div style="font-size:11px;font-weight:600;color:var(--text-muted);text-transform:uppercase;margin-bottom:4px">${this.txGroupLabel(g.tx_group)}</div>
            <div style="font-size:20px;font-weight:700;color:var(--success)">${this.moneyFmt(g.total_monthly)}/mo</div>
            <div style="font-size:12px;color:var(--text-muted)">${g.opportunity_count} opportunities &middot; avg +${(g.avg_lift_pp || 0).toFixed(1)}pp</div>
          </div>`;
        }
        html += '</div>';
      }

      // Opportunity cards
      if (opportunities.length === 0) {
        html += '<div class="empty-state"><h3>No Opportunities</h3><p>No lift opportunities found. Run analysis first.</p></div>';
      } else {
        html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px">';
        for (const o of opportunities) {
          const label = this.formatEntityLabel(o);
          const isCluster = o.entity_type === 'cluster';
          const conf = typeof o.confidence === 'object' ? (o.confidence?.level || 'low') : (o.confidence || 'low');
          const borderColor = this.confidenceBorder(conf);
          const monthlyRev = o.monthly_revenue_impact || 0;
          const revColor = this.revenueColor(monthlyRev);
          const curGw = typeof o.current_gateway === 'object' ? o.current_gateway : {};
          const bestGw = typeof o.best_gateway === 'object' ? o.best_gateway : {};

          html += `<div class="card" style="padding:0;border-left:4px solid ${borderColor};overflow:hidden">
            <div style="padding:16px 16px 12px;display:flex;justify-content:space-between;align-items:flex-start">
              <div>
                <div style="font-size:14px;font-weight:600">${label}</div>
                ${isCluster && o.bins ? `<div style="font-size:11px;color:var(--text-muted)">${o.bins.length} BINs</div>` : ''}
                <span class="pill pill-active" style="font-size:11px;margin-top:4px;display:inline-block">${this.txGroupLabel(o.tx_group)}</span>
              </div>
              <div style="text-align:right">
                ${this.confidencePill(conf)}
                <div style="font-size:18px;font-weight:700;color:${revColor};margin-top:4px">${this.moneyFmt(monthlyRev)}/mo</div>
              </div>
            </div>
            <div style="padding:0 16px 12px;font-size:13px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px">
              <div><span style="color:var(--text-muted)">Current:</span> <strong style="color:var(--danger)">${(o.current_rate || curGw.rate || 0).toFixed(1)}%</strong></div>
              <div><span style="color:var(--text-muted)">Expected:</span> <strong style="color:var(--success)">${(o.best_rate || bestGw.rate || 0).toFixed(1)}%</strong></div>
              <div><span style="color:var(--text-muted)">Lift:</span> <strong style="color:var(--success)">+${(o.lift_pp || 0).toFixed(1)}pp</strong></div>
            </div>
            <div style="padding:8px 16px;font-size:12px;color:var(--text-muted);background:var(--bg);border-top:1px solid var(--border);display:flex;justify-content:space-between">
              <span>${formatNum(o.monthly_attempts || 0)} orders/mo &middot; avg ${this.moneyFmt(o.avg_order_total)}</span>
              <span>${this.moneyFmt(o.annual_revenue_impact || monthlyRev * 12)}/yr</span>
            </div>
          </div>`;
        }
        html += '</div>';
      }

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load lift opportunities</h3><p>${err.message}</p></div>`;
    }
  },

  // ── Confidence Layer ──

  async renderConfidence(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading confidence data...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/confidence-layer`).then(r => r.json());
      const entries = data.entries || data.confidence || data || [];

      let html = `<div class="card"><div class="table-wrap"><table>
        <thead><tr>
          <th>BIN</th><th>Gateway</th><th>Sample Size</th><th>Consistency</th>
          <th>Recency</th><th>Overall Score</th><th>Stability</th><th>Flags</th>
        </tr></thead>
        <tbody>`;

      if (entries.length === 0) {
        html += '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px">No confidence data found.</td></tr>';
      } else {
        for (const e of entries) {
          const level = e.overall_level || e.confidence_level || (e.overall_score >= 0.8 ? 'HIGH' : e.overall_score >= 0.5 ? 'MEDIUM' : 'LOW');
          html += `<tr>
            <td><strong>${e.bin || '—'}</strong></td>
            <td>${e.gateway || e.gateway_name || '—'}</td>
            <td>${formatNum(e.sample_size || e.sample)}</td>
            <td>${e.consistency != null ? (e.consistency * 100).toFixed(0) + '%' : '—'}</td>
            <td>${e.recency != null ? (e.recency * 100).toFixed(0) + '%' : '—'}</td>
            <td>${this.confidencePill(level)} <span style="font-size:12px;color:var(--text-muted)">${e.overall_score != null ? (e.overall_score * 100).toFixed(0) + '%' : ''}</span></td>
            <td>${e.stability || '—'}</td>
            <td style="font-size:12px">${(e.flags || []).length > 0
              ? e.flags.map(f => `<span class="pill" style="background:var(--warning-light);color:var(--warning);margin:1px">${f}</span>`).join(' ')
              : '<span style="color:var(--text-muted)">none</span>'}</td>
          </tr>`;
        }
      }

      html += '</tbody></table></div></div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load confidence data</h3><p>${err.message}</p></div>`;
    }
  },

  // ── Trend Detection ──

  async renderTrends(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading trends...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/trend-detection`).then(r => r.json());

      const alerts = data.alerts || [];
      const binTrends = data.bin_trends || [];
      const gwTrends = data.gateway_trends || [];
      const declineChanges = data.decline_changes || [];

      let html = '';

      // Alerts section grouped by priority
      html += `<div class="card" style="margin-bottom:20px;padding:16px">
        <h3 style="font-size:15px;font-weight:600;margin-bottom:12px">Alerts</h3>`;

      if (alerts.length === 0) {
        html += '<p style="color:var(--text-muted)">No active alerts.</p>';
      } else {
        const byPriority = {};
        for (const a of alerts) {
          const p = a.priority || 'P4';
          if (!byPriority[p]) byPriority[p] = [];
          byPriority[p].push(a);
        }

        for (const p of ['P1', 'P2', 'P3', 'P4']) {
          if (!byPriority[p] || byPriority[p].length === 0) continue;
          html += `<div style="margin-bottom:12px">
            <div style="margin-bottom:6px">${this.priorityPill(p)} <span style="font-size:12px;color:var(--text-muted)">${byPriority[p].length} alert(s)</span></div>`;
          for (const a of byPriority[p]) {
            const borderColor = { P1: 'var(--danger)', P2: '#ea580c', P3: 'var(--warning)', P4: 'var(--border)' }[p];
            html += `<div style="border-left:3px solid ${borderColor};padding:8px 12px;margin-bottom:6px;background:var(--bg);border-radius:0 var(--radius) var(--radius) 0">
              <div style="font-weight:600;font-size:13px">${a.entity || a.bin || a.gateway || '—'} — ${a.type || a.alert_type || '—'}</div>
              <div style="font-size:12px;color:var(--text-secondary)">${a.message || a.description || '—'}</div>
              ${a.detected_at ? `<div style="font-size:11px;color:var(--text-muted);margin-top:2px">${timeAgo(a.detected_at)}</div>` : ''}
            </div>`;
          }
          html += '</div>';
        }
      }
      html += '</div>';

      // BIN trends table
      html += `<div class="card" style="margin-bottom:20px">
        <h3 style="padding:16px 16px 0;font-size:15px;font-weight:600">BIN Trends</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>BIN</th><th>Issuer</th><th>Direction</th><th>Change</th><th>Period</th><th>Volume</th></tr></thead>
          <tbody>`;

      if (binTrends.length === 0) {
        html += '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:16px">No BIN trends detected.</td></tr>';
      } else {
        for (const t of binTrends) {
          const dirColor = t.direction === 'improving' ? 'var(--success)' : t.direction === 'declining' ? 'var(--danger)' : 'var(--text-secondary)';
          html += `<tr>
            <td><strong>${t.bin || '—'}</strong></td>
            <td>${t.issuer || '—'}</td>
            <td style="color:${dirColor};font-weight:600">${t.direction || '—'}</td>
            <td>${t.change != null ? (t.change > 0 ? '+' : '') + t.change.toFixed(1) + 'pp' : '—'}</td>
            <td>${t.period || '—'}</td>
            <td>${formatNum(t.volume || t.attempts)}</td>
          </tr>`;
        }
      }
      html += '</tbody></table></div></div>';

      // Gateway trends table
      html += `<div class="card" style="margin-bottom:20px">
        <h3 style="padding:16px 16px 0;font-size:15px;font-weight:600">Gateway Trends</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Gateway</th><th>Direction</th><th>Change</th><th>Period</th><th>Volume</th></tr></thead>
          <tbody>`;

      if (gwTrends.length === 0) {
        html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:16px">No gateway trends detected.</td></tr>';
      } else {
        for (const t of gwTrends) {
          const dirColor = t.direction === 'improving' ? 'var(--success)' : t.direction === 'declining' ? 'var(--danger)' : 'var(--text-secondary)';
          html += `<tr>
            <td><strong>${t.gateway || t.gateway_name || '—'}</strong></td>
            <td style="color:${dirColor};font-weight:600">${t.direction || '—'}</td>
            <td>${t.change != null ? (t.change > 0 ? '+' : '') + t.change.toFixed(1) + 'pp' : '—'}</td>
            <td>${t.period || '—'}</td>
            <td>${formatNum(t.volume || t.attempts)}</td>
          </tr>`;
        }
      }
      html += '</tbody></table></div></div>';

      // Decline pattern changes
      html += `<div class="card">
        <h3 style="padding:16px 16px 0;font-size:15px;font-weight:600">Decline Pattern Changes</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Decline Reason</th><th>Direction</th><th>Change</th><th>Previous</th><th>Current</th></tr></thead>
          <tbody>`;

      if (declineChanges.length === 0) {
        html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:16px">No decline pattern changes detected.</td></tr>';
      } else {
        for (const d of declineChanges) {
          const dirColor = d.direction === 'increasing' ? 'var(--danger)' : d.direction === 'decreasing' ? 'var(--success)' : 'var(--text-secondary)';
          html += `<tr>
            <td><strong>${d.decline_reason || d.reason || '—'}</strong></td>
            <td style="color:${dirColor};font-weight:600">${d.direction || '—'}</td>
            <td>${d.change != null ? (d.change > 0 ? '+' : '') + d.change.toFixed(1) + '%' : '—'}</td>
            <td>${d.previous != null ? formatNum(d.previous) : '—'}</td>
            <td>${d.current != null ? formatNum(d.current) : '—'}</td>
          </tr>`;
        }
      }
      html += '</tbody></table></div></div>';

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load trends</h3><p>${err.message}</p></div>`;
    }
  },

  // ── Price Points ──

  pricePointLevel: 2,

  async renderPricePoints(preloaded) {
    const el = document.getElementById('analyticsContent');
    if (!preloaded) { el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading price points...</div>'; }

    try {
      const level = this.pricePointLevel || 2;
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/price-points?level=${level}`).then(r => r.json());
      const groups = data.groups || data || [];

      // Sort by totalAttempts DESC
      const sorted = [...groups].sort((a, b) => (b.totalAttempts || 0) - (a.totalAttempts || 0));

      const bucketKeys = ['$0-25', '$26-50', '$51-75', '$76-100', '$100+'];

      let html = '';

      // Level switcher
      const levels = [
        { level: 1, label: 'Level 1: Bank' },
        { level: 2, label: 'Level 2: Bank+Brand' },
        { level: 3, label: 'Level 3: +Type+Prepaid' },
        { level: 4, label: 'Level 4: +Level' },
      ];
      html += `<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
        ${levels.map(l => `<button class="btn btn-sm ${level === l.level ? 'btn-primary' : 'btn-secondary'}" onclick="Analytics.switchPricePointLevel(${l.level})">${l.label}</button>`).join('')}
      </div>`;

      // Table
      html += `<div class="card"><div class="table-wrap"><table>
        <thead><tr>
          <th>Group</th>
          <th>$0-25</th>
          <th>$26-50</th>
          <th>$51-75</th>
          <th style="border-left:2px solid var(--accent)">$76-100</th>
          <th>$100+</th>
          <th>Best</th>
          <th>Attempts</th>
          <th>Flag</th>
        </tr></thead>
        <tbody>`;

      if (sorted.length === 0) {
        html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No price point data found. Run analysis first.</td></tr>';
      } else {
        for (const g of sorted) {
          const buckets = g.buckets || {};
          const best = g.bestBucket || '';
          html += '<tr>';
          html += `<td><strong>${g.groupLabel || '—'}</strong></td>`;
          for (const bk of bucketKeys) {
            const bucket = buckets[bk];
            const rate = bucket != null ? (typeof bucket === 'object' ? bucket.rate : bucket) : null;
            const isBest = bk === best;
            const isCurrent = bk === '$76-100';
            let style = '';
            if (isBest) style += 'background:#ecfdf5;';
            if (isCurrent) style += 'border-left:2px solid var(--accent);';
            if (rate == null) {
              html += `<td style="${style}color:var(--text-muted)">—</td>`;
            } else {
              html += `<td style="${style}font-weight:600">${rate.toFixed(1)}%</td>`;
            }
          }
          html += `<td>${best || '—'}</td>`;
          html += `<td>${formatNum(g.totalAttempts || 0)}</td>`;
          html += `<td>${g.hasOptimizeFlag ? '<span class="pill" style="background:#fffbeb;color:#92400e;font-weight:600">Optimize</span>' : ''}</td>`;
          html += '</tr>';
        }
      }

      html += '</tbody></table></div></div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load price points</h3><p>${err.message}</p></div>`;
    }
  },

  switchPricePointLevel(level) {
    this.pricePointLevel = level;
    delete this._cache['price-points'];
    this.runTab('price-points');
  },

  // ── CRM Rules ──

  // TX type label helper
  txGroupLabel(group) {
    const map = { INITIALS: 'Initials', REBILLS: 'Rebills', UPSELLS: 'Upsells', STRAIGHT_SALES: 'Straight Sales',
      cp_initial: 'Initials', initial_salvage: 'Salvage', tp_rebill: 'Rebills', tp_rebill_salvage: 'Rebill Salvage',
      upsell: 'Upsells', upsell_cascade: 'Upsell Cascade', sticky_cof_rebill: 'COF Rebill', straight_sale: 'Straight Sale' };
    return map[group] || group || '—';
  },

  // Revenue color helper
  revenueColor(amount) {
    if (amount >= 500) return 'var(--success)';
    if (amount >= 100) return 'var(--warning)';
    return 'var(--text-muted)';
  },

  // Confidence border color
  confidenceBorder(level) {
    const l = (level || '').toUpperCase();
    if (l === 'HIGH') return 'var(--success)';
    if (l === 'MEDIUM') return 'var(--warning)';
    return 'var(--danger)';
  },

  // CRM Rules tab state
  crmTab: 'INITIALS',
  crmStageFilter: null,

  async renderCrmRules(preloaded) {
    const el = document.getElementById('analyticsContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading routing rules...</div>';

    try {
      const data = preloaded || await fetch(`/api/analytics/${this.clientId}/crm-rules`).then(r => r.json());
      const allRules = data.rules || [];
      const summary = data.summary || {};
      this._crmRules = allRules;
      this._crmSummary = summary;
      this._crmProcessorRules = data.processorRules || [];

      const tab = this.crmTab;
      let rules = allRules.filter(r => r.txGroup === tab);
      if (this.crmStageFilter != null) {
        rules = rules.filter(r => r.stage === this.crmStageFilter);
      }
      if (this.crmLevelFilter != null) {
        rules = rules.filter(r => r.level === this.crmLevelFilter);
      }
      rules = rules.filter(r => r.status !== 'dismissed' && r.status !== 'archived' && r.status !== 'merged');
      if (this.crmConfFilter) {
        rules = rules.filter(r => (r.confidence || 'LOW').toUpperCase() === this.crmConfFilter);
      }

      // Sort: new_gateway cards first, then by revenue impact descending
      rules.sort((a, b) => {
        const aNew = a.gatewayInsight?.state === 'new_gateway' ? 1 : 0;
        const bNew = b.gatewayInsight?.state === 'new_gateway' ? 1 : 0;
        if (aNew !== bNew) return bNew - aNew;
        return (b.expectedImpact?.monthly_revenue_impact || 0) - (a.expectedImpact?.monthly_revenue_impact || 0);
      });

      const allProcRules = data.processorRules || [];
      let procRules = allProcRules.filter(r => r.txGroup === tab);
      if (this.crmLevelFilter != null) {
        procRules = procRules.filter(r => r.level === this.crmLevelFilter);
      }
      if (this.crmConfFilter) {
        procRules = procRules.filter(r => (r.confidence || 'LOW').toUpperCase() === this.crmConfFilter);
      }

      // Tab counts — use V2 initials qualifying bank counts if cached, else beast rule counts
      const initSummary = this._foV2InitCache?.main?.summary;
      const upsellSummary = this._foV2InitCache?.upsell?.summary;
      const rebillSummary = this._foV2Cache?.summary;
      const tabCounts = {
        INITIALS: initSummary ? (initSummary.qualifyingBanks || 0) : allRules.filter(r => r.txGroup === 'INITIALS').length,
        UPSELLS: upsellSummary ? (upsellSummary.qualifyingBanks || 0) : allRules.filter(r => r.txGroup === 'UPSELLS').length,
        REBILLS: rebillSummary ? (rebillSummary.qualifyingBanks || 0) : (summary.rebillOpportunities || 0),
      };
      const procCounts = { INITIALS: 0, UPSELLS: 0 };
      const totalMonthly = summary.totalMonthlyRevenue || 0;
      const totalAnnual = summary.totalAnnualRevenue || 0;
      const stages = summary.byStage || {};

      let html = '';

      // Compute per-tab revenue from rules
      const tabRules = allRules.filter(r => r.txGroup === tab);
      const tabMonthly = Math.round(tabRules.reduce((s, r) => s + (r.expectedImpact?.monthly_revenue_impact || 0), 0) * 100) / 100;
      const tabAnnual = Math.round(tabMonthly * 12 * 100) / 100;

      // Compute confidence counts for current tab (all rules + proc rules)
      const allTabCards = [...tabRules, ...procRules];
      const confCounts = { HIGH: 0, MEDIUM: 0, LOW: 0 };
      for (const r of allTabCards) {
        const c = (r.confidence || 'LOW').toUpperCase();
        if (confCounts[c] != null) confCounts[c]++;
      }
      const confFilter = this.crmConfFilter || null;

      // Tabs: Initials | Upsells | Rebills | Playbook
      html += `<div class="tabs" style="margin-bottom:12px">
        ${['INITIALS', 'UPSELLS', 'REBILLS', 'PLAYBOOK'].map(t => {
          const label = t === 'INITIALS' ? 'Initials' : t === 'UPSELLS' ? 'Upsells' : t === 'REBILLS' ? 'Rebills' : 'Playbook';
          const count = t === 'PLAYBOOK' ? (this._playbookCache?.summary?.totalRows || '') : (tabCounts[t] || 0);
          const extra = t !== 'PLAYBOOK' && procCounts[t] ? ' + ' + procCounts[t] + ' insights' : '';
          return `<div class="tab ${tab === t ? 'active' : ''}" onclick="Analytics.setCrmTab('${t}')">${label}${count ? ' (' + count + ')' : ''}${extra}</div>`;
        }).join('')}
      </div>`;

      if (tab === 'PLAYBOOK') {
        await this._renderPlaybook(el, html);
        return;
      }

      if (tab === 'REBILLS') {
        // Flow Optix V2 cards
        await this._renderFlowOptixV2(el, html);
        return;
      }

      if (tab === 'INITIALS' || tab === 'UPSELLS') {
        // Flow Optix V2 drill-down cards for initials/upsells
        await this._renderFlowOptixV2Initials(el, html, tab);
        return;
      }
    } catch (err) {
      el.innerHTML = `<div class="empty-state"><h3>Failed to load rules</h3><p>${err.message}</p></div>`;
    }
  },

  _copyBinsBtnHtml(bins) {
    if (!bins || bins.length === 0) return '';
    const text = bins.join(', ');
    const id = 'cb_' + Math.random().toString(36).slice(2, 8);
    return ` <button id="${id}" onclick="navigator.clipboard.writeText('${text}');var b=document.getElementById('${id}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:none;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:0 4px;font-family:inherit">Copy</button>`;
  },

  // Shared decline section renderer for both gateway rule and processor affinity cards
  _declineSection(softDeclines, hardDeclines, issuerExceptions, softId, hardId, attemptGroups) {
    if (!softId) softId = 'sd_' + Math.random().toString(36).slice(2, 8);
    if (!hardId) hardId = 'hd_' + Math.random().toString(36).slice(2, 8);
    const softReasons = (softDeclines || []);
    const hardReasons = (hardDeclines || []);
    const exceptions = (issuerExceptions || []);

    let html = '';

    // ALLOW soft declines
    const softText = softReasons.map(d => d.reason).join(', ');
    html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:13px">
      <div style="display:flex;align-items:center;gap:8px">
        <span style="color:#0F6E56;font-weight:600;font-size:14px">ALLOW</span>
        <span style="font-size:13px;color:var(--text-secondary)">soft declines</span>
        <button id="${softId}" onclick="navigator.clipboard.writeText('${(softText || 'Allow: soft declines').replace(/'/g, "\\'")}');var b=document.getElementById('${softId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy</button>
      </div>
    </div>`;

    // BLOCK hard declines
    const hardText = hardReasons.map(d => d.reason).join(', ');
    html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:13px">
      <div style="display:flex;align-items:center;gap:8px">
        <span style="color:var(--danger);font-weight:600;font-size:14px">BLOCK</span>
        <span style="font-size:13px;color:var(--text-secondary)">hard declines</span>
        <button id="${hardId}" onclick="navigator.clipboard.writeText('${(hardText || 'Block: hard declines').replace(/'/g, "\\'")}');var b=document.getElementById('${hardId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy</button>
      </div>
    </div>`;

    // Attempt exceptions — grouped by attempt number
    const attGroups = attemptGroups || [];
    if (attGroups.length > 0) {
      html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:13px">
        <div style="font-weight:600;color:#92400e;font-size:12px;margin-bottom:8px">Attempt exceptions</div>`;

      for (const g of attGroups) {
        const isBlock = g.action === 'block';
        const color = isBlock ? 'var(--danger)' : '#0F6E56';
        const label = isBlock ? 'Block' : 'Allow';
        const reasonCount = g.reasons.length;
        const reasonText = g.reasons.join(', ');
        const copyText = `${label} on Att ${g.attempt}: ${reasonText}`;
        const copyId = 'aex_' + Math.random().toString(36).slice(2, 8);

        html += `<div style="display:flex;align-items:center;gap:10px;padding:4px 0">
          <span style="font-family:'IBM Plex Mono',monospace;font-size:13px;color:${color};white-space:nowrap;min-width:36px">Att ${g.attempt}</span>
          <span style="font-size:13px;color:${color};font-weight:600">${label}</span>
          <span style="font-size:13px;color:var(--text-secondary)">&middot; ${reasonCount} reason${reasonCount > 1 ? 's' : ''}</span>
          <button id="${copyId}" onclick="navigator.clipboard.writeText('${copyText.replace(/'/g, "\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy</button>
        </div>`;
      }

      html += `</div>`;
    }

    return html;
  },

  // Per-card level analysis cache
  _levelAnalysisCache: {},
  _serverCacheInfo: null,

  _tabToEndpoint(tab) {
    const map = {
      'bin-profiles': 'bin-profiles', 'bin-clusters': 'bin-clusters',
      'gateway-profiles': 'gateway-profiles', 'decline-matrix': 'decline-matrix',
      'txtype-analysis': 'txtype-analysis', 'routing': 'routing-recommendations',
      'lift-opportunities': 'lift-opportunities', 'confidence': 'confidence-layer',
      'trends': 'trend-detection', 'price-points': 'price-points', 'crm-rules': 'crm-rules',
    };
    return map[tab] || tab;
  },

  _beastRuleCard(r, tab) {
    const conf = (r.confidence || 'LOW').toUpperCase();
    const imp = r.expectedImpact || {};
    const monthlyRev = imp.monthly_revenue_impact || 0;
    const bc = r.beastConfig || {};
    const isUpsell = tab === 'UPSELLS';
    const stage = r.stage || 1;
    const confColors = { HIGH: '#0F6E56', MEDIUM: '#92400e', LOW: 'var(--text-muted)' };
    const confBg = { HIGH: '#ecfdf5', MEDIUM: '#fffbeb', LOW: '#f3f4f6' };

    // Copy config text (hidden behind button)
    const configText = `Rule Name: ${bc.ruleName || r.ruleName}\\nCycle: ${bc.cycle || r.cycleLabel}\\nGroup Type: ${bc.groupType || r.groupType}\\nGroup Conditions: ${bc.groupConditions || r.groupConditions}\\nTarget Type: ${bc.targetType || r.targetType}\\nTarget: ${bc.target || r.targetValue}`;
    const configId = 'cfg_' + Math.random().toString(36).slice(2, 8);

    // BIN chips
    const bins = r.binsInGroup || [];
    const showBins = bins.length > 0 && r.groupType === 'bin';
    const binChips = showBins ? bins.slice(0, 3).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:13px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:2px 7px">${b}</span>`
    ).join(' ') + (bins.length > 3 ? ` <span style="font-size:13px;color:var(--text-secondary)">+${bins.length - 3} more</span>` : '') : '';

    // Target display — show gateway alias + approval rate
    const toGwIds = r.appliesTo?.to_gateway || [];
    const mp = r.midProgress || [];
    const toGw = toGwIds.length > 0 ? mp.find(m => m.gateway_id === toGwIds[0]) : mp[0];
    const toName = toGw ? toGw.gateway_name : r.targetValue;
    const toRate = toGw ? ` &rarr; ${toGw.rate}%` : '';
    const targetLine = `Route to: <strong>${toName}</strong>${toRate}`;
    const typeLine = `Type: ${r.targetType === 'mid' ? 'MID' : 'Acquirer'}`;

    // Action buttons
    let actionsHtml;
    if (isUpsell) {
      actionsHtml = `<button class="btn btn-sm btn-secondary" style="flex:1" disabled>Save for Later</button>
        <button class="btn btn-sm btn-secondary" onclick="Analytics.dismissRule('${r.ruleId}')">Dismiss</button>`;
    } else if (stage === 1) {
      actionsHtml = `<button class="btn btn-sm btn-primary" style="flex:1" onclick="Analytics.activateRule('${r.ruleId}')">Mark Active</button>
        <button class="btn btn-sm btn-secondary" onclick="Analytics.dismissRule('${r.ruleId}')">Dismiss</button>`;
    } else if (stage === 2) {
      actionsHtml = `<button class="btn btn-sm btn-primary" style="flex:1">View Progress</button>
        <button class="btn btn-sm btn-secondary" onclick="Analytics.dismissRule('${r.ruleId}')">Dismiss</button>`;
    } else {
      actionsHtml = `<button class="btn btn-sm btn-primary" style="flex:1" onclick="Analytics.activateRule('${r.ruleId}')">Mark Active</button>
        <button class="btn btn-sm btn-secondary" onclick="Analytics.dismissRule('${r.ruleId}')">Dismiss</button>`;
    }

    // Store rule data for lazy-load (keyed by ruleId)
    if (!this._ruleDataMap) this._ruleDataMap = {};
    this._ruleDataMap[r.ruleId] = r;

    // Gateway insight banner
    const gi = r.gatewayInsight || {};
    let bannerHtml = '';
    let cardBorder = 'border-left:2.5px solid #0F6E56';
    if (gi.state === 'new_gateway') {
      cardBorder = 'border:2px solid #378ADD';
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E6F1FB;border-bottom:0.5px solid #B5D4F4;font-size:12px;color:#0C447C">
        <span style="font-size:14px;flex-shrink:0">&#9432;</span>
        <span style="font-size:11px;font-weight:600;padding:2px 8px;border-radius:3px;background:#dbeafe;color:#1e40af">New gateway</span>
        <span>New gateway available — <strong>${gi.name}</strong> just activated</span>
        <span style="margin-left:auto;font-weight:600;color:#0F6E56;white-space:nowrap">+${(gi.lift || 0).toFixed(1)}pp potential</span>
      </div>`;
    } else if (gi.state === 'inactive_better') {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E6F1FB;border-bottom:0.5px solid #B5D4F4;font-size:12px;color:#0C447C">
        <span style="font-size:14px;flex-shrink:0">&#9432;</span>
        <span>Adding <strong>${gi.name}</strong> MID would improve to <strong>${(gi.rate || 0).toFixed(1)}%</strong></span>
        <span style="margin-left:auto;font-weight:600;color:#0F6E56;white-space:nowrap">+${(gi.lift || 0).toFixed(1)}pp potential</span>
      </div>`;
    } else {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E1F5EE;border-bottom:0.5px solid #9FE1CB;font-size:12px;color:#0F6E56">
        <span style="font-size:14px;flex-shrink:0">&#10003;</span>
        <span>Optimal routing — best available MID active</span>
      </div>`;
    }

    return `<div class="card" style="padding:0;${cardBorder};overflow:hidden;min-width:0">
      ${bannerHtml}
      <div style="padding:18px 24px 14px">
        <div style="display:flex;justify-content:space-between;align-items:baseline">
          <div style="font-size:16px;font-weight:600;color:var(--text)">${r.ruleName}</div>
          <span style="font-size:12px;color:var(--text-secondary)">${r.ruleId}</span>
        </div>
        ${(r.level || 1) === 5 ? this._l5SubtitleHtml(r) : ''}
        <div style="display:flex;gap:4px;margin-top:8px;align-items:center;flex-wrap:wrap">
          ${this._levelTrackerHtml(r.level || 1)}
        </div>
        ${(r.level || 1) === 5 ? this._l5WarningHtml(r) : ''}
        <div style="display:flex;gap:6px;margin-top:8px;align-items:center;flex-wrap:wrap">
          <span style="font-size:12px;font-weight:600;padding:3px 9px;border-radius:3px;background:${confBg[conf]};color:${confColors[conf]}">${conf}</span>
          ${isUpsell ? '<span style="font-size:12px;padding:3px 9px;border-radius:3px;background:#fff7ed;color:#ea580c">PENDING</span>' : ''}
          ${r.split_from_rule_id ? `<span style="font-size:12px;padding:3px 9px;border-radius:3px;background:#dbeafe;color:#1e40af">&larr; Split from ${r._parentName || r.split_from_rule_id}</span>` : ''}
        </div>
      </div>
      <div style="display:flex;border-top:1px solid var(--border);font-size:15px;text-align:center">
        <div style="flex:1;padding:12px 0;border-right:1px solid var(--border)"><strong style="color:#0F6E56">+${(imp.lift_pp || 0).toFixed(1)}pp</strong><div style="font-size:11px;color:var(--text-secondary)">lift</div></div>
        <div style="flex:1;padding:12px 0;border-right:1px solid var(--border)"><strong>${this.moneyFmt(monthlyRev)}</strong><div style="font-size:11px;color:var(--text-secondary)">/mo projected</div></div>
        <div style="flex:1;padding:12px 0"><strong>${formatNum(imp.monthly_attempts || 0)}</strong><div style="font-size:11px;color:var(--text-secondary)">orders/mo</div></div>
      </div>
      <div style="padding:10px 24px;font-size:15px;border-top:1px solid var(--border)">
        <div style="color:var(--text-secondary)">${targetLine}</div>
        <div style="color:var(--text-secondary);margin-top:2px">${typeLine}</div>
      </div>
      ${this._declineSection(r.softDeclines || [], r.hardDeclines || [], r.issuerExceptions || [], null, null, r.attemptGroups || [])}
      ${showBins ? `<div style="padding:8px 24px;display:flex;align-items:center;gap:8px;flex-wrap:wrap;border-top:1px solid var(--border)">
        <span style="font-size:13px;color:var(--text-secondary)">${bins.length} BIN${bins.length > 1 ? 's' : ''}:</span>
        ${binChips}${this._copyBinsBtnHtml(bins)}
      </div>` : ''}
      ${r.split_from_rule_id ? `<div style="padding:6px 24px;font-size:12px;border-top:1px solid var(--border)">
        <a href="#" onclick="event.preventDefault();Analytics.mergeBackPrompt('${r.ruleId}')" style="color:var(--text-secondary);text-decoration:none">&larr; Merge back into ${r._parentName || 'parent group'}</a>
      </div>` : ''}
      ${r.splitSuggestion ? this._renderSplitSection(r.splitSuggestion, r.ruleId, r.level || 1) : ''}
      <div style="padding:6px 24px 10px;font-size:12px;color:var(--text-secondary)">
        Sample: ${formatNum(r.sampleSize || 0)} orders &middot; ${(imp.current_rate || 0).toFixed(1)}% current &rarr; ${(imp.expected_rate || 0).toFixed(1)}% expected
      </div>
      <div id="hist_${r.ruleId.replace(/[^a-zA-Z0-9]/g, '_')}"></div>
      <div style="padding:10px 24px;display:flex;gap:10px;align-items:center;border-top:1px solid var(--border)">
        ${actionsHtml}
        <button id="${configId}" onclick="navigator.clipboard.writeText('${configText}');var b=document.getElementById('${configId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy Config',1500)" style="background:none;border:none;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:4px 8px;margin-left:auto">Copy Config</button>
      </div>
    </div>`;
  },

  /**
   * Lazy-load level analysis for a specific rule card.
   * Fetches on-demand, caches per card, toggles visibility on re-click.
   */
  async loadLevelAnalysis(ruleId) {
    const r = (this._ruleDataMap || {})[ruleId];
    if (!r) return;

    const laId = 'la_' + ruleId.replace(/[^a-zA-Z0-9]/g, '_');
    const container = document.getElementById(laId);
    if (!container) return;

    // Toggle: if already loaded and visible, collapse
    if (container.dataset.loaded === 'true') {
      container.style.display = container.style.display === 'none' ? '' : 'none';
      return;
    }

    // Check per-card cache
    if (this._levelAnalysisCache[ruleId]) {
      container.innerHTML = this._renderLevelSection(this._levelAnalysisCache[ruleId], r);
      container.dataset.loaded = 'true';
      return;
    }

    // Show loading state
    container.innerHTML = '<div style="padding:8px 16px;border-top:1px solid var(--border);font-size:11px;color:var(--text-muted)"><span class="spinner" style="width:12px;height:12px;display:inline-block;vertical-align:middle;margin-right:6px"></span>Analyzing sub-groups...</div>';

    // Build query params from rule's appliesTo
    const at = r.appliesTo || {};
    const params = new URLSearchParams();
    params.set('tx_group', r.txGroup || 'INITIALS');
    if (at.issuer_bank) params.set('issuer_bank', at.issuer_bank);
    if (at.card_brand) params.set('card_brand', at.card_brand);
    if (at.card_type) params.set('card_type', at.card_type);
    if (at.is_prepaid != null) params.set('is_prepaid', at.is_prepaid);
    if (at.card_level) params.set('card_level', at.card_level);

    try {
      const res = await fetch(`/api/analytics/${this.clientId}/level-analysis?${params}`);
      const la = await res.json();

      // Cache it
      this._levelAnalysisCache[ruleId] = la;

      // Render
      const html = this._renderLevelSection(la, r);
      container.innerHTML = html;
      container.dataset.loaded = 'true';
    } catch (err) {
      container.innerHTML = `<div style="padding:8px 16px;border-top:1px solid var(--border);font-size:11px;color:var(--danger)">Failed to load: ${err.message}</div>`;
    }
  },

  /**
   * Render the level analysis section (promotion / gathering / outlier).
   * Returns HTML string. If no signal, returns a minimal message.
   */
  _renderLevelSection(la, r) {
    if (!la) return '';

    let promotionHtml = '';
    let gatheringHtml = '';
    let outlierHtml = '';
    const self = this;

    // Promotion preview — variance detected, split recommended
    if (la.variance?.shouldPromote === true) {
      const subGroups = la.subGroups || [];
      let sgRows = '';
      for (const sg of subGroups) {
        const sgBins = sg.bins || [];
        const sgBinChips = sgBins.slice(0, 3).map(b =>
          `<span style="font-family:'IBM Plex Mono',monospace;font-size:10px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 5px">${b}</span>`
        ).join(' ') + (sgBins.length > 3 ? ` <span style="font-size:10px;color:var(--text-muted)">+${sgBins.length - 3} more</span>` : '');
        const diff = sg.diffVsCurrent != null ? (sg.diffVsCurrent >= 0 ? '+' : '') + sg.diffVsCurrent.toFixed(1) + 'pp' : '';
        const diffColor = sg.diffVsCurrent >= 0 ? '#0F6E56' : 'var(--danger)';
        sgRows += `<div style="padding:4px 0;display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:11px">
          <span style="font-weight:600;min-width:100px">${sg.label || ''}</span>
          <span>${(sg.rate || 0).toFixed(1)}%</span>
          <span style="color:${diffColor}">${diff}</span>
          <span style="color:var(--text-muted)">${sg.attempts || 0} att</span>
        </div>
        <div style="padding:2px 0 6px;display:flex;align-items:center;gap:4px;flex-wrap:wrap">
          ${sgBinChips}${self._copyBinsBtnHtml(sgBins)}
        </div>`;
      }
      const variancePp = la.variance.value != null ? la.variance.value.toFixed(1) : '?';
      const nextLevel = (r.level || 1) + 1;
      promotionHtml = `<div style="padding:8px 16px;border-top:1px solid var(--border);background:#fffbeb">
        <div style="font-size:11px;font-weight:600;color:#92400e;margin-bottom:6px">Variance detected &mdash; split recommended</div>
        ${sgRows}
        <div style="font-size:11px;color:var(--text-muted);margin-top:4px">${variancePp}pp variance &mdash; separate rules would improve both</div>
        <div style="margin-top:8px;display:flex;gap:8px;align-items:center">
          <button class="btn btn-sm btn-primary" onclick="Analytics.promoteRule('${r.ruleId}',${nextLevel})">Promote to L${nextLevel}</button>
          <span style="font-size:10px;color:var(--text-muted)">Copy BINs above and create groups in your routing rules before confirming</span>
        </div>
      </div>`;
    }

    // Gathering section — sub-groups exist but not enough data yet
    if (!la.variance?.shouldPromote && la.subGroups && la.subGroups.length > 0) {
      const gatheringSgs = la.subGroups.filter(sg => (sg.attempts || 0) < 30);
      const readySgs = la.subGroups.filter(sg => (sg.attempts || 0) >= 30);
      if (gatheringSgs.length > 0) {
        let gatherRows = '';
        for (const sg of la.subGroups) {
          const att = sg.attempts || 0;
          const pct = Math.min(100, Math.round((att / 30) * 100));
          const barColor = att >= 30 ? '#0F6E56' : '#dbeafe';
          gatherRows += `<div style="padding:3px 0;font-size:11px;display:flex;align-items:center;gap:8px">
            <span style="min-width:100px;font-weight:500">${sg.label || ''}</span>
            <div style="flex:1;height:6px;background:var(--bg);border-radius:3px;overflow:hidden;border:1px solid var(--border)">
              <div style="width:${pct}%;height:100%;background:${barColor};border-radius:3px"></div>
            </div>
            <span style="color:var(--text-muted);min-width:50px;text-align:right">${att}/30</span>
          </div>`;
        }
        const needsMore = gatheringSgs.map(sg => {
          const need = 30 - (sg.attempts || 0);
          return `${sg.label || '?'} needs ${need} more`;
        });
        gatheringHtml = `<div style="padding:8px 16px;border-top:1px solid var(--border);background:#f8fafc">
          <div style="font-size:11px;font-weight:600;color:var(--text-muted);margin-bottom:6px">Next level &mdash; watching for split signal</div>
          ${gatherRows}
          <div style="font-size:10px;color:var(--text-muted);margin-top:4px">Need 30+ per sub-group &middot; ${readySgs.length} ready &middot; ${needsMore.join(' &middot; ')}</div>
        </div>`;
      }
    }

    // Outlier box
    if (la.outliers && la.outliers.length > 0) {
      let outlierRows = '';
      for (const ol of la.outliers) {
        const olRate = (ol.rate || 0).toFixed(1);
        const rateColor = (ol.deviation || 0) >= 0 ? '#0F6E56' : 'var(--danger)';
        const devSign = (ol.deviation || 0) >= 0 ? '+' : '';
        outlierRows += `<div style="padding:3px 0;font-size:11px;display:flex;align-items:center;gap:8px">
          <span style="font-family:'IBM Plex Mono',monospace;font-size:11px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px">${ol.bin || ''}</span>
          <span style="color:${rateColor};font-weight:600">${olRate}%</span>
          <span style="color:var(--text-muted)">${devSign}${(ol.deviation || 0).toFixed(1)}pp vs group</span>
          <span style="color:var(--text-muted)">${ol.attempts || 0} att</span>
        </div>`;
      }
      outlierHtml = `<div style="padding:8px 16px;border-top:1px solid var(--border);background:#fef2f2">
        <div style="font-size:11px;font-weight:600;color:var(--danger);margin-bottom:6px">BIN outliers detected</div>
        ${outlierRows}
        <div style="font-size:10px;color:var(--text-muted);margin-top:4px">Copy BIN and create group in your routing rules before confirming</div>
      </div>`;
    }

    // No signal at all
    if (!promotionHtml && !gatheringHtml && !outlierHtml) {
      return `<div style="padding:6px 16px;border-top:1px solid var(--border);font-size:11px;color:var(--text-muted)">No split signal &mdash; continue gathering data</div>`;
    }

    return promotionHtml + gatheringHtml + outlierHtml;
  },

  _levelTrackerHtml(activeLevel) {
    const levels = [
      { n: 1, label: 'Bank' },
      { n: 2, label: 'Brand' },
      { n: 3, label: 'Type/Prepaid' },
      { n: 4, label: 'Level' },
      { n: 5, label: 'BIN' },
    ];
    return levels.map(l => {
      let bg, color;
      if (l.n < activeLevel) { bg = '#ecfdf5'; color = '#0F6E56'; }       // done = green
      else if (l.n === activeLevel) { bg = '#dbeafe'; color = '#1e40af'; } // active = blue
      else { bg = '#f3f4f6'; color = '#9ca3af'; }                          // pending = gray
      return `<span style="font-size:11px;padding:3px 9px;border-radius:3px;background:${bg};color:${color};font-weight:600;display:inline-block">L${l.n} ${l.label}</span>`;
    }).join(' <span style="color:var(--border)">&rarr;</span> ');
  },

  _processorAffinityCard(r) {
    const imp = r.expectedImpact || {};
    const conf = (r.confidence || 'LOW').toUpperCase();
    const confColors = { HIGH: '#1e40af', MEDIUM: '#92400e', LOW: 'var(--text-muted)' };
    const confBg = { HIGH: '#dbeafe', MEDIUM: '#fffbeb', LOW: '#f3f4f6' };

    const bins = r.binsInGroup || [];
    const showBins = bins.length > 0 && r.groupType === 'bin';
    const binChips = showBins ? bins.slice(0, 5).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:11px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px">${b}</span>`
    ).join(' ') + (bins.length > 5 ? ` <span style="font-size:11px;color:var(--text-muted)">+${bins.length - 5} more</span>` : '') : '';

    // Copy profile text
    const profileId = 'pa_' + Math.random().toString(36).slice(2, 8);
    let profileText = `${r.ruleName}\\nProcessor: ${r.bestProcessor?.name || '?'} (${(r.bestProcessor?.rate || 0).toFixed(1)}%)\\nCurrent: ${r.currentProcessor?.name || '?'} (${(r.currentProcessor?.rate || 0).toFixed(1)}%)\\nLift: +${(imp.lift_pp || 0).toFixed(1)}pp`;
    if (bins.length > 0) profileText += `\\nBINs: ${bins.join(', ')}`;

    // Soft/hard decline copy IDs
    const softId = 'sd_' + Math.random().toString(36).slice(2, 8);
    const hardId = 'hd_' + Math.random().toString(36).slice(2, 8);
    const softDeclines = r.softDeclines || [];
    const hardDeclines = r.hardDeclines || [];
    const issuerExceptions = r.issuerExceptions || [];

    let html = `<div class="card" style="padding:0;border-left:2.5px solid #185FA5;overflow:hidden">`;

    // Header: name + ID
    html += `<div style="padding:14px 16px 10px">
      <div style="display:flex;justify-content:space-between;align-items:baseline">
        <div style="font-size:14px;font-weight:600;color:var(--text)">${r.ruleName}</div>
        <span style="font-size:11px;color:var(--text-muted)">${r.ruleId}</span>
      </div>`;

    // Level tracker pills
    html += `<div style="display:flex;gap:4px;margin-top:6px;align-items:center;flex-wrap:wrap">
      ${this._levelTrackerHtml(r.level || 1)}
    </div>`;

    // Badges: Confidence + PROCESSOR AFFINITY
    html += `<div style="display:flex;gap:6px;margin-top:6px;align-items:center">
      <span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px;background:${confBg[conf]};color:${confColors[conf]}">${conf}</span>
      <span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px;background:#dbeafe;color:#185FA5">PROCESSOR AFFINITY</span>
    </div>`;
    html += `</div>`;

    // Blue insight banner
    html += `<div style="display:flex;align-items:center;gap:8px;padding:8px 16px;background:#E6F1FB;border-bottom:0.5px solid #B5D4F4;font-size:12px;color:#0C447C">
      <span style="font-size:14px;flex-shrink:0">&#9432;</span>
      <span>Adding <strong>${r.bestProcessor?.name || '?'}</strong> MID would improve to <strong>${(imp.expected_rate || 0).toFixed(1)}%</strong></span>
      <span style="margin-left:auto;font-weight:600;color:#0F6E56;white-space:nowrap">+${(imp.lift_pp || 0).toFixed(1)}pp potential</span>
    </div>`;

    // Stats bar
    html += `<div style="display:flex;border-top:1px solid var(--border);font-size:13px;text-align:center">
      <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong style="color:#185FA5">+${(imp.lift_pp || 0).toFixed(1)}pp</strong><div style="font-size:10px;color:var(--text-muted)">lift</div></div>
      <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong>${r.bestProcessor?.name || '?'}</strong> <span style="color:#0F6E56">${(r.bestProcessor?.rate || 0).toFixed(1)}%</span><div style="font-size:10px;color:var(--text-muted)">best processor</div></div>
      <div style="flex:1;padding:10px 0"><strong>${r.currentProcessor?.name || '?'}</strong> <span style="color:var(--danger)">${(r.currentProcessor?.rate || 0).toFixed(1)}%</span><div style="font-size:10px;color:var(--text-muted)">current processor</div></div>
    </div>`;

    // Decline handling
    html += this._declineSection(softDeclines, hardDeclines, issuerExceptions, softId, hardId, r.attemptGroups || []);

    // BIN chips + Copy BINs + Copy Profile
    html += `<div style="padding:6px 16px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;border-top:1px solid var(--border)">
      ${showBins ? `<span style="font-size:11px;color:var(--text-muted)">${bins.length} BIN${bins.length > 1 ? 's' : ''}:</span>${binChips}${this._copyBinsBtnHtml(bins)}` : ''}
      <button id="${profileId}" onclick="navigator.clipboard.writeText('${profileText}');var b=document.getElementById('${profileId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy Profile',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-muted);font-size:10px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy Profile</button>
    </div>`;

    // Apply when note
    html += `<div style="padding:8px 16px;font-size:11px;font-style:italic;color:#185FA5;border-top:1px solid var(--border)">
      Apply when: Adding new ${r.bestProcessor?.name || ''} MIDs &rarr; route these BINs there immediately
    </div>`;

    html += `</div>`;
    return html;
  },

  // =========================================================================
  // Flow Optix V2 — Rebill card system rebuild
  // =========================================================================
  async _renderPlaybook(el, htmlPrefix) {
    let data;
    if (this._playbookCache) {
      data = this._playbookCache;
    } else {
      const res = await fetch(`/api/analytics/${this.clientId}/routing-playbook`);
      data = await res.json();
      this._playbookCache = data;
    }
    // Load active implementations for button states
    try {
      const implRes = await fetch(`/api/implementations/${this.clientId}/active`);
      this._implStatusCache = await implRes.json();
    } catch { this._implStatusCache = []; }
    if (!data || !data.rows) {
      el.innerHTML = htmlPrefix + '<div class="empty-state"><h3>No Playbook Data</h3><p>Run analysis to generate routing playbook.</p></div>';
      return;
    }

    const rows = data.rows;
    const summary = data.summary;
    let html = htmlPrefix;

    // Summary bar
    html += `<div class="card" style="padding:12px 20px;margin-bottom:12px;display:flex;flex-wrap:wrap;gap:16px;align-items:center">
      <div><span style="font-size:20px;font-weight:700">${summary.totalRows}</span> <span style="font-size:13px;color:var(--text-secondary)">routing rules</span></div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        ${summary.untested ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#EDE9FE;color:#6D28D9;font-weight:600">' + summary.untested + ' Untested</span>' : ''}
        ${summary.hostile ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FEE2E2;color:#991B1B;font-weight:600">' + summary.hostile + ' Hostile</span>' : ''}
        ${summary.resistant ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FED7AA;color:#9A3412;font-weight:600">' + summary.resistant + ' Resistant</span>' : ''}
        ${summary.viable ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#FEF3C7;color:#92400E;font-weight:600">' + summary.viable + ' Viable</span>' : ''}
        ${summary.strong ? '<span style="font-size:11px;padding:3px 10px;border-radius:12px;background:#D1FAE5;color:#065F46;font-weight:600">' + summary.strong + ' Strong</span>' : ''}
      </div>
      <div style="display:flex;gap:8px">
        <span style="font-size:11px;color:var(--text-secondary)">${summary.confident} confident</span>
        <span style="font-size:11px;color:var(--text-secondary)">${summary.earlySignal} early signal</span>
        <span style="font-size:11px;color:var(--text-secondary)">${summary.rebillBlockers} rebill blockers</span>
      </div>
      <div style="margin-left:auto"><button class="btn btn-secondary btn-sm" onclick="Analytics.exportPlaybookCsv()">Export CSV</button></div>
    </div>`;

    // Filter buttons
    const filter = this._playbookFilter || null;
    html += `<div style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap">
      ${[
        { key: null, label: 'All', count: rows.length },
        { key: 'UNTESTED', label: 'Untested', count: summary.untested, bg: '#EDE9FE', color: '#6D28D9' },
        { key: 'HOSTILE', label: 'Hostile', count: summary.hostile, bg: '#FEE2E2', color: '#991B1B' },
        { key: 'RESISTANT', label: 'Resistant', count: summary.resistant, bg: '#FED7AA', color: '#9A3412' },
        { key: 'VIABLE', label: 'Viable', count: summary.viable, bg: '#FEF3C7', color: '#92400E' },
        { key: 'STRONG', label: 'Strong', count: summary.strong, bg: '#D1FAE5', color: '#065F46' },
        { key: 'PREPAID', label: 'Prepaid', count: summary.prepaid, bg: '#F5F3FF', color: '#7C3AED' },
      ].map(f => {
        const active = filter === f.key;
        const style = active ? 'background:var(--text);color:white' : f.bg ? 'background:'+f.bg+';color:'+f.color : 'background:var(--bg);color:var(--text);border:1px solid var(--border)';
        return '<button style="font-size:11px;font-weight:600;padding:4px 14px;border-radius:16px;border:none;cursor:pointer;'+style+'" onclick="Analytics._playbookFilter='+(f.key?"'"+f.key+"'":"null")+';Analytics.renderCrmRules()">'+f.label+' '+f.count+'</button>';
      }).join('')}
    </div>`;

    // Filter rows
    let filtered = rows;
    if (filter === 'UNTESTED') filtered = rows.filter(r => r.rebillTier.includes('UNTESTED'));
    else if (filter === 'HOSTILE') filtered = rows.filter(r => r.rebillTier.includes('HOSTILE'));
    else if (filter === 'RESISTANT') filtered = rows.filter(r => r.rebillTier.includes('RESISTANT'));
    else if (filter === 'VIABLE') filtered = rows.filter(r => r.rebillTier.includes('VIABLE'));
    else if (filter === 'STRONG') filtered = rows.filter(r => r.rebillTier.includes('STRONG'));
    else if (filter === 'PREPAID') filtered = rows.filter(r => r.isPrepaid);

    // Render cards in columns (same layout as V2 cards)
    if (filtered.length > 0) {
      const colCount = Math.max(1, Math.min(4, Math.floor((el.offsetWidth || 900) / 320)));
      const cols = Array.from({ length: colCount }, () => []);
      for (let i = 0; i < filtered.length; i++) {
        cols[i % colCount].push(filtered[i]);
      }
      html += '<div style="display:flex;gap:12px;align-items:flex-start">';
      for (const col of cols) {
        html += '<div style="flex:1;min-width:0">';
        for (const row of col) {
          html += this._playbookCard(row);
        }
        html += '</div>';
      }
      html += '</div>';
    } else {
      html += '<div class="empty-state"><h3>No matching rules</h3></div>';
    }

    el.innerHTML = html;
  },

  _playbookCard(r) {
    const baseTier = r.rebillTier.replace('Early: ', '');
    const tierBg = baseTier === 'UNTESTED' ? '#EDE9FE' : baseTier === 'HOSTILE' ? '#FEE2E2' : baseTier === 'RESISTANT' ? '#FED7AA' : baseTier === 'VIABLE' ? '#FEF3C7' : '#D1FAE5';
    const tierColor = baseTier === 'UNTESTED' ? '#6D28D9' : baseTier === 'HOSTILE' ? '#991B1B' : baseTier === 'RESISTANT' ? '#9A3412' : baseTier === 'VIABLE' ? '#92400E' : '#065F46';
    const confBg = r.confidenceTier === 'Confident' ? '#D1FAE5' : '#FEF3C7';
    const confColor = r.confidenceTier === 'Confident' ? '#065F46' : '#92400E';
    const expandId = 'pb_' + Math.random().toString(36).slice(2, 8);
    const copyId = 'pbc_' + Math.random().toString(36).slice(2, 8);
    const binText = (r.bins || []).join(', ');

    let html = `<div class="card" style="margin-bottom:12px;padding:0;border-left:4px solid ${tierColor};overflow:hidden">`;

    // ── HEADER ──
    html += `<div style="padding:16px 20px 12px;display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
      <div style="min-width:0">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <span style="font-size:15px;font-weight:600">${r.issuer_bank}</span>
          ${r.isPrepaid ? '<span style="font-size:9px;padding:2px 6px;border-radius:3px;background:#EDE9FE;color:#6D28D9;font-weight:600">PREPAID</span>'
            : ''}
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${tierBg};color:${tierColor};font-weight:600">${r.rebillTier}</span>
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${confBg};color:${confColor};font-weight:600">${r.confidenceTier}</span>
          ${r.isRebillBlocker ? '<span style="font-size:9px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B;font-weight:600">REBILL BLOCKER</span>' : ''}
        </div>
        <div style="font-size:11px;color:var(--text-secondary);margin-top:4px">${r.acquired} customers | ${r.binCount} BINs</div>
      </div>
      <button id="${copyId}" onclick="navigator.clipboard.writeText('${binText.replace(/'/g, "\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy BINs',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:10px;cursor:pointer;padding:3px 10px;flex-shrink:0">Copy BINs</button>
    </div>`;

    // ── 4 METRIC BOXES ──
    const initTop = r.initialBest[0];
    const upsTop = r.upsellBest[0];
    const cascTop = r.cascadeChain[0];
    html += `<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:0 20px 12px">`;

    // Box 1: Initial
    html += `<div style="background:#E1F5EE;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#065F46;font-weight:600">INITIAL</div>
      <div style="font-size:13px;font-weight:600;color:#065F46;margin-top:2px">${initTop ? initTop.processor + ' ' + initTop.rate + '%' : 'no data'}</div>
      <div style="font-size:9px;color:#065F46">${initTop ? '(' + initTop.app + '/' + initTop.att + ')' : ''}</div>
    </div>`;

    // Box 2: Cascade
    html += `<div style="background:#EDE9FE;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#6D28D9;font-weight:600">CASCADE</div>
      <div style="font-size:12px;font-weight:500;color:#6D28D9;margin-top:2px">${r.cascadeChain.length > 0 ? r.cascadeChain.map(c => c.name).join(' &rarr; ') : 'no data'}</div>
      <div style="font-size:9px;color:#6D28D9">${cascTop ? cascTop.rate + '% save rate' : ''}</div>
    </div>`;

    // Box 3: Upsell
    html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#5F5E5A;font-weight:600">UPSELL</div>
      <div style="font-size:13px;font-weight:500;color:var(--text);margin-top:2px">${upsTop ? upsTop.processor + ' ' + upsTop.rate + '%' : 'no data'}</div>
      <div style="font-size:9px;color:#5F5E5A">${upsTop ? '(' + upsTop.app + '/' + upsTop.att + ')' : ''}</div>
    </div>`;

    // Box 4: Rebill
    const rebTop = r.rebillBest[0];
    if (baseTier === 'UNTESTED') {
      html += `<div style="background:#EDE9FE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#6D28D9;font-weight:600">REBILL — UNTESTED</div>
        <div style="font-size:11px;font-weight:600;color:#6D28D9;margin-top:2px">0% at $97.48</div>
        <div style="font-size:9px;color:#6D28D9">Test price drop (${r.c1.att} att)</div>
      </div>`;
    } else if (r.c1.att === 0) {
      html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#5F5E5A;font-weight:600">REBILL</div>
        <div style="font-size:11px;color:#5F5E5A;margin-top:2px">No rebill data</div>
      </div>`;
    } else {
      html += `<div style="background:${tierBg};border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:${tierColor};font-weight:600">REBILL C1: ${r.c1.rate}%</div>
        <div style="font-size:12px;font-weight:500;color:${tierColor};margin-top:2px">${rebTop ? rebTop.processor + ' ' + rebTop.c1_rate + '%' : 'no data'}</div>
        <div style="font-size:9px;color:${tierColor}">C2: ${r.c2.rate}% (${r.c2.app}/${r.c2.att})</div>
      </div>`;
    }

    html += '</div>';

    // ── EXPAND TRIGGER ──
    html += `<div onclick="var e=document.getElementById('${expandId}');e.style.display=e.style.display==='none'?'':'none'" style="border-top:1px dashed var(--border);padding:6px 0;text-align:center;cursor:pointer">
      <span style="font-size:11px;color:var(--text-muted)">&#9660; Details</span>
    </div>`;

    // ── EXPANDED DETAILS ──
    html += `<div id="${expandId}" style="display:none;padding:0 20px 16px;border-top:1px solid var(--border)">`;

    // Section: Initial routing
    html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Initial Routing</div>`;
    if (r.initialBest.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.initialBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#D1FAE5;color:#065F46">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
      }
      html += '</div>';
    }
    if (r.initialBlock.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
      for (const p of r.initialBlock) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att} att)</span>`;
      }
      html += '</div>';
    }
    const initTop_ = r.initialBest[0];
    html += this._implButton(r, 'initial_routing', initTop_ ? initTop_.processor : '', initTop_ ? initTop_.rate : '');
    html += '</div>';

    // Section: Cascade
    if (r.cascadeChain.length > 0 || r.cascadeOn.length > 0 || r.cascadeSkip.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Cascade Chain</div>`;
      if (r.cascadeChain.length > 0) {
        html += '<div style="font-size:12px;margin-bottom:4px">' + r.cascadeChain.map(c => `<span style="font-weight:500">${c.name}</span> ${c.rate}%`).join(' &rarr; ') + '</div>';
      }
      if (r.cascadeOn.length > 0) {
        html += '<div style="font-size:11px;color:#065F46;margin-bottom:2px">Cascade on: ' + r.cascadeOn.map(d => d.reason + ' (' + d.recoveryRate + '% recovery)').join(', ') + '</div>';
      }
      if (r.cascadeSkip.length > 0) {
        html += '<div style="font-size:11px;color:#991B1B">Skip: ' + r.cascadeSkip.map(d => d.reason).join(', ') + '</div>';
      }
      const cascTop_ = r.cascadeChain[0];
      html += this._implButton(r, 'cascade', cascTop_ ? cascTop_.name : '', cascTop_ ? cascTop_.rate : '');
      html += '</div>';
    }

    // Section: Upsell
    if (r.upsellBest.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Upsell Routing</div>`;
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px">';
      for (const p of r.upsellBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#F1EFE8;color:#5F5E5A">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
      }
      html += '</div></div>';
    }

    // Section: Rebill
    html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Rebill Routing &mdash; ${r.rebillTier}</div>`;
    html += `<div style="font-size:12px;margin-bottom:6px">C1: <strong>${r.c1.rate}%</strong> (${r.c1.app}/${r.c1.att}) | C2: <strong>${r.c2.rate}%</strong> (${r.c2.app}/${r.c2.att})</div>`;
    if (r.rebillBest.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.rebillBest) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#D1FAE5;color:#065F46">${p.processor} C1:${p.c1_rate}% (${p.c1_app}/${p.c1_att})</span>`;
      }
      html += '</div>';
    }
    if (r.rebillBlock.length > 0) {
      html += '<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:4px">';
      for (const p of r.rebillBlock) {
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.total_att} att)</span>`;
      }
      html += '</div>';
    }
    if (r.priceStrategy) {
      const ps = r.priceStrategy;
      const psBg = ps.tier === 'UNTESTED' ? '#EDE9FE' : ps.tier === 'HOSTILE' ? '#FEE2E2' : '#FEF3C7';
      const psColor = ps.tier === 'UNTESTED' ? '#6D28D9' : ps.tier === 'HOSTILE' ? '#991B1B' : '#92400E';
      const psBorder = ps.tier === 'UNTESTED' ? '#DDD6FE' : ps.tier === 'HOSTILE' ? '#FECACA' : '#FDE68A';
      html += `<div style="margin-top:6px;padding:8px 10px;border-radius:6px;background:${psBg};border:1px solid ${psBorder}">
        <div style="font-size:10px;font-weight:700;color:${psColor};margin-bottom:4px">PRICE STRATEGY — ${ps.tier}</div>
        <div style="font-size:12px;color:${psColor}">${ps.recommendation}</div>`;
      if (ps.scenarios) {
        for (const s of ps.scenarios) {
          if (s.price) {
            html += `<div style="font-size:10px;color:${psColor};margin-top:2px;margin-left:8px">At $${s.price}: need ${s.breakEvenRate}% to break even (2x current = $${s.rpaAt2x} RPA)</div>`;
          } else {
            html += `<div style="font-size:10px;color:${psColor};margin-top:2px;margin-left:8px">${s.label}: ${s.rate}% &rarr; $${s.rpa.toFixed(2)} RPA</div>`;
          }
        }
      }
      html += '</div>';
    } else if (!r.rebillTier.includes('STRONG') && r.priceOptimization) {
      const po = r.priceOptimization;
      html += `<div style="font-size:11px;padding:4px 8px;border-radius:3px;background:#FEF3C7;color:#92400E;display:inline-block;margin-bottom:4px">Price: $${po.currentPrice} &rarr; $${po.optimalPrice} (+$${Math.round(po.monthlyImpact)}/mo)</div>`;
    }
    if (r.isPrepaid) {
      html += '<div style="font-size:11px;color:#6D28D9;margin-top:2px">Prepaid &mdash; no CPA cost, any rebill is pure margin</div>';
    }
    const rebTop_ = r.rebillBest[0];
    html += this._implButton(r, 'rebill_routing', rebTop_ ? rebTop_.processor : '', rebTop_ ? rebTop_.c1_rate : '');
    html += '</div>';

    // Section: Salvage
    if (r.salvageSequence.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Rebill Salvage Sequence</div>`;
      for (const s of r.salvageSequence) {
        if (s.isStop) {
          html += `<div style="font-size:11px;color:#991B1B;margin-bottom:2px">Att ${s.attempt}: <strong>STOP</strong> &mdash; ${s.stopMessage}</div>`;
        } else {
          html += `<div style="font-size:11px;margin-bottom:2px">Att ${s.attempt}: <strong>${s.processor}</strong> ${s.rate}% &middot; RPA $${s.rpa}</div>`;
        }
      }
      if (r.rebillRetryOn.length > 0) {
        html += '<div style="font-size:11px;color:#065F46;margin-top:4px">Retry on: ' + r.rebillRetryOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
      }
      if (r.rebillStopOn.length > 0) {
        html += '<div style="font-size:11px;color:#991B1B;margin-top:2px">Stop on: ' + r.rebillStopOn.map(d => d.reason).join(', ') + '</div>';
      }
      const salvTop_ = r.salvageSequence.find(s => !s.isStop);
      html += this._implButton(r, 'salvage', salvTop_ ? salvTop_.processor : '', salvTop_ ? salvTop_.rate : '');
      html += '</div>';
    }

    // Section: Lifecycle
    if (r.acquisitionAffinity.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Lifecycle Context</div>`;
      html += '<div style="display:flex;flex-wrap:wrap;gap:6px">';
      for (const a of r.acquisitionAffinity) {
        const bg = a.rebRate >= 20 ? '#D1FAE5' : a.rebRate >= 10 ? '#FEF3C7' : '#FEE2E2';
        const color = a.rebRate >= 20 ? '#065F46' : a.rebRate >= 10 ? '#92400E' : '#991B1B';
        html += `<span style="font-size:11px;padding:3px 8px;border-radius:3px;background:${bg};color:${color}">Acq ${a.processor} &rarr; ${a.rebRate}% rebill (${a.rebApp}/${a.rebAtt})</span>`;
      }
      html += '</div></div>';
    }

    // Section: L4 Sub-Groups (mini-playbooks per card type)
    if (r.l4Groups && r.l4Groups.length > 0) {
      html += `<div style="margin-top:12px"><div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Card Type Routing</div>`;
      for (const o of r.l4Groups) {
        const label = [o.card_brand, o.is_prepaid ? 'Prepaid' : '', o.card_type].filter(Boolean).join(' ');
        const copyId = 'ol_' + Math.random().toString(36).slice(2, 8);
        const olBins = (o.bins || []).join(', ');
        const lvlBg = o.routingLevel === 'own' ? '#D1FAE5' : o.routingLevel === 'partial' ? '#FEF3C7' : '#F1EFE8';
        const lvlColor = o.routingLevel === 'own' ? '#065F46' : o.routingLevel === 'partial' ? '#92400E' : '#5F5E5A';
        const lvlLabel = o.routingLevel === 'own' ? 'Own routing' : o.routingLevel === 'partial' ? 'Partial data' : 'Use bank fallback';
        let signals = [];
        if (o.isInitOutlier) {
          const arrow = o.initDelta > 0 ? '&#9650;' : '&#9660;';
          const color = o.initDelta > 0 ? '#065F46' : '#991B1B';
          signals.push(`<span style="color:${color}">Init ${o.initRate}% (${arrow}${Math.abs(o.initDelta)}pp) <span style="font-size:9px;color:var(--text-muted)">(${o.initApp}/${o.initAtt})</span></span>`);
        }
        if (o.isC1Outlier) {
          const arrow = o.c1Delta > 0 ? '&#9650;' : '&#9660;';
          const color = o.c1Delta > 0 ? '#065F46' : '#991B1B';
          signals.push(`<span style="color:${color}">C1 ${o.c1Rate}% (${arrow}${Math.abs(o.c1Delta)}pp) <span style="font-size:9px;color:var(--text-muted)">(${o.c1App}/${o.c1Att})</span></span>`);
        }

        html += `<div style="padding:8px;margin-bottom:6px;border:1px solid var(--border);border-radius:6px;background:var(--bg)">`;
        // Header
        html += `<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px">
          <span style="font-weight:600;font-size:12px">${label}</span>
          <span style="font-size:9px;padding:2px 6px;border-radius:3px;background:${lvlBg};color:${lvlColor};font-weight:600">${lvlLabel}</span>
          <span style="color:var(--text-muted);font-size:10px">${o.bin_count} BINs</span>
          ${signals.length > 0 ? '<span style="font-size:10px">' + signals.join(' &middot; ') + '</span>' : ''}
          <button id="${copyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${olBins.replace(/'/g, "\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:9px;cursor:pointer;padding:1px 6px;margin-left:auto">Copy BINs</button>
        </div>`;

        if (o.routingLevel !== 'fallback') {
          // Initial routing
          if (o.initRouting && o.initRouting.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">INIT</span>';
            for (const p of o.initRouting) {
              const bg = p.app > 0 ? '#D1FAE5' : '#FEE2E2';
              const color = p.app > 0 ? '#065F46' : '#991B1B';
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:${bg};color:${color}">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
            }
            for (const p of (o.initBlock || [])) {
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att})</span>`;
            }
            html += '</div>';
          }
          // Cascade
          if (o.cascadeTargets && o.cascadeTargets.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">CASC</span>';
            html += '<span style="font-size:10px">' + o.cascadeTargets.map(c => `<span style="padding:2px 6px;border-radius:3px;background:#EDE9FE;color:#6D28D9">${c.name} ${c.rate}%</span>`).join(' &rarr; ') + '</span>';
            html += '</div>';
          }
          if (o.cascadeOn && o.cascadeOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#065F46">Cascade on: ' + o.cascadeOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
          }
          if (o.cascadeSkip && o.cascadeSkip.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#991B1B">Skip: ' + o.cascadeSkip.map(d => d.reason).join(', ') + '</div>';
          }
          // Rebill routing
          if (o.rebillRouting && o.rebillRouting.length > 0) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">REBILL</span>';
            for (const p of o.rebillRouting) {
              const bg = p.app > 0 ? '#D1FAE5' : '#FEE2E2';
              const color = p.app > 0 ? '#065F46' : '#991B1B';
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:${bg};color:${color}">${p.processor} ${p.rate}% (${p.app}/${p.att})</span>`;
            }
            for (const p of (o.rebillBlock || [])) {
              html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEE2E2;color:#991B1B">Block ${p.processor} 0% (${p.att})</span>`;
            }
            html += '</div>';
          }
          // Salvage
          if (o.salvageSequence && o.salvageSequence.length > 0 && !o.salvageSequence[0].isStop) {
            html += '<div style="margin:4px 0;display:flex;flex-wrap:wrap;gap:3px;align-items:center"><span style="font-size:9px;font-weight:600;color:var(--text-secondary);min-width:50px">SALVAGE</span>';
            for (const s of o.salvageSequence) {
              if (s.isStop) {
                html += `<span style="font-size:10px;color:#991B1B">Att ${s.attempt}: STOP &mdash; ${s.stopMessage}</span>`;
              } else {
                html += `<span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FEF3C7;color:#92400E">Att ${s.attempt}: ${s.processor} ${s.rate}% &middot; $${s.rpa} RPA</span>`;
              }
            }
            html += '</div>';
          }
          if (o.rebillRetryOn && o.rebillRetryOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#065F46">Retry on: ' + o.rebillRetryOn.map(d => d.reason + ' (' + d.recoveryRate + '%)').join(', ') + '</div>';
          }
          if (o.rebillStopOn && o.rebillStopOn.length > 0) {
            html += '<div style="margin:2px 0 2px 56px;font-size:10px;color:#991B1B">Stop on: ' + o.rebillStopOn.map(d => d.reason).join(', ') + '</div>';
          }
        }

        html += '</div>'; // end L4 card
      }
      html += '</div>';
    }

    html += '</div>'; // end expanded
    html += '</div>'; // end card
    return html;
  },

  exportPlaybookCsv() {
    if (!this._playbookCache || !this._playbookCache.rows) return;
    const rows = this._playbookCache.rows;
    const esc = (s) => '"' + String(s || '').replace(/"/g, '""') + '"';
    const fmtProcs = (ps) => ps.map(p => p.processor + ' ' + p.rate + '% (' + p.app + '/' + p.att + ')').join('; ');
    const fmtBlock = (ps) => ps.map(p => p.processor + ' 0% (' + p.att + ' att)').join('; ');

    const headers = [
      'Row_Type', 'Bank', 'Card_Brand', 'Is_Prepaid', 'Card_Type', 'Routing_Level',
      'Prepaid_Pct', 'Tier', 'Confidence', 'Customers', 'BINs', 'BIN_Count',
      'Init_Rate', 'Init_Best_Processors', 'Init_Block',
      'Cascade_Chain', 'Cascade_On', 'Cascade_Skip',
      'Upsell_Best_Processors',
      'C1_Rate', 'C1_App', 'C1_Att', 'C2_Rate', 'C2_App', 'C2_Att',
      'Rebill_Best_Processors', 'Rebill_Block',
      'Salvage_Sequence',
      'Rebill_Retry_On', 'Rebill_Stop_On',
      'Acq_Affinity',
      'Price_Strategy', 'Price_Optimization',
      'Rebill_Blocker', 'Not_Rebill_Worthy',
      'Init_Delta_pp', 'C1_Delta_pp',
    ];
    const csvRows = [headers.join(',')];

    for (const r of rows) {
      // Bank-level row (fallback)
      const salvFull = r.salvageSequence.map(s => s.isStop ? 'Att' + s.attempt + ': STOP' : 'Att' + s.attempt + ': ' + s.processor + ' ' + s.rate + '% $' + s.rpa + ' RPA').join('; ');
      const priceStrat = r.priceStrategy ? r.priceStrategy.recommendation : '';
      const priceOpt = r.priceOptimization ? '$' + r.priceOptimization.currentPrice + ' > $' + r.priceOptimization.optimalPrice + ' (+$' + Math.round(r.priceOptimization.monthlyImpact) + '/mo)' : '';
      const prepaidPct = r.prepaidInfo ? r.prepaidInfo.pct + '%' : '';

      csvRows.push([
        'BANK', esc(r.issuer_bank), '', r.isPrepaid ? 1 : 0, '', 'fallback',
        prepaidPct, r.rebillTier, r.confidenceTier, r.acquired,
        esc((r.bins || []).join('; ')), r.binCount,
        r.initialBest[0] ? r.initialBest[0].rate : '', esc(fmtProcs(r.initialBest)), esc(fmtBlock(r.initialBlock)),
        esc(r.cascadeChain.map(c => c.name + ' ' + c.rate + '%').join(' > ')),
        esc(r.cascadeOn.map(d => d.reason + ' ' + d.recoveryRate + '%').join('; ')),
        esc(r.cascadeSkip.map(d => d.reason).join('; ')),
        esc(fmtProcs(r.upsellBest)),
        r.c1.rate, r.c1.app, r.c1.att, r.c2.rate, r.c2.app, r.c2.att,
        esc(fmtProcs(r.rebillBest)), esc(fmtBlock(r.rebillBlock)),
        esc(salvFull),
        esc(r.rebillRetryOn.map(d => d.reason + ' ' + d.recoveryRate + '%').join('; ')),
        esc(r.rebillStopOn.map(d => d.reason).join('; ')),
        esc(r.acquisitionAffinity.map(a => a.processor + ' ' + a.rebRate + '%').join('; ')),
        esc(priceStrat), esc(priceOpt),
        r.isRebillBlocker ? 1 : 0, r.notRebillWorthy ? 1 : 0,
        '', '',
      ].join(','));

      // L4 sub-rows
      for (const o of (r.l4Groups || [])) {
        const label = [o.card_brand, o.is_prepaid ? 'Prepaid' : '', o.card_type].filter(Boolean).join(' ');
        const oSalv = (o.salvageSequence || []).map(s => s.isStop ? 'Att' + s.attempt + ': STOP' : 'Att' + s.attempt + ': ' + s.processor + ' ' + s.rate + '% $' + s.rpa + ' RPA').join('; ');
        const oInitRate = o.initRate !== null ? o.initRate : '';
        const oC1Rate = o.c1Rate !== null ? o.c1Rate : '';

        csvRows.push([
          'L4', esc(r.issuer_bank), esc(o.card_brand || ''), o.is_prepaid ? 1 : 0, esc(o.card_type || ''), o.routingLevel,
          '', '', '', '',
          esc((o.bins || []).join('; ')), o.bin_count,
          oInitRate, esc(fmtProcs(o.initRouting || [])), esc(fmtBlock(o.initBlock || [])),
          esc((o.cascadeTargets || []).map(c => c.name + ' ' + c.rate + '%').join(' > ')),
          esc((o.cascadeOn || []).map(d => d.reason + ' ' + d.recoveryRate + '%').join('; ')),
          esc((o.cascadeSkip || []).map(d => d.reason).join('; ')),
          '',
          oC1Rate, o.c1App || '', o.c1Att || '', '', '', '',
          esc(fmtProcs(o.rebillRouting || [])), esc(fmtBlock(o.rebillBlock || [])),
          esc(oSalv),
          esc((o.rebillRetryOn || []).map(d => d.reason + ' ' + d.recoveryRate + '%').join('; ')),
          esc((o.rebillStopOn || []).map(d => d.reason).join('; ')),
          '',
          '', '',
          '', '',
          o.initDelta !== null ? (o.initDelta > 0 ? '+' : '') + o.initDelta : '',
          o.c1Delta !== null ? (o.c1Delta > 0 ? '+' : '') + o.c1Delta : '',
        ].join(','));
      }
    }
    const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'routing_playbook.csv'; a.click();
    URL.revokeObjectURL(url);
  },

  // =========================================================================
  // Implementation Tracking — Playbook integration
  // =========================================================================

  _getImplStatus(issuerBank, isPrepaid, ruleType, cardBrand, cardType) {
    if (!this._implStatusCache) return null;
    return this._implStatusCache.find(i =>
      i.issuer_bank === issuerBank &&
      i.is_prepaid === (isPrepaid ? 1 : 0) &&
      i.rule_type === ruleType &&
      (cardBrand ? i.card_brand === cardBrand : !i.card_brand) &&
      (cardType ? i.card_type === cardType : !i.card_type)
    ) || null;
  },

  _implButton(r, ruleType, recProcessor, recRate, cardBrand, cardType) {
    const impl = this._getImplStatus(r.issuer_bank, r.isPrepaid, ruleType, cardBrand || null, cardType || null);
    const dataAttr = `data-bank="${(r.issuer_bank || '').replace(/"/g, '&quot;')}" data-prepaid="${r.isPrepaid ? 1 : 0}" data-rule="${ruleType}" data-rec-proc="${(recProcessor || '').replace(/"/g, '&quot;')}" data-rec-rate="${recRate || ''}" data-brand="${(cardBrand || '').replace(/"/g, '&quot;')}" data-type="${(cardType || '').replace(/"/g, '&quot;')}"`;

    if (!impl) {
      return `<button class="impl-btn" ${dataAttr} onclick="Analytics.showImplForm(this)" style="font-size:10px;padding:3px 10px;border-radius:4px;border:1px solid #059669;background:#ECFDF5;color:#059669;cursor:pointer;margin-top:6px;font-weight:600">Implement</button>`;
    }

    const cp = impl.latest_checkpoint ? (typeof impl.latest_checkpoint === 'string' ? JSON.parse(impl.latest_checkpoint) : impl.latest_checkpoint) : null;
    const att = cp?.attempts || 0;
    const lift = cp?.lift_pp != null ? (cp.lift_pp >= 0 ? '+' : '') + cp.lift_pp.toFixed(1) + 'pp' : '';

    switch (impl.status) {
      case 'waiting':
      case 'collecting':
        return `<span style="font-size:10px;padding:3px 10px;border-radius:4px;background:#DBEAFE;color:#1D4ED8;font-weight:600;display:inline-block;margin-top:6px">Tracking: ${att}/${impl.min_sample_target} att</span>`;
      case 'evaluating':
        return `<span style="font-size:10px;padding:3px 10px;border-radius:4px;background:#DBEAFE;color:#1D4ED8;font-weight:600;display:inline-block;margin-top:6px">Evaluating: ${lift} (${att} att)</span>`;
      case 'confirmed':
        return `<span style="font-size:10px;padding:3px 10px;border-radius:4px;background:#D1FAE5;color:#065F46;font-weight:600;display:inline-block;margin-top:6px">Confirmed ${lift}</span>`;
      case 'regression':
        return `<span style="font-size:10px;padding:3px 10px;border-radius:4px;background:#FEE2E2;color:#991B1B;font-weight:600;display:inline-block;margin-top:6px;cursor:pointer" onclick="Analytics.showRollbackInfo(${impl.id})" title="Click for rollback info">Regression ${lift}</span>`;
      case 'inconclusive':
        return `<span style="font-size:10px;padding:3px 10px;border-radius:4px;background:#F3F4F6;color:#6B7280;font-weight:600;display:inline-block;margin-top:6px">Inconclusive ${lift}</span>`;
      default:
        return '';
    }
  },

  async showImplForm(btn) {
    const bank = btn.dataset.bank;
    const isPrepaid = parseInt(btn.dataset.prepaid);
    const ruleType = btn.dataset.rule;
    const recProc = btn.dataset.recProc;
    const recRate = btn.dataset.recRate;
    const cardBrand = btn.dataset.brand || null;
    const cardType = btn.dataset.type || null;

    const ruleLabels = {
      initial_routing: 'Initial Routing',
      cascade: 'Cascade Chain',
      upsell_routing: 'Upsell Routing',
      rebill_routing: 'Rebill Routing',
      salvage: 'Salvage Sequence',
    };

    // Fetch gateways for dropdown
    let gateways = [];
    try {
      const gwRes = await fetch(`/api/config/clients/${this.clientId}`);
      const gwData = await gwRes.json();
      gateways = (gwData.gateways || []).filter(g => g.lifecycle_state !== 'closed' && !g.exclude_from_analysis);
    } catch {}

    // Build unique processor list from gateways
    const processors = [...new Set(gateways.map(g => g.processor_name).filter(Boolean))].sort();

    const prepaidLabel = isPrepaid ? 'Prepaid' : 'Non-Prepaid';
    const groupLabel = cardBrand ? `${bank} / ${prepaidLabel} / ${cardBrand} ${cardType || ''}` : `${bank} (${prepaidLabel})`;

    // Build modal
    const modalId = 'implModal_' + Math.random().toString(36).slice(2, 8);
    const html = `
      <div id="${modalId}" style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:9999;display:flex;align-items:center;justify-content:center" onclick="if(event.target===this)this.remove()">
        <div style="background:var(--card-bg,#fff);border-radius:12px;padding:24px;max-width:480px;width:90%;max-height:80vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,0.3)">
          <div style="font-size:16px;font-weight:700;margin-bottom:4px">Implement ${ruleLabels[ruleType] || ruleType}</div>
          <div style="font-size:13px;color:var(--text-secondary);margin-bottom:16px">${groupLabel}</div>

          ${recProc ? `<div style="padding:10px;border-radius:6px;background:#ECFDF5;margin-bottom:16px">
            <div style="font-size:10px;font-weight:600;color:#065F46">RECOMMENDATION</div>
            <div style="font-size:14px;font-weight:600;color:#065F46;margin-top:2px">${recProc} ${recRate ? recRate + '%' : ''}</div>
          </div>` : ''}

          <div style="margin-bottom:12px">
            <label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px">Processor / Gateway implemented</label>
            <select id="${modalId}_proc" style="width:100%;padding:8px;border:1px solid var(--border);border-radius:6px;font-size:13px;background:var(--bg)">
              ${processors.map(p => `<option value="${p}" ${p === recProc ? 'selected' : ''}>${p}</option>`).join('')}
            </select>
          </div>

          <div style="margin-bottom:12px">
            <label style="font-size:12px;font-weight:600;display:block;margin-bottom:4px">Gateway</label>
            <select id="${modalId}_gw" style="width:100%;padding:8px;border:1px solid var(--border);border-radius:6px;font-size:13px;background:var(--bg)">
              ${gateways.map(g => `<option value="${g.gateway_id}" ${g.processor_name === recProc ? 'selected' : ''}>${g.gateway_id} — ${g.gateway_alias || g.gateway_descriptor || ''} (${g.processor_name || ''})</option>`).join('')}
            </select>
          </div>

          <div style="margin-bottom:16px">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
              <input type="checkbox" id="${modalId}_split" onchange="document.getElementById('${modalId}_splitConfig').style.display=this.checked?'block':'none'" style="width:16px;height:16px">
              <span style="font-size:12px;font-weight:600">Split traffic (A/B test)</span>
            </label>
            <div id="${modalId}_splitConfig" style="display:none;margin-top:8px;padding:12px;border:1px solid var(--border);border-radius:6px">
              <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
                <label style="font-size:11px;font-weight:600">New gateway %</label>
                <input type="range" id="${modalId}_splitPct" min="10" max="90" value="70" style="flex:1" oninput="document.getElementById('${modalId}_splitLabel').textContent=this.value+'% new / '+(100-this.value)+'% old'">
                <span id="${modalId}_splitLabel" style="font-size:11px;min-width:110px">70% new / 30% old</span>
              </div>
              <div>
                <label style="font-size:11px;font-weight:600;display:block;margin-bottom:4px">Old processor (control)</label>
                <select id="${modalId}_oldProc" style="width:100%;padding:6px;border:1px solid var(--border);border-radius:4px;font-size:12px;background:var(--bg)">
                  ${processors.map(p => `<option value="${p}">${p}</option>`).join('')}
                </select>
              </div>
            </div>
          </div>

          <div style="display:flex;gap:8px;justify-content:flex-end">
            <button onclick="document.getElementById('${modalId}').remove()" style="padding:8px 16px;border:1px solid var(--border);border-radius:6px;background:var(--bg);cursor:pointer;font-size:13px">Cancel</button>
            <button onclick="Analytics._submitImpl('${modalId}', '${bank.replace(/'/g, "\\'")}', ${isPrepaid}, '${ruleType}', ${cardBrand ? "'" + cardBrand.replace(/'/g, "\\'") + "'" : 'null'}, ${cardType ? "'" + cardType.replace(/'/g, "\\'") + "'" : 'null'}, '${(recProc || '').replace(/'/g, "\\'")}')" style="padding:8px 20px;border:none;border-radius:6px;background:#059669;color:#fff;cursor:pointer;font-size:13px;font-weight:600">Start Tracking</button>
          </div>
        </div>
      </div>`;

    document.body.insertAdjacentHTML('beforeend', html);

    // Filter gateways when processor changes
    const procSelect = document.getElementById(`${modalId}_proc`);
    const gwSelect = document.getElementById(`${modalId}_gw`);
    procSelect.addEventListener('change', () => {
      const sel = procSelect.value;
      gwSelect.innerHTML = gateways
        .filter(g => g.processor_name === sel)
        .map(g => `<option value="${g.gateway_id}">${g.gateway_id} — ${g.gateway_alias || g.gateway_descriptor || ''}</option>`)
        .join('');
    });
    // Trigger initial filter
    procSelect.dispatchEvent(new Event('change'));
  },

  async _submitImpl(modalId, bank, isPrepaid, ruleType, cardBrand, cardType, recProc) {
    const proc = document.getElementById(`${modalId}_proc`).value;
    const gwId = document.getElementById(`${modalId}_gw`).value;
    const isSplit = document.getElementById(`${modalId}_split`).checked;

    let splitConfig = null;
    if (isSplit) {
      const pct = parseInt(document.getElementById(`${modalId}_splitPct`).value);
      const oldProc = document.getElementById(`${modalId}_oldProc`).value;
      splitConfig = { new_pct: pct, old_pct: 100 - pct, old_processor: oldProc };
    }

    const body = {
      issuer_bank: bank,
      is_prepaid: isPrepaid,
      card_brand: cardBrand,
      card_type: cardType,
      rule_type: ruleType,
      recommended_processor: recProc || null,
      actual_processor: proc,
      actual_gateway_ids: JSON.stringify([parseInt(gwId)]),
      split_config: splitConfig,
    };

    try {
      const res = await fetch(`/api/implementations/${this.clientId}/mark`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (data.error) { alert('Error: ' + data.error); return; }

      // Close modal and refresh playbook
      document.getElementById(modalId).remove();
      this._playbookCache = null;
      this.renderCrmRules();
    } catch (err) {
      alert('Failed to mark implementation: ' + err.message);
    }
  },

  showRollbackInfo(implId) {
    fetch(`/api/implementations/${this.clientId}/detail/${implId}`)
      .then(r => r.json())
      .then(data => {
        const impl = data.implementation;
        const msg = `REGRESSION DETECTED\n\n${impl.verdict_reason}\n\nRollback to: ${impl.rollback_to_processor || 'N/A'}\nGateway IDs: ${impl.rollback_to_gateway_ids || 'N/A'}\n\nGo to Implementation Tracker tab for full details.`;
        alert(msg);
      })
      .catch(() => alert('Failed to load rollback info'));
  },

  async _renderFlowOptixV2Initials(el, htmlPrefix, tab) {
    let foData;
    if (this._foV2InitCache) {
      foData = this._foV2InitCache;
    } else {
      const res = await fetch(`/api/analytics/${this.clientId}/flow-optix-v2-initials`);
      foData = await res.json();
      this._foV2InitCache = foData;
    }

    const mode = tab === 'UPSELLS' ? 'upsell' : 'main';
    const modeData = foData?.[mode];
    if (!modeData || !modeData.cards) {
      el.innerHTML = htmlPrefix + `<div class="empty-state"><h3>No ${tab === 'UPSELLS' ? 'Upsell' : 'Initial'} Data</h3><p>Run analysis to generate routing recommendations.</p></div>`;
      return;
    }

    const cards = modeData.cards || [];
    const summary = modeData.summary || {};
    let html = htmlPrefix;

    // Confidence filter
    const confFilter = this.crmConfFilter || null;
    const confCounts = summary.confidenceCounts || {};
    html += `<div style="display:flex;gap:8px;margin-bottom:16px">
      ${[
        { key: null, label: 'All', count: (summary.qualifyingBanks || 0) + ' qualifying', color: null, bg: null },
        { key: 'HIGH', label: 'High', count: confCounts.HIGH || 0, color: '#1D9E75', bg: '#EAF3DE' },
        { key: 'MEDIUM', label: 'Medium', count: confCounts.MEDIUM || 0, color: '#854F0B', bg: '#FAEEDA' },
        { key: 'GATH', label: 'Gathering', count: confCounts.GATHERING || 0, color: '#888780', bg: '#F6F5F0' },
      ].map(f => {
        const active = confFilter === f.key;
        const style = active ? 'background:var(--text);color:white' : f.bg ? 'background:'+f.bg+';color:'+f.color : 'background:var(--bg);color:var(--text);border:1px solid var(--border)';
        return '<button style="font-size:13px;font-weight:600;padding:6px 16px;border-radius:20px;border:none;cursor:pointer;'+style+'" onclick="Analytics.setCrmConfFilter('+(f.key?"'"+f.key+"'":'null')+')">'+f.label+' '+f.count+'</button>';
      }).join('')}
    </div>`;

    // Revenue summary
    const recCards = cards.filter(c => c.hasRecommendation);
    const initMonthly = Math.round(recCards.reduce((s, c) => {
      if (!c.bestActive) return s;
      return s + c.bestActive.rpa * Math.round(c.totalAttempts * 30 / 180);
    }, 0) * 100) / 100;
    html += `<div class="card" style="padding:12px 20px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
      <div style="display:flex;gap:24px;align-items:center">
        <div><span style="font-size:20px;font-weight:700;color:var(--success)">${this.moneyFmt(initMonthly)}</span> <span style="color:var(--text-secondary);font-size:13px">/mo projected</span></div>
        <div><span style="font-size:20px;font-weight:700;color:var(--text)">${this.moneyFmt(initMonthly * 12)}</span> <span style="color:var(--text-secondary);font-size:13px">/yr impact</span></div>
      </div>
      <button class="btn btn-secondary btn-sm" onclick="Analytics.exportCrmRules()">Export CSV</button>
    </div>`;

    // Section header
    const sectionLabel = tab === 'UPSELLS' ? 'Upsell routing analysis' : 'Initial routing analysis';
    html += `<div style="margin-bottom:16px"><div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">${sectionLabel} &mdash; ${summary.totalBanks || 0} banks, ${summary.qualifyingBanks || 0} qualifying</div></div>`;

    // Filter cards by confidence tier
    let filtered = cards;
    if (confFilter === 'HIGH') filtered = cards.filter(c => c.maxApproved >= 20);
    else if (confFilter === 'MEDIUM') filtered = cards.filter(c => c.maxApproved >= 10 && c.maxApproved < 20);

    if (filtered.length === 0) {
      html += '<div class="empty-state"><h3>No matching cards</h3></div>';
      el.innerHTML = html;
      return;
    }

    // Split into qualifying and gathering
    const qualifying = filtered.filter(c => c.hasRecommendation);
    const gathering = filtered.filter(c => !c.hasRecommendation);

    if (qualifying.length > 0) {
      const colCount = Math.max(1, Math.min(4, Math.floor((el.offsetWidth || 900) / 350)));
      const cols = Array.from({ length: colCount }, () => []);
      for (let i = 0; i < qualifying.length; i++) {
        cols[i % colCount].push(qualifying[i]);
      }
      html += `<div style="display:flex;gap:12px;align-items:flex-start">`;
      for (const col of cols) {
        html += '<div style="flex:1;min-width:0">';
        for (const card of col) {
          html += this._foV2Card(card);
        }
        html += '</div>';
      }
      html += '</div>';
    }

    // Gathering section
    if (gathering.length > 0) {
      const gathId = 'gath_init_' + Math.random().toString(36).slice(2, 8);
      html += `<div style="margin-top:16px;padding:12px 20px;background:var(--bg);border-radius:var(--radius);border:1px solid var(--border)">
        <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:13px;color:var(--text-secondary)">${gathering.length} banks gathering data &mdash; need 20+ approved</span>
          <button class="btn btn-sm btn-secondary" onclick="var e=document.getElementById('${gathId}');e.style.display=e.style.display==='none'?'':'none';this.textContent=e.style.display==='none'?'Show list':'Hide list'">Show list</button>
        </div>
        <div id="${gathId}" style="display:none;margin-top:10px;max-height:400px;overflow-y:auto">
          ${gathering.map(c => `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:12px">
            <span style="color:var(--text)">${c.issuer_bank}</span>
            <span style="color:var(--text-secondary)">(${c.totalApproved}/${c.totalAttempts}) &middot; need ${Math.max(0, 20 - c.totalApproved)} more</span>
          </div>`).join('')}
        </div>
      </div>`;
    }

    el.innerHTML = html;
  },

  async _renderFlowOptixV2(el, htmlPrefix) {
    let foData;
    if (this._foV2Cache) {
      foData = this._foV2Cache;
    } else {
      const res = await fetch(`/api/analytics/${this.clientId}/flow-optix-v2`);
      foData = await res.json();
      this._foV2Cache = foData;
    }
    if (!foData || !foData.cards) {
      el.innerHTML = htmlPrefix + '<div class="empty-state"><h3>No Rebill Data</h3><p>Run analysis to generate rebill routing recommendations.</p></div>';
      return;
    }

    const cards = foData.cards || [];
    const summary = foData.summary || {};
    let html = htmlPrefix;

    // Confidence filter
    const confFilter = this.crmConfFilter || null;
    const confCounts = summary.confidenceCounts || {};
    html += `<div style="display:flex;gap:8px;margin-bottom:16px">
      ${[
        { key: null, label: 'All', count: (summary.qualifyingBanks || 0) + ' qualifying', color: null, bg: null },
        { key: 'HIGH', label: 'High', count: confCounts.HIGH || 0, color: '#1D9E75', bg: '#EAF3DE' },
        { key: 'MEDIUM', label: 'Medium', count: confCounts.MEDIUM || 0, color: '#854F0B', bg: '#FAEEDA' },
        { key: 'GATH', label: 'Gathering', count: confCounts.GATHERING || 0, color: '#888780', bg: '#F6F5F0' },
      ].map(f => {
        const active = confFilter === f.key;
        const style = active ? 'background:var(--text);color:white' : f.bg ? 'background:'+f.bg+';color:'+f.color : 'background:var(--bg);color:var(--text);border:1px solid var(--border)';
        return '<button style="font-size:13px;font-weight:600;padding:6px 16px;border-radius:20px;border:none;cursor:pointer;'+style+'" onclick="Analytics.setCrmConfFilter('+(f.key?"'"+f.key+"'":'null')+')">'+f.label+' '+f.count+'</button>';
      }).join('')}
    </div>`;

    // Revenue (only from cards with recommendations)
    const recCards = cards.filter(c => c.hasRecommendation);
    const rebillMonthly = Math.round(recCards.reduce((s, c) => {
      if (!c.bestActive) return s;
      return s + c.bestActive.rpa * Math.round(c.totalAttempts * 30 / 180);
    }, 0) * 100) / 100;
    html += `<div class="card" style="padding:12px 20px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
      <div style="display:flex;gap:24px;align-items:center">
        <div><span style="font-size:20px;font-weight:700;color:var(--success)">${this.moneyFmt(rebillMonthly)}</span> <span style="color:var(--text-secondary);font-size:13px">/mo projected</span></div>
        <div><span style="font-size:20px;font-weight:700;color:var(--text)">${this.moneyFmt(rebillMonthly * 12)}</span> <span style="color:var(--text-secondary);font-size:13px">/yr impact</span></div>
      </div>
      <button class="btn btn-secondary btn-sm" onclick="Analytics.exportCrmRules()">Export CSV</button>
    </div>`;

    // Section header
    html += '<div style="margin-bottom:16px"><div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">Rebill routing rules — ready to implement</div></div>';

    // Filter cards by confidence tier
    let filtered = cards;
    if (confFilter === 'HIGH') filtered = cards.filter(c => c.maxApproved >= 20);
    else if (confFilter === 'MEDIUM') filtered = cards.filter(c => c.maxApproved >= 10 && c.maxApproved < 20);

    if (filtered.length === 0) {
      html += '<div class="empty-state"><h3>No matching cards</h3></div>';
      el.innerHTML = html;
      return;
    }

    // Qualifying cards grid
    const qualifying = filtered.filter(c => c.hasRecommendation);
    const gathering = filtered.filter(c => !c.hasRecommendation);

    if (qualifying.length > 0) {
      // Split cards into fixed columns manually so expanding one doesn't reflow others
      const colCount = Math.max(1, Math.min(4, Math.floor((el.offsetWidth || 900) / 350)));
      const cols = Array.from({ length: colCount }, () => []);
      for (let i = 0; i < qualifying.length; i++) {
        cols[i % colCount].push(qualifying[i]);
      }
      html += `<div style="display:flex;gap:12px;align-items:flex-start">`;
      for (const col of cols) {
        html += '<div style="flex:1;min-width:0">';
        for (const card of col) {
          html += this._foV2Card(card);
        }
        html += '</div>';
      }
      html += '</div>';
    }

    // Gathering section — collapsed by default
    if (gathering.length > 0) {
      const gathId = 'gath_' + Math.random().toString(36).slice(2, 8);
      html += `<div style="margin-top:16px;padding:12px 20px;background:var(--bg);border-radius:var(--radius);border:1px solid var(--border)">
        <div style="display:flex;align-items:center;gap:12px">
          <span style="font-size:13px;color:var(--text-secondary)">${gathering.length} banks gathering data &mdash; need 20+ approved</span>
          <button class="btn btn-sm btn-secondary" onclick="var e=document.getElementById('${gathId}');e.style.display=e.style.display==='none'?'':'none';this.textContent=e.style.display==='none'?'Show list':'Hide list'">Show list</button>
        </div>
        <div id="${gathId}" style="display:none;margin-top:10px;max-height:400px;overflow-y:auto">
          ${gathering.map(c => `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:12px">
            <span style="color:var(--text)">${c.issuer_bank}</span>
            <span style="color:var(--text-secondary)">(${c.totalApproved}/${c.totalAttempts}) &middot; need ${Math.max(0, 20 - c.totalApproved)} more</span>
          </div>`).join('')}
        </div>
      </div>`;
    }

    el.innerHTML = html;
  },

  _foV2Card(card) {
    const borderColor = card.hasRecommendation ? '#1D9E75' : card.hasActiveProcessor ? '#BA7517' : '#A32D2D';
    const copyId = 'fv2_' + Math.random().toString(36).slice(2, 8);
    const binText = card.bins.join(', ');
    const expandId = 'exp_' + Math.random().toString(36).slice(2, 8);
    const confLabel = card.maxApproved >= 20 ? 'HIGH' : card.maxApproved >= 10 ? 'MED' : 'LOW';
    const confBg = card.maxApproved >= 20 ? '#EAF3DE' : card.maxApproved >= 10 ? '#FAEEDA' : '#F1EFE8';
    const confColor = card.maxApproved >= 20 ? '#3B6D11' : card.maxApproved >= 10 ? '#854F0B' : '#5F5E5A';
    const qualBrands = card.brands.filter(b => b.qualifies);

    let html = `<div class="card" style="padding:0;border-left:3px solid ${borderColor};overflow:hidden;min-width:0;margin-bottom:12px;position:relative;min-height:180px">`;

    // CONFIDENCE BADGE — top left corner
    html += `<span style="position:absolute;top:6px;left:8px;font-size:8px;padding:1px 5px;border-radius:3px;background:${confBg};color:${confColor};font-weight:600">${confLabel}</span>`;

    // HEADER
    const approvalRate = card.totalAttempts > 0 ? Math.round(card.totalApproved / card.totalAttempts * 10000) / 100 : 0;
    html += `<div style="padding:20px 20px 10px">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
        <span style="font-size:15px;font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0">${card.issuer_bank}</span>
        <span style="font-size:15px;font-weight:700;color:#4f7df9;white-space:nowrap;flex-shrink:0">${approvalRate}% <span style="font-size:11px;font-weight:500;color:var(--text-secondary)">(${card.totalApproved}/${formatNum(card.totalAttempts)})</span></span>
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-top:3px">
        <div style="font-size:10px;color:var(--text-secondary)">${card.binCount} BINs &middot; ${card.processorCount} processors${card.brandCount > 0 ? ' &middot; ' + card.brandCount + ' brand' + (card.brandCount > 1 ? 's' : '') : ''}</div>
        <button id="${copyId}" onclick="navigator.clipboard.writeText('${binText.replace(/'/g, "\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy BINs',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:10px;cursor:pointer;padding:2px 8px;flex-shrink:0">Copy BINs</button>
      </div>${card.rebillSignal?.blocking ? `<div style="margin-top:4px;padding:3px 8px;background:#FEE2E2;border-radius:3px;display:inline-block"><span style="font-size:9px;font-weight:600;color:#991B1B">Rebill blocker &mdash; ${card.rebillSignal.rate}% rebill rate (${card.rebillSignal.app}/${card.rebillSignal.att})</span></div>` : card.rebillSignal && !card.rebillSignal.noData && card.rebillSignal.rate !== null && card.rebillSignal.rate < 15 ? `<div style="margin-top:4px;padding:3px 8px;background:#FEF3C7;border-radius:3px;display:inline-block"><span style="font-size:9px;font-weight:600;color:#92400E">Low rebill rate &mdash; ${card.rebillSignal.rate}% (${card.rebillSignal.app}/${card.rebillSignal.att})</span></div>` : ''}
    </div>`;

    // COLLAPSED STATE — 3 metric boxes
    html += `<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;padding:0 20px 12px">`;

    // Box 1: Best active — check if multiple levels disagree
    const activeNames = new Set();
    for (const br of qualBrands) {
      for (const l3 of (br.l3Groups || [])) {
        if (l3.recommendedActive) activeNames.add(l3.recommendedActive.name);
        for (const l4 of (l3.l4Groups || [])) {
          if (l4.recommendedActive) activeNames.add(l4.recommendedActive.name);
        }
      }
    }
    const multiLevelActive = activeNames.size > 1;

    if (multiLevelActive) {
      html += `<div style="background:#FAEEDA;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#854F0B;font-weight:600">Best active today</div>
        <div style="font-size:11px;font-weight:500;color:#854F0B;margin-top:2px">Multiple processors</div>
        <div style="font-size:9px;color:#854F0B">Varies by sub-level — expand to see</div>
      </div>`;
    } else if (card.bestActive) {
      html += `<div style="background:#E1F5EE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#3B6D11;font-weight:600">Best active today</div>
        <div style="font-size:12px;font-weight:500;color:#1D9E75;margin-top:2px">${card.bestActive.name} ${card.bestActive.rate}%</div>
        <div style="font-size:9px;color:#3B6D11">(${card.bestActive.app}/${card.bestActive.att}) &middot; $${card.bestActive.rpa}/att</div>
      </div>`;
    } else {
      html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#5F5E5A;font-weight:600">Best active today</div>
        <div style="font-size:11px;color:#5F5E5A;margin-top:2px">No active data yet</div>
      </div>`;
    }

    // Box 2: Brand approval rates — side by side
    const brandRates = (card.brands || []).filter(b => b.qualifies).map(b => ({
      brand: b.card_brand,
      rate: b.totalAttempts > 0 ? Math.round(b.totalApproved / b.totalAttempts * 10000) / 100 : 0,
      app: b.totalApproved,
      att: b.totalAttempts,
    }));
    if (brandRates.length > 0) {
      html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px;display:grid;grid-template-columns:repeat(${brandRates.length},1fr);gap:4px;overflow:hidden">`;
      for (const br of brandRates) {
        html += `<div style="min-width:0;overflow:hidden">
          <div style="font-size:8px;color:#5F5E5A;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${br.brand}</div>
          <div style="font-size:13px;font-weight:700;color:var(--text);margin-top:1px">${br.rate}%</div>
          <div style="font-size:8px;color:#5F5E5A">(${br.app}/${br.att})</div>
        </div>`;
      }
      html += '</div>';
    } else {
      html += `<div style="background:#F1EFE8;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#5F5E5A;font-weight:600">Brands</div>
        <div style="font-size:11px;color:#5F5E5A;margin-top:2px">No qualifying brands</div>
      </div>`;
    }

    // Box 3: Cascade targets (initials) or Price opportunity (rebills)
    if (card.cascadeTargets && card.cascadeTargets.length > 0) {
      const cascRate = card.cascadeTotal > 0 ? Math.round(card.cascadeApproved / card.cascadeTotal * 100) : 0;
      const topTarget = card.cascadeTargets.filter(t => t.active)[0] || card.cascadeTargets[0];
      html += `<div style="background:#EDE9FE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#6D28D9;font-weight:600">Cascade saves</div>
        <div style="font-size:12px;font-weight:500;color:#6D28D9;margin-top:2px">${cascRate}% (${card.cascadeApproved}/${card.cascadeTotal})</div>
        <div style="font-size:9px;color:#6D28D9">Best: ${topTarget.name} ${topTarget.rate}%</div>
      </div>`;
    } else if (card.cascadeTargets) {
      html += `<div style="background:#F5F3FF;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#6D28D9;font-weight:600">Cascade saves</div>
        <div style="font-size:11px;color:#6D28D9;margin-top:2px">No cascade data</div>
      </div>`;
    } else if (card.priceOptimization) {
      const po = card.priceOptimization;
      html += `<div style="background:#FAEEDA;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#854F0B;font-weight:600">Price opportunity</div>
        <div style="font-size:12px;font-weight:500;color:#854F0B;margin-top:2px">$${po.currentPrice} &rarr; $${po.optimalPrice}</div>
        <div style="font-size:9px;color:#854F0B">+$${Math.round(po.monthlyImpact).toLocaleString()}/mo</div>
      </div>`;
    } else {
      html += `<div style="background:#E1F5EE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#3B6D11;font-weight:600">Price</div>
        <div style="font-size:11px;color:#3B6D11;margin-top:2px">Optimal</div>
      </div>`;
    }

    html += '</div>';

    // EXPAND TRIGGER — bottom of collapsed card
    const expandBtnId = expandId + '_btn';
    html += `<div id="${expandBtnId}" onclick="document.getElementById('${expandId}').style.display='';this.style.display='none'" style="border-top:1px dashed var(--border);padding:6px 0;text-align:center;cursor:pointer">
      <span style="font-size:11px;color:var(--text-muted)">&#9660;</span>
    </div>`;

    // EXPANDED STATE
    html += `<div id="${expandId}" style="display:none">`;

    // Brand tabs (only if qualifying brands exist)
    if (qualBrands.length > 0) {
      html += `<div style="display:flex;gap:4px;padding:0 20px 8px;border-top:1px solid var(--border);padding-top:8px">`;
      for (let i = 0; i < qualBrands.length; i++) {
        const br = qualBrands[i];
        const brConf = br.totalApproved >= 20 ? 'HIGH' : br.totalApproved >= 10 ? 'MED' : 'LOW';
        const tabId = expandId + '_tab_' + i;
        const panelId = expandId + '_panel_' + i;
        const isFirst = i === 0;
        html += `<button id="${tabId}" onclick="document.querySelectorAll('[data-cardpanel=\\'${expandId}\\']').forEach(e=>e.style.display='none');document.querySelectorAll('[data-cardtab=\\'${expandId}\\']').forEach(e=>{e.style.background='var(--bg)';e.style.color='var(--text-secondary)'});document.getElementById('${panelId}').style.display='';this.style.background='var(--text)';this.style.color='white'" data-cardtab="${expandId}" style="font-size:10px;font-weight:600;padding:4px 10px;border-radius:4px;border:none;cursor:pointer;${isFirst ? 'background:var(--text);color:white' : 'background:var(--bg);color:var(--text-secondary)'}">${br.card_brand} <span style="font-size:9px;font-weight:400">${brConf} (${br.totalApproved}/${br.totalAttempts})</span></button>`;
      }
      html += '</div>';
    }

    // Brand panels
    for (let i = 0; i < qualBrands.length; i++) {
      const br = qualBrands[i];
      const panelId = expandId + '_panel_' + i;
      html += `<div id="${panelId}" data-cardpanel="${expandId}" style="${i > 0 ? 'display:none' : ''}">`;
      html += this._foV2BrandPanel(br, card);
      html += '</div>';
    }

    // Non-qualifying brands note
    const nonQual = card.brands.filter(b => !b.qualifies);
    if (nonQual.length > 0) {
      html += `<div style="padding:6px 20px;font-size:10px;color:var(--text-secondary);border-top:1px solid var(--border)">${nonQual.length} more brand${nonQual.length > 1 ? 's' : ''} gathering data</div>`;
    }

    // COLLAPSE TRIGGER — bottom of expanded
    html += `<div onclick="this.parentElement.style.display='none';document.getElementById('${expandBtnId}').style.display=''" style="border-top:1px dashed var(--border);padding:6px 0;text-align:center;cursor:pointer">
      <span style="font-size:11px;color:var(--text-muted)">&#9650;</span>
    </div>`;

    html += '</div>'; // end expanded

    html += '</div>'; // card
    return html;
  },

  _foV2BrandPanel(br, bankCard) {
    let html = '';
    const ci = br.currentImpl;

    // ELEMENT 3 — LEVEL TRACKER
    const deepest = this._foV2DeepestLevel(br);
    html += `<div style="padding:4px 20px 8px;display:flex;gap:3px;flex-wrap:wrap">`;
    const lvls = [{n:1,l:'Bank'},{n:2,l:'Brand'},{n:3,l:'Prepaid'},{n:4,l:'Type'},{n:5,l:'Level'},{n:6,l:'BIN'}];
    for (const lv of lvls) {
      let bg, color;
      if (lv.n < deepest) { bg = '#EAF3DE'; color = '#3B6D11'; }
      else if (lv.n === deepest) { bg = '#1e40af'; color = 'white'; }
      else { bg = 'white'; color = '#888780'; }
      html += `<span style="font-size:9px;padding:2px 7px;border-radius:3px;background:${bg};color:${color};font-weight:600;border:${lv.n>deepest?'1px solid var(--border)':'none'}">L${lv.n} ${lv.l}</span>`;
      if (lv.n < 6) html += '<span style="color:var(--border);font-size:9px">&rarr;</span>';
    }
    html += '</div>';

    // ELEMENT 4 — PRICE OPTIMIZATION
    if (br.priceOptimization) {
      const po = br.priceOptimization;
      html += `<div style="padding:8px 20px;background:#FAEEDA;border-bottom:0.5px solid #FAC775">
        <div style="font-size:10px;font-weight:600;text-transform:uppercase;color:#854F0B;margin-bottom:6px">Price optimization available</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px">
          <div><div style="font-size:9px;color:#854F0B">Current price</div><div style="font-size:12px;font-weight:500;color:#854F0B">$${po.currentPrice}</div><div style="font-size:9px;color:#854F0B">${po.currentRate}% &middot; $${po.currentRpa}/att</div></div>
          <div><div style="font-size:9px;color:#854F0B">Optimal price</div><div style="font-size:12px;font-weight:500;color:#1D9E75">$${po.optimalPrice}</div><div style="font-size:9px;color:#854F0B">${po.optimalRate}% &middot; $${po.optimalRpa}/att</div></div>
          <div><div style="font-size:9px;color:#854F0B">Estimated impact</div><div style="font-size:12px;font-weight:500;color:#1D9E75">+$${Math.round(po.monthlyImpact).toLocaleString()}/mo</div></div>
        </div>
      </div>`;
    }

    // ELEMENT 5 — PROCESSOR PERFORMANCE BY SUB-GROUP (nested expandable)
    html += `<div style="padding:8px 20px;border-top:1px solid var(--border)">
      <div style="font-size:10px;font-weight:600;text-transform:uppercase;color:var(--text-secondary);margin-bottom:8px">Processor performance by sub-group</div>`;

    const l3Groups = br.l3Groups || [];
    const anyL3Qualifies = l3Groups.some(l3 => l3.qualifies);

    if (!anyL3Qualifies) {
      // Brand-level optimization — no L3 has enough data
      if (ci?.bestActive) {
        html += `<div style="background:#E1F5EE;border-radius:4px;padding:5px 8px;margin-bottom:3px;display:flex;align-items:center;gap:6px">
          <span style="font-size:9px;font-weight:600;color:#3B6D11">Best active</span>
          <span style="font-size:10px;font-weight:500;color:#1D9E75">${ci.bestActive.name} ${ci.bestActive.rate}%</span>
          <span style="font-size:9px;color:#3B6D11">$${ci.bestActive.rpa}/att</span>
        </div>`;
        if (ci.bestHistorical) {
          html += `<div style="background:#F1EFE8;border-radius:4px;padding:5px 8px;margin-bottom:3px;display:flex;align-items:center;gap:6px">
            <span style="font-size:9px;font-weight:600;color:#5F5E5A">Best historical</span>
            <span style="font-size:10px;font-weight:500">${ci.bestHistorical.name} ${ci.bestHistorical.rate}%</span>
            <span style="font-size:9px;color:#854F0B">need MID</span>
          </div>`;
        }
      }
      html += `<div style="font-size:9px;font-style:italic;color:var(--text-secondary);margin:4px 0">Sub-groups need 10+ approved for deeper analysis</div>`;
      for (const l3 of l3Groups) {
        html += this._foV2GatheringRow(l3.is_prepaid ? 'Prepaid' : 'Non-prepaid', 'L3', l3.att, l3.app, l3.bins);
      }
    } else {
      // Render each L3 group as expandable row
      for (const l3 of l3Groups) {
        if (!l3.qualifies) {
          html += this._foV2GatheringRow(l3.is_prepaid ? 'Prepaid' : 'Non-prepaid', 'L3', l3.att, l3.app, l3.bins);
          continue;
        }
        const l3Id = 'el3_' + Math.random().toString(36).slice(2, 8);
        const l3CopyId = 'l3c_' + Math.random().toString(36).slice(2, 8);
        const l3BinText = (l3.bins || []).join(', ');
        const l3Procs = l3.processors || [];
        const l3Active = l3.recommendedActive || l3Procs.find(p => p.active);
        const l3BestByRpa = l3.recommendedBest || [...l3Procs].filter(p => !p.active).sort((a, b) => b.rpa - a.rpa)[0];
        const l3ActiveRate = l3Active ? l3Active.rawRate : 0;
        const l3BestRate = l3BestByRpa ? l3BestByRpa.rawRate : 0;
        const l3Same = l3Active && l3BestByRpa && l3Active.name === l3BestByRpa.name;
        // Check if L4 children disagree on best active
        const l4ActiveNames = new Set();
        for (const l4 of (l3.l4Groups || [])) {
          if (l4.qualifies && l4.recommendedActive) l4ActiveNames.add(l4.recommendedActive.name);
        }
        const l4sDisagree = l4ActiveNames.size > 1;
        const l3MultiActive = l4sDisagree || this._foV2MultipleProcs(l3Procs, true);
        const l3MultiHist = this._foV2MultipleProcs(l3Procs, false);
        const _cBg = c => c === 'HIGH' ? '#EAF3DE' : c === 'MEDIUM' ? '#FAEEDA' : '#F1EFE8';
        const _cCl = c => c === 'HIGH' ? '#3B6D11' : c === 'MEDIUM' ? '#854F0B' : '#5F5E5A';
        const pillBg = l3.is_prepaid ? '#EEEDFE' : '#F1EFE8';
        const pillColor = l3.is_prepaid ? '#3C3489' : '#5F5E5A';

        // L3 collapsed row
        html += `<div style="border:1px solid var(--border);border-radius:6px;margin-bottom:4px;overflow:hidden">
          <div onclick="var e=document.getElementById('${l3Id}');e.style.display=e.style.display==='none'?'':'none'" style="display:grid;grid-template-columns:auto 1fr 1fr 1fr 1fr 1fr 1fr auto;gap:2px 6px;align-items:center;padding:8px 12px;cursor:pointer;background:var(--bg)">
            <span style="font-size:12px;font-weight:600;padding:3px 8px;border-radius:3px;background:${pillBg};color:${pillColor}">L3 ${l3.is_prepaid ? 'Prepaid' : 'Non-prepaid'}</span>
            <span style="font-size:11px;color:var(--text-secondary)">${l3.binCount} BINs</span>
            ${l3MultiActive ? `<span style="font-size:12px;color:#854F0B;font-weight:500;grid-column:span 3">${l4sDisagree ? 'Multiple processors per sub-level' : 'Multiple processors'}</span>` :
              l3Active ? `<span style="font-size:12px;color:#1D9E75;font-weight:500">${l3Active.name}</span><span style="font-size:12px;color:#1D9E75;font-weight:500">${l3ActiveRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(l3Active.confidence)};color:${_cCl(l3Active.confidence)}">${l3Active.confidence} (${l3Active.app}/${l3Active.att})</span>` : '<span style="font-size:11px;color:#854F0B">No active MID</span><span></span><span></span>'}
            ${l3MultiHist && !l3Same ? '<span style="font-size:12px;color:#5F5E5A;font-weight:500;grid-column:span 2">Multiple processors</span>' :
              !l3Same && l3BestByRpa ? `<span style="font-size:12px;color:#5F5E5A;font-weight:500">${l3BestByRpa.name} ${l3BestRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(l3BestByRpa.confidence)};color:${_cCl(l3BestByRpa.confidence)}">${l3BestByRpa.confidence} (${l3BestByRpa.app}/${l3BestByRpa.att})</span>` : '<span></span><span></span>'}
            <button id="${l3CopyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${l3BinText.replace(/'/g, "\\'")}');var b=document.getElementById('${l3CopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:11px;cursor:pointer;padding:2px 8px">Copy</button>
          </div>
          <div id="${l3Id}" style="display:none;padding:6px 10px;border-top:1px solid var(--border)">`;

        // Price + LTV row
        if (l3.bestPrice) {
          html += `<div style="background:var(--bg);border-radius:5px;padding:6px 10px;margin-bottom:6px;display:flex;align-items:center;gap:8px">
            <span style="font-size:12px;color:var(--text-secondary)">Price:</span>
            <span style="font-size:13px;font-weight:500">$${l3.bestPrice}</span>
            <span style="font-size:12px;color:#1D9E75">LTV $${l3.avgOrderValue}</span>
            <span style="font-size:11px;padding:2px 6px;border-radius:3px;background:#F1EFE8;color:#5F5E5A">${l3.bestPriceConf}${l3.bestPriceConf === 'LOW' || l3.bestPriceConf === 'GATHERING' ? ' &middot; global default' : ''}</span>
          </div>`;
        }

        // L3 processors summary
        if (l3.singleProcessor) html += `<div style="font-size:11px;color:#854F0B;background:#FAEEDA;border-radius:3px;padding:3px 8px;margin-bottom:4px;display:inline-block">Only 1 processor available</div>`;
        html += this._foV2ProcRows(l3.processors || []);

        // L4 groups inside L3
        for (const l4 of (l3.l4Groups || [])) {
          if (l4.att === 0) continue;
          if (!l4.qualifies) {
            html += this._foV2GatheringRow(l4.card_type, 'L4', l4.att, l4.app, l4.bins);
            continue;
          }
          const l4Id = 'el4_' + Math.random().toString(36).slice(2, 8);
          const l4CopyId = 'l4c_' + Math.random().toString(36).slice(2, 8);
          const l4BinText = (l4.bins || []).join(', ');
          const l4Procs = l4.processors || [];
          const l4Active = l4.recommendedActive || l4Procs.find(p => p.active);
          const l4BestByRpa = l4.recommendedBest || [...l4Procs].filter(p => !p.active).sort((a, b) => b.rpa - a.rpa)[0];
          const l4ActiveRate = l4Active ? l4Active.rawRate : 0;
          const l4BestRate = l4BestByRpa ? l4BestByRpa.rawRate : 0;
          const l4Same = l4Active && l4BestByRpa && l4Active.name === l4BestByRpa.name;
          const l4MultiActive = this._foV2MultipleProcs(l4Procs, true);
          const l4MultiHist = this._foV2MultipleProcs(l4Procs, false);

          // L4 collapsed row
          html += `<div style="border:1px solid var(--border);border-radius:5px;margin:4px 0;overflow:hidden">
            <div onclick="var e=document.getElementById('${l4Id}');e.style.display=e.style.display==='none'?'':'none'" style="display:grid;grid-template-columns:26px 1fr 1fr 1fr 1fr 1fr 1fr auto;gap:2px 6px;align-items:center;padding:6px 10px;cursor:pointer;background:white">
              <span style="font-size:11px;padding:2px 5px;border-radius:3px;background:#EAF3DE;color:#3B6D11;font-weight:600;text-align:center">L4</span>
              <span style="font-size:13px;font-weight:500">${l4.card_type}</span>
              ${l4MultiActive ? '<span style="font-size:12px;color:#1D9E75;font-weight:500;grid-column:span 3">Multiple processors</span>' :
                l4Active ? `<span style="font-size:12px;color:#1D9E75;font-weight:500">${l4Active.name}</span><span style="font-size:12px;color:#1D9E75;font-weight:500">${l4ActiveRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(l4Active.confidence)};color:${_cCl(l4Active.confidence)}">${l4Active.confidence} (${l4Active.app}/${l4Active.att})</span>` : '<span style="font-size:11px;color:#854F0B">No active MID</span><span></span><span></span>'}
              ${l4MultiHist && !l4Same ? '<span style="font-size:12px;color:#5F5E5A;font-weight:500;grid-column:span 2">Multiple processors</span>' :
                !l4Same && l4BestByRpa ? `<span style="font-size:12px;color:#5F5E5A;font-weight:500">${l4BestByRpa.name} ${l4BestRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(l4BestByRpa.confidence)};color:${_cCl(l4BestByRpa.confidence)}">${l4BestByRpa.confidence} (${l4BestByRpa.app}/${l4BestByRpa.att})</span>` : '<span></span><span></span>'}
              <button id="${l4CopyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${l4BinText.replace(/'/g, "\\'")}');var b=document.getElementById('${l4CopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:11px;cursor:pointer;padding:2px 8px">Copy</button>
            </div>
            <div id="${l4Id}" style="display:none;padding:5px 8px;border-top:1px solid var(--border)">`;

          // L4 processors
          if (l4.singleProcessor) html += `<div style="font-size:11px;color:#854F0B;background:#FAEEDA;border-radius:3px;padding:3px 8px;margin-bottom:4px;display:inline-block">Only 1 processor available</div>`;
          html += this._foV2ProcRows(l4.processors || []);

          // L5 groups inside L4
          for (const l5 of (l4.l5Groups || [])) {
            if (!l5.qualifies) {
              const l5gCopyId = 'l5gc_' + Math.random().toString(36).slice(2, 8);
              const l5gBinText = (l5.bins || []).join(', ');
              html += `<div style="margin:3px 0;display:grid;grid-template-columns:26px 1fr 1fr 1fr 1fr 1fr 1fr auto;gap:2px 6px;align-items:center">
                <span style="font-size:11px;padding:2px 5px;border-radius:3px;background:#FAEEDA;color:#854F0B;font-weight:600;text-align:center">L5</span>
                <span style="font-size:12px;color:var(--text-secondary)">${l5.card_level}</span>
                <span style="font-size:11px;font-style:italic;color:var(--text-secondary);grid-column:span 5">Gathering (${l5.app}/${l5.att}) &mdash; need ${Math.max(0, 10 - l5.app)} more</span>
                <button id="${l5gCopyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${l5gBinText.replace(/'/g, "\\'")}');var b=document.getElementById('${l5gCopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:11px;cursor:pointer;padding:2px 8px">Copy</button>
              </div>`;
              continue;
            }

            const l5Id = 'el5_' + Math.random().toString(36).slice(2, 8);
            const l5CopyId = 'l5c_' + Math.random().toString(36).slice(2, 8);
            const l5BinText = (l5.bins || []).join(', ');
            const l5Procs = l5.processors || [];
            const bestOverall = l5.recommendedBest || [...l5Procs].filter(p => !p.active).sort((a, b) => b.rpa - a.rpa)[0];
            const bestActive = l5.recommendedActive || l5Procs.find(p => p.active);
            const bestActiveRate = bestActive ? bestActive.rawRate : 0;
            const bestOverallRate = bestOverall ? bestOverall.rawRate : 0;
            const sameProc = bestOverall && bestActive && bestOverall.name === bestActive.name;
            const l5MultiActive = this._foV2MultipleProcs(l5Procs, true);
            const l5MultiHist = this._foV2MultipleProcs(l5Procs, false);

            // L5 collapsed row
            html += `<div style="border:1px solid var(--border);border-radius:4px;margin:3px 0;overflow:hidden">
              <div onclick="var e=document.getElementById('${l5Id}');e.style.display=e.style.display==='none'?'':'none'" style="display:grid;grid-template-columns:26px 1fr 1fr 1fr 1fr 1fr 1fr auto;gap:2px 6px;align-items:center;padding:6px 10px;cursor:pointer;background:white">
                <span style="font-size:11px;padding:2px 5px;border-radius:3px;background:#1e40af;color:white;font-weight:600;text-align:center">L5</span>
                <span style="font-size:12px;font-weight:500">${l5.card_level}</span>
                ${l5MultiActive ? '<span style="font-size:12px;color:#1D9E75;font-weight:500;grid-column:span 3">Multiple processors</span>' :
                  bestActive ? `<span style="font-size:12px;color:#1D9E75;font-weight:500">${bestActive.name}</span><span style="font-size:12px;color:#1D9E75;font-weight:500">${bestActiveRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(bestActive.confidence)};color:${_cCl(bestActive.confidence)}">${bestActive.confidence} (${bestActive.app}/${bestActive.att})</span>` : '<span style="font-size:11px;color:#854F0B">No active MID</span><span></span><span></span>'}
                ${l5MultiHist && !sameProc ? '<span style="font-size:12px;color:#5F5E5A;font-weight:500;grid-column:span 2">Multiple processors</span>' :
                  !sameProc && bestOverall ? `<span style="font-size:12px;color:#5F5E5A;font-weight:500">${bestOverall.name} ${bestOverallRate}%</span><span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${_cBg(bestOverall.confidence)};color:${_cCl(bestOverall.confidence)}">${bestOverall.confidence} (${bestOverall.app}/${bestOverall.att})</span>` : '<span></span><span></span>'}
                <button id="${l5CopyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${l5BinText.replace(/'/g, "\\'")}');var b=document.getElementById('${l5CopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:11px;cursor:pointer;padding:2px 8px">Copy</button>
              </div>
              <div id="${l5Id}" style="display:none;padding:6px 10px;border-top:1px solid var(--border)">`;

            // L5 expanded content — processors
            if (l5.singleProcessor) html += `<div style="font-size:11px;color:#854F0B;margin-bottom:3px">Only 1 processor available</div>`;
            html += this._foV2ProcRows(l5Procs);

            // BIN outliers
            if (l5.binOutliers && l5.binOutliers.length > 0) {
              html += `<div style="margin-top:4px;background:#FAEEDA;border-radius:4px;padding:6px 10px">
                <div style="font-size:11px;font-weight:600;color:#854F0B">BIN outliers (&gt;5pp from ${l5.binOutliers[0].l5Rate}% avg)</div>`;
              for (const bo of l5.binOutliers.slice(0, 3)) {
                const sign = bo.delta > 0 ? '+' : '';
                html += `<div style="font-size:11px;font-family:monospace;color:#854F0B">${bo.bin} ${bo.rate}% (${sign}${bo.delta}pp) &middot; Best: ${bo.bestProcessor} ${bo.bestRate}%</div>`;
              }
              html += '</div>';
            }

            html += '</div></div>'; // L5 expanded + L5 container
          }

          // Salvage sequence inside L4
          if (l4.salvageSequence && l4.salvageSequence.length > 0) {
            html += `<div style="margin-top:6px;padding-top:6px;border-top:1px solid var(--border)">
              <div style="font-size:11px;font-weight:600;text-transform:uppercase;color:var(--text-secondary);margin-bottom:4px">Decline salvage ${l4.salvageLevel && l4.salvageLevel !== 'L4' ? '<span style="font-size:10px;font-weight:400;font-style:italic;text-transform:none">(using ' + l4.salvageLevel + ' data — insufficient L4 salvage)</span>' : ''}</div>`;
            for (const s of l4.salvageSequence) {
              if (s.isStop) {
                html += `<div style="background:#FCEBEB;border-radius:4px;padding:5px 10px;margin-bottom:3px;font-size:12px;color:#A32D2D">
                  ${s.label} — STOP &middot; RPA below $3 &middot; ~${formatNum(s.volume)}/mo
                </div>`;
              } else {
                html += `<div style="background:var(--bg);border-radius:4px;padding:5px 10px;margin-bottom:3px;display:flex;align-items:center;gap:6px">
                  <span style="font-size:11px;color:var(--text-secondary);min-width:70px">${s.label}</span>
                  <span style="font-size:12px;font-weight:500">${s.processor}</span>
                  ${s.price ? '<span style="font-size:11px;padding:2px 5px;border-radius:3px;background:white;border:1px solid var(--border);color:var(--text-secondary)">$' + s.price + '</span>' : ''}
                  <span style="font-size:11px;color:var(--text-secondary)">${s.rate}%</span>
                  <span style="font-size:12px;font-weight:500;color:#1D9E75">$${s.rpa}/att</span>
                  <span style="font-size:11px;color:var(--text-secondary);margin-left:auto">~${formatNum(s.volume)}/mo</span>
                </div>`;
              }
            }
            html += '</div>';
          }

          html += '</div></div>'; // L4 expanded + L4 container
        }

        // Salvage at L3 level when no L4 qualifies — bubble up best salvage from L4s
        const anyL4Qualifies = (l3.l4Groups || []).some(l4 => l4.qualifies);
        if (!anyL4Qualifies) {
          let bestSalvage = null;
          for (const l4 of (l3.l4Groups || [])) {
            if (l4.salvageSequence && l4.salvageSequence.length > (bestSalvage?.length || 0)) {
              bestSalvage = l4.salvageSequence;
            }
          }
          if (bestSalvage && bestSalvage.length > 0) {
            html += `<div style="margin-top:6px;padding-top:6px;border-top:1px solid var(--border)">
              <div style="font-size:11px;font-weight:600;text-transform:uppercase;color:var(--text-secondary);margin-bottom:4px">Decline salvage <span style="font-size:10px;font-weight:400;font-style:italic;text-transform:none">(L3 level — sub-groups gathering)</span></div>`;
            for (const s of bestSalvage) {
              if (s.isStop) {
                html += `<div style="background:#FCEBEB;border-radius:4px;padding:5px 10px;margin-bottom:3px;font-size:12px;color:#A32D2D">
                  ${s.label} — STOP &middot; RPA below $3 &middot; ~${formatNum(s.volume)}/mo
                </div>`;
              } else {
                html += `<div style="background:var(--bg);border-radius:4px;padding:5px 10px;margin-bottom:3px;display:flex;align-items:center;gap:6px">
                  <span style="font-size:11px;color:var(--text-secondary);min-width:70px">${s.label}</span>
                  <span style="font-size:12px;font-weight:500">${s.processor}</span>
                  ${s.price ? '<span style="font-size:11px;padding:2px 5px;border-radius:3px;background:white;border:1px solid var(--border);color:var(--text-secondary)">$' + s.price + '</span>' : ''}
                  <span style="font-size:11px;color:var(--text-secondary)">${s.rate}%</span>
                  <span style="font-size:12px;font-weight:500;color:#1D9E75">$${s.rpa}/att</span>
                  <span style="font-size:11px;color:var(--text-secondary);margin-left:auto">~${formatNum(s.volume)}/mo</span>
                </div>`;
              }
            }
            html += '</div>';
          }
        }

        html += '</div></div>'; // L3 expanded + L3 container
      }
    }

    html += '</div>'; // section

    // ELEMENT 6 — ACQUISITION PRIORITY
    if (br.acquisitionPriority.length > 0) {
      html += `<div style="padding:8px 20px;border-top:1px solid var(--border)">
        <div style="font-size:10px;font-weight:600;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Processor acquisition priority</div>`;
      for (let i = 0; i < Math.min(br.acquisitionPriority.length, 5); i++) {
        const a = br.acquisitionPriority[i];
        html += `<div style="background:var(--bg);border-radius:4px;padding:5px 8px;margin-bottom:3px;display:flex;align-items:center;gap:6px">
          <span style="font-size:9px;font-weight:600;padding:1px 5px;border-radius:3px;background:#dbeafe;color:#1e40af">#${i + 1}</span>
          <span style="font-size:11px;font-weight:500">${a.processor}</span>
          <span style="font-size:9px;color:var(--text-secondary)">${a.wins}</span>
          <span style="font-size:9px;padding:1px 4px;border-radius:2px;background:#FAEEDA;color:#854F0B">HISTORICAL</span>
          <span style="font-size:10px;font-weight:500;color:#1D9E75;margin-left:auto">+$${Math.round(a.revenueUnlock).toLocaleString()}/mo</span>
        </div>`;
      }
      html += '</div>';
    } else if (br.hasRecommendation) {
      html += `<div style="padding:6px 20px;border-top:1px solid var(--border);font-size:10px;color:#1D9E75">&#10003; Best available processors active</div>`;
    }

    // ELEMENT 7 — DECLINE HANDLING
    html += `<div style="padding:8px 20px;border-top:1px solid var(--border)">
      <div style="font-size:10px;font-weight:600;text-transform:uppercase;color:var(--text-secondary);margin-bottom:4px">Decline handling</div>
      <div style="display:flex;align-items:center;gap:6px;padding:2px 0">
        <span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#EAF3DE;color:#3B6D11;font-weight:600">Allow</span>
        <span style="font-size:10px;color:var(--text-secondary)">Insufficient funds &middot; Pick up card SF &middot; Do Not Honor &middot; Issuer Declined</span>
      </div>
      <div style="display:flex;align-items:center;gap:6px;padding:2px 0">
        <span style="font-size:10px;padding:2px 6px;border-radius:3px;background:#FCEBEB;color:#A32D2D;font-weight:600">Block</span>
        <span style="font-size:10px;color:var(--text-secondary)">Account Closed &middot; Pick up card L &middot; Blocked first used &middot; Issuer Declined MCC</span>
      </div>
    </div>`;

    // BIN list
    const binChips = br.bins.slice(0, 5).map(b => '<span style="font-family:monospace;font-size:10px;background:var(--bg);border:1px solid var(--border);border-radius:3px;padding:1px 5px">' + b + '</span>').join(' ');
    const moreCount = br.bins.length > 5 ? ' <span style="font-size:10px;color:var(--text-secondary)">+' + (br.bins.length - 5) + ' more</span>' : '';
    html += `<div style="padding:6px 20px;border-top:1px solid var(--border);display:flex;align-items:center;gap:4px;flex-wrap:wrap">${binChips}${moreCount}</div>`;

    return html;
  },

  // Check if multiple processors are within 5pp of the top rate
  _foV2MultipleProcs(procs, activeOnly) {
    const filtered = (procs || []).filter(p => activeOnly ? p.active : !p.active).filter(p => p.app > 0);
    if (filtered.length <= 1) return false;
    const topRate = Math.max(...filtered.map(p => p.weightedRate != null ? p.weightedRate : p.rawRate));
    const within5pp = filtered.filter(p => {
      const r = p.weightedRate != null ? p.weightedRate : p.rawRate;
      return topRate - r <= 5;
    });
    return within5pp.length > 1;
  },

  _foV2ProcRows(processors) {
    let html = '';
    const filtered = (processors || []).filter(p => p.app > 0 || p.active).slice(0, 5);
    if (filtered.length === 0) return '<div style="font-size:9px;font-style:italic;color:var(--text-secondary);padding:2px 0">No processor data</div>';
    for (const p of filtered) {
      const isActive = p.active;
      const bg = isActive ? 'transparent' : 'var(--bg)';
      const dotColor = isActive ? '#1D9E75' : '#888780';
      const isBest = filtered[0] === p;
      const nameColor = isBest ? '#1D9E75' : 'var(--text)';
      const rateColor = isBest ? '#1D9E75' : 'var(--text-secondary)';
      const rate = p.weightedRate != null ? p.weightedRate : p.rawRate;
      const confLabel = p.confidence === 'HIGH' ? 'HIGH' : p.confidence === 'MEDIUM' ? 'MED' : 'LOW';
      const cBg = p.confidence === 'HIGH' ? '#EAF3DE' : p.confidence === 'MEDIUM' ? '#FAEEDA' : '#F1EFE8';
      const cCl = p.confidence === 'HIGH' ? '#3B6D11' : p.confidence === 'MEDIUM' ? '#854F0B' : '#5F5E5A';
      const lastSeen = !isActive && p.lastSeen ? ' &middot; ' + p.lastSeen.slice(5, 10) : '';
      html += `<div style="display:flex;align-items:center;gap:6px;padding:3px 0;background:${bg};border-radius:4px;${!isActive?'padding:3px 6px':''}">
        <span style="width:6px;height:6px;border-radius:50%;background:${dotColor};flex-shrink:0"></span>
        <span style="font-size:${isActive?'13':'12'}px;font-weight:500;color:${nameColor};min-width:70px">${p.name}</span>
        <span style="font-size:12px;color:${rateColor};font-weight:${isBest?'500':'400'}">${rate}%</span>
        <span style="font-size:11px;padding:2px 6px;border-radius:3px;background:${cBg};color:${cCl}">${confLabel} (${p.app}/${p.att})</span>
        ${!isActive ? '<span style="font-size:11px;padding:2px 6px;border-radius:3px;background:#F1EFE8;color:#5F5E5A">HIST'+lastSeen+'</span>' : ''}
      </div>`;
    }
    return html;
  },

  _foV2GatheringRow(label, levelTag, att, app, bins) {
    const remaining = Math.max(0, 10 - (app || 0));
    const copyId = bins && bins.length > 0 ? 'gc_' + Math.random().toString(36).slice(2, 8) : null;
    const binText = bins ? bins.join(', ') : '';
    return `<div style="display:flex;align-items:center;gap:6px;padding:4px 0;margin:2px 0">
      ${levelTag ? '<span style="font-size:11px;padding:2px 6px;border-radius:3px;background:#F1EFE8;color:#5F5E5A;font-weight:600">' + levelTag + '</span>' : ''}
      <span style="font-size:12px;color:var(--text-secondary)">${label}</span>
      <span style="font-size:11px;font-style:italic;color:var(--text-secondary)">Gathering (${app}/${att}) &mdash; need ${remaining} more approved</span>
      ${copyId ? `<button id="${copyId}" onclick="event.stopPropagation();navigator.clipboard.writeText('${binText.replace(/'/g, "\\\\'")}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="margin-left:auto;background:none;border:1px solid var(--border);border-radius:3px;color:var(--text-secondary);font-size:11px;cursor:pointer;padding:2px 8px">Copy</button>` : ''}
    </div>`;
  },

  _foV2DeepestLevel(card) {
    let deepest = 2; // card is always at L2 (bank+brand)
    for (const l3 of (card.l3Groups || [])) {
      if (l3.qualifies) deepest = Math.max(deepest, 3);
      for (const l4 of (l3.l4Groups || [])) {
        if (l4.qualifies) deepest = Math.max(deepest, 4);
        for (const l5 of (l4.l5Groups || [])) {
          if (l5.qualifies) deepest = Math.max(deepest, 5);
          if (l5.binOutliers && l5.binOutliers.length > 0) deepest = Math.max(deepest, 6);
        }
      }
    }
    return deepest;
  },

  async _renderFlowOptix(el, htmlPrefix) {
    const level = this.flowOptixLevel || 2;

    // Fetch flow-optix data
    let foData;
    if (this._flowOptixCache && this._flowOptixLevel === level) {
      foData = this._flowOptixCache;
    } else {
      const res = await fetch(`/api/analytics/${this.clientId}/flow-optix?level=${level}`);
      foData = await res.json();
      this._flowOptixCache = foData;
      this._flowOptixLevel = level;
    }

    const cards = foData.cards || [];
    const summary = foData.summary || {};

    let html = htmlPrefix;

    // Confidence filter tiles — single set for all rebill sections
    const confCounts = { HIGH: 0, MEDIUM: 0, LOW: 0 };
    for (const c of cards) {
      const conf = (c.confidence || 'LOW').toUpperCase();
      if (confCounts[conf] != null) confCounts[conf]++;
    }
    const paCardsAll = foData.processorAffinityCards || [];
    const totalAll = cards.length + paCardsAll.length;
    const confFilter = this.crmConfFilter || null;
    html += `<div style="display:flex;gap:8px;margin-bottom:16px">
      ${[
        { key: null, label: 'All', count: totalAll },
        { key: 'HIGH', label: 'HIGH', count: confCounts.HIGH, color: '#0F6E56', bg: '#ecfdf5' },
        { key: 'MEDIUM', label: 'MEDIUM', count: confCounts.MEDIUM, color: '#92400e', bg: '#fffbeb' },
        { key: 'LOW', label: 'LOW', count: confCounts.LOW, color: 'var(--text-muted)', bg: '#f3f4f6' },
      ].map(f => {
        const active = confFilter === f.key;
        const style = active
          ? 'background:var(--text);color:white'
          : f.bg ? `background:${f.bg};color:${f.color}` : 'background:var(--bg);color:var(--text);border:1px solid var(--border)';
        return `<button style="font-size:11px;font-weight:600;padding:4px 12px;border-radius:4px;border:none;cursor:pointer;${style}" onclick="Analytics.setCrmConfFilter(${f.key ? "'" + f.key + "'" : 'null'})">${f.label} ${f.count}</button>`;
      }).join('')}
    </div>`;

    // Revenue summary — only actionable cards with lift, using actual avg order value from summary
    const avgRebillVal = summary.avgOrderValue || 70;
    const rebillMonthly = Math.round(cards.reduce((s, c) => {
      const gw = c.gateway || {};
      if ((gw.liftPp || 0) <= 0) return s;
      if (c.verdict === 'REVIEW') return s;
      const att = c.totalAttempts || 0;
      const monthlyAtt = Math.round(att * 30 / 180);
      return s + monthlyAtt * (gw.liftPp || 0) / 100 * avgRebillVal;
    }, 0) * 100) / 100;
    const rebillAnnual = Math.round(rebillMonthly * 12 * 100) / 100;
    html += `<div class="card" style="padding:12px 20px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px">
      <div style="display:flex;gap:24px;align-items:center">
        <div><span style="font-size:20px;font-weight:700;color:var(--success)">${this.moneyFmt(rebillMonthly)}</span> <span style="color:var(--text-secondary);font-size:13px">/mo projected</span></div>
        <div><span style="font-size:20px;font-weight:700;color:var(--text)">${this.moneyFmt(rebillAnnual)}</span> <span style="color:var(--text-secondary);font-size:13px">/yr impact</span></div>
      </div>
      <button class="btn btn-secondary btn-sm" onclick="Analytics.exportCrmRules()">Export CSV</button>
    </div>`;

    // Processor intelligence (loaded async)
    html += '<div id="proc_intelligence"></div>';

    // Section header
    html += `<div style="margin-bottom:16px">
      <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">Rebill routing rules — ready to implement</div>
    </div>`;

    // Filter by confidence
    let filteredCards = cards;
    if (confFilter) {
      filteredCards = cards.filter(c => (c.confidence || 'LOW').toUpperCase() === confFilter);
    }

    if (filteredCards.length === 0) {
      html += '<div class="empty-state"><h3>No Rebill Routing Data</h3><p>Run analysis to generate rebill routing recommendations.</p></div>';
      el.innerHTML = html;
      return;
    }

    // Render cards in 4-column grid
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px">';
    for (const card of filteredCards) {
      html += this._flowOptixUnifiedCard(card);
    }
    html += '</div>';

    // Section 2: Processor Affinity
    const paCards = foData.processorAffinityCards || [];
    if (paCards.length > 0) {
      html += `<div style="margin-top:24px;margin-bottom:12px;padding-top:16px;border-top:1px solid var(--border)">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted)">Processor affinity — reference when adding new MIDs</div>
      </div>`;
      html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px">';
      for (const pa of paCards) {
        html += this._flowOptixAffinityCard(pa);
      }
      html += '</div>';
    }

    el.innerHTML = html;

    // Load processor intelligence (non-blocking)
    this._loadProcessorIntelligence();
  },

  async _loadProcessorIntelligence() {
    const el = document.getElementById('proc_intelligence');
    if (!el) return;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/processor-intelligence`);
      const data = await res.json();
      const acq = data.acquisitionPriority || [];
      const caps = data.capStatus || [];
      const degradation = data.degradation || [];

      let html = '';

      // Active processor health
      const activeProcs = caps.reduce((m, s) => {
        const proc = s.processor_name || 'Unknown';
        if (!m[proc]) m[proc] = { sales: 0, cap: 0, mids: 0 };
        m[proc].sales += s.sales || 0;
        m[proc].cap += s.cap || 0;
        m[proc].mids++;
        return m;
      }, {});

      if (Object.keys(activeProcs).length > 0) {
        html += `<div class="card" style="padding:16px 24px;margin-bottom:12px">
          <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-secondary);margin-bottom:10px">Active processor health</div>`;
        for (const [name, p] of Object.entries(activeProcs)) {
          const pct = p.cap > 0 ? Math.round((p.sales / p.cap) * 100) : 0;
          const barColor = pct >= 95 ? 'var(--danger)' : pct >= 80 ? '#f59e0b' : '#0F6E56';
          html += `<div style="display:flex;align-items:center;gap:12px;padding:4px 0">
            <span style="font-size:13px;font-weight:600;min-width:80px">${name}</span>
            <div style="flex:1;background:var(--border);border-radius:3px;height:8px;max-width:200px">
              <div style="background:${barColor};border-radius:3px;height:8px;width:${Math.min(100, pct)}%"></div>
            </div>
            <span style="font-size:12px;color:var(--text-secondary)">$${Math.round(p.sales).toLocaleString()}/$${Math.round(p.cap).toLocaleString()} (${pct}%)</span>
            <span style="font-size:11px;color:var(--text-muted)">${p.mids} MID${p.mids !== 1 ? 's' : ''}</span>
          </div>`;
        }
        html += `</div>`;
      }

      // Acquisition priority
      if (acq.length > 0) {
        html += `<div class="card" style="padding:16px 24px;margin-bottom:12px">
          <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-secondary);margin-bottom:10px">Processor acquisition priority</div>`;
        for (const p of acq) {
          const badge = p.data_source === 'historical'
            ? '<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#f3f4f6;color:#92400e;margin-left:4px">HISTORICAL</span>'
            : '';
          html += `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:13px">
            <span style="font-weight:600;color:var(--text-secondary);min-width:20px">#${p.rank_order}</span>
            <span style="font-weight:600">${p.processor_name}</span>${badge}
            <span style="color:#0F6E56;font-weight:600">+$${Math.round(p.revenue_unlock_monthly).toLocaleString()}/mo</span>
            <span style="color:var(--text-secondary)">${p.groups_covered} groups · ${p.avg_approval_rate.toFixed(1)}% avg</span>
          </div>`;
        }
        html += `</div>`;
      }

      // Degradation alerts
      if (degradation.length > 0) {
        html += `<div class="card" style="padding:12px 24px;margin-bottom:12px;border-left:3px solid var(--danger)">
          <div style="font-size:11px;font-weight:600;color:var(--danger);margin-bottom:6px">Degradation alerts</div>`;
        for (const a of degradation) {
          const label = a.is_issuer_level ? 'Issuer behavior change' : 'Gateway degradation';
          html += `<div style="font-size:12px;padding:2px 0">
            ${label}: ${a.issuer_bank || ''} ${a.card_brand || ''} — ${a.drop_pp}pp drop on GW${a.gateway_id}
          </div>`;
        }
        html += `</div>`;
      }

      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = '';
    }
  },

  _flowOptixUnifiedCard(card) {
    const BUCKET_MIDPOINTS = { '$0-25': 13, '$26-50': 38, '$51-75': 63, '$76-100': 88, '$100+': 125 };
    const verdictColors = { 'GATEWAY + PRICE': '#0F6E56', 'GATEWAY ONLY': '#185FA5', 'PRICE + GATEWAY': '#92400e', 'REVIEW': '#9ca3af' };
    const verdictBg = { 'GATEWAY + PRICE': '#ecfdf5', 'GATEWAY ONLY': '#dbeafe', 'PRICE + GATEWAY': '#fffbeb', 'REVIEW': '#f3f4f6' };

    const bins = card.bins || [];
    const binChips = bins.slice(0, 3).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:13px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:2px 7px">${b}</span>`
    ).join(' ') + (bins.length > 3 ? ` <span style="font-size:13px;color:var(--text-secondary)">+${bins.length - 3} more</span>` : '');

    const cardLevel = card.level || 2;
    const conf = (card.confidence || 'LOW').toUpperCase();
    const confColors = { HIGH: '#0F6E56', MEDIUM: '#92400e', LOW: 'var(--text-muted)' };
    const confBg = { HIGH: '#ecfdf5', MEDIUM: '#fffbeb', LOW: '#f3f4f6' };

    // Gateway insight banner — use new routing engine selection
    const re = card.routingEngine || {};
    const sel = re.selection || {};
    let bannerHtml, cardBorder = 'border-left:2.5px solid #0F6E56';
    if (sel.type === 'CLEAR_WINNER' && sel.variance > 5 && sel.secondary) {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E6F1FB;border-bottom:0.5px solid #B5D4F4;font-size:12px;color:#0C447C">
        <span style="font-size:14px;flex-shrink:0">&#9432;</span>
        <span>Route to <strong>${sel.primary?.name || '?'}</strong> — ${sel.primary?.rate?.toFixed(1) || 0}% vs ${sel.secondary?.name || '?'} ${sel.secondary?.rate?.toFixed(1) || 0}%</span>
        <span style="margin-left:auto;font-weight:600;color:#0F6E56;white-space:nowrap">+${(sel.variance || 0).toFixed(1)}pp</span>
      </div>`;
    } else if (sel.type === 'SPLIT_TEST') {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E6F1FB;border-bottom:0.5px solid #B5D4F4;font-size:12px;color:#0C447C">
        <span style="font-size:14px;flex-shrink:0">&#9432;</span>
        <span>Split test recommended — ${sel.primary?.name || '?'} vs ${sel.secondary?.name || '?'}</span>
        <span style="margin-left:auto;font-size:11px;color:var(--text-secondary)">${(sel.variance || 0).toFixed(1)}pp variance</span>
      </div>`;
    } else if (sel.type === 'SINGLE') {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#fffbeb;border-bottom:0.5px solid #fde68a;font-size:12px;color:#92400e">
        <span style="font-size:14px;flex-shrink:0">&#9888;</span>
        <span>Single processor — ${sel.primary?.name || '?'} only. Add second processor to A/B test.</span>
      </div>`;
    } else if (sel.type === 'GATHERING') {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#f3f4f6;border-bottom:0.5px solid var(--border);font-size:12px;color:var(--text-secondary)">
        <span style="font-size:14px;flex-shrink:0">&#9201;</span>
        <span>Gathering data — building processor comparison</span>
      </div>`;
    } else {
      bannerHtml = `<div style="display:flex;align-items:center;gap:8px;padding:8px 24px;background:#E1F5EE;border-bottom:0.5px solid #9FE1CB;font-size:12px;color:#0F6E56">
        <span style="font-size:14px;flex-shrink:0">&#10003;</span>
        <span>Optimal routing — best available MID active</span>
      </div>`;
    }

    let html = `<div class="card" style="padding:0;${cardBorder};overflow:hidden;min-width:0">`;
    html += bannerHtml;

    // Header: title + meta
    html += `<div style="padding:14px 24px 10px">
      <div style="display:flex;justify-content:space-between;align-items:baseline">
        <span style="font-size:16px;font-weight:600">${card.groupLabel || '—'}</span>
        <span style="font-size:12px;color:var(--text-secondary)">${formatNum(card.totalAttempts)} att &middot; ${bins.length} BIN${bins.length !== 1 ? 's' : ''}</span>
      </div>`;

    // L5 subtitle
    if (cardLevel === 5 && bins.length === 1) {
      html += this._l5SubtitleHtml({ appliesTo: card.appliesTo || {} });
    }

    // Level tracker
    html += `<div style="display:flex;gap:4px;margin-top:8px;align-items:center;flex-wrap:wrap">
        ${this._levelTrackerHtml(cardLevel)}
      </div>`;

    // L5 warning
    if (cardLevel === 5 && bins.length === 1) {
      html += this._l5WarningHtml({ binsInGroup: bins, groupConditions: bins[0] });
    }

    // Badges
    html += `<div style="display:flex;gap:6px;margin-top:8px;align-items:center;flex-wrap:wrap">
        <span style="font-size:12px;font-weight:600;padding:3px 9px;border-radius:3px;background:${confBg[conf]};color:${confColors[conf]}">${conf}</span>
        <span style="font-size:12px;font-weight:600;padding:3px 9px;border-radius:3px;background:${verdictBg[card.verdict]};color:${verdictColors[card.verdict]}">${card.verdict}</span>
        ${card.is_prepaid ? '<span style="font-size:12px;padding:3px 9px;border-radius:3px;background:#dbeafe;color:#1e40af">PREPAID</span>' : ''}${card.price?.isLowPrice ? ' <span style="font-size:12px;padding:3px 9px;border-radius:3px;background:#fffbeb;color:#92400e">LOW PRICE</span>' : ''}
      </div>
    </div>`;

    // ROUTING SEQUENCE — uses re/sel from banner section above

    if (sel.type === 'GATHERING') {
      // Gathering state — progress bars per processor
      const gp = sel.gatheringProcessors || [];
      if (gp.length > 0) {
        html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:12px">
          <div style="font-weight:600;color:var(--text-secondary);font-size:11px;margin-bottom:6px">Building processor data</div>`;
        for (const p of gp) {
          const pct = Math.min(100, Math.round((p.approved / 10) * 100));
          const need = Math.max(0, 10 - p.approved);
          html += `<div style="padding:3px 0">
            <div style="display:flex;align-items:center;gap:8px"><span style="font-size:12px">${p.name}</span><span style="font-size:11px;color:var(--text-secondary)">${p.approved} approved · need ${need} more</span></div>
            <div style="background:var(--border);border-radius:3px;height:5px;margin-top:3px"><div style="background:var(--accent);border-radius:3px;height:5px;width:${pct}%"></div></div>
          </div>`;
        }
        html += `</div>`;
      }
    } else {
      // Has recommendation — show routing sequence
      html += `<div style="border-top:1px solid var(--border);padding:4px 0">`;

      // Natural attempt — primary processor
      if (sel.primary) {
        const confBadge = sel.primary.confidence === 'HIGH' ? '' : sel.primary.confidence === 'MEDIUM'
          ? '<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#fffbeb;color:#92400e">MEDIUM</span>'
          : '<span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#f3f4f6;color:var(--text-muted)">LOW</span>';
        const histBadge = sel.primary.historical ? ' <span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#f3f4f6;color:#92400e">HISTORICAL</span>' : '';
        const typeBadge = sel.type === 'SPLIT_TEST' ? ' <span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#dbeafe;color:#1e40af">SPLIT TEST A</span>' : '';
        const midNote = sel.bestMid?.standout ? ` <span style="font-size:10px;color:var(--text-secondary)">(${sel.bestMid.name})</span>` : '';

        html += `<div style="padding:3px 24px;font-size:12px;display:flex;align-items:center;gap:6px">
          <span style="color:var(--text-secondary);width:70px;flex-shrink:0;font-size:11px">Natural</span>
          <strong>${sel.primary.name}</strong>${midNote} &rarr; <strong style="color:#0F6E56">${sel.primary.rate.toFixed(1)}%</strong>
          ${confBadge}${histBadge}${typeBadge}
        </div>`;
      }

      // Split test B
      if (sel.type === 'SPLIT_TEST' && sel.secondary) {
        html += `<div style="padding:3px 24px;font-size:12px;display:flex;align-items:center;gap:6px">
          <span style="color:var(--text-secondary);width:70px;flex-shrink:0;font-size:11px">Natural B</span>
          <strong>${sel.secondary.name}</strong> &rarr; <strong>${sel.secondary.rate.toFixed(1)}%</strong>
          <span style="font-size:9px;padding:1px 5px;border-radius:3px;background:#dbeafe;color:#1e40af">SPLIT TEST B</span>
        </div>`;
      }

      // Fallback — always shown if secondary exists and not split test
      if (sel.secondary && sel.type !== 'SPLIT_TEST') {
        html += `<div style="padding:3px 24px;font-size:12px;display:flex;align-items:center;gap:6px;color:var(--text-secondary)">
          <span style="width:70px;flex-shrink:0;font-size:11px">Fallback</span>
          <span>${sel.secondary.name} &rarr; ${sel.secondary.rate.toFixed(1)}%</span>
        </div>`;
      } else if (!sel.secondary && sel.type !== 'SPLIT_TEST') {
        html += `<div style="padding:8px 24px;background:#FCEBEB;border-bottom:0.5px solid #F7C1C1;font-size:12px;color:#A32D2D;display:flex;align-items:center;gap:8px">
          <span style="font-size:14px;flex-shrink:0">&#9888;</span>
          <span>No fallback processor — add a second processor MID</span>
        </div>`;
      }

      // Single processor note
      if (sel.type === 'SINGLE' && sel.missingProcessors?.length > 0) {
        html += `<div style="padding:3px 24px;font-size:11px;color:#92400e">
          Add ${sel.missingProcessors.join(' or ')} to A/B test
        </div>`;
      }

      html += `</div>`;

      // Cap alerts shown in Active Processor Health section only — not on individual cards
    }

    // Price row (kept from original)
    if (card.price) {
      const ltv = card.price.ltv || {};
      html += `<div style="padding:5px 24px;border-top:1px solid var(--border);font-size:12px;display:flex;align-items:center;gap:6px">
        <span style="font-size:10px;font-weight:600;color:var(--text-secondary);width:60px">PRICE</span>
        <strong>${card.price.recommendedBucket}</strong> — LTV <strong>$${Math.round(ltv.ltvReduced || 0)}</strong> vs $${Math.round(ltv.ltvFull || 0)} at full price
      </div>`;
    }

    // ALLOW / BLOCK — simplified (no individual chips)
    {
      const att2 = (card.salvage?.attempts || []).find(a => a.attemptNumber === 2 && !a.isStop);
      const allowList = (att2?.allowReasons || []);
      const blockList = (att2?.blockReasons || []);
      const allowText = allowList.map(r => r.reason).join(', ');
      const blockText = blockList.map(r => r.reason).join(', ');
      const aSoftId = 'rsa_' + Math.random().toString(36).slice(2, 8);
      const aHardId = 'rsh_' + Math.random().toString(36).slice(2, 8);

      html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:13px">
        <div style="display:flex;align-items:center;gap:8px">
          <span style="color:#0F6E56;font-weight:600;font-size:14px">ALLOW</span>
          <span style="font-size:13px;color:var(--text-secondary)">soft declines</span>
          <button id="${aSoftId}" onclick="navigator.clipboard.writeText('${(allowText || 'Allow: soft declines').replace(/'/g, "\\'")}');var b=document.getElementById('${aSoftId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy</button>
        </div>
      </div>`;
      html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:13px">
        <div style="display:flex;align-items:center;gap:8px">
          <span style="color:var(--danger);font-weight:600;font-size:14px">BLOCK</span>
          <span style="font-size:13px;color:var(--text-secondary)">hard declines</span>
          <button id="${aHardId}" onclick="navigator.clipboard.writeText('${(blockText || 'Block: hard declines').replace(/'/g, "\\'")}');var b=document.getElementById('${aHardId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy</button>
        </div>
      </div>`;
    }

    // Salvage: viability warnings OR attempt rows
    if (card.salvage?.notViable || card.salvage?.notWorthRetrying) {
      html += `<div style="padding:6px 16px;border-top:1px solid var(--border);font-size:11px;color:#92400e">
        Salvage not viable — focus on routing and price only.
      </div>`;
    } else if (card.salvage && !card.salvage.insufficient) {
      // Attempt rows — skip Att 1 (now in routing sequence), show Att 2+ only
      const salvageAtts = (card.salvage.attempts || []).filter(a => a.attemptNumber >= 2 || a.isStop);
      if (salvageAtts.length > 0) {
      html += `<div style="border-top:1px solid var(--border);padding:4px 0">`;
      for (const att of salvageAtts) {
        if (att.isStop) {
          html += `<div style="padding:2px 16px;font-size:11px;display:flex;gap:6px">
            <span style="color:var(--text-muted);width:36px;flex-shrink:0">Att ${att.attemptNumber}+</span>
            <span style="color:var(--danger)">STOP — ${att.stopReason || 'revenue < $3'}</span>
          </div>`;
        } else {
          const isSwitch = att.recommendation === 'switch';
          const gwName = att.processor || att.gateway || '—';
          const switchLabel = isSwitch && att.attemptNumber > 1 ? ' Switch' : '';
          html += `<div style="padding:2px 16px;font-size:11px;display:flex;align-items:center">
            <span style="color:var(--text-muted);width:36px;flex-shrink:0">Att ${att.attemptNumber}</span>
            <span style="width:120px;flex-shrink:0">${gwName}</span>
            <span style="font-family:'IBM Plex Mono',monospace;width:55px;flex-shrink:0">${att.bucket}</span>
            <strong style="width:42px;text-align:right;flex-shrink:0">${(att.approvalRate || 0).toFixed(1)}%</strong>
            <span style="color:var(--text-muted);width:50px;text-align:right;flex-shrink:0">$${Math.round(att.estRevenue || 0)}/att</span>
            ${switchLabel ? `<span style="color:#0F6E56;font-weight:600;margin-left:8px;font-size:10px">${switchLabel}</span>` : ''}
          </div>`;
          // Per-attempt allow/block removed — shown in DECLINE HANDLING section above
        }
      }
      html += `</div>`;
      } // end salvageAtts.length > 0
    } else if (card.salvage?.insufficient) {
      html += `<div style="padding:6px 16px;border-top:1px solid var(--border);font-size:11px;color:var(--text-muted)">Insufficient salvage data</div>`;
    }

    // BIN chips + Copy Profile — single row
    const copyId = 'foc_' + Math.random().toString(36).slice(2, 8);
    let rawCopyText = card.copyProfileText || '';
    // Add decline handling to copy text
    if (card.salvage?.eligibilityStats) {
      const att2Copy = (card.salvage?.attempts || []).find(a => a.attemptNumber === 2 && !a.isStop);
      if (att2Copy) {
        const al = (att2Copy.allowReasons || []).slice(0, 4);
        const bl = (att2Copy.blockReasons || []).slice(0, 4);
        if (al.length || bl.length) {
          rawCopyText += '\n\nDecline handling:';
          if (al.length) {
            rawCopyText += '\n  Allow retry:';
            for (const r of al) {
              const rl = r.reason.toLowerCase();
              let t = '';
              if (rl.includes('insufficient')) t = ' → wait 3-4 days, if still declined wait 8-10 days';
              else if (rl.includes('pick up card') && rl.includes('sf')) t = ' → wait 1-2 days';
              else t = ' → wait 1-2 days';
              rawCopyText += `\n    ${r.reason}${t}`;
            }
          }
          if (bl.length) {
            rawCopyText += '\n  Never retry:';
            for (const r of bl) rawCopyText += `\n    ${r.reason}`;
          }
        }
      }
    }
    if (card.salvage?.notViable) rawCopyText += '\n\nSalvage: Not viable — focus on attempt 1 only';
    else if (card.salvage?.notWorthRetrying) rawCopyText += '\n\nSalvage: Not viable — focus on attempt 1 only';
    const copyText = rawCopyText.replace(/'/g, "\\'").replace(/\n/g, '\\n');

    // Progress bars / outlier monitoring (from split suggestion)
    html += card.splitSuggestion ? this._renderSplitSection(card.splitSuggestion, card.groupKey, cardLevel) : '';

    html += `<div style="padding:8px 24px;border-top:1px solid var(--border);display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      ${binChips}${this._copyBinsBtnHtml(bins)}
      <button id="${copyId}" onclick="navigator.clipboard.writeText('${copyText}');var b=document.getElementById('${copyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy Profile',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);font-size:12px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy Profile</button>
    </div>`;

    html += `</div>`;
    return html;
  },

  _flowOptixAffinityCard(pa) {
    const bins = pa.bins || [];
    const binChips = bins.slice(0, 5).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:11px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px">${b}</span>`
    ).join(' ') + (bins.length > 5 ? ` <span style="font-size:11px;color:var(--text-muted)">+${bins.length - 5} more</span>` : '');

    const conf = pa.totalAttempts >= 200 ? 'HIGH' : pa.totalAttempts >= 100 ? 'MEDIUM' : 'LOW';
    const confColors = { HIGH: '#1e40af', MEDIUM: '#92400e', LOW: 'var(--text-muted)' };
    const confBg = { HIGH: '#dbeafe', MEDIUM: '#fffbeb', LOW: '#f3f4f6' };
    const profileId = 'fopa_' + Math.random().toString(36).slice(2, 8);
    let profileText = `${pa.groupLabel || '—'}\\nBest: ${pa.bestProcessor?.name || '?'} (${(pa.bestProcessor?.rate || 0).toFixed(1)}%)\\nActive: ${pa.bestActiveProcessor?.name || '?'} (${(pa.bestActiveProcessor?.rate || 0).toFixed(1)}%)\\nLift: +${(pa.liftPp || 0).toFixed(1)}pp`;
    if (bins.length > 0) profileText += `\\nBINs: ${bins.join(', ')}`;

    // Determine confidence-based level for tracker (use attempt thresholds)
    const trackerLevel = pa.totalAttempts >= 200 ? 3 : pa.totalAttempts >= 100 ? 2 : 1;

    let html = `<div class="card" style="margin-bottom:0;padding:0;border-left:3px solid #185FA5;overflow:hidden">`;

    // Header
    html += `<div style="padding:12px 16px 8px">
      <div style="display:flex;justify-content:space-between;align-items:baseline">
        <div style="display:flex;align-items:center;gap:8px">
          <span style="font-size:14px;font-weight:600">${pa.groupLabel || '—'}</span>
        </div>
        <span style="font-size:11px;color:var(--text-muted)">${formatNum(pa.totalAttempts)} att &middot; ${bins.length} BINs</span>
      </div>`;

    // Level tracker pills
    html += `<div style="display:flex;gap:4px;margin-top:6px;align-items:center;flex-wrap:wrap">
      ${this._levelTrackerHtml(trackerLevel)}
    </div>`;

    // Badges
    html += `<div style="display:flex;gap:6px;margin-top:6px;align-items:center">
      <span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px;background:${confBg[conf]};color:${confColors[conf]}">${conf}</span>
      <span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px;background:#dbeafe;color:#185FA5">PROCESSOR AFFINITY</span>
    </div>`;
    html += `</div>`;

    // Blue insight banner
    html += `<div style="padding:10px 16px;background:#eff6ff;border-top:1px solid #bfdbfe;font-size:12px;color:#1e40af">
      Adding <strong>${pa.bestProcessor?.name || '?'}</strong> MID would improve to <strong>${(pa.bestProcessor?.rate || 0).toFixed(1)}%</strong>
      <span style="margin-left:4px;font-weight:600">+${(pa.liftPp || 0).toFixed(1)}pp lift</span>
    </div>`;

    // Stats bar
    html += `<div style="display:flex;border-top:1px solid var(--border);font-size:13px;text-align:center">
      <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong style="color:#185FA5">+${(pa.liftPp || 0).toFixed(1)}pp</strong><div style="font-size:10px;color:var(--text-muted)">lift</div></div>
      <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong>${pa.bestProcessor?.name || '?'}</strong> <span style="color:#0F6E56">${(pa.bestProcessor?.rate || 0).toFixed(1)}%</span><div style="font-size:10px;color:var(--text-muted)">best processor</div></div>
      <div style="flex:1;padding:10px 0"><strong>${pa.bestActiveProcessor?.name || '?'}</strong> <span style="color:var(--danger)">${(pa.bestActiveProcessor?.rate || 0).toFixed(1)}%</span><div style="font-size:10px;color:var(--text-muted)">active best</div></div>
    </div>`;

    // Decline handling (from salvage data if available)
    if (pa.salvage && !pa.salvage.insufficient) {
      const att2 = (pa.salvage.attempts || []).find(a => a.attemptNumber === 2 && !a.isStop);
      if (att2) {
        const allowList = (att2.allowReasons || []).slice(0, 4);
        const blockList = (att2.blockReasons || []).slice(0, 4);
        html += `<div style="padding:5px 16px;border-top:1px solid var(--border);font-size:10px">
          <span style="font-size:10px;font-weight:600;color:var(--text-muted)">DECLINE HANDLING</span>`;
        if (allowList.length) {
          html += `<div style="margin-top:2px"><span style="color:#0F6E56">Allow: ${allowList.map(r => r.reason).join(' &middot; ')}${att2.allowMore > 0 ? ' +' + att2.allowMore : ''}</span></div>`;
        }
        if (blockList.length) {
          html += `<div style="margin-top:1px"><span style="color:var(--danger)">Block: ${blockList.map(r => r.reason).join(' &middot; ')}${att2.blockMore > 0 ? ' +' + att2.blockMore : ''}</span></div>`;
        }
        html += `</div>`;
      }
    }

    // Salvage recommendations (same structure as main cards)
    if (pa.salvage && !pa.salvage.insufficient && !pa.salvage.notViable && !pa.salvage.notWorthRetrying) {
      html += `<div style="border-top:1px solid var(--border);padding:4px 0">
        <div style="padding:3px 16px;font-size:10px;font-weight:600;color:var(--text-muted)">SALVAGE SEQUENCE</div>`;
      for (const att of (pa.salvage.attempts || [])) {
        if (att.isStop) {
          html += `<div style="padding:2px 16px;font-size:11px;display:flex;gap:6px">
            <span style="color:var(--text-muted);width:36px;flex-shrink:0">Att ${att.attemptNumber}+</span>
            <span style="color:var(--danger)">STOP — ${att.stopReason || 'revenue < $3'}</span>
          </div>`;
        } else {
          const isSwitch = att.recommendation === 'switch';
          const gwName = att.processor || att.gateway || '—';
          const switchLabel = isSwitch && att.attemptNumber > 1 ? ' Switch' : '';
          html += `<div style="padding:2px 16px;font-size:11px;display:flex;align-items:center">
            <span style="color:var(--text-muted);width:36px;flex-shrink:0">Att ${att.attemptNumber}</span>
            <span style="width:120px;flex-shrink:0">${gwName}</span>
            <span style="font-family:'IBM Plex Mono',monospace;width:55px;flex-shrink:0">${att.bucket || ''}</span>
            <strong style="width:42px;text-align:right;flex-shrink:0">${(att.approvalRate || 0).toFixed(1)}%</strong>
            <span style="color:var(--text-muted);width:50px;text-align:right;flex-shrink:0">$${Math.round(att.estRevenue || 0)}/att</span>
            ${switchLabel ? `<span style="color:#0F6E56;font-weight:600;margin-left:8px;font-size:10px">${switchLabel}</span>` : ''}
          </div>`;
        }
      }
      html += `</div>`;
    } else if (pa.salvage?.notViable || pa.salvage?.notWorthRetrying) {
      html += `<div style="padding:6px 16px;border-top:1px solid var(--border);font-size:11px;color:#92400e">
        Salvage not viable — focus on routing and price only.
      </div>`;
    }

    // Price recommendation if LTV justified
    if (pa.price) {
      const ltv = pa.price.ltv || {};
      html += `<div style="padding:5px 16px;border-top:1px solid var(--border);font-size:12px;display:flex;align-items:center;gap:6px">
        <span style="font-size:10px;font-weight:600;color:var(--text-muted);width:42px">PRICE</span>
        <strong>${pa.price.recommendedBucket}</strong> — LTV <strong>$${Math.round(ltv.ltvReduced || 0)}</strong> vs $${Math.round(ltv.ltvFull || 0)} at full price
      </div>`;
    }

    // BIN chips + Copy BINs + Copy Profile
    html += `<div style="padding:6px 16px;border-top:1px solid var(--border);display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      ${binChips}${this._copyBinsBtnHtml(bins)}
      <button id="${profileId}" onclick="navigator.clipboard.writeText('${profileText}');var b=document.getElementById('${profileId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy Profile',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-muted);font-size:10px;cursor:pointer;padding:2px 8px;margin-left:auto">Copy Profile</button>
    </div>`;

    // Apply when note
    html += `<div style="padding:8px 16px;font-size:11px;font-style:italic;color:#185FA5;border-top:1px solid var(--border)">
      Apply when: Adding new ${pa.bestProcessor?.name || ''} MIDs &rarr; route these BINs there immediately
    </div>`;

    html += `</div>`;
    return html;
  },

  switchFlowOptixLevel(level) {
    this.flowOptixLevel = level;
    this._flowOptixCache = null;
    if (this._crmRules) {
      this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
    }
  },

  setCrmTab(tab) {
    this.crmTab = tab;
    this.crmStageFilter = null;
    this.crmLevelFilter = null;
    this.crmConfFilter = null;
    if (this._crmRules) {
      this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
    } else {
      this.renderCrmRules();
    }
  },

  setCrmConfFilter(conf) {
    this.crmConfFilter = conf;
    if (this._crmRules) {
      this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
    }
  },

  setCrmStageFilter(stage) {
    this.crmStageFilter = stage;
    if (this._crmRules) {
      this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
    }
  },

  setCrmLevelFilter(level) {
    this.crmLevelFilter = level;
    if (this._crmRules) {
      this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
    }
  },

  async activateRule(ruleId) {
    const rule = (this._crmRules || []).find(r => r.ruleId === ruleId);
    if (!rule) return;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/beast-rules/${ruleId}/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(rule),
      });
      const data = await res.json();
      if (data.success) {
        rule.status = 'active';
        rule.stage = rule.targetType === 'mid' ? 4 : 2;
        this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
      } else {
        alert('Error: ' + (data.error || 'Unknown'));
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  async dismissRule(ruleId) {
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/beast-rules/${ruleId}/dismiss`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        const rule = (this._crmRules || []).find(r => r.ruleId === ruleId);
        if (rule) rule.status = 'dismissed';
        this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  _dismissedRuleCard(r) {
    const imp = r.expectedImpact || {};
    const bins = r.binsInGroup || [];
    const showBins = bins.length > 0 && r.groupType === 'bin';
    const binChips = showBins ? bins.slice(0, 3).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:11px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px">${b}</span>`
    ).join(' ') + (bins.length > 3 ? ` <span style="font-size:11px;color:var(--text-muted)">+${bins.length - 3} more</span>` : '') : '';

    return `<div class="card" style="padding:0;border-left:2.5px solid #9ca3af;overflow:hidden;opacity:0.7">
      <div style="padding:14px 16px 10px">
        <div style="display:flex;justify-content:space-between;align-items:baseline">
          <div style="font-size:14px;font-weight:600;color:var(--text-muted)">${r.ruleName}</div>
          <span style="font-size:11px;color:var(--text-muted)">${r.ruleId}</span>
        </div>
        <div style="display:flex;gap:6px;margin-top:6px;align-items:center">
          <span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:3px;background:#f3f4f6;color:#9ca3af">DISMISSED</span>
          <span style="font-size:11px;color:var(--text-muted)">Level ${r.level || '?'} &middot; ${r.levelLabel || ''}</span>
        </div>
      </div>
      <div style="display:flex;border-top:1px solid var(--border);font-size:13px;text-align:center">
        <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong style="color:var(--text-muted)">+${(imp.lift_pp || 0).toFixed(1)}pp</strong><div style="font-size:10px;color:var(--text-muted)">lift</div></div>
        <div style="flex:1;padding:10px 0;border-right:1px solid var(--border)"><strong>${this.moneyFmt(imp.monthly_revenue_impact || 0)}</strong><div style="font-size:10px;color:var(--text-muted)">/mo projected</div></div>
        <div style="flex:1;padding:10px 0"><strong>${formatNum(imp.monthly_attempts || 0)}</strong><div style="font-size:10px;color:var(--text-muted)">orders/mo</div></div>
      </div>
      ${showBins ? `<div style="padding:6px 16px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;border-top:1px solid var(--border)">
        <span style="font-size:11px;color:var(--text-muted)">${bins.length} BIN${bins.length > 1 ? 's' : ''}:</span>
        ${binChips}${this._copyBinsBtnHtml(bins)}
      </div>` : ''}
      <div style="padding:8px 16px;display:flex;gap:8px;align-items:center;border-top:1px solid var(--border)">
        <button class="btn btn-sm btn-primary" onclick="Analytics.restoreRule('${r.ruleId}')">Restore</button>
      </div>
    </div>`;
  },

  async restoreRule(ruleId) {
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/beast-rules/${ruleId}/restore`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        const rule = (this._crmRules || []).find(r => r.ruleId === ruleId);
        if (rule) { rule.status = 'recommended'; rule.stage = 1; }
        this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  async restoreAllDismissed() {
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/beast-rules/restore-all`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        for (const rule of (this._crmRules || [])) {
          if (rule.status === 'dismissed') { rule.status = 'recommended'; rule.stage = 1; }
        }
        this.renderCrmRules({ rules: this._crmRules, summary: this._crmSummary || {}, processorRules: this._crmProcessorRules || [] });
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  // --- L5 Ancestry ---
  _toSentenceCase(s) {
    if (!s) return s;
    return s.toLowerCase().replace(/(?:^|\s|[-/])\w/g, m => m.toUpperCase());
  },

  _l5SubtitleHtml(r) {
    const a = r.appliesTo || {};
    const bank = this._toSentenceCase(a.issuer_bank || 'Unknown');
    const brand = this._toSentenceCase(a.card_brand || '—');
    const type = this._toSentenceCase(a.card_type || '—');
    const level = this._toSentenceCase(a.card_level || '—');
    return `<div style="margin-top:4px;font-size:12px;color:var(--text-secondary)">${bank} &middot; ${brand} &middot; ${type} &middot; ${level}</div>`;
  },

  _l5WarningHtml(r) {
    const bin = (r.binsInGroup || [])[0] || r.groupConditions || '?';
    return `<div style="margin-top:8px;font-size:11px;color:#92400e;background:#fffbeb;padding:6px 8px;border-radius:4px">Remove BIN ${bin} from routing rules before activating this rule</div>`;
  },

  // --- Split / Gathering / Outlier Section (level-aware) ---
  _renderSplitSection(s, ruleId, level) {
    if (!s) return '';
    // L5: no split section
    if (level >= 5) return '';

    // L4: outlier monitoring (not progress bars)
    if (level === 4) {
      const outliers = s.outliers || [];
      const totalBins = s.totalBinsMonitored || 0;
      let html = `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:12px">
        <div style="font-weight:600;color:var(--text-secondary);font-size:11px;margin-bottom:6px">Monitoring ${totalBins} BINs for outliers</div>`;
      if (outliers.length > 0) {
        for (const o of outliers) {
          const dir = o.deviation > 0 ? '+' : '';
          const color = o.deviation > 0 ? '#0F6E56' : 'var(--danger)';
          html += `<div style="display:flex;align-items:center;gap:8px;padding:2px 0">
            <span style="font-family:'IBM Plex Mono',monospace;font-size:12px">${o.bin}</span>
            <span style="font-size:12px;color:${color};font-weight:600">${dir}${o.deviation.toFixed(1)}pp</span>
            <span style="font-size:11px;color:var(--text-secondary)">${o.rate.toFixed(1)}% · ${o.attempts} att</span>
          </div>`;
        }
      } else {
        html += `<div style="font-size:12px;color:var(--text-secondary)">No outliers detected</div>`;
      }
      html += `</div>`;
      return html;
    }

    // L1-L3: split suggestion (if ready) then gathering progress for remaining
    let html = '';
    if (s.splitReady) {
      html += this._renderSplitSuggestion(s, ruleId);
    }

    // Gathering/progress section for all sub-groups
    const sgs = s.subGroupStatus || [];
    if (sgs.length === 0) return html;

    const levelLabels = { 1: 'Brand', 2: 'Type', 3: 'Level' };
    const splitLabel = levelLabels[level] || 'sub-group';
    html += `<div style="padding:8px 24px;border-top:1px solid var(--border);font-size:12px">
      <div style="font-weight:600;color:var(--text-secondary);font-size:11px;margin-bottom:8px">Watching for ${splitLabel} split</div>`;

    for (const sg of sgs) {
      if (sg.alreadySplit) {
        html += `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;color:var(--text-secondary)">
          <span style="font-size:12px">${sg.label}</span>
          <span style="font-size:12px;color:#0F6E56">&#10003; split</span>
        </div>`;
      } else if (sg.gathering) {
        const pct = Math.round(sg.progress * 100);
        html += `<div style="padding:3px 0">
          <div style="display:flex;align-items:center;gap:8px">
            <span style="font-size:12px">${sg.label}</span>
            <span style="font-size:11px;color:var(--text-secondary)">${sg.attempts}/30 att</span>
          </div>
          <div style="background:var(--border);border-radius:3px;height:5px;margin-top:3px">
            <div style="background:var(--accent);border-radius:3px;height:5px;width:${pct}%"></div>
          </div>
        </div>`;
      } else {
        html += `<div style="display:flex;align-items:center;gap:8px;padding:3px 0">
          <span style="font-size:12px">${sg.label}</span>
          <span style="font-size:12px;color:#0F6E56">&#10003; ready</span>
          <span style="font-size:11px;color:var(--text-secondary)">${sg.rate.toFixed(1)}% · ${sg.attempts} att</span>
        </div>`;
      }
    }
    html += `</div>`;
    return html;
  },

  // --- Split Suggestions (rendered from cached analytics data, never lazy-loaded) ---

  _renderSplitSuggestion(s, ruleId) {
    if (!s || !s.splitSubGroup) return '';

    const sg = s.splitSubGroup;
    const confColors = { HIGH: '#0F6E56', MEDIUM: '#92400e', LOW: '#b45309' };
    const confBg = { HIGH: '#ecfdf5', MEDIUM: '#fffbeb', LOW: '#fff7ed' };
    const sgBinChips = sg.bins.slice(0, 3).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:10px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 5px">${b}</span>`
    ).join(' ') + (sg.bins.length > 3 ? ` <span style="font-size:10px;color:var(--text-muted)">+${sg.bins.length - 3} more</span>` : '');
    const remBins = s.remainingBins || [];
    const remChips = remBins.slice(0, 3).map(b =>
      `<span style="font-family:'IBM Plex Mono',monospace;font-size:10px;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:1px 5px">${b}</span>`
    ).join(' ') + (remBins.length > 3 ? ` <span style="font-size:10px;color:var(--text-muted)">+${remBins.length - 3} more</span>` : '');

    const sgCopyId = 'sgcp_' + Math.random().toString(36).slice(2, 8);
    const remCopyId = 'rmcp_' + Math.random().toString(36).slice(2, 8);
    const sgBinText = sg.bins.join(', ');
    const remBinText = remBins.join(', ');

    let html = '';

    // Green banner
    html += `<div style="padding:8px 16px;background:#ecfdf5;border-top:2px solid #0F6E56;font-size:12px;color:#0F6E56;display:flex;align-items:center;gap:8px">
      <span style="font-size:14px">&#9998;</span>
      <span><strong>Ready to split</strong> — ${sg.label} behaves differently</span>
    </div>`;

    // Split preview
    html += `<div style="padding:10px 16px;border-top:1px solid var(--border);font-size:11px">
      <div style="font-weight:600;font-size:10px;text-transform:uppercase;color:var(--text-muted);margin-bottom:8px">Split recommended</div>`;

    // Sub-group being split out
    const parentRule = (this._ruleDataMap || {})[ruleId];
    const bestGw = parentRule?.midProgress?.[0] || parentRule?.gateway?.bestGateway || {};
    const bestGwName = bestGw.gateway_name || bestGw.processor || '';
    const bestGwRate = bestGw.rate || 0;

    html += `<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;padding:8px 10px;margin-bottom:8px">
      <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
        <strong style="font-size:11px">${sg.label}</strong>
        <span style="font-size:10px;color:#0F6E56">${sg.rate.toFixed(1)}%</span>
        <span style="font-size:10px;color:var(--text-muted)">${sg.attempts} att</span>
        <span style="font-size:10px;color:${s.variance > 0 ? '#0F6E56' : 'var(--danger)'}">+${s.variance.toFixed(1)}pp vs siblings</span>
        <span style="font-size:9px;font-weight:600;padding:1px 5px;border-radius:3px;background:${confBg[sg.confidence]};color:${confColors[sg.confidence]}">${sg.confidence}</span>
      </div>
      ${bestGwName ? `<div style="font-size:10px;color:var(--text-secondary);margin-top:3px">Route to: <strong>${bestGwName}</strong>${bestGwRate ? ' &rarr; ' + bestGwRate.toFixed(1) + '%' : ''}</div>` : ''}
      <div style="display:flex;align-items:center;gap:4px;margin-top:4px;flex-wrap:wrap">
        ${sgBinChips}
        <button id="${sgCopyId}" onclick="navigator.clipboard.writeText('${sgBinText.replace(/'/g, "\\'")}');var b=document.getElementById('${sgCopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy BINs',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-muted);font-size:9px;cursor:pointer;padding:1px 5px;margin-left:auto">Copy BINs</button>
      </div>
    </div>`;

    // Remaining group — hide rate if under 30 attempts
    const remAtt = s.remainingAttempts || 0;
    const remRateHtml = remAtt >= 30
      ? `<span style="font-size:10px;color:var(--text-muted)">${s.remainingRate.toFixed(1)}%</span><span style="font-size:10px;color:var(--text-muted)">${remAtt} att</span>`
      : `<span style="font-size:10px;color:var(--text-muted)">${remAtt} att — gathering data</span>`;
    html += `<div style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px 10px;margin-bottom:8px">
      <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
        <strong style="font-size:11px">Remaining</strong>
        ${remRateHtml}
      </div>
      <div style="display:flex;align-items:center;gap:4px;margin-top:4px;flex-wrap:wrap">
        ${remChips}
        <button id="${remCopyId}" onclick="navigator.clipboard.writeText('${remBinText.replace(/'/g, "\\'")}');var b=document.getElementById('${remCopyId}');b.textContent='Copied!';setTimeout(()=>b.textContent='Copy BINs',1500)" style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--text-muted);font-size:9px;cursor:pointer;padding:1px 5px;margin-left:auto">Copy BINs</button>
      </div>
    </div>`;

    // Variance note
    html += `<div style="font-size:10px;color:var(--text-muted);margin-bottom:8px">${s.variance.toFixed(1)}pp variance — splitting improves routing precision for both groups</div>`;

    // Low confidence warning
    if (sg.confidence === 'LOW') {
      html += `<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:6px;padding:6px 10px;margin-bottom:8px;font-size:10px;color:#92400e">
        Only ${sg.attempts} attempts — low confidence. Consider waiting for 75+ before splitting. Split anyway?
      </div>`;
    } else if (sg.confidence === 'MEDIUM') {
      html += `<div style="font-size:10px;color:#92400e;margin-bottom:8px">Medium confidence — ${sg.attempts} attempts.</div>`;
    }

    // Confirm button
    html += `<button class="btn btn-sm" style="width:100%;background:#0F6E56;color:white;border:none;padding:8px;border-radius:6px;cursor:pointer;font-weight:600" onclick="Analytics.confirmSplitAction('${ruleId}')">Confirm split</button>`;
    html += `<div style="font-size:9px;color:var(--text-muted);margin-top:4px;text-align:center">Copy BINs above, create groups in your routing rules, then confirm here. This cannot be auto-undone — use Merge back if needed.</div>`;
    html += `</div>`;

    // Gathering state
    if (s.subGroupStatus && s.subGroupStatus.length > 0) {
      const gathId = 'gath_' + Math.random().toString(36).slice(2, 8);
      html += `<div style="padding:6px 16px;border-top:1px solid var(--border);font-size:10px">
        <div style="cursor:pointer;color:var(--text-muted)" onclick="var e=document.getElementById('${gathId}');e.style.display=e.style.display==='none'?'':'none'">Sub-group progress &#9662;</div>
        <div id="${gathId}" style="display:none;margin-top:6px">`;
      for (const sg2 of s.subGroupStatus) {
        if (sg2.alreadySplit) {
          html += `<div style="display:flex;align-items:center;gap:6px;padding:2px 0;color:var(--text-muted)"><span>${sg2.label}</span><span style="color:#0F6E56">&#10003; split</span></div>`;
        } else if (sg2.gathering) {
          const pct = Math.round(sg2.progress * 100);
          html += `<div style="padding:2px 0"><div style="display:flex;align-items:center;gap:6px"><span>${sg2.label}</span><span>${sg2.attempts}/30 att</span></div>
            <div style="background:var(--border);border-radius:3px;height:4px;margin-top:2px"><div style="background:var(--accent);border-radius:3px;height:4px;width:${pct}%"></div></div></div>`;
        } else {
          html += `<div style="display:flex;align-items:center;gap:6px;padding:2px 0"><span>${sg2.label}</span><span>${sg2.rate.toFixed(1)}%</span><span>${sg2.attempts} att</span></div>`;
        }
      }
      html += `</div></div>`;
    }

    return html;
  },

  async loadSplitHistory(ruleId) {
    const el = document.getElementById('hist_' + ruleId.replace(/[^a-zA-Z0-9]/g, '_'));
    if (!el) return;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/split/history/${ruleId}`);
      const data = await res.json();
      const history = data.history || [];
      if (history.length === 0) return;

      const histId = 'hexp_' + Math.random().toString(36).slice(2, 8);
      let html = `<div style="padding:4px 16px;border-top:1px solid var(--border);font-size:10px">
        <div style="cursor:pointer;color:var(--text-muted);display:flex;align-items:center;gap:6px" onclick="var e=document.getElementById('${histId}');e.style.display=e.style.display==='none'?'':'none';this.querySelector('span:last-child').textContent=e.style.display==='none'?'Show':'Hide'">
          <span>Split history</span><span>Show</span>
        </div>
        <div id="${histId}" style="display:none;margin-top:6px">`;

      for (const h of history) {
        const date = h.splitAt ? new Date(h.splitAt).toLocaleDateString() : '';
        if (h.type === 'merge') {
          const mergeDate = h.mergedAt ? new Date(h.mergedAt).toLocaleDateString() : date;
          html += `<div style="padding:2px 0;color:var(--text-muted)">${mergeDate} Merged ${h.childName} back in${h.mergedReason ? ' — ' + h.mergedReason : ''}</div>`;
        } else {
          html += `<div style="display:flex;align-items:center;gap:6px;padding:2px 0">
            <span style="color:var(--text-muted)">${date}</span>
            <span>Split <strong>${h.childName}</strong> out</span>
            <span style="color:var(--text-muted)">${h.variance ? h.variance.toFixed(1) + 'pp' : ''} at ${h.attempts || '?'} att</span>
            <span style="font-size:9px;color:var(--text-muted)">${h.confidence || ''}</span>
            ${h.isActive ? `<a href="#" onclick="event.preventDefault();Analytics.mergeBackPrompt('${h.childRuleId}')" style="font-size:9px;color:var(--text-muted);margin-left:auto">Merge back &rarr;</a>` : ''}
          </div>`;
        }
      }

      html += '</div></div>';
      el.innerHTML = html;
    } catch (err) { /* silent */ }
  },

  async confirmSplitAction(ruleId) {
    const s = this._splitCache[ruleId];
    if (!s) { alert('No split data loaded.'); return; }
    const sg = s.splitSubGroup;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/split/confirm`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ruleId,
          ruleType: 'beast',
          splitData: {
            subGroupKey: sg.key,
            subGroupLabel: sg.label,
            bins: sg.bins,
            variance: s.variance,
            attempts: sg.attempts,
            siblingRate: s.siblingRate,
            txGroup: this.crmTab || 'INITIALS',
            level: s.level,
          },
        }),
      });
      const data = await res.json();
      if (data.success) {
        delete this._splitCache[ruleId];
        alert('Split confirmed. New card: ' + data.childRuleId + '. Update your routing rules accordingly.');
        this.renderCrmRules();
      } else {
        alert('Error: ' + (data.error || 'Unknown'));
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  async _loadConvergenceReview() {
    const el = document.getElementById('convergence_review');
    if (!el) return;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/split/convergence`);
      const data = await res.json();
      const converged = data.converged || [];
      if (converged.length === 0) { el.innerHTML = ''; return; }

      let html = `<div style="margin-top:24px;padding-top:16px;border-top:1px solid var(--border)">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
          <span style="font-size:13px;color:#92400e;font-weight:600">Review merges</span>
          <span style="font-size:12px;color:var(--text-muted)">— ${converged.length} split${converged.length > 1 ? 's' : ''} may no longer be needed</span>
        </div>`;

      for (const c of converged) {
        html += `<div class="card" style="padding:10px 16px;margin-bottom:8px;border-left:2.5px solid #f59e0b">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <strong style="font-size:12px">${c.childName}</strong>
            <span style="font-size:11px;color:var(--text-muted)">&larr; ${c.parentName}</span>
          </div>
          <div style="font-size:11px;color:var(--text-muted);margin-top:4px">
            Was ${c.originalVariance.toFixed(1)}pp at split &middot; Now ${c.currentVariance.toFixed(1)}pp &middot; Consider merging back
          </div>
          <button class="btn btn-sm btn-secondary" style="margin-top:6px" onclick="Analytics.mergeBackPrompt('${c.childRuleId}')">Merge back</button>
        </div>`;
      }
      html += '</div>';
      el.innerHTML = html;
    } catch (err) {
      el.innerHTML = '';
    }
  },

  async mergeBackPrompt(childRuleId) {
    const rule = (this._crmRules || []).find(r => r.ruleId === childRuleId);
    const parentName = rule?._parentName || 'parent group';
    const bins = rule?.binsInGroup || [];
    if (!confirm(`This will merge ${bins.length} BINs back into ${parentName}.\n\nBefore confirming:\n1. Update your routing rules to remove the split group rule\n2. Add these BINs back to the parent group in your routing rules\n\nProceed?`)) return;
    try {
      const res = await fetch(`/api/analytics/${this.clientId}/split/merge-back`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ childRuleId, ruleType: 'beast' }),
      });
      const data = await res.json();
      if (data.success) {
        alert('Merge complete. Reanalysis pending on next recompute.');
        this.renderCrmRules();
      } else {
        alert('Error: ' + (data.error || 'Unknown'));
      }
    } catch (err) { alert('Error: ' + err.message); }
  },

  exportCrmRules() {
    const rules = this._crmRules || [];
    if (rules.length === 0) { alert('No rules to export.'); return; }
    const rows = rules.map(r => ({
      rule_id: r.ruleId || '', rule_name: r.ruleName || '', tx_group: r.txGroup || '',
      group_type: r.groupType || '', group_conditions: r.groupConditions || '',
      target_type: r.targetType || '', target: r.targetValue || '',
      confidence: r.confidence || '', sample_size: r.sampleSize || '',
      lift_pp: r.expectedImpact?.lift_pp || '', monthly_revenue: r.expectedImpact?.monthly_revenue_impact || '',
      beast_cycle: r.beastConfig?.cycle || '', beast_target: r.beastConfig?.target || '',
    }));
    this.csvExport(rows, `beast-rules-${this.clientId}.csv`);
  },
};
