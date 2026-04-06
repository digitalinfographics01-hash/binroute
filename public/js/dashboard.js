/**
 * BinRoute — Dashboard Screen
 */
const Dashboard = {
  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading dashboard...</div>';

    try {
      const [res, obRes] = await Promise.all([
        fetch(`/api/dashboard/${clientId}`),
        fetch(`/api/config/clients/${clientId}/onboarding-status`),
      ]);
      const data = await res.json();
      const onboarding = await obRes.json();

      App.updateConfigBadge(data.incompleteGateways);
      App.updateSyncTime(data.syncState);

      let html = '';
      if (!onboarding.analyticsReady) {
        html += this.buildOnboardingChecklist(onboarding, clientId);
      }
      html += this.buildHtml(data, clientId);
      main.innerHTML = html;
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Failed to load dashboard</h3><p>${err.message}</p></div>`;
    }
  },

  buildOnboardingChecklist(ob, clientId) {
    const navMap = {
      gateways_synced: 'config',
      products_synced: 'products',
      products_grouped: 'products',
      sequence_tagged: 'products',
      campaigns_tagged: 'config',
      mid_configured: 'config',
    };

    let stepsHtml = ob.steps.map(s => {
      const icon = s.done
        ? '<span style="color:var(--success);font-size:16px">&#10003;</span>'
        : '<span style="color:var(--text-muted);font-size:16px">&#9675;</span>';
      const textStyle = s.done ? 'color:var(--text-secondary)' : 'font-weight:500';
      const detail = s.detail ? `<span style="color:var(--text-muted);font-size:12px;margin-left:8px">${s.detail}</span>` : '';
      const nav = !s.done && navMap[s.key]
        ? `<a href="#" onclick="App.navigate('${navMap[s.key]}');return false" style="font-size:12px;margin-left:8px;color:var(--accent)">Go &rarr;</a>`
        : '';
      return `<div style="display:flex;align-items:center;gap:10px;padding:6px 0;${textStyle}">
        ${icon} <span>${s.label}</span>${detail}${nav}
      </div>`;
    }).join('');

    return `
      <div class="card" style="border-left:4px solid var(--warning);margin-bottom:20px">
        <div class="card-header">
          <span class="card-title">Onboarding Checklist</span>
          <span style="font-size:13px;color:var(--text-secondary)">${ob.completedCount}/${ob.totalSteps} complete (${ob.pct}%)</span>
        </div>
        <div style="margin-bottom:12px">
          <div class="progress-bar"><div class="progress-bar-fill" style="width:${ob.pct}%;background:${ob.pct === 100 ? 'var(--success)' : 'var(--warning)'}"></div></div>
        </div>
        ${stepsHtml}
        <div style="margin-top:12px;font-size:12px;color:var(--text-muted)">
          Complete all steps before running analytics for accurate results.
        </div>
      </div>`;
  },

  buildHtml(data, clientId) {
    const k = data.kpis;
    const p0Alerts = data.alerts.filter(a => a.priority === 'P0');
    const hasP0 = p0Alerts.length > 0;
    const hasIncomplete = data.incompleteGateways > 0;

    let html = `
      <div class="main-header">
        <h2>Dashboard</h2>
        <div style="display:flex;gap:8px">
          <button class="btn btn-secondary" onclick="Dashboard.triggerSync(${clientId})">
            &#8635; Sync Data
          </button>
          <button class="btn btn-primary" onclick="Dashboard.triggerAnalysis(${clientId})">
            &#9881; Run Analysis
          </button>
        </div>
      </div>`;

    // P0 Banner
    if (hasP0) {
      html += `<div class="banner banner-p0">
        <span class="banner-icon">&#9888;</span>
        <div><strong>MID Closure Detected</strong> — ${p0Alerts.length} MID(s) closed. Immediate BIN rerouting required.
          <a href="#" onclick="App.navigate('lifecycle');return false" style="color:inherit;text-decoration:underline;margin-left:8px">View Details</a>
        </div>
      </div>`;
    }

    // Config Banner
    if (hasIncomplete) {
      html += `<div class="banner banner-config">
        <span class="banner-icon">&#9881;</span>
        <div><strong>${data.incompleteGateways} MID(s) need configuration</strong> — Missing processor name, bank name, or MCC code.
          <a href="#" onclick="App.navigate('config');return false" style="color:inherit;text-decoration:underline;margin-left:8px">Configure Now</a>
        </div>
      </div>`;
    }

    // KPI Row
    html += `<div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-label">Approval Rate (90d)</div>
        <div class="kpi-value ${k.approval_rate >= 70 ? 'success' : k.approval_rate >= 50 ? 'warning' : 'danger'}">${k.approval_rate ? k.approval_rate.toFixed(1) + '%' : '—'}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Open Recommendations</div>
        <div class="kpi-value accent">${formatNum(k.open_recommendations)}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Active MIDs</div>
        <div class="kpi-value">${formatNum(k.active_mids)}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Unrouted BINs</div>
        <div class="kpi-value ${k.unrouted_bins > 0 ? 'warning' : ''}">${formatNum(k.unrouted_bins)}</div>
      </div>
    </div>`;

    // Two-column layout: MID performance + Alerts
    html += '<div class="grid-2">';

    // MID Performance Table
    html += `<div class="card">
      <div class="card-header">
        <span class="card-title">MID Performance (90 days)</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>MID</th><th>Gateway Name</th><th>Status</th><th>Transactions</th><th>Approval Rate</th>
          </tr></thead>
          <tbody>`;

    if (data.midPerformance.length === 0) {
      html += '<tr><td colspan="5" style="text-align:center;color:var(--text-muted);padding:24px">No MID data yet. Sync data to populate.</td></tr>';
    }

    for (const mid of data.midPerformance) {
      html += `<tr>
        <td><strong>${mid.gateway_id}</strong></td>
        <td>${gwDisplayName(mid)}</td>
        <td>${pillHtml(mid.lifecycle_state)}</td>
        <td>${formatNum(mid.total_orders)}</td>
        <td>${rateBarHtml(mid.approval_rate)}</td>
      </tr>`;
    }

    html += '</tbody></table></div></div>';

    // Alert Panel
    html += `<div class="card">
      <div class="card-header">
        <span class="card-title">Alerts</span>
        <span class="card-subtitle">${data.alerts.length} active</span>
      </div>`;

    if (data.alerts.length === 0) {
      html += '<div style="text-align:center;padding:24px;color:var(--text-muted)">No active alerts</div>';
    }

    for (const alert of data.alerts) {
      html += `<div class="alert-item">
        ${priorityHtml(alert.priority)}
        <div class="alert-content">
          <div class="alert-title">${alert.title}</div>
          <div class="alert-desc">${alert.description || ''}</div>
          <div class="alert-time">${timeAgo(alert.created_at)}</div>
        </div>
        <button class="btn btn-sm btn-secondary" onclick="Dashboard.resolveAlert(${alert.id}, ${clientId})">Resolve</button>
      </div>`;
    }

    html += '</div></div>'; // end grid-2

    // Top Optimization Windows
    html += `<div class="card">
      <div class="card-header">
        <span class="card-title">Top Optimization Windows</span>
        <span class="card-subtitle">Ranked by volume x lift</span>
      </div>`;

    if (data.topWindows.length === 0) {
      html += '<div style="text-align:center;padding:24px;color:var(--text-muted)">No optimization windows detected. Run analysis to check.</div>';
    } else {
      html += `<div class="table-wrap"><table>
        <thead><tr><th>BIN</th><th>Current MID</th><th>Recommended MID</th><th>Lift</th><th>Confidence</th><th>Volume</th><th>Action</th></tr></thead>
        <tbody>`;

      for (const w of data.topWindows) {
        html += `<tr>
          <td><strong>${w.bin}</strong></td>
          <td>${w.current_gateway_name || 'GW ' + w.current_gateway_id}</td>
          <td>${w.recommended_gateway_name || 'GW ' + w.recommended_gateway_id}</td>
          <td style="color:var(--success);font-weight:600">+${w.expected_lift?.toFixed(1) || '?'}pp</td>
          <td>${(w.confidence_score * 100).toFixed(0)}%</td>
          <td>${formatNum(w.transaction_volume)}</td>
          <td><button class="btn btn-sm btn-primary" onclick="App.navigate('recommendations')">View</button></td>
        </tr>`;
      }

      html += '</tbody></table></div>';
    }

    html += '</div>';

    // Implementation Tracker + BIN Tier Summary
    html += '<div class="grid-2">';

    // Implementation Tracker
    html += `<div class="card">
      <div class="card-header">
        <span class="card-title">Implementation Tracker</span>
      </div>`;

    if (data.implementations.length === 0) {
      html += '<div style="text-align:center;padding:24px;color:var(--text-muted)">No implementations tracked yet.</div>';
    } else {
      html += '<div class="table-wrap"><table><thead><tr><th>BIN</th><th>Change</th><th>Status</th><th>Marked</th></tr></thead><tbody>';
      for (const impl of data.implementations) {
        html += `<tr>
          <td><strong>${impl.bin}</strong></td>
          <td>${impl.current_gateway_name || 'GW ' + impl.current_gateway_id} &rarr; ${impl.recommended_gateway_name || 'GW ' + impl.recommended_gateway_id}</td>
          <td>${pillHtml(impl.result)}</td>
          <td>${timeAgo(impl.marked_at)}</td>
        </tr>`;
      }
      html += '</tbody></table></div>';
    }

    html += '</div>';

    // BIN Tier Summary
    html += `<div class="card">
      <div class="card-header">
        <span class="card-title">BIN Tier Summary</span>
      </div>`;

    if (data.tierSummary.length === 0) {
      html += '<div style="text-align:center;padding:24px;color:var(--text-muted)">Run analysis to generate tier data.</div>';
    } else {
      html += '<div class="table-wrap"><table><thead><tr><th>Tier</th><th>BINs</th><th>Transactions</th><th>Avg Approval</th></tr></thead><tbody>';
      for (const t of data.tierSummary) {
        const tierLabel = t.tier === 1 ? 'Tier 1 (Top 80%)' : t.tier === 2 ? 'Tier 2 (Next 15%)' : 'Tier 3 (Long Tail)';
        html += `<tr>
          <td><strong>${tierLabel}</strong></td>
          <td>${formatNum(t.bin_count)}</td>
          <td>${formatNum(t.total_tx)}</td>
          <td>${rateBarHtml(t.avg_approval_rate)}</td>
        </tr>`;
      }
      html += '</tbody></table></div>';
    }

    html += '</div></div>'; // end grid-2

    return html;
  },

  async triggerSync(clientId) {
    if (!confirm('Start a full data sync? This may take a while.')) return;
    const main = document.getElementById('mainContent');
    const btn = event.target;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Syncing...';

    try {
      const res = await fetch(`/api/actions/sync/${clientId}`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        alert(`Sync complete! Orders: ${data.stats.orders}, Gateways: ${data.stats.gateways}, Customers: ${data.stats.customers}`);
      } else {
        alert('Sync error: ' + data.error);
      }
    } catch (err) {
      alert('Sync failed: ' + err.message);
    }

    this.render(clientId);
  },

  async triggerAnalysis(clientId) {
    const btn = event.target;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Analyzing...';

    try {
      const res = await fetch(`/api/actions/analyze/${clientId}`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        alert(`Analysis complete! Recommendations: ${data.optimization.recommendations}, Degrading MIDs: ${data.degradation.degraded}`);
      } else {
        alert('Analysis error: ' + data.error);
      }
    } catch (err) {
      alert('Analysis failed: ' + err.message);
    }

    this.render(clientId);
  },

  async resolveAlert(alertId, clientId) {
    await fetch(`/api/actions/alerts/${alertId}/resolve`, { method: 'POST' });
    this.render(clientId);
  },
};
