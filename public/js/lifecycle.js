/**
 * BinRoute — MID Lifecycle Screen
 */
const Lifecycle = {
  activeTab: 'all',

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading...</div>';

    try {
      const [midsRes, closedRes, rampRes] = await Promise.all([
        fetch(`/api/lifecycle/${clientId}`),
        fetch(`/api/lifecycle/${clientId}/closed`),
        fetch(`/api/lifecycle/${clientId}/ramp-up`),
      ]);

      const mids = await midsRes.json();
      const closed = await closedRes.json();
      const rampUp = await rampRes.json();

      main.innerHTML = this.buildHtml(clientId, mids, closed, rampUp);
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml(clientId, mids, closed, rampUp) {
    const closedCount = mids.filter(m => m.lifecycle_state === 'closed').length;
    const degradingCount = mids.filter(m => m.lifecycle_state === 'degrading').length;
    const rampUpCount = mids.filter(m => m.lifecycle_state === 'ramp-up').length;

    let html = `
      <div class="main-header">
        <h2>MID Lifecycle</h2>
        <button class="btn btn-secondary" onclick="Lifecycle.checkStatus(${clientId})">&#8635; Check MID Status</button>
      </div>

      <div class="tabs">
        <div class="tab ${this.activeTab === 'all' ? 'active' : ''}" onclick="Lifecycle.switchTab('all', ${clientId})">
          All MIDs (${mids.length})
        </div>
        <div class="tab ${this.activeTab === 'closed' ? 'active' : ''}" onclick="Lifecycle.switchTab('closed', ${clientId})">
          Closures ${closedCount > 0 ? `<span class="nav-badge" style="position:static;margin-left:4px;background:var(--danger)">${closedCount}</span>` : ''}
        </div>
        <div class="tab ${this.activeTab === 'rampup' ? 'active' : ''}" onclick="Lifecycle.switchTab('rampup', ${clientId})">
          Ramp-up (${rampUpCount})
        </div>
      </div>`;

    if (this.activeTab === 'all') {
      html += this.buildAllTab(clientId, mids);
    } else if (this.activeTab === 'closed') {
      html += this.buildClosedTab(clientId, closed);
    } else if (this.activeTab === 'rampup') {
      html += this.buildRampUpTab(clientId, rampUp);
    }

    return html;
  },

  buildAllTab(clientId, mids) {
    let html = `<div class="card"><div class="table-wrap"><table>
      <thead><tr>
        <th>Gateway</th><th>Gateway Name</th><th>State</th>
        <th>Orders (90d)</th><th>Approval (90d)</th><th>Approval (7d)</th><th>Trend</th><th>Actions</th>
      </tr></thead><tbody>`;

    for (const mid of mids) {
      const trend7d = mid.approval_rate_7d != null && mid.approval_rate_90d != null
        ? mid.approval_rate_7d - mid.approval_rate_90d : null;
      const trendIcon = trend7d == null ? '—' :
        trend7d >= 1 ? `<span style="color:var(--success)">&#9650; +${trend7d.toFixed(1)}</span>` :
        trend7d <= -1 ? `<span style="color:var(--danger)">&#9660; ${trend7d.toFixed(1)}</span>` :
        `<span style="color:var(--text-muted)">&#9654; ${trend7d.toFixed(1)}</span>`;

      html += `<tr>
        <td><strong>${mid.gateway_id}</strong></td>
        <td>${gwDisplayName(mid)}</td>
        <td>${pillHtml(mid.lifecycle_state)}</td>
        <td>${formatNum(mid.total_orders_90d)}</td>
        <td>${rateBarHtml(mid.approval_rate_90d)}</td>
        <td>${rateBarHtml(mid.approval_rate_7d)}</td>
        <td>${trendIcon}</td>
        <td><button class="btn btn-sm btn-secondary" onclick="Lifecycle.showTrend(${clientId}, ${mid.gateway_id})">Chart</button></td>
      </tr>`;
    }

    if (mids.length === 0) {
      html += '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px">No MIDs found.</td></tr>';
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  buildClosedTab(clientId, closed) {
    if (closed.length === 0) {
      return '<div class="empty-state"><div class="icon">&#10003;</div><h3>No Closed MIDs</h3><p>All MIDs are operational.</p></div>';
    }

    let html = '';
    for (const c of closed) {
      html += `<div class="card">
        <div class="card-header">
          <span class="card-title" style="color:var(--danger)">&#9888; Gateway ${c.gateway_id} — ${gwDisplayName(c)}</span>
        </div>`;

      if (c.affectedBins.length === 0) {
        html += '<p style="color:var(--text-muted)">No affected BINs found in recent data.</p>';
      } else {
        html += `<div class="table-wrap"><table>
          <thead><tr><th>Affected BIN</th><th>Network</th><th>Volume</th><th>Best Replacement</th><th>2nd Best</th><th>3rd Best</th></tr></thead>
          <tbody>`;

        for (const bin of c.affectedBins) {
          const alts = bin.alternatives || [];
          html += `<tr>
            <td><strong>${bin.bin}</strong></td>
            <td>${bin.cc_type || '—'}</td>
            <td>${formatNum(bin.volume)}</td>`;

          for (let i = 0; i < 3; i++) {
            if (alts[i]) {
              html += `<td>${alts[i].gateway_alias || alts[i].gateway_descriptor || 'GW ' + alts[i].gateway_id} (${alts[i].weighted_approval_rate?.toFixed(1) || '?'}%)</td>`;
            } else {
              html += '<td style="color:var(--text-muted)">—</td>';
            }
          }
          html += '</tr>';
        }
        html += '</tbody></table></div>';
      }
      html += '</div>';
    }
    return html;
  },

  buildRampUpTab(clientId, rampUp) {
    if (rampUp.length === 0) {
      return '<div class="empty-state"><h3>No MIDs in Ramp-up</h3><p>New MIDs will appear here when detected.</p></div>';
    }

    let html = `<div class="card"><div class="table-wrap"><table>
      <thead><tr><th>Gateway</th><th>Gateway Name</th><th>Date Added</th><th>Orders</th><th>Approval Rate</th><th>Confidence Progress</th><th>Ready</th></tr></thead>
      <tbody>`;

    for (const r of rampUp) {
      html += `<tr>
        <td><strong>${r.gateway_id}</strong></td>
        <td>${gwDisplayName(r)}</td>
        <td>${r.date_added ? timeAgo(r.date_added) : '—'}</td>
        <td>${formatNum(r.total_orders)}</td>
        <td>${rateBarHtml(r.approval_rate)}</td>
        <td>
          <div class="progress-bar"><div class="progress-bar-fill" style="width:${r.confidence_progress}%"></div></div>
          <div class="progress-label">${r.confidence_progress}% (${r.total_orders}/30 txns)</div>
        </td>
        <td>${r.ready_for_routing ? '<span style="color:var(--success);font-weight:600">Ready</span>' : '<span style="color:var(--text-muted)">Monitoring</span>'}</td>
      </tr>`;
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  switchTab(tab, clientId) {
    this.activeTab = tab;
    this.render(clientId);
  },

  async checkStatus(clientId) {
    const btn = event.target;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Checking...';
    try {
      const res = await fetch(`/api/actions/mid-check/${clientId}`, { method: 'POST' });
      const data = await res.json();
      alert(`MID check complete. Changes detected: ${data.changes}`);
    } catch (err) {
      alert('Error: ' + err.message);
    }
    this.render(clientId);
  },

  async showTrend(clientId, gatewayId) {
    try {
      const res = await fetch(`/api/lifecycle/${clientId}/trend/${gatewayId}`);
      const trend = await res.json();

      if (trend.length === 0) {
        alert('No trend data available for this MID.');
        return;
      }

      // Simple text-based trend display (could be upgraded to chart library later)
      const overlay = document.createElement('div');
      overlay.className = 'modal-overlay';

      let chartHtml = `<div class="modal" style="max-width:700px">
        <h3>Gateway ${gatewayId} — Daily Approval Rate (90 days)</h3>
        <div class="table-wrap" style="max-height:400px;overflow-y:auto">
          <table><thead><tr><th>Date</th><th>Total</th><th>Approved</th><th>Rate</th></tr></thead><tbody>`;

      for (const day of trend) {
        chartHtml += `<tr>
          <td>${day.day}</td>
          <td>${day.total}</td>
          <td>${day.approved}</td>
          <td>${rateBarHtml(day.approval_rate, 200)}</td>
        </tr>`;
      }

      chartHtml += `</tbody></table></div>
        <div class="modal-actions">
          <button class="btn btn-secondary" onclick="this.closest('.modal-overlay').remove()">Close</button>
        </div>
      </div>`;

      overlay.innerHTML = chartHtml;
      document.body.appendChild(overlay);
    } catch (err) {
      alert('Error loading trend: ' + err.message);
    }
  },
};
