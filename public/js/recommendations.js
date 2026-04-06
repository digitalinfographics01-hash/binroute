/**
 * BinRoute — Recommendations Screen
 */
const Recommendations = {
  filters: { status: '', mcc: '', transaction_type: '', bin: '' },

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading recommendations...</div>';

    try {
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(this.filters)) {
        if (v) params.set(k, v);
      }

      const [recsRes, summaryRes] = await Promise.all([
        fetch(`/api/recommendations/${clientId}?${params}`),
        fetch(`/api/recommendations/${clientId}/summary`),
      ]);

      const recs = await recsRes.json();
      const summary = await summaryRes.json();

      main.innerHTML = this.buildHtml(clientId, recs, summary);
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml(clientId, recs, summary) {
    let html = `
      <div class="main-header">
        <h2>Recommendations</h2>
      </div>`;

    // Summary cards
    const openCount = summary.find(s => s.status === 'open')?.count || 0;
    const implCount = summary.find(s => s.status === 'implemented')?.count || 0;
    const confirmedCount = summary.find(s => s.status === 'confirmed')?.count || 0;

    html += `<div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-label">Open</div>
        <div class="kpi-value accent">${openCount}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Implemented</div>
        <div class="kpi-value">${implCount}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Confirmed</div>
        <div class="kpi-value success">${confirmedCount}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Avg Lift (Open)</div>
        <div class="kpi-value success">${summary.find(s => s.status === 'open')?.avg_lift?.toFixed(1) || '—'}pp</div>
      </div>
    </div>`;

    // Filter bar
    html += `<div class="filter-bar">
      <select onchange="Recommendations.setFilter('status', this.value, ${clientId})">
        <option value="">All Statuses</option>
        <option value="open" ${this.filters.status === 'open' ? 'selected' : ''}>Open</option>
        <option value="implemented" ${this.filters.status === 'implemented' ? 'selected' : ''}>Implemented</option>
        <option value="confirmed" ${this.filters.status === 'confirmed' ? 'selected' : ''}>Confirmed</option>
        <option value="inconclusive" ${this.filters.status === 'inconclusive' ? 'selected' : ''}>Inconclusive</option>
        <option value="regression" ${this.filters.status === 'regression' ? 'selected' : ''}>Regression</option>
        <option value="dismissed" ${this.filters.status === 'dismissed' ? 'selected' : ''}>Dismissed</option>
      </select>
      <input placeholder="Filter by BIN..." value="${this.filters.bin}" onchange="Recommendations.setFilter('bin', this.value, ${clientId})">
      <input placeholder="Filter by MCC..." value="${this.filters.mcc}" onchange="Recommendations.setFilter('mcc', this.value, ${clientId})">
      <select onchange="Recommendations.setFilter('transaction_type', this.value, ${clientId})">
        <option value="">All TX Types</option>
        <option value="cp_initial">CP Initial</option>
        <option value="cp_upsell">CP Upsell</option>
        <option value="trial_conversion">Trial Conversion</option>
        <option value="recurring_rebill">Recurring Rebill</option>
        <option value="simulated_cp_rebill">Simulated CP Rebill</option>
        <option value="salvage_attempt">Salvage Attempt</option>
      </select>
    </div>`;

    // Recommendation table
    html += `<div class="card">
      <div class="table-wrap"><table>
        <thead><tr>
          <th>BIN</th><th>Network</th><th>Current MID</th><th>Recommended MID</th>
          <th>Confidence</th><th>Expected Lift</th><th>Volume</th><th>Status</th><th>Actions</th>
        </tr></thead>
        <tbody>`;

    if (recs.length === 0) {
      html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No recommendations found.</td></tr>';
    }

    for (const rec of recs) {
      html += `<tr>
        <td><strong>${rec.bin}</strong></td>
        <td>${rec.cc_type || '—'}</td>
        <td>GW ${rec.current_gateway_id}<br><span style="font-size:11px;color:var(--text-muted)">${rec.current_gateway_name || ''}</span></td>
        <td>GW ${rec.recommended_gateway_id}<br><span style="font-size:11px;color:var(--text-muted)">${rec.recommended_gateway_name || ''}</span></td>
        <td><strong>${rec.confidence_score != null ? (rec.confidence_score * 100).toFixed(0) + '%' : '—'}</strong></td>
        <td style="color:var(--success);font-weight:600">+${rec.expected_lift?.toFixed(1) || '?'}pp</td>
        <td>${formatNum(rec.transaction_volume)}</td>
        <td>${pillHtml(rec.status)}</td>
        <td>
          ${rec.status === 'open' ? `
            <button class="btn btn-sm btn-success" onclick="Recommendations.implement(${rec.id}, ${clientId})">Implement</button>
            <button class="btn btn-sm btn-secondary" onclick="Recommendations.dismiss(${rec.id}, ${clientId})">Dismiss</button>
          ` : ''}
          <button class="btn btn-sm btn-secondary" onclick="Recommendations.showDetail(${rec.id}, ${clientId})">Detail</button>
        </td>
      </tr>`;
    }

    html += '</tbody></table></div></div>';

    // Detail section for individual recommendations
    for (const rec of recs) {
      html += `<div id="rec-detail-${rec.id}" style="display:none" class="rec-card">
        <div class="rec-summary">${rec.summary || 'No summary available.'}</div>
        <div class="rec-meta">
          <div>MCC: <strong>${rec.mcc_code || '—'}</strong></div>
          <div>TX Type: <strong>${rec.transaction_type || 'All'}</strong></div>
          <div>Current Rate: <strong>${rec.current_approval_rate?.toFixed(1) || '—'}%</strong></div>
          <div>Expected Rate: <strong>${rec.recommended_approval_rate?.toFixed(1) || '—'}%</strong></div>
          <div>Priority Score: <strong>${rec.priority_score?.toFixed(1) || '—'}</strong></div>
        </div>
      </div>`;
    }

    return html;
  },

  setFilter(key, value, clientId) {
    this.filters[key] = value;
    this.render(clientId);
  },

  async implement(recId, clientId) {
    if (!confirm('Mark this recommendation as implemented? A 7-day waiting period will begin.')) return;
    try {
      const res = await fetch(`/api/recommendations/${recId}/implement`, { method: 'POST' });
      const data = await res.json();
      if (data.error) {
        alert('Error: ' + data.error);
      } else {
        alert('Marked as implemented. 7-day comparison period begins now.');
      }
    } catch (err) {
      alert('Error: ' + err.message);
    }
    this.render(clientId);
  },

  async dismiss(recId, clientId) {
    if (!confirm('Dismiss this recommendation?')) return;
    await fetch(`/api/recommendations/${recId}/dismiss`, { method: 'POST' });
    this.render(clientId);
  },

  showDetail(recId) {
    const el = document.getElementById(`rec-detail-${recId}`);
    if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
  },
};
