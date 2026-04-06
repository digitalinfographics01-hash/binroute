/**
 * Implementation Tracker — Monitors playbook routing rules that have been implemented.
 */
const Implementations = {
  clientId: null,
  data: null,
  filter: null,     // status filter
  ruleFilter: null,  // rule_type filter

  async render(clientId) {
    this.clientId = clientId;
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:24px"><div class="loading">Loading implementation tracker...</div></div>';

    try {
      const res = await fetch(`/api/implementations/${clientId}/dashboard`);
      this.data = await res.json();
    } catch (err) {
      main.innerHTML = `<div style="padding:24px"><div class="empty-state"><h3>Error</h3><p>${err.message}</p></div></div>`;
      return;
    }

    this._render();
  },

  _render() {
    const main = document.getElementById('mainContent');
    const { summary, implementations } = this.data;

    let html = '<div style="padding:24px">';

    // ── Title ──
    html += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">';
    html += '<h2 style="margin:0;font-size:22px">Implementation Tracker</h2>';
    html += `<span style="font-size:12px;color:var(--text-secondary)">${summary.total} total implementations</span>`;
    html += '</div>';

    // ── Scorecard ──
    html += '<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px">';

    html += this._scorecard('Confirmed', summary.confirmed, '#D1FAE5', '#065F46',
      summary.total_confirmed_lift_pp ? '+' + summary.total_confirmed_lift_pp.toFixed(1) + 'pp total' : '');
    html += this._scorecard('Tracking', summary.waiting + summary.collecting + summary.evaluating, '#DBEAFE', '#1D4ED8',
      `${summary.waiting}w / ${summary.collecting}c / ${summary.evaluating}e`);
    html += this._scorecard('Regressed', summary.regression, '#FEE2E2', '#991B1B', '');
    html += this._scorecard('Total Lift', '', '#F0FDF4', '#166534',
      summary.total_confirmed_lift_pp >= 0 ? '+' + summary.total_confirmed_lift_pp.toFixed(1) + 'pp' : summary.total_confirmed_lift_pp.toFixed(1) + 'pp',
      true);
    html += this._scorecard('Revenue Impact', '', '#ECFDF5', '#047857',
      summary.est_monthly_revenue_impact >= 0
        ? '+$' + summary.est_monthly_revenue_impact.toLocaleString() + '/mo'
        : '-$' + Math.abs(summary.est_monthly_revenue_impact).toLocaleString() + '/mo',
      true);

    html += '</div>';

    // ── Filters ──
    html += '<div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">';
    const filters = [
      { key: null, label: 'All', count: implementations.length },
      { key: 'active', label: 'Tracking', count: implementations.filter(i => ['waiting','collecting','evaluating'].includes(i.status)).length },
      { key: 'confirmed', label: 'Confirmed', count: implementations.filter(i => i.status === 'confirmed').length },
      { key: 'regression', label: 'Regressed', count: implementations.filter(i => i.status === 'regression').length },
      { key: 'inconclusive', label: 'Inconclusive', count: implementations.filter(i => i.status === 'inconclusive').length },
    ];
    for (const f of filters) {
      const active = this.filter === f.key;
      const style = active
        ? 'background:var(--text);color:var(--bg);border:1px solid var(--text)'
        : 'background:var(--bg);color:var(--text-secondary);border:1px solid var(--border)';
      html += `<button onclick="Implementations.filter=${f.key === null ? 'null' : "'" + f.key + "'"};Implementations._render()" style="font-size:11px;font-weight:600;padding:4px 14px;border-radius:16px;cursor:pointer;${style}">${f.label} ${f.count}</button>`;
    }

    // Rule type filters
    html += '<span style="margin-left:8px;border-left:1px solid var(--border);padding-left:8px"></span>';
    const ruleTypes = ['initial_routing', 'cascade', 'rebill_routing', 'salvage'];
    const ruleLabels = { initial_routing: 'Initial', cascade: 'Cascade', rebill_routing: 'Rebill', salvage: 'Salvage' };
    for (const rt of ruleTypes) {
      const count = implementations.filter(i => i.rule_type === rt).length;
      if (count === 0) continue;
      const active = this.ruleFilter === rt;
      const style = active
        ? 'background:#6366F1;color:#fff;border:1px solid #6366F1'
        : 'background:var(--bg);color:var(--text-secondary);border:1px solid var(--border)';
      html += `<button onclick="Implementations.ruleFilter=${active ? 'null' : "'" + rt + "'"};Implementations._render()" style="font-size:11px;font-weight:600;padding:4px 12px;border-radius:16px;cursor:pointer;${style}">${ruleLabels[rt]} ${count}</button>`;
    }
    html += '</div>';

    // ── Implementation Cards ──
    let filtered = implementations;
    if (this.filter === 'active') {
      filtered = filtered.filter(i => ['waiting', 'collecting', 'evaluating'].includes(i.status));
    } else if (this.filter) {
      filtered = filtered.filter(i => i.status === this.filter);
    }
    if (this.ruleFilter) {
      filtered = filtered.filter(i => i.rule_type === this.ruleFilter);
    }

    if (filtered.length === 0) {
      html += '<div class="empty-state" style="margin-top:40px"><h3>No Implementations</h3><p>Mark routing rules as implemented from the Playbook tab to start tracking.</p></div>';
    } else {
      for (const impl of filtered) {
        html += this._implCard(impl);
      }
    }

    html += '</div>';
    main.innerHTML = html;

    // Update sidebar badge
    this._updateBadge();
  },

  _scorecard(label, count, bg, color, subtitle, isValueOnly) {
    return `<div style="padding:14px 16px;border-radius:8px;background:${bg}">
      <div style="font-size:10px;font-weight:600;color:${color};text-transform:uppercase">${label}</div>
      ${isValueOnly
        ? `<div style="font-size:20px;font-weight:700;color:${color};margin-top:4px">${subtitle}</div>`
        : `<div style="font-size:24px;font-weight:700;color:${color};margin-top:4px">${count}</div>
           ${subtitle ? `<div style="font-size:10px;color:${color};opacity:0.7;margin-top:2px">${subtitle}</div>` : ''}`
      }
    </div>`;
  },

  _implCard(impl) {
    const statusColors = {
      waiting: { bg: '#F3F4F6', color: '#6B7280', label: 'Waiting' },
      collecting: { bg: '#DBEAFE', color: '#1D4ED8', label: 'Collecting' },
      evaluating: { bg: '#DBEAFE', color: '#1D4ED8', label: 'Evaluating' },
      confirmed: { bg: '#D1FAE5', color: '#065F46', label: 'Confirmed' },
      regression: { bg: '#FEE2E2', color: '#991B1B', label: 'Regression' },
      inconclusive: { bg: '#F3F4F6', color: '#6B7280', label: 'Inconclusive' },
      rolled_back: { bg: '#FEF3C7', color: '#92400E', label: 'Rolled Back' },
    };
    const ruleLabels = {
      initial_routing: 'Initial', cascade: 'Cascade',
      upsell_routing: 'Upsell', rebill_routing: 'Rebill', salvage: 'Salvage',
    };
    const s = statusColors[impl.status] || statusColors.waiting;
    const expandId = 'impl_' + impl.id;
    const prepaidLabel = impl.is_prepaid ? ' (Prepaid)' : '';
    const l4Label = impl.card_brand ? ` / ${impl.card_brand} ${impl.card_type || ''}` : '';

    let html = `<div class="card" style="margin-bottom:10px;padding:0;border-left:4px solid ${s.color};overflow:hidden">`;

    // Header
    html += `<div style="padding:14px 20px 10px;display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
      <div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <span style="font-size:14px;font-weight:600">${impl.issuer_bank}${prepaidLabel}${l4Label}</span>
          <span style="font-size:9px;padding:2px 8px;border-radius:3px;background:#E0E7FF;color:#4338CA;font-weight:600">${ruleLabels[impl.rule_type] || impl.rule_type}</span>
          <span style="font-size:9px;padding:2px 8px;border-radius:3px;background:${s.bg};color:${s.color};font-weight:600">${s.label}</span>
          ${impl.has_split ? '<span style="font-size:9px;padding:2px 8px;border-radius:3px;background:#FEF3C7;color:#92400E;font-weight:600">SPLIT</span>' : ''}
        </div>
        <div style="font-size:11px;color:var(--text-secondary);margin-top:4px">
          ${impl.actual_processor} &mdash; ${impl.days_since}d ago
          ${impl.recommended_processor && impl.recommended_processor !== impl.actual_processor ? ` (rec: ${impl.recommended_processor})` : ''}
        </div>
      </div>
    </div>`;

    // Metrics row
    html += '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;padding:0 20px 12px">';

    // Baseline
    html += `<div style="background:#F3F4F6;border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:#6B7280;font-weight:600">BASELINE</div>
      <div style="font-size:16px;font-weight:700;color:var(--text);margin-top:2px">${impl.baseline_rate}%</div>
      <div style="font-size:9px;color:#6B7280">${impl.baseline_attempts} att / 30d</div>
    </div>`;

    // Current
    const currentRate = impl.current_rate != null ? impl.current_rate + '%' : '—';
    html += `<div style="background:${s.bg};border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:${s.color};font-weight:600">CURRENT</div>
      <div style="font-size:16px;font-weight:700;color:${s.color};margin-top:2px">${currentRate}</div>
      <div style="font-size:9px;color:${s.color}">${impl.current_attempts} att</div>
    </div>`;

    // Lift
    const liftStr = impl.lift_pp != null ? (impl.lift_pp >= 0 ? '+' : '') + impl.lift_pp.toFixed(1) + 'pp' : '—';
    const liftColor = impl.lift_pp > 0 ? '#065F46' : impl.lift_pp < 0 ? '#991B1B' : '#6B7280';
    const liftBg = impl.lift_pp > 0 ? '#D1FAE5' : impl.lift_pp < 0 ? '#FEE2E2' : '#F3F4F6';
    html += `<div style="background:${liftBg};border-radius:6px;padding:8px 10px">
      <div style="font-size:9px;color:${liftColor};font-weight:600">LIFT</div>
      <div style="font-size:16px;font-weight:700;color:${liftColor};margin-top:2px">${liftStr}</div>
    </div>`;

    // Progress / Verdict
    if (['waiting', 'collecting', 'evaluating'].includes(impl.status)) {
      const pct = Math.min(100, Math.round((impl.current_attempts / impl.min_sample_target) * 100));
      html += `<div style="background:#DBEAFE;border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:#1D4ED8;font-weight:600">PROGRESS</div>
        <div style="font-size:14px;font-weight:700;color:#1D4ED8;margin-top:2px">${impl.sample_progress}</div>
        <div style="height:4px;background:#93C5FD;border-radius:2px;margin-top:4px">
          <div style="height:100%;width:${pct}%;background:#1D4ED8;border-radius:2px"></div>
        </div>
      </div>`;
    } else {
      html += `<div style="background:${s.bg};border-radius:6px;padding:8px 10px">
        <div style="font-size:9px;color:${s.color};font-weight:600">VERDICT</div>
        <div style="font-size:12px;font-weight:600;color:${s.color};margin-top:4px">${s.label}</div>
      </div>`;
    }

    html += '</div>';

    // Split data (if applicable)
    if (impl.has_split && impl.new_side && impl.old_side) {
      html += `<div style="padding:0 20px 12px;display:flex;gap:12px">
        <div style="flex:1;padding:8px;border-radius:6px;border:1px solid #DBEAFE;background:#EFF6FF">
          <div style="font-size:9px;font-weight:600;color:#1D4ED8">NEW SIDE (${impl.split_config?.new_pct || 70}%)</div>
          <div style="font-size:14px;font-weight:700;color:#1D4ED8">${impl.new_side.rate}% <span style="font-size:10px;font-weight:400">(${impl.new_side.approvals}/${impl.new_side.attempts})</span></div>
        </div>
        <div style="flex:1;padding:8px;border-radius:6px;border:1px solid var(--border);background:var(--bg)">
          <div style="font-size:9px;font-weight:600;color:var(--text-secondary)">OLD SIDE (${impl.split_config?.old_pct || 30}%)</div>
          <div style="font-size:14px;font-weight:700;color:var(--text)">${impl.old_side.rate}% <span style="font-size:10px;font-weight:400">(${impl.old_side.approvals}/${impl.old_side.attempts})</span></div>
        </div>
        <div style="flex:1;padding:8px;border-radius:6px;border:1px solid #E5E7EB;background:#F9FAFB">
          <div style="font-size:9px;font-weight:600;color:#6B7280">BASELINE</div>
          <div style="font-size:14px;font-weight:700;color:#6B7280">${impl.baseline_rate}%</div>
        </div>
      </div>`;
    }

    // Cohort data (for rebill)
    if (impl.rule_type === 'rebill_routing' && impl.cohort) {
      html += `<div style="padding:0 20px 12px;font-size:11px;color:var(--text-secondary)">
        Cohort: ${impl.cohort.customers_acquired} customers acquired post-impl &rarr; ${impl.cohort.rebills_attempted} C1 rebills attempted, ${impl.cohort.rebills_approved} approved
      </div>`;
    }

    // Actions row
    html += '<div style="padding:0 20px 12px;display:flex;gap:8px">';
    html += `<button onclick="Implementations.toggleDetail(${impl.id},'${expandId}')" style="font-size:11px;padding:4px 12px;border:1px solid var(--border);border-radius:4px;background:var(--bg);cursor:pointer;color:var(--text-secondary)">View Details</button>`;
    if (impl.status === 'regression') {
      html += `<button onclick="Implementations.rollback(${impl.id})" style="font-size:11px;padding:4px 12px;border:1px solid #DC2626;border-radius:4px;background:#FEE2E2;cursor:pointer;color:#991B1B;font-weight:600">Rollback</button>`;
    }
    if (!['superseded', 'archived', 'rolled_back'].includes(impl.status)) {
      html += `<button onclick="Implementations.archive(${impl.id})" style="font-size:11px;padding:4px 12px;border:1px solid var(--border);border-radius:4px;background:var(--bg);cursor:pointer;color:var(--text-muted)">Archive</button>`;
    }
    html += '</div>';

    // Expandable detail section
    html += `<div id="${expandId}" style="display:none;padding:0 20px 16px;border-top:1px solid var(--border)">
      <div style="text-align:center;color:var(--text-muted);padding:12px">Loading...</div>
    </div>`;

    // Verdict reason
    if (impl.verdict_reason) {
      html += `<div style="padding:8px 20px;font-size:11px;color:${s.color};background:${s.bg};border-top:1px solid var(--border)">
        ${impl.verdict_reason}
      </div>`;
    }

    html += '</div>';
    return html;
  },

  async toggleDetail(implId, expandId) {
    const el = document.getElementById(expandId);
    if (!el) return;

    if (el.style.display !== 'none') {
      el.style.display = 'none';
      return;
    }

    el.style.display = '';
    el.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:12px">Loading checkpoints...</div>';

    try {
      const res = await fetch(`/api/implementations/${this.clientId}/detail/${implId}`);
      const data = await res.json();
      el.innerHTML = this._detailView(data);
    } catch (err) {
      el.innerHTML = `<div style="color:var(--danger);padding:12px">${err.message}</div>`;
    }
  },

  _detailView(data) {
    const { implementation: impl, checkpoints } = data;
    let html = '<div style="margin-top:12px">';

    // Implementation info
    html += `<div style="margin-bottom:16px">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:6px">Implementation Details</div>
      <div style="font-size:12px;display:grid;grid-template-columns:auto 1fr;gap:4px 16px">
        <span style="color:var(--text-secondary)">Processor:</span><span>${impl.actual_processor}</span>
        <span style="color:var(--text-secondary)">Gateway IDs:</span><span>${impl.actual_gateway_ids || '—'}</span>
        <span style="color:var(--text-secondary)">Implemented:</span><span>${impl.implemented_at}</span>
        <span style="color:var(--text-secondary)">Rule Level:</span><span>${impl.rule_level === 'l4' ? 'L4 (Card Type)' : 'Bank Level'}</span>
        ${impl.rollback_to_processor ? `<span style="color:var(--text-secondary)">Rollback to:</span><span>${impl.rollback_to_processor}</span>` : ''}
      </div>
    </div>`;

    // Checkpoint timeline
    if (checkpoints.length > 0) {
      html += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-secondary);margin-bottom:8px">Checkpoint Timeline</div>';
      html += '<table style="width:100%;font-size:11px;border-collapse:collapse">';
      html += '<tr style="border-bottom:1px solid var(--border)">';
      html += '<th style="text-align:left;padding:6px 8px;color:var(--text-secondary)">Day</th>';
      html += '<th style="text-align:right;padding:6px 8px;color:var(--text-secondary)">Attempts</th>';
      html += '<th style="text-align:right;padding:6px 8px;color:var(--text-secondary)">Approvals</th>';
      html += '<th style="text-align:right;padding:6px 8px;color:var(--text-secondary)">Rate</th>';
      html += '<th style="text-align:right;padding:6px 8px;color:var(--text-secondary)">Baseline</th>';
      html += '<th style="text-align:right;padding:6px 8px;color:var(--text-secondary)">Lift</th>';
      html += '<th style="text-align:center;padding:6px 8px;color:var(--text-secondary)">Sample</th>';
      html += '<th style="text-align:left;padding:6px 8px;color:var(--text-secondary)">Status</th>';
      html += '</tr>';

      for (const cp of checkpoints) {
        const liftColor = cp.lift_pp > 0 ? '#065F46' : cp.lift_pp < 0 ? '#991B1B' : '#6B7280';
        const liftStr = cp.lift_pp >= 0 ? '+' + cp.lift_pp.toFixed(1) : cp.lift_pp.toFixed(1);
        html += `<tr style="border-bottom:1px solid var(--border)">
          <td style="padding:6px 8px">Day ${cp.checkpoint_day}</td>
          <td style="padding:6px 8px;text-align:right">${cp.post_attempts}</td>
          <td style="padding:6px 8px;text-align:right">${cp.post_approvals}</td>
          <td style="padding:6px 8px;text-align:right;font-weight:600">${cp.post_approval_rate != null ? cp.post_approval_rate + '%' : '—'}</td>
          <td style="padding:6px 8px;text-align:right;color:var(--text-secondary)">${cp.baseline_rate}%</td>
          <td style="padding:6px 8px;text-align:right;color:${liftColor};font-weight:600">${liftStr}pp</td>
          <td style="padding:6px 8px;text-align:center">${cp.meets_minimum_sample ? '<span style="color:#065F46">&#10003;</span>' : '<span style="color:#D97706">&#9679;</span>'}</td>
          <td style="padding:6px 8px;font-size:10px">${cp.status_at_checkpoint || '—'}</td>
        </tr>`;

        // Confounding factors
        if (cp.confounding_factors && cp.confounding_factors.length > 0) {
          html += `<tr><td colspan="8" style="padding:4px 8px;background:#FEF3C7;font-size:10px;color:#92400E">
            Confounding: ${cp.confounding_factors.map(f => f.description).join('; ')}
          </td></tr>`;
        }
      }
      html += '</table>';
    } else {
      html += '<div style="font-size:12px;color:var(--text-muted);padding:12px 0">No checkpoints recorded yet. Evaluation starts after the settling period.</div>';
    }

    html += '</div>';
    return html;
  },

  async rollback(implId) {
    if (!confirm('Mark this implementation as rolled back? This is just a tracking action — you still need to revert the routing in Sticky.io manually.')) return;

    try {
      const res = await fetch(`/api/implementations/${implId}/rollback`, { method: 'POST' });
      const data = await res.json();
      if (data.error) { alert('Error: ' + data.error); return; }
      alert(`Rolled back. Revert to: ${data.rollback_to_processor || 'previous processor'}\nGateway IDs: ${data.rollback_to_gateway_ids || 'N/A'}`);
      this.render(this.clientId);
    } catch (err) {
      alert('Failed: ' + err.message);
    }
  },

  async archive(implId) {
    if (!confirm('Archive this implementation? It will be hidden from the active list.')) return;

    try {
      await fetch(`/api/implementations/${implId}/archive`, { method: 'POST' });
      this.render(this.clientId);
    } catch (err) {
      alert('Failed: ' + err.message);
    }
  },

  _updateBadge() {
    const badge = document.getElementById('implBadge');
    if (!badge || !this.data) return;
    const count = this.data.summary.regression || 0;
    if (count > 0) {
      badge.textContent = count;
      badge.style.display = 'inline';
    } else {
      badge.style.display = 'none';
    }
  },

  // Called externally to update badge without full render
  async updateBadge(clientId) {
    try {
      const res = await fetch(`/api/implementations/${clientId}/dashboard`);
      const data = await res.json();
      const badge = document.getElementById('implBadge');
      if (!badge) return;
      const count = data.summary.regression || 0;
      if (count > 0) {
        badge.textContent = count;
        badge.style.display = 'inline';
      } else {
        badge.style.display = 'none';
      }
    } catch {}
  },
};
