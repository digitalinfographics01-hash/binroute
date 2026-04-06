/**
 * BinRoute — Backend Setup Screen
 */
const Config = {
  activeTab: 'mids',

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading configuration...</div>';

    try {
      const [gwRes, incRes, rulesRes, cyclesRes, logRes, campRes] = await Promise.all([
        fetch(`/api/config/gateways/${clientId}`),
        fetch(`/api/config/gateways/${clientId}/incomplete`),
        fetch(`/api/config/tx-rules/${clientId}`),
        fetch(`/api/config/cycle-groups/${clientId}`),
        fetch(`/api/config/changelog/${clientId}`),
        fetch(`/api/config/campaigns/${clientId}`),
      ]);

      const gateways = await gwRes.json();
      const incomplete = await incRes.json();
      const rules = await rulesRes.json();
      const cycles = await cyclesRes.json();
      const changelog = await logRes.json();
      const campaigns = await campRes.json();

      App.updateConfigBadge(incomplete.count);

      main.innerHTML = this.buildHtml(clientId, gateways, incomplete, rules, cycles, changelog, campaigns);
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml(clientId, gateways, incomplete, rules, cycles, changelog, campaigns) {
    // Completion progress
    const totalActive = gateways.filter(g => g.lifecycle_state !== 'closed').length;
    const completeCount = totalActive - incomplete.count;
    const completePct = totalActive > 0 ? Math.round((completeCount / totalActive) * 100) : 100;

    let html = `
      <div class="main-header">
        <h2>Backend Setup</h2>
        <div style="display:flex;gap:8px">
          <button class="btn btn-secondary" onclick="Config.syncGateways(${clientId})">&#8635; Sync Gateways</button>
          <a href="/api/templates/mid-config/${clientId}" class="btn btn-secondary" download>&#8615; Download Template</a>
        </div>
      </div>

      <div class="tabs">
        <div class="tab ${this.activeTab === 'mids' ? 'active' : ''}" onclick="Config.switchTab('mids', ${clientId})">
          MID Configuration ${incomplete.count > 0 ? `<span class="nav-badge" style="position:static;margin-left:6px">${incomplete.count}</span>` : ''}
        </div>
        <div class="tab ${this.activeTab === 'txrules' ? 'active' : ''}" onclick="Config.switchTab('txrules', ${clientId})">TX Type Mapping</div>
        <div class="tab ${this.activeTab === 'cycles' ? 'active' : ''}" onclick="Config.switchTab('cycles', ${clientId})">Cycle Grouping</div>
        <div class="tab ${this.activeTab === 'log' ? 'active' : ''}" onclick="Config.switchTab('log', ${clientId})">Change Log</div>
      </div>`;

    if (this.activeTab === 'mids') {
      html += this.buildMidTab(clientId, gateways, incomplete, completePct);
    } else if (this.activeTab === 'txrules') {
      html += this.buildTxRulesTab(clientId, rules, campaigns);
    } else if (this.activeTab === 'cycles') {
      html += this.buildCyclesTab(clientId, cycles);
    } else if (this.activeTab === 'log') {
      html += this.buildLogTab(changelog);
    }

    return html;
  },

  buildMidTab(clientId, gateways, incomplete, completePct) {
    let html = `
      <div style="display:flex;gap:16px;margin-bottom:20px;align-items:center">
        <div style="flex:1">
          <div style="font-size:13px;font-weight:500;margin-bottom:4px">Configuration Completeness</div>
          <div class="progress-bar"><div class="progress-bar-fill" style="width:${completePct}%"></div></div>
          <div class="progress-label">${completePct}% complete</div>
        </div>
      </div>

      <div class="upload-zone" onclick="Config.showUpload(${clientId})" id="uploadZone">
        <div class="icon">&#128196;</div>
        <p>Drop CSV/Excel here or click to upload MID configuration</p>
      </div>

      <div class="card" style="margin-top:16px">
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>Gateway ID</th><th>Gateway Name</th><th>Status</th>
              <th>Processor Name</th><th>Bank Name</th><th>MCC Code</th>
              <th>Acquiring BIN</th><th>Analysis</th><th>Actions</th>
            </tr></thead>
            <tbody>`;

    for (const gw of gateways) {
      const isIncomplete = gw.lifecycle_state !== 'closed' &&
        (!gw.processor_name || !gw.bank_name || !gw.mcc_code);
      const rowClass = isIncomplete ? 'highlight-amber' : '';

      const isPaused = gw.exclude_from_analysis === 1;
      const pausedBadge = isPaused ? '<span style="font-size:10px;padding:2px 7px;border-radius:3px;background:#fffbeb;color:#92400e;font-weight:600">Paused</span>' : '';

      html += `<tr class="${rowClass}">
        <td><strong>${gw.gateway_id}</strong></td>
        <td>${gwDisplayName(gw)}</td>
        <td>${pillHtml(gw.lifecycle_state)}</td>
        <td>${gw.processor_name || '<span class="required-dot"></span>'}</td>
        <td>${gw.bank_name || '<span class="required-dot"></span>'}</td>
        <td>${gw.mcc_code || '<span class="required-dot"></span>'}</td>
        <td>${gw.acquiring_bin || '—'}</td>
        <td style="white-space:nowrap">
          ${pausedBadge}
          <label style="cursor:pointer;margin-left:4px;font-size:11px;color:var(--text-secondary)" title="${isPaused ? 'Resume analysis' : 'Pause analysis'}">
            <input type="checkbox" ${isPaused ? 'checked' : ''} onchange="Config.toggleExclude(${clientId}, ${gw.gateway_id}, this)" style="vertical-align:middle"> Exclude
          </label>
        </td>
        <td><button class="btn btn-sm btn-secondary" onclick="Config.editGateway(${clientId}, ${gw.gateway_id})">Edit</button></td>
      </tr>`;
    }

    if (gateways.length === 0) {
      html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No gateways found. Click "Sync Gateways" to pull from Sticky.io.</td></tr>';
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  buildTxRulesTab(clientId, rules, campaigns) {
    let html = `
      <div class="card">
        <div class="card-header">
          <span class="card-title">Transaction Type Mapping Rules</span>
          <button class="btn btn-sm btn-primary" onclick="Config.addTxRule(${clientId})">+ Add Rule</button>
        </div>
        <p style="font-size:12px;color:var(--text-secondary);margin-bottom:16px">Map campaign/product combinations to transaction types. CP simulation flags mark campaigns that use third-party CP tools.</p>
        <div class="table-wrap"><table>
          <thead><tr><th>Campaign ID</th><th>Product ID</th><th>Assigned Type</th><th>CP Simulation</th><th>Notes</th><th>Actions</th></tr></thead>
          <tbody>`;

    for (const rule of rules) {
      html += `<tr>
        <td>${rule.campaign_id || 'All'}</td>
        <td>${rule.product_id || 'All'}</td>
        <td>${pillHtml(rule.assigned_type)}</td>
        <td>${rule.is_cp_simulation ? '<span style="color:var(--warning);font-weight:600">Yes</span>' : 'No'}</td>
        <td>${rule.notes || '—'}</td>
        <td><button class="btn btn-sm btn-danger" onclick="Config.deleteTxRule(${rule.id}, ${clientId})">Delete</button></td>
      </tr>`;
    }

    if (rules.length === 0) {
      html += '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px">No rules configured. Add rules to customize transaction classification.</td></tr>';
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  buildCyclesTab(clientId, cycles) {
    let html = `
      <div class="card">
        <div class="card-header">
          <span class="card-title">Cycle Grouping Configuration</span>
          <button class="btn btn-sm btn-primary" onclick="Config.addCycleGroup(${clientId})">+ Add Group</button>
        </div>
        <div class="table-wrap"><table>
          <thead><tr><th>Group Name</th><th>Min Cycle</th><th>Max Cycle</th></tr></thead>
          <tbody>`;

    for (const c of cycles) {
      html += `<tr><td>${c.group_name}</td><td>${c.min_cycle}</td><td>${c.max_cycle || '∞'}</td></tr>`;
    }

    if (cycles.length === 0) {
      html += '<tr><td colspan="3" style="text-align:center;color:var(--text-muted);padding:24px">No cycle groups configured. Default: 0=Initial, 1=Trial, 2+=Recurring</td></tr>';
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  buildLogTab(changelog) {
    let html = `<div class="card"><div class="card-header"><span class="card-title">Change Log</span></div>
      <div class="table-wrap"><table><thead><tr><th>Time</th><th>Entity</th><th>Field</th><th>Old</th><th>New</th><th>By</th></tr></thead><tbody>`;

    for (const entry of changelog) {
      html += `<tr>
        <td>${timeAgo(entry.created_at)}</td>
        <td>${entry.entity_type} #${entry.entity_id}</td>
        <td>${entry.field_name || '—'}</td>
        <td>${entry.old_value || '—'}</td>
        <td>${entry.new_value || '—'}</td>
        <td>${entry.changed_by || 'system'}</td>
      </tr>`;
    }

    if (changelog.length === 0) {
      html += '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:24px">No changes recorded yet.</td></tr>';
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  switchTab(tab, clientId) {
    this.activeTab = tab;
    this.render(clientId);
  },

  async syncGateways(clientId) {
    const btn = event.target;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Syncing...';
    try {
      const res = await fetch(`/api/actions/sync-gateways/${clientId}`, { method: 'POST' });
      const data = await res.json();
      alert(`Synced ${data.count} gateways.`);
    } catch (err) {
      alert('Error: ' + err.message);
    }
    this.render(clientId);
  },

  editGateway(clientId, gatewayId) {
    // Inline edit modal
    const main = document.getElementById('mainContent');
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal">
        <h3>Edit Gateway ${gatewayId}</h3>
        <div class="form-group"><label>Processor Name</label><input id="editProcessor"></div>
        <div class="form-group"><label>Bank Name</label><input id="editBank"></div>
        <div class="form-group"><label>MCC Code</label><input id="editMcc"></div>
        <div class="form-group"><label>Vertical Label</label><input id="editVertical"></div>
        <div class="form-group"><label>Acquiring BIN</label><input id="editAcqBin"></div>
        <div class="modal-actions">
          <button class="btn btn-secondary" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
          <button class="btn btn-primary" id="editSaveBtn">Save</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);

    // Pre-fill
    fetch(`/api/config/gateways/${clientId}`).then(r => r.json()).then(gateways => {
      const gw = gateways.find(g => g.gateway_id === gatewayId);
      if (gw) {
        document.getElementById('editProcessor').value = gw.processor_name || '';
        document.getElementById('editBank').value = gw.bank_name || '';
        document.getElementById('editMcc').value = gw.mcc_code || '';
        document.getElementById('editVertical').value = gw.mcc_label || '';
        document.getElementById('editAcqBin').value = gw.acquiring_bin || '';
      }
    });

    document.getElementById('editSaveBtn').onclick = async () => {
      const data = {
        processor_name: document.getElementById('editProcessor').value || null,
        bank_name: document.getElementById('editBank').value || null,
        mcc_code: document.getElementById('editMcc').value || null,
        mcc_label: document.getElementById('editVertical').value || null,
        acquiring_bin: document.getElementById('editAcqBin').value || null,
      };
      await fetch(`/api/config/gateways/${clientId}/${gatewayId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      overlay.remove();
      this.render(clientId);
    };
  },

  async toggleExclude(clientId, gatewayId, checkbox) {
    const newVal = checkbox.checked ? 1 : 0;
    try {
      const res = await fetch(`/api/config/gateways/${clientId}/${gatewayId}/exclude`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ exclude_from_analysis: newVal }),
      });
      if (!res.ok) throw new Error('Failed to update');
      // Update badge in place without full re-render
      const td = checkbox.closest('td');
      const badge = td.querySelector('span');
      if (newVal === 1) {
        if (!badge) {
          td.insertAdjacentHTML('afterbegin', '<span style="font-size:10px;padding:2px 7px;border-radius:3px;background:#fffbeb;color:#92400e;font-weight:600">Paused</span>');
        }
        checkbox.parentElement.title = 'Resume analysis';
      } else {
        if (badge && badge.textContent === 'Paused') badge.remove();
        checkbox.parentElement.title = 'Pause analysis';
      }
    } catch (err) {
      alert('Error toggling exclude: ' + err.message);
      checkbox.checked = !checkbox.checked;
    }
  },

  showUpload(clientId) {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.csv,.xlsx,.xls';
    input.onchange = async (e) => {
      const file = e.target.files[0];
      if (!file) return;

      // For now, handle CSV parsing client-side
      const text = await file.text();
      const lines = text.trim().split('\n');
      const headers = lines[0].split(',').map(h => h.trim());
      const gateways = lines.slice(1).map(line => {
        const vals = line.split(',').map(v => v.trim());
        const obj = {};
        headers.forEach((h, i) => { obj[h] = vals[i] || null; });
        return obj;
      }).filter(g => g.gateway_id);

      try {
        const res = await fetch(`/api/config/gateways/${clientId}/bulk`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ gateways }),
        });
        const data = await res.json();
        alert(`Updated ${data.updated} gateways from upload.`);
        this.render(clientId);
      } catch (err) {
        alert('Upload error: ' + err.message);
      }
    };
    input.click();
  },

  addTxRule(clientId) {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal">
        <h3>Add TX Type Rule</h3>
        <div class="form-group"><label>Campaign ID (blank = all)</label><input id="ruleCampaign" type="number"></div>
        <div class="form-group"><label>Product ID (blank = all)</label><input id="ruleProduct" type="number"></div>
        <div class="form-group"><label>Assigned Type</label>
          <select id="ruleType">
            <option value="cp_initial">CP Initial</option>
            <option value="cp_upsell">CP Upsell</option>
            <option value="trial_conversion">Trial Conversion</option>
            <option value="recurring_rebill">Recurring Rebill</option>
            <option value="simulated_cp_rebill">Simulated CP Rebill</option>
            <option value="salvage_attempt">Salvage Attempt</option>
          </select>
        </div>
        <div class="form-group"><label><input type="checkbox" id="ruleCpSim"> This is a CP simulation (third-party CP tool)</label></div>
        <div class="form-group"><label>Notes</label><input id="ruleNotes"></div>
        <div class="modal-actions">
          <button class="btn btn-secondary" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
          <button class="btn btn-primary" id="ruleSaveBtn">Save</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);

    document.getElementById('ruleSaveBtn').onclick = async () => {
      await fetch(`/api/config/tx-rules/${clientId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          campaign_id: document.getElementById('ruleCampaign').value || null,
          product_id: document.getElementById('ruleProduct').value || null,
          assigned_type: document.getElementById('ruleType').value,
          is_cp_simulation: document.getElementById('ruleCpSim').checked,
          notes: document.getElementById('ruleNotes').value || null,
        }),
      });
      overlay.remove();
      this.render(clientId);
    };
  },

  async deleteTxRule(ruleId, clientId) {
    if (!confirm('Delete this rule?')) return;
    await fetch(`/api/config/tx-rules/${ruleId}`, { method: 'DELETE' });
    this.render(clientId);
  },

  addCycleGroup(clientId) {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal">
        <h3>Add Cycle Group</h3>
        <div class="form-group"><label>Group Name</label><input id="cgName" placeholder="e.g. Early Recurring"></div>
        <div class="form-group"><label>Min Cycle</label><input id="cgMin" type="number" value="0"></div>
        <div class="form-group"><label>Max Cycle (blank = unlimited)</label><input id="cgMax" type="number"></div>
        <div class="modal-actions">
          <button class="btn btn-secondary" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
          <button class="btn btn-primary" id="cgSaveBtn">Save</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);

    document.getElementById('cgSaveBtn').onclick = async () => {
      await fetch(`/api/config/cycle-groups/${clientId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          group_name: document.getElementById('cgName').value,
          min_cycle: parseInt(document.getElementById('cgMin').value, 10),
          max_cycle: document.getElementById('cgMax').value ? parseInt(document.getElementById('cgMax').value, 10) : null,
        }),
      });
      overlay.remove();
      this.render(clientId);
    };
  },
};
