/**
 * BinRoute — Master BIN Management Screen (cross-client)
 */
const MasterBins = {
  activeTab: 'unmatched',
  unmatchedFilter: 'all',
  searchQuery: '',
  allSearch: '',
  allSourceFilter: '',
  allCardTypeFilter: '',
  allPrepaidFilter: '',
  unmatchedData: [],
  allData: [],

  async render() {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading master BIN data...</div>';

    try {
      const [unmatchedRes, allRes] = await Promise.all([
        fetch('/api/master-bins/unmatched'),
        fetch('/api/master-bins/all'),
      ]);

      this.unmatchedData = await unmatchedRes.json();
      this.allData = await allRes.json();

      main.innerHTML = this.buildHtml();
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error loading master BIN data</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml() {
    const unmatchedCount = this.unmatchedData.length;
    const totalOrders = this.unmatchedData.reduce((sum, b) => sum + (b.order_count || 0), 0);

    let html = `
      <div class="main-header">
        <h2>Master BIN Management</h2>
        <p style="color:var(--text-muted);margin-top:4px">All BINs across all clients</p>
      </div>

      <div class="tabs">
        <div class="tab ${this.activeTab === 'unmatched' ? 'active' : ''}" onclick="MasterBins.switchTab('unmatched')">
          Unmatched BINs ${unmatchedCount > 0 ? `<span class="nav-badge" style="position:static;margin-left:6px">${unmatchedCount}</span>` : ''}
        </div>
        <div class="tab ${this.activeTab === 'all' ? 'active' : ''}" onclick="MasterBins.switchTab('all')">All BINs</div>
      </div>`;

    if (this.activeTab === 'unmatched') {
      html += this.buildUnmatchedTab(unmatchedCount, totalOrders);
    } else {
      html += this.buildAllTab();
    }

    return html;
  },

  buildUnmatchedTab(unmatchedCount, totalOrders) {
    let filtered = [...this.unmatchedData];

    if (this.unmatchedFilter === 'no_entry') {
      filtered = filtered.filter(b => b.gap_type === 'no_entry');
    } else if (this.unmatchedFilter === 'missing_fields') {
      filtered = filtered.filter(b => b.gap_type === 'missing_fields');
    }

    if (this.searchQuery) {
      const q = this.searchQuery.toLowerCase();
      filtered = filtered.filter(b =>
        String(b.bin).toLowerCase().includes(q) ||
        (b.client_names || '').toLowerCase().includes(q)
      );
    }

    filtered.sort((a, b) => (b.order_count || 0) - (a.order_count || 0));

    let html = `
      <div class="banner banner-config">
        <span class="banner-icon">&#128203;</span>
        <div><strong id="masterUnmatchedCounter">${unmatchedCount} BINs unmatched</strong> — covering ${formatNum(totalOrders)} orders across all clients</div>
      </div>

      <div class="filter-bar">
        <div class="tabs" style="border:none;margin:0">
          <div class="tab ${this.unmatchedFilter === 'all' ? 'active' : ''}" onclick="MasterBins.setUnmatchedFilter('all')">All</div>
          <div class="tab ${this.unmatchedFilter === 'no_entry' ? 'active' : ''}" onclick="MasterBins.setUnmatchedFilter('no_entry')">No Entry</div>
          <div class="tab ${this.unmatchedFilter === 'missing_fields' ? 'active' : ''}" onclick="MasterBins.setUnmatchedFilter('missing_fields')">Missing Fields</div>
        </div>
        <input type="text" placeholder="Search by BIN or client..." value="${this.searchQuery}" oninput="MasterBins.searchQuery=this.value;MasterBins.rebuildContent()" style="margin-left:auto">
      </div>

      <div class="card" style="padding:0;overflow:hidden">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>BIN</th>
                <th>Orders</th>
                <th>Approval %</th>
                <th>Network</th>
                <th>Clients</th>
                <th>Gap</th>
                <th>Issuer Bank</th>
                <th>Card Type</th>
                <th>Card Level</th>
              </tr>
            </thead>
            <tbody id="masterUnmatchedBody">`;

    if (filtered.length === 0) {
      html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No unmatched BINs found.</td></tr>';
    }

    for (const b of filtered) {
      html += this.unmatchedRowHtml(b);
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  unmatchedRowHtml(b) {
    const gapBadge = b.gap_type === 'no_entry'
      ? '<span class="pill" style="background:var(--danger-light);color:var(--danger)">No Entry</span>'
      : '<span class="pill" style="background:var(--warning-light);color:#92400e">Missing Fields</span>';

    const cardTypeOptions = ['', 'CREDIT', 'DEBIT'].map(opt =>
      `<option value="${opt}" ${(b.card_type || '').toUpperCase() === opt ? 'selected' : ''}>${opt || '-- select --'}</option>`
    ).join('');

    const prepaidIcon = b.is_prepaid ? '<span class="pill" style="background:var(--accent-light);color:var(--accent);font-size:10px">Prepaid</span>' : '';

    const clients = (b.client_names || '—').split(',').map(c =>
      `<span class="pill" style="background:var(--accent-light);color:var(--accent);font-size:10px;margin:1px">${c.trim()}</span>`
    ).join('');

    return `
      <tr id="master-bin-row-${b.bin}" data-bin="${b.bin}">
        <td><strong style="font-family:'IBM Plex Mono',monospace">${b.bin}</strong></td>
        <td>${formatNum(b.order_count)}</td>
        <td>${rateBarHtml(b.approval_rate)}</td>
        <td>${b.card_network || '—'}</td>
        <td>${clients}</td>
        <td>${gapBadge}</td>
        <td>
          <input type="text" class="inline-input" value="${b.issuer_bank || ''}" placeholder="Issuer bank..."
            data-bin="${b.bin}" data-field="issuer_bank"
            onblur="MasterBins.saveBin('${b.bin}')"
            onkeydown="if(event.key==='Enter'){this.blur()}">
        </td>
        <td>
          <select class="inline-select" data-bin="${b.bin}" data-field="card_type"
            onchange="MasterBins.saveBin('${b.bin}')">
            ${cardTypeOptions}
          </select>
        </td>
        <td>
          <input type="text" class="inline-input" value="${b.card_level || ''}" placeholder="Card level..."
            data-bin="${b.bin}" data-field="card_level"
            onblur="MasterBins.onCardLevelBlur('${b.bin}', this)"
            onkeydown="if(event.key==='Enter'){this.blur()}">
          ${prepaidIcon}
        </td>
      </tr>`;
  },

  buildAllTab() {
    let filtered = [...this.allData];

    if (this.allSearch) {
      const q = this.allSearch.toLowerCase();
      filtered = filtered.filter(b =>
        String(b.bin).toLowerCase().includes(q) ||
        (b.issuer_bank || '').toLowerCase().includes(q)
      );
    }

    if (this.allSourceFilter) {
      filtered = filtered.filter(b => b.source === this.allSourceFilter);
    }
    if (this.allCardTypeFilter) {
      filtered = filtered.filter(b => b.card_type === this.allCardTypeFilter);
    }
    if (this.allPrepaidFilter) {
      filtered = filtered.filter(b => {
        if (this.allPrepaidFilter === 'yes') return b.is_prepaid;
        if (this.allPrepaidFilter === 'no') return !b.is_prepaid;
        return true;
      });
    }

    const sources = [...new Set(this.allData.map(b => b.source).filter(Boolean))];
    const cardTypes = [...new Set(this.allData.map(b => b.card_type).filter(Boolean))];

    let html = `
      <div class="filter-bar">
        <input type="text" placeholder="Search by BIN or bank name..." value="${this.allSearch}" oninput="MasterBins.allSearch=this.value;MasterBins.rebuildContent()">
        <select onchange="MasterBins.allSourceFilter=this.value;MasterBins.rebuildContent()">
          <option value="">All Sources</option>
          ${sources.map(s => `<option value="${s}" ${this.allSourceFilter === s ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
        <select onchange="MasterBins.allCardTypeFilter=this.value;MasterBins.rebuildContent()">
          <option value="">All Card Types</option>
          ${cardTypes.map(t => `<option value="${t}" ${this.allCardTypeFilter === t ? 'selected' : ''}>${t}</option>`).join('')}
        </select>
        <select onchange="MasterBins.allPrepaidFilter=this.value;MasterBins.rebuildContent()">
          <option value="">All Prepaid</option>
          <option value="yes" ${this.allPrepaidFilter === 'yes' ? 'selected' : ''}>Prepaid Only</option>
          <option value="no" ${this.allPrepaidFilter === 'no' ? 'selected' : ''}>Non-Prepaid</option>
        </select>
      </div>

      <div class="card" style="padding:0;overflow:hidden">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>BIN</th>
                <th>Issuer Bank</th>
                <th>Brand</th>
                <th>Type</th>
                <th>Level</th>
                <th>Source</th>
                <th>Clients</th>
                <th>Orders</th>
                <th>Approval %</th>
              </tr>
            </thead>
            <tbody id="masterAllBody">`;

    if (filtered.length === 0) {
      html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No BINs found.</td></tr>';
    }

    for (const b of filtered) {
      html += this.allRowHtml(b);
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  allRowHtml(b) {
    const cardTypeOptions = ['', 'CREDIT', 'DEBIT'].map(opt =>
      `<option value="${opt}" ${(b.card_type || '').toUpperCase() === opt ? 'selected' : ''}>${opt || '-- select --'}</option>`
    ).join('');

    const prepaidIcon = b.is_prepaid ? '<span class="pill" style="background:var(--accent-light);color:var(--accent);font-size:10px">Prepaid</span>' : '';

    return `
      <tr id="master-bin-row-${b.bin}" data-bin="${b.bin}">
        <td><strong style="font-family:'IBM Plex Mono',monospace">${b.bin}</strong></td>
        <td>
          <input type="text" class="inline-input" value="${b.issuer_bank || ''}" placeholder="Issuer bank..."
            data-bin="${b.bin}" data-field="issuer_bank"
            onblur="MasterBins.saveBin('${b.bin}')"
            onkeydown="if(event.key==='Enter'){this.blur()}">
        </td>
        <td>${b.card_brand || '—'}</td>
        <td>
          <select class="inline-select" data-bin="${b.bin}" data-field="card_type"
            onchange="MasterBins.saveBin('${b.bin}')">
            ${cardTypeOptions}
          </select>
        </td>
        <td>
          <input type="text" class="inline-input" value="${b.card_level || ''}" placeholder="Card level..."
            data-bin="${b.bin}" data-field="card_level"
            onblur="MasterBins.onCardLevelBlur('${b.bin}', this)"
            onkeydown="if(event.key==='Enter'){this.blur()}">
          ${prepaidIcon}
        </td>
        <td>${b.source || '—'}</td>
        <td>${b.client_count || 0}</td>
        <td>${formatNum(b.order_count)}</td>
        <td>${rateBarHtml(b.approval_rate)}</td>
      </tr>`;
  },

  // ── Tab / filter controls ──

  switchTab(tab) {
    this.activeTab = tab;
    this.rebuildContent();
  },

  setUnmatchedFilter(filter) {
    this.unmatchedFilter = filter;
    this.rebuildContent();
  },

  rebuildContent() {
    const main = document.getElementById('mainContent');
    main.innerHTML = this.buildHtml();
  },

  // ── Inline editing ──

  onCardLevelBlur(bin, input) {
    this.saveBin(bin);
  },

  async saveBin(bin) {
    const row = document.getElementById(`master-bin-row-${bin}`);
    if (!row) return;

    const issuerInput = row.querySelector('input[data-field="issuer_bank"]');
    const cardTypeSelect = row.querySelector('select[data-field="card_type"]');
    const cardLevelInput = row.querySelector('input[data-field="card_level"]');

    const cardLevel = cardLevelInput ? cardLevelInput.value.trim() || null : null;

    const data = {
      issuer_bank: issuerInput ? issuerInput.value.trim() || null : null,
      card_type: cardTypeSelect ? cardTypeSelect.value || null : null,
      card_level: cardLevel,
      is_prepaid: cardLevel && cardLevel.toLowerCase().includes('prepaid'),
    };

    try {
      const res = await fetch(`/api/master-bins/${bin}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || 'Save failed');
      }

      // Flash row green
      row.classList.add('row-saved');
      setTimeout(() => row.classList.remove('row-saved'), 1000);

      // Update local data
      this.updateLocalData(bin, data);

      // If unmatched tab and all required fields are now filled, remove the row
      if (this.activeTab === 'unmatched') {
        const allFilled = data.issuer_bank && data.card_type && data.card_level;
        if (allFilled) {
          row.style.transition = 'opacity 0.3s';
          row.style.opacity = '0';
          setTimeout(() => {
            this.unmatchedData = this.unmatchedData.filter(b => String(b.bin) !== String(bin));
            row.remove();

            const banner = document.getElementById('masterUnmatchedCounter');
            if (banner && banner.parentElement) {
              const totalOrders = this.unmatchedData.reduce((sum, b) => sum + (b.order_count || 0), 0);
              banner.parentElement.innerHTML =
                `<strong id="masterUnmatchedCounter">${this.unmatchedData.length} BINs unmatched</strong> — covering ${formatNum(totalOrders)} orders across all clients`;
            }
          }, 300);
        }
      }

      // Update sidebar badge
      this.updateBadge();
    } catch (err) {
      row.classList.add('row-error');
      setTimeout(() => row.classList.remove('row-error'), 1000);
    }
  },

  updateLocalData(bin, data) {
    const allEntry = this.allData.find(b => String(b.bin) === String(bin));
    if (allEntry) {
      if (data.issuer_bank !== undefined) allEntry.issuer_bank = data.issuer_bank;
      if (data.card_type !== undefined) allEntry.card_type = data.card_type;
      if (data.card_level !== undefined) allEntry.card_level = data.card_level;
      if (data.is_prepaid !== undefined) allEntry.is_prepaid = data.is_prepaid;
    }

    const unmatchedEntry = this.unmatchedData.find(b => String(b.bin) === String(bin));
    if (unmatchedEntry) {
      if (data.issuer_bank !== undefined) unmatchedEntry.issuer_bank = data.issuer_bank;
      if (data.card_type !== undefined) unmatchedEntry.card_type = data.card_type;
      if (data.card_level !== undefined) unmatchedEntry.card_level = data.card_level;
      if (data.is_prepaid !== undefined) unmatchedEntry.is_prepaid = data.is_prepaid;
    }
  },

  async updateBadge() {
    try {
      const res = await fetch('/api/master-bins/unmatched-count');
      const data = await res.json();
      const badge = document.getElementById('masterBinsBadge');
      if (!badge) return;
      if (data.count > 0) {
        badge.textContent = data.count;
        badge.style.display = 'inline';
      } else {
        badge.style.display = 'none';
      }
    } catch (err) {
      // Silently fail badge update
    }
  },
};
