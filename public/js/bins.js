/**
 * BinRoute — BIN Management Screen
 */
const Bins = {
  activeTab: 'unmatched',
  unmatchedFilter: 'all',   // all | no_entry | missing_fields
  searchQuery: '',
  allSearch: '',
  allSourceFilter: '',
  allCardTypeFilter: '',
  allPrepaidFilter: '',
  unmatchedData: [],
  allData: [],

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading BIN data...</div>';

    try {
      const [unmatchedRes, allRes] = await Promise.all([
        fetch(`/api/bins/${clientId}/unmatched`),
        fetch(`/api/bins/${clientId}/all`),
      ]);

      this.unmatchedData = await unmatchedRes.json();
      this.allData = await allRes.json();

      main.innerHTML = this.buildHtml(clientId);
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error loading BIN data</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml(clientId) {
    const unmatchedCount = this.unmatchedData.length;
    const totalOrders = this.unmatchedData.reduce((sum, b) => sum + (b.order_count || 0), 0);

    let html = `
      <div class="main-header">
        <h2>BIN Management</h2>
      </div>

      <div class="tabs">
        <div class="tab ${this.activeTab === 'unmatched' ? 'active' : ''}" onclick="Bins.switchTab('unmatched', ${clientId})">
          Unmatched BINs ${unmatchedCount > 0 ? `<span class="nav-badge" style="position:static;margin-left:6px">${unmatchedCount}</span>` : ''}
        </div>
        <div class="tab ${this.activeTab === 'all' ? 'active' : ''}" onclick="Bins.switchTab('all', ${clientId})">All BINs</div>
      </div>`;

    if (this.activeTab === 'unmatched') {
      html += this.buildUnmatchedTab(clientId, unmatchedCount, totalOrders);
    } else {
      html += this.buildAllTab(clientId);
    }

    return html;
  },

  buildUnmatchedTab(clientId, unmatchedCount, totalOrders) {
    // Filter data
    let filtered = [...this.unmatchedData];

    if (this.unmatchedFilter === 'no_entry') {
      filtered = filtered.filter(b => b.gap_type === 'no_entry');
    } else if (this.unmatchedFilter === 'missing_fields') {
      filtered = filtered.filter(b => b.gap_type === 'missing_fields');
    }

    if (this.searchQuery) {
      const q = this.searchQuery.toLowerCase();
      filtered = filtered.filter(b => String(b.bin).toLowerCase().includes(q));
    }

    // Sort by order_count DESC
    filtered.sort((a, b) => (b.order_count || 0) - (a.order_count || 0));

    let html = `
      <div class="banner banner-config">
        <span class="banner-icon">&#128203;</span>
        <div><strong id="unmatchedCounter">${unmatchedCount} BINs unmatched</strong> — covering ${formatNum(totalOrders)} orders</div>
      </div>

      <div class="filter-bar">
        <div class="tabs" style="border:none;margin:0">
          <div class="tab ${this.unmatchedFilter === 'all' ? 'active' : ''}" onclick="Bins.setUnmatchedFilter('all', ${clientId})">All</div>
          <div class="tab ${this.unmatchedFilter === 'no_entry' ? 'active' : ''}" onclick="Bins.setUnmatchedFilter('no_entry', ${clientId})">No Entry</div>
          <div class="tab ${this.unmatchedFilter === 'missing_fields' ? 'active' : ''}" onclick="Bins.setUnmatchedFilter('missing_fields', ${clientId})">Missing Fields</div>
        </div>
        <input type="text" placeholder="Search by BIN..." value="${this.searchQuery}" oninput="Bins.searchQuery=this.value;Bins.rebuildContent(${clientId})" style="margin-left:auto">
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
                <th>Gap</th>
                <th>Issuer Bank</th>
                <th>Card Type</th>
                <th>Card Level</th>
              </tr>
            </thead>
            <tbody id="unmatchedBody">`;

    if (filtered.length === 0) {
      html += '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:24px">No unmatched BINs found.</td></tr>';
    }

    for (const b of filtered) {
      html += this.unmatchedRowHtml(clientId, b);
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  unmatchedRowHtml(clientId, b) {
    const gapBadge = b.gap_type === 'no_entry'
      ? '<span class="pill" style="background:var(--danger-light);color:var(--danger)">No Entry</span>'
      : '<span class="pill" style="background:var(--warning-light);color:#92400e">Missing Fields</span>';

    const cardTypeOptions = ['', 'CREDIT', 'DEBIT'].map(opt =>
      `<option value="${opt}" ${(b.card_type || '').toUpperCase() === opt ? 'selected' : ''}>${opt || '-- select --'}</option>`
    ).join('');

    const prepaidIcon = b.is_prepaid ? '<span class="pill" style="background:var(--accent-light);color:var(--accent);font-size:10px">Prepaid</span>' : '';

    return `
      <tr id="bin-row-${b.bin}" data-bin="${b.bin}">
        <td><strong style="font-family:'IBM Plex Mono',monospace">${b.bin}</strong></td>
        <td>${formatNum(b.order_count)}</td>
        <td>${rateBarHtml(b.approval_rate)}</td>
        <td>${b.card_network || '—'}</td>
        <td>${gapBadge}</td>
        <td>
          <input type="text" class="inline-input" value="${b.issuer_bank || ''}" placeholder="Issuer bank..."
            data-bin="${b.bin}" data-field="issuer_bank"
            onblur="Bins.saveBin(${clientId}, '${b.bin}')"
            onkeydown="if(event.key==='Enter'){this.blur()}">
        </td>
        <td>
          <select class="inline-select" data-bin="${b.bin}" data-field="card_type"
            onchange="Bins.saveBin(${clientId}, '${b.bin}')">
            ${cardTypeOptions}
          </select>
        </td>
        <td>
          <input type="text" class="inline-input" value="${b.card_level || ''}" placeholder="Card level..."
            data-bin="${b.bin}" data-field="card_level"
            onblur="Bins.onCardLevelBlur(${clientId}, '${b.bin}', this)"
            onkeydown="if(event.key==='Enter'){this.blur()}">
          ${prepaidIcon}
        </td>
      </tr>`;
  },

  buildAllTab(clientId) {
    let filtered = [...this.allData];

    // Apply search
    if (this.allSearch) {
      const q = this.allSearch.toLowerCase();
      filtered = filtered.filter(b =>
        String(b.bin).toLowerCase().includes(q) ||
        (b.issuer_bank || '').toLowerCase().includes(q)
      );
    }

    // Apply filters
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

    // Collect unique sources for filter dropdown
    const sources = [...new Set(this.allData.map(b => b.source).filter(Boolean))];
    const cardTypes = [...new Set(this.allData.map(b => b.card_type).filter(Boolean))];

    let html = `
      <div class="filter-bar">
        <input type="text" placeholder="Search by BIN or bank name..." value="${this.allSearch}" oninput="Bins.allSearch=this.value;Bins.rebuildContent(${clientId})">
        <select onchange="Bins.allSourceFilter=this.value;Bins.rebuildContent(${clientId})">
          <option value="">All Sources</option>
          ${sources.map(s => `<option value="${s}" ${this.allSourceFilter === s ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
        <select onchange="Bins.allCardTypeFilter=this.value;Bins.rebuildContent(${clientId})">
          <option value="">All Card Types</option>
          ${cardTypes.map(t => `<option value="${t}" ${this.allCardTypeFilter === t ? 'selected' : ''}>${t}</option>`).join('')}
        </select>
        <select onchange="Bins.allPrepaidFilter=this.value;Bins.rebuildContent(${clientId})">
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
                <th>Orders</th>
                <th>Approval %</th>
              </tr>
            </thead>
            <tbody id="allBody">`;

    if (filtered.length === 0) {
      html += '<tr><td colspan="9" style="text-align:center;color:var(--text-muted);padding:24px">No BINs found.</td></tr>';
    }

    for (const b of filtered) {
      html += this.allRowHtml(clientId, b);
    }

    html += '</tbody></table></div></div>';
    return html;
  },

  allRowHtml(clientId, b) {
    const cardTypeOptions = ['', 'CREDIT', 'DEBIT'].map(opt =>
      `<option value="${opt}" ${(b.card_type || '').toUpperCase() === opt ? 'selected' : ''}>${opt || '-- select --'}</option>`
    ).join('');

    const prepaidIcon = b.is_prepaid ? '<span class="pill" style="background:var(--accent-light);color:var(--accent);font-size:10px">Prepaid</span>' : '';

    return `
      <tr id="bin-row-${b.bin}" data-bin="${b.bin}">
        <td><strong style="font-family:'IBM Plex Mono',monospace">${b.bin}</strong></td>
        <td>
          <input type="text" class="inline-input" value="${b.issuer_bank || ''}" placeholder="Issuer bank..."
            data-bin="${b.bin}" data-field="issuer_bank"
            onblur="Bins.saveBin(${clientId}, '${b.bin}')"
            onkeydown="if(event.key==='Enter'){this.blur()}">
        </td>
        <td>${b.card_brand || '—'}</td>
        <td>
          <select class="inline-select" data-bin="${b.bin}" data-field="card_type"
            onchange="Bins.saveBin(${clientId}, '${b.bin}')">
            ${cardTypeOptions}
          </select>
        </td>
        <td>
          <input type="text" class="inline-input" value="${b.card_level || ''}" placeholder="Card level..."
            data-bin="${b.bin}" data-field="card_level"
            onblur="Bins.onCardLevelBlur(${clientId}, '${b.bin}', this)"
            onkeydown="if(event.key==='Enter'){this.blur()}">
          ${prepaidIcon}
        </td>
        <td>${b.source || '—'}</td>
        <td>${formatNum(b.order_count)}</td>
        <td>${rateBarHtml(b.approval_rate)}</td>
      </tr>`;
  },

  // ── Tab / filter controls ──

  switchTab(tab, clientId) {
    this.activeTab = tab;
    this.rebuildContent(clientId);
  },

  setUnmatchedFilter(filter, clientId) {
    this.unmatchedFilter = filter;
    this.rebuildContent(clientId);
  },

  rebuildContent(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = this.buildHtml(clientId);
  },

  // ── Inline editing ──

  onCardLevelBlur(clientId, bin, input) {
    // is_prepaid is auto-derived from card_level in saveBin()
    this.saveBin(clientId, bin);
  },

  async saveBin(clientId, bin) {
    const row = document.getElementById(`bin-row-${bin}`);
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
      const res = await fetch(`/api/bins/${clientId}/${bin}`, {
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
            // Remove from unmatched data
            this.unmatchedData = this.unmatchedData.filter(b => String(b.bin) !== String(bin));
            row.remove();

            // Update counter in banner
            const banner = document.getElementById('unmatchedCounter');
            if (banner && banner.parentElement) {
              const totalOrders = this.unmatchedData.reduce((sum, b) => sum + (b.order_count || 0), 0);
              banner.parentElement.innerHTML =
                `<strong id="unmatchedCounter">${this.unmatchedData.length} BINs unmatched</strong> — covering ${formatNum(totalOrders)} orders`;
            }
          }, 300);
        }
      }

      // Update sidebar badge
      this.updateBadge(clientId);
    } catch (err) {
      // Flash row red
      row.classList.add('row-error');
      setTimeout(() => row.classList.remove('row-error'), 1000);
    }
  },

  updateLocalData(bin, data) {
    // Update in allData
    const allEntry = this.allData.find(b => String(b.bin) === String(bin));
    if (allEntry) {
      if (data.issuer_bank !== undefined) allEntry.issuer_bank = data.issuer_bank;
      if (data.card_type !== undefined) allEntry.card_type = data.card_type;
      if (data.card_level !== undefined) allEntry.card_level = data.card_level;
      if (data.is_prepaid !== undefined) allEntry.is_prepaid = data.is_prepaid;
    }

    // Update in unmatchedData
    const unmatchedEntry = this.unmatchedData.find(b => String(b.bin) === String(bin));
    if (unmatchedEntry) {
      if (data.issuer_bank !== undefined) unmatchedEntry.issuer_bank = data.issuer_bank;
      if (data.card_type !== undefined) unmatchedEntry.card_type = data.card_type;
      if (data.card_level !== undefined) unmatchedEntry.card_level = data.card_level;
      if (data.is_prepaid !== undefined) unmatchedEntry.is_prepaid = data.is_prepaid;
    }
  },

  async updateBadge(clientId) {
    try {
      const res = await fetch(`/api/bins/${clientId}/unmatched-count`);
      const data = await res.json();
      const badge = document.getElementById('binsBadge');
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
