/**
 * BinRoute — Product Groups screen
 */
const Products = {
  filter: 'all', // all | unassigned | assigned
  sortBy: 'product_id', // product_id | product_name
  sortDir: 'asc',
  data: [],

  async render(clientId) {
    const main = document.getElementById('mainContent');
    main.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading products...</div>';

    try {
      const [products, unassignedRes, dashData, groups] = await Promise.all([
        fetch(`/api/products/${clientId}`).then(r => r.json()),
        fetch(`/api/products/${clientId}/unassigned-count`).then(r => r.json()),
        fetch(`/api/dashboard/${clientId}`).then(r => r.ok ? r.json() : {}),
        fetch(`/api/products/${clientId}/groups`).then(r => r.json()),
      ]);
      const syncState = dashData.syncState || [];

      this.data = products;
      const unassigned = unassignedRes.count;

      // Build group sequence cards
      this._groupSequenceCards = groups.sort((a, b) => a.group_name.localeCompare(b.group_name)).map(g => {
        const seqOptions = ['', 'main', 'upsell'].map(v => {
          const label = v === '' ? '—' : v.charAt(0).toUpperCase() + v.slice(1);
          return `<option value="${v}" ${(g.product_sequence || '') === v ? 'selected' : ''}>${label}</option>`;
        }).join('');
        const borderColor = g.product_sequence === 'main' ? '#1D9E75' : g.product_sequence === 'upsell' ? '#4f7df9' : 'var(--border)';
        return `<div style="display:flex;align-items:center;gap:8px;padding:6px 10px;border:1px solid ${borderColor};border-radius:6px;background:white">
          <span style="font-size:12px;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${g.group_name}</span>
          <select style="font-size:11px;padding:2px 6px;border:1px solid var(--border);border-radius:4px;cursor:pointer" onchange="Products.saveSequence(${clientId}, ${g.id}, this.value)">
            ${seqOptions}
          </select>
        </div>`;
      }).join('');

      // Get product sync timestamp
      const productSync = Array.isArray(syncState)
        ? syncState.find(s => s.sync_type === 'product_sync')
        : null;
      const lastSynced = productSync?.last_sync_at ? timeAgo(productSync.last_sync_at) : 'never';

      // Update sidebar badge
      this.updateBadge(unassigned);

      // Filter
      let filtered = products;
      if (this.filter === 'unassigned') {
        filtered = products.filter(p => !p.group_name);
      } else if (this.filter === 'assigned') {
        filtered = products.filter(p => p.group_name);
      }

      // Sort
      filtered = [...filtered].sort((a, b) => {
        const dir = this.sortDir === 'asc' ? 1 : -1;
        if (this.sortBy === 'product_id') {
          return (parseInt(a.product_id) - parseInt(b.product_id)) * dir;
        }
        if (this.sortBy === 'product_name') {
          const na = (a.product_name || '').toLowerCase();
          const nb = (b.product_name || '').toLowerCase();
          return na.localeCompare(nb) * dir;
        }
        return 0;
      });

      main.innerHTML = this.buildHtml(clientId, filtered, unassigned, lastSynced, products.length);
    } catch (err) {
      main.innerHTML = `<div class="empty-state"><h3>Error loading products</h3><p>${err.message}</p></div>`;
    }
  },

  buildHtml(clientId, products, unassigned, lastSynced, totalCount) {
    const seedBtn = totalCount === 0
      ? `<button class="btn btn-primary" onclick="Products.seedProducts(${clientId})">Load Products from Orders</button>`
      : '';

    return `
      <div class="main-header">
        <div>
          <h2>Product Groups</h2>
          ${unassigned > 0 ? `<p style="color:var(--warning);margin-top:4px;font-size:13px">${unassigned} product${unassigned !== 1 ? 's' : ''} not yet assigned to a group</p>` : '<p style="color:var(--success);margin-top:4px;font-size:13px">All products assigned</p>'}
        </div>
        <div style="display:flex;gap:8px;align-items:center">
          ${seedBtn}
          <button class="btn btn-secondary" onclick="Products.syncFromSticky(${clientId})" id="syncProductsBtn">Sync Products from Sticky</button>
          <span style="font-size:12px;color:var(--text-secondary)">Last sync: ${lastSynced}</span>
        </div>
      </div>

      <div class="tabs" style="margin-bottom:16px">
        <div class="tab ${this.filter === 'all' ? 'active' : ''}" onclick="Products.setFilter('all', ${clientId})">All <span style="opacity:0.6">(${totalCount})</span></div>
        <div class="tab ${this.filter === 'unassigned' ? 'active' : ''}" onclick="Products.setFilter('unassigned', ${clientId})">Unassigned <span style="opacity:0.6">(${unassigned})</span></div>
        <div class="tab ${this.filter === 'assigned' ? 'active' : ''}" onclick="Products.setFilter('assigned', ${clientId})">Assigned <span style="opacity:0.6">(${totalCount - unassigned})</span></div>
      </div>

      <div class="card" style="padding:16px;margin-bottom:16px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div style="font-size:13px;font-weight:600">Group Sequence (Main / Upsell)</div>
          <button class="btn btn-sm btn-secondary" onclick="var el=document.getElementById('groupSeqTable');el.style.display=el.style.display==='none'?'':'none'">Toggle</button>
        </div>
        <div id="groupSeqTable" style="display:none">
          <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">
            ${this._groupSequenceCards || ''}
          </div>
        </div>
      </div>

      ${products.length === 0
        ? `<div class="empty-state"><h3>No products found</h3><p>${totalCount === 0 ? 'Click "Load Products from Orders" to discover products from your order data.' : 'No products match this filter.'}</p></div>`
        : `<div class="card" style="padding:0;overflow:hidden">
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th class="sortable" onclick="Products.toggleSort('product_id', ${clientId})" style="cursor:pointer">Product ID ${this.sortBy === 'product_id' ? (this.sortDir === 'asc' ? '&#9650;' : '&#9660;') : '<span style="opacity:0.3">&#9650;</span>'}</th>
                    <th class="sortable" onclick="Products.toggleSort('product_name', ${clientId})" style="cursor:pointer">Product Name ${this.sortBy === 'product_name' ? (this.sortDir === 'asc' ? '&#9650;' : '&#9660;') : '<span style="opacity:0.3">&#9650;</span>'}</th>
                    <th>Product Group</th>
                    <th>Product Type</th>
                    <th style="width:40px"></th>
                  </tr>
                </thead>
                <tbody>
                  ${products.map(p => this.rowHtml(clientId, p)).join('')}
                </tbody>
              </table>
            </div>
          </div>`
      }`;
  },

  rowHtml(clientId, p) {
    const typeOptions = [
      { value: '', label: '-- select --' },
      { value: 'initial', label: 'Initial' },
      { value: 'rebill', label: 'Rebill' },
      { value: 'initial_rebill', label: 'Initial + Rebill' },
      { value: 'straight_sale', label: 'Straight Sale' },
    ];
    const typeSelect = typeOptions.map(o =>
      `<option value="${o.value}" ${p.product_type === o.value ? 'selected' : ''}>${o.label}</option>`
    ).join('');

    return `
      <tr id="row-${p.product_id}">
        <td><code style="font-size:13px">${p.product_id}</code></td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis">${p.product_name || '<span style="color:var(--text-secondary)">—</span>'}</td>
        <td>
          <input type="text"
            class="inline-input"
            value="${p.group_name || ''}"
            placeholder="Enter group name..."
            data-product="${p.product_id}"
            data-field="product_group"
            onblur="Products.saveField(${clientId}, '${p.product_id}', this)">
        </td>
        <td>
          <select class="inline-select"
            data-product="${p.product_id}"
            data-field="product_type"
            onchange="Products.saveType(${clientId}, '${p.product_id}', this)">
            ${typeSelect}
          </select>
        </td>
        <td>
          <span class="save-indicator" id="indicator-${p.product_id}"></span>
        </td>
      </tr>`;
  },

  async saveField(clientId, productId, input) {
    const row = document.getElementById(`row-${productId}`);
    const groupValue = input.value.trim();
    const typeSelect = row.querySelector('select[data-field="product_type"]');
    const typeValue = typeSelect ? typeSelect.value : '';

    await this._save(clientId, productId, groupValue, typeValue);
  },

  async saveType(clientId, productId, select) {
    const row = document.getElementById(`row-${productId}`);
    const groupInput = row.querySelector('input[data-field="product_group"]');
    const groupValue = groupInput ? groupInput.value.trim() : '';
    const typeValue = select.value;

    // Validate: type requires group
    if (typeValue && !groupValue) {
      this.showIndicator(productId, 'error', 'Set group first');
      select.value = '';
      return;
    }

    await this._save(clientId, productId, groupValue, typeValue);
  },

  async saveSequence(clientId, groupId, value) {
    try {
      await fetch(`/api/products/${clientId}/group/${groupId}/sequence`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ product_sequence: value || null }),
      });
    } catch (err) {
      console.error('Failed to save sequence:', err);
    }
  },

  async _save(clientId, productId, groupName, productType) {
    const indicator = document.getElementById(`indicator-${productId}`);

    try {
      const res = await fetch(`/api/products/${clientId}/${productId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          product_group: groupName || null,
          product_type: productType || null,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        this.showIndicator(productId, 'error', err.error);
        return;
      }

      this.showIndicator(productId, 'success');

      // Update unassigned count in badge
      const countRes = await fetch(`/api/products/${clientId}/unassigned-count`);
      const { count } = await countRes.json();
      this.updateBadge(count);
    } catch (err) {
      this.showIndicator(productId, 'error', err.message);
    }
  },

  showIndicator(productId, type, msg) {
    const el = document.getElementById(`indicator-${productId}`);
    if (!el) return;

    if (type === 'success') {
      el.innerHTML = '<span style="color:var(--success);font-size:16px">&#10003;</span>';
    } else {
      el.innerHTML = `<span style="color:var(--danger);font-size:11px" title="${msg || 'Error'}">&#10007;</span>`;
    }
    setTimeout(() => { el.innerHTML = ''; }, 2000);
  },

  toggleSort(column, clientId) {
    if (this.sortBy === column) {
      this.sortDir = this.sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this.sortBy = column;
      this.sortDir = 'asc';
    }
    this.render(clientId);
  },

  setFilter(filter, clientId) {
    this.filter = filter;
    this.render(clientId);
  },

  async seedProducts(clientId) {
    const btn = event.target;
    btn.disabled = true;
    btn.textContent = 'Loading...';

    try {
      const res = await fetch(`/api/products/${clientId}/seed`, { method: 'POST' });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      this.render(clientId);
    } catch (err) {
      alert('Error: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Load Products from Orders';
    }
  },

  async syncFromSticky(clientId) {
    const btn = document.getElementById('syncProductsBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;display:inline-block;vertical-align:middle;margin-right:6px"></span> Syncing...';

    try {
      const res = await fetch(`/api/products/${clientId}/sync`, { method: 'POST' });
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      this.render(clientId);
    } catch (err) {
      alert('Sync error: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Sync Products from Sticky';
    }
  },

  updateBadge(count) {
    const badge = document.getElementById('productsBadge');
    if (!badge) return;
    if (count > 0) {
      badge.textContent = count;
      badge.style.display = 'inline';
    } else {
      badge.style.display = 'none';
    }
  },
};
