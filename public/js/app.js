/**
 * BinRoute — Main application controller
 */
const App = {
  currentScreen: 'dashboard',
  currentClient: null,
  clients: [],

  async init() {
    await this.loadClients();
    this.bindNav();
    this.navigate('dashboard');
    if (this.currentClient) Bins.updateBadge(this.currentClient);
    MasterBins.updateBadge();
  },

  async loadClients() {
    try {
      const res = await fetch('/api/config/clients');
      this.clients = await res.json();
      const select = document.getElementById('clientSelect');
      select.innerHTML = this.clients.length === 0
        ? '<option value="">No clients configured</option>'
        : this.clients.map(c => `<option value="${c.id}">${c.name}</option>`).join('');

      if (this.clients.length > 0) {
        this.currentClient = this.clients[0].id;
        select.value = this.currentClient;
      }

      select.addEventListener('change', (e) => {
        this.currentClient = parseInt(e.target.value, 10);
        this.navigate(this.currentScreen);
        Bins.updateBadge(this.currentClient);
      });
    } catch (err) {
      console.error('Failed to load clients:', err);
    }
  },

  bindNav() {
    document.querySelectorAll('.nav-item').forEach(item => {
      item.addEventListener('click', () => {
        this.navigate(item.dataset.screen);
      });
    });
  },

  navigate(screen) {
    this.currentScreen = screen;

    // Update nav active state
    document.querySelectorAll('.nav-item').forEach(item => {
      item.classList.toggle('active', item.dataset.screen === screen);
    });

    // Render screen
    const main = document.getElementById('mainContent');
    // Network Analysis and Master BINs don't require a client selection
    if (screen === 'network') {
      NetworkAnalysis.render();
      return;
    }
    if (screen === 'master-bins') {
      MasterBins.render();
      return;
    }

    if (!this.currentClient) {
      main.innerHTML = `
        <div class="empty-state">
          <div class="icon">&#128274;</div>
          <h3>No Client Selected</h3>
          <p>Add a client to get started, or select one from the sidebar.</p>
          <button class="btn btn-primary" style="margin-top:16px" onclick="App.showAddClient()">Add Client</button>
        </div>`;
      return;
    }

    switch (screen) {
      case 'dashboard': Dashboard.render(this.currentClient); break;
      case 'config': Config.render(this.currentClient); break;
      case 'recommendations': Recommendations.render(this.currentClient); break;
      case 'lifecycle': Lifecycle.render(this.currentClient); break;
      case 'products': Products.render(this.currentClient); break;
      case 'analytics': Analytics.render(this.currentClient); break;
      case 'implementations': Implementations.render(this.currentClient); break;
      case 'bins': Bins.render(this.currentClient); break;
    }
  },

  showAddClient() {
    const main = document.getElementById('mainContent');
    main.innerHTML = `
      <div class="card" style="max-width:500px;margin:40px auto">
        <h3 style="margin-bottom:20px">Add New Client</h3>
        <div class="form-group">
          <label>Client Name</label>
          <input id="newClientName" placeholder="e.g. Derma Lumiere">
        </div>
        <div class="form-group">
          <label>Sticky.io Base URL</label>
          <input id="newClientUrl" placeholder="e.g. youraccount.sticky.io">
        </div>
        <div class="form-group">
          <label>API Username</label>
          <input id="newClientUser" placeholder="username">
        </div>
        <div class="form-group">
          <label>API Password</label>
          <input id="newClientPass" type="password" placeholder="password">
        </div>
        <div class="form-group">
          <label>Alert Threshold (% drop)</label>
          <input id="newClientThreshold" type="number" value="5" step="0.5">
        </div>
        <button class="btn btn-primary" onclick="App.addClient()">Add Client</button>
      </div>`;
  },

  async addClient() {
    const stickyUrl = document.getElementById('newClientUrl').value;
    const data = {
      name: document.getElementById('newClientName').value,
      sticky_base_url: stickyUrl,
      sticky_username: document.getElementById('newClientUser').value,
      sticky_password: document.getElementById('newClientPass').value,
      alert_threshold: parseFloat(document.getElementById('newClientThreshold').value) || 5.0,
    };

    try {
      const res = await fetch('/api/config/clients', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error((await res.json()).error);
      const newClient = await res.json();

      // Auto-extract sticky domain for cascade CSV matching
      const domain = stickyUrl.replace(/https?:\/\//, '').split('.')[0].toLowerCase();
      if (domain) {
        await fetch(`/api/config/clients/${newClient.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sticky_domain: domain }),
        });
      }

      await this.loadClients();
      this.navigate('dashboard');
    } catch (err) {
      alert('Error: ' + err.message);
    }
  },

  // Update config badge
  updateConfigBadge(count) {
    const badge = document.getElementById('configBadge');
    if (count > 0) {
      badge.textContent = count;
      badge.style.display = 'inline';
    } else {
      badge.style.display = 'none';
    }
  },

  // Update sync time
  updateSyncTime(syncState) {
    const el = document.getElementById('lastSync');
    if (!syncState || syncState.length === 0) {
      el.textContent = 'never';
      return;
    }
    const latest = syncState.reduce((a, b) =>
      (a.last_sync_at || '') > (b.last_sync_at || '') ? a : b
    );
    el.textContent = latest.last_sync_at ? timeAgo(latest.last_sync_at) : 'never';
  },
};

// ── Utilities ──

function timeAgo(dateStr) {
  if (!dateStr) return 'never';
  const date = new Date(dateStr + 'Z');
  const now = new Date();
  const diff = Math.floor((now - date) / 1000);
  if (diff < 60) return 'just now';
  if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  return Math.floor(diff / 86400) + 'd ago';
}

function rateBarHtml(rate, maxWidth = 120) {
  const color = rate >= 80 ? 'var(--success)' :
    rate >= 60 ? 'var(--warning)' :
    'var(--danger)';
  return `<div class="rate-bar">
    <div class="rate-bar-track" style="max-width:${maxWidth}px">
      <div class="rate-bar-fill" style="width:${Math.min(rate, 100)}%;background:${color}"></div>
    </div>
    <span class="rate-bar-value" style="color:${color}">${rate != null ? rate.toFixed(1) + '%' : '—'}</span>
  </div>`;
}

function pillHtml(status) {
  const label = status.replace(/_/g, ' ').replace(/-/g, ' ');
  return `<span class="pill pill-${status}">${label}</span>`;
}

function priorityHtml(priority) {
  return `<span class="priority priority-${priority}">${priority}</span>`;
}

function formatNum(n) {
  if (n == null) return '—';
  return n.toLocaleString();
}

/**
 * Parse a gateway alias like "Closed-Dignified_BBVA_3138_(2)" into a clean display name.
 * Pattern: [Closed-]<Processor>_<Bank>_<MID#>[_<Cap>K]_(<GW#>)
 * Returns: { display, processor, bank, midNum, cap, isClosed }
 */
function parseAlias(alias) {
  if (!alias) return { display: '—' };

  // Match structured aliases: Closed-Processor_Bank_MID_Cap_(GW) or Processor_Bank_MID_(GW)
  const m = alias.match(/^(Closed-)?(.+?)_([A-Z][A-Z0-9]+)_(\d+)(?:_(\d+K))?_\((\d+)\)$/);
  if (m) {
    const isClosed = !!m[1];
    const processor = m[2];
    const bank = m[3];
    const midNum = m[4];
    const cap = m[5] || null;
    const display = `${processor} ${bank} #${midNum}${cap ? ' (' + cap + ')' : ''}`;
    return { display, processor, bank, midNum, cap, isClosed };
  }

  // No match — return alias as-is (e.g. "Dry Run Testing")
  return { display: alias };
}

/**
 * Get the best display name for a gateway.
 * Shows gateway_alias (human-readable name), falls back to descriptor.
 */
function gwDisplayName(gw) {
  return gw.gateway_alias || gw.gateway_descriptor || '—';
}

// Start
document.addEventListener('DOMContentLoaded', () => App.init());
