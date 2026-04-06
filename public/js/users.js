/**
 * BinRoute — User Management (admin only)
 */
const Users = {
  data: null,

  async render() {
    const el = document.getElementById('mainContent');
    el.innerHTML = '<div style="padding:40px;text-align:center"><div class="spinner"></div> Loading users...</div>';

    try {
      const res = await fetch('/api/users');
      this.data = await res.json();
      this._renderTable();
    } catch (err) {
      el.innerHTML = `<div class="card"><p style="color:var(--danger)">Failed to load users: ${err.message}</p></div>`;
    }
  },

  _renderTable() {
    const el = document.getElementById('mainContent');
    const { users, clients } = this.data;

    const clientMap = {};
    clients.forEach(c => { clientMap[c.id] = c.name; });

    let html = `
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
          <h3>User Management</h3>
          <button class="btn btn-primary" onclick="Users.showAddForm()">Add User</button>
        </div>
        <div id="userFormArea"></div>
        <table>
          <thead>
            <tr>
              <th>Username</th>
              <th>Role</th>
              <th>Assigned Clients</th>
              <th>Created</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>`;

    for (const u of users) {
      const roleLabel = { admin: 'Admin', manager: 'Manager', client: 'Client' }[u.role] || u.role;
      const rolePill = `<span class="pill pill-${u.role === 'admin' ? 'active' : u.role === 'manager' ? 'warming_up' : 'closed'}">${roleLabel}</span>`;
      const clientNames = u.role === 'admin'
        ? '<span style="color:var(--text-muted)">All clients</span>'
        : u.client_ids.map(id => clientMap[id] || `#${id}`).join(', ') || '<span style="color:var(--danger)">None</span>';

      html += `
            <tr>
              <td><strong>${u.username}</strong></td>
              <td>${rolePill}</td>
              <td>${clientNames}</td>
              <td>${u.created_at ? u.created_at.substring(0, 10) : '—'}</td>
              <td>
                <button class="btn btn-sm" onclick="Users.showEditForm(${u.id})">Edit</button>
                ${u.id !== 1 ? `<button class="btn btn-sm btn-danger" onclick="Users.deleteUser(${u.id}, '${u.username}')">Delete</button>` : ''}
              </td>
            </tr>`;
    }

    html += `</tbody></table></div>`;
    el.innerHTML = html;
  },

  showAddForm() {
    const { clients } = this.data;
    const area = document.getElementById('userFormArea');
    area.innerHTML = `
      <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px">
        <h4 style="margin-bottom:12px">New User</h4>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <div class="form-group">
            <label>Username</label>
            <input id="uf_username" placeholder="username">
          </div>
          <div class="form-group">
            <label>Password</label>
            <input id="uf_password" type="password" placeholder="password">
          </div>
          <div class="form-group">
            <label>Role</label>
            <select id="uf_role" onchange="Users._toggleClientSelect()">
              <option value="client">Client</option>
              <option value="manager">Manager</option>
              <option value="admin">Admin</option>
            </select>
          </div>
          <div class="form-group" id="uf_clients_wrap">
            <label>Assigned Clients</label>
            <div id="uf_clients_list">
              ${clients.map(c => `<label style="display:block;margin:4px 0"><input type="checkbox" value="${c.id}" class="uf_client_cb"> ${c.name}</label>`).join('')}
            </div>
          </div>
        </div>
        <div style="margin-top:12px">
          <button class="btn btn-primary" onclick="Users.submitAdd()">Create User</button>
          <button class="btn" onclick="document.getElementById('userFormArea').innerHTML=''">Cancel</button>
        </div>
      </div>`;
  },

  showEditForm(userId) {
    const user = this.data.users.find(u => u.id === userId);
    if (!user) return;
    const { clients } = this.data;
    const area = document.getElementById('userFormArea');
    area.innerHTML = `
      <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px">
        <h4 style="margin-bottom:12px">Edit User: ${user.username}</h4>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <div class="form-group">
            <label>Username</label>
            <input id="uf_username" value="${user.username}">
          </div>
          <div class="form-group">
            <label>New Password <span style="color:var(--text-muted)">(leave blank to keep)</span></label>
            <input id="uf_password" type="password" placeholder="">
          </div>
          <div class="form-group">
            <label>Role</label>
            <select id="uf_role" onchange="Users._toggleClientSelect()">
              <option value="client" ${user.role === 'client' ? 'selected' : ''}>Client</option>
              <option value="manager" ${user.role === 'manager' ? 'selected' : ''}>Manager</option>
              <option value="admin" ${user.role === 'admin' ? 'selected' : ''}>Admin</option>
            </select>
          </div>
          <div class="form-group" id="uf_clients_wrap">
            <label>Assigned Clients</label>
            <div id="uf_clients_list">
              ${clients.map(c => `<label style="display:block;margin:4px 0"><input type="checkbox" value="${c.id}" class="uf_client_cb" ${user.client_ids.includes(c.id) ? 'checked' : ''}> ${c.name}</label>`).join('')}
            </div>
          </div>
        </div>
        <div style="margin-top:12px">
          <button class="btn btn-primary" onclick="Users.submitEdit(${userId})">Save Changes</button>
          <button class="btn" onclick="document.getElementById('userFormArea').innerHTML=''">Cancel</button>
        </div>
      </div>`;
    this._toggleClientSelect();
  },

  _toggleClientSelect() {
    const role = document.getElementById('uf_role').value;
    const wrap = document.getElementById('uf_clients_wrap');
    if (wrap) {
      wrap.style.display = role === 'admin' ? 'none' : '';
    }
  },

  _getFormData() {
    const username = document.getElementById('uf_username').value.trim();
    const password = document.getElementById('uf_password').value;
    const role = document.getElementById('uf_role').value;
    const checkboxes = document.querySelectorAll('.uf_client_cb:checked');
    const client_ids = Array.from(checkboxes).map(cb => parseInt(cb.value, 10));
    return { username, password, role, client_ids };
  },

  async submitAdd() {
    const data = this._getFormData();
    if (!data.username || !data.password) return alert('Username and password required');

    try {
      const res = await fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error);
      this.render();
    } catch (err) {
      alert('Error: ' + err.message);
    }
  },

  async submitEdit(userId) {
    const data = this._getFormData();
    if (!data.username) return alert('Username required');
    if (!data.password) delete data.password; // don't send empty password

    try {
      const res = await fetch(`/api/users/${userId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
      });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error);
      this.render();
    } catch (err) {
      alert('Error: ' + err.message);
    }
  },

  async deleteUser(userId, username) {
    if (!confirm(`Delete user "${username}"? This cannot be undone.`)) return;

    try {
      const res = await fetch(`/api/users/${userId}`, { method: 'DELETE' });
      const result = await res.json();
      if (!res.ok) throw new Error(result.error);
      this.render();
    } catch (err) {
      alert('Error: ' + err.message);
    }
  },
};
